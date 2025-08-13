"""
Core Worker Implementation

This module contains the main Worker class that handles job processing,
worker lifecycle management, and coordination with the queue system.
"""

import asyncio
import os
import signal
import socket
import sys
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional, Sequence, cast

import cloudpickle
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue
from nats.errors import TimeoutError

from ..connection import ensure_stream, nats_jetstream, Config
import nats
from ..exceptions import NaqException, SerializationError
from ..events.shared_logger import get_shared_async_logger, configure_shared_logger
from ..models import Job, JOB_STATUS
from ..results import Results
from ..services import ServiceManager, ConnectionService, StreamService, JobService, KVStoreService
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
    JOB_STATUS_KV_NAME,
    JOB_STATUS_TTL_SECONDS,
    NAQ_PREFIX,
    RESULT_KV_NAME,
    WORKER_KV_NAME,
    WORKER_STATUS,
    DEFAULT_ACK_WAIT_SECONDS,
    ACK_WAIT_PER_QUEUE,
    DEFAULT_QUEUE_NAME,
)
from ..utils import run_async_from_sync, setup_logging
from .failed import FailedJobHandler
from .jobs import JobStatusManager
from .status import WorkerStatusManager


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs. Handles retries, dependencies,
    and reports its status via heartbeats.
    """

    def __init__(
        self,
        queues: Optional[Sequence[str] | str] = None,
        nats_url: str = DEFAULT_NATS_URL,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
        ack_wait: Optional[
            int | Dict[str, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
        services: Optional[ServiceManager] = None,
    ):
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            queues = [DEFAULT_QUEUE_NAME]

        # Preserve order while ensuring uniqueness using dict.fromkeys()
        self.queue_names: List[str] = list(dict.fromkeys(queues))
        self.subjects: List[str] = [
            f"{NAQ_PREFIX}.queue.{name}" for name in self.queue_names
        ]

        # Add current path to sys.path by default
        if os.getcwd() not in sys.path:
            sys.path.insert(0, os.getcwd())

        # Add custom module paths to sys.path
        if module_paths:
            if isinstance(module_paths, str):
                module_paths = [module_paths]
            for path in module_paths:
                if path not in sys.path:
                    sys.path.insert(0, path)

        self._nats_url = nats_url
        self._concurrency = concurrency

        # Generate a unique ID if name is not provided, otherwise use name as base
        base_name = worker_name or f"naq-worker-{socket.gethostname()}"
        self.worker_id = f"{base_name}-{os.getpid()}-{uuid.uuid4().hex[:6]}"

        self._heartbeat_interval = heartbeat_interval
        self._worker_ttl = worker_ttl
        # Ack wait configuration
        self._ack_wait_arg: Optional[int | Dict[str, int]] = ack_wait

        # Connection and state variables
        self._nc: Optional[NatsClient] = None
        self._js: Optional[JetStreamContext] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._consumers: Dict[
            str, Any
        ] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Service configuration
        self._config = {
            'nats': {
                'url': nats_url,
            },
            'jobs': {
                'default_timeout': 300,  # 5 minutes
                'max_retries': 3,
                'retry_delay': 1.0,
            },
            'events': {
                'nats_url': nats_url,
                'stream_name': 'NAQ_JOB_EVENTS',
                'subject_prefix': 'naq.jobs.events',
                'batch_size': 100,
                'flush_interval': 5.0,
                'max_buffer_size': 10000,
            }
        }
        
        # Use provided ServiceManager or create our own
        self._services = services
        self._connection_service = None
        self._stream_service = None
        self._job_service = None
        self._kv_store_service = None
        self._event_service = None

        # Create component managers
        self._worker_status_manager = WorkerStatusManager(self)
        self._job_status_manager = JobStatusManager(self)
        self._failed_job_handler = FailedJobHandler(self)
        
        # Event logger (initialized lazily to avoid circular dependencies)
        self._event_logger = None

        setup_logging()  # Setup logging
        
        # Configure shared event logger
        configure_shared_logger(storage_url=nats_url)

    async def _get_event_logger(self):
        """Get the shared event logger instance."""
        if self._event_logger is None:
            # Get the shared event logger instance
            self._event_logger = await get_shared_async_logger()
        return self._event_logger

    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        if self._nc is None or not self._nc.is_connected:
            # Get or create services
            if self._services is None:
                self._services = ServiceManager(self._config)
            
            # Get connection service
            connection_service = await self._services.get_service(ConnectionService)
            self._connection_service = cast(ConnectionService, connection_service)
            
            # Get stream service
            stream_service = await self._services.get_service(StreamService)
            self._stream_service = cast(StreamService, stream_service)
            
            # Get job service
            job_service = await self._services.get_service(JobService)
            self._job_service = cast(JobService, job_service)
            
            # Get KV store service
            kv_store_service = await self._services.get_service(KVStoreService)
            self._kv_store_service = cast(KVStoreService, kv_store_service)
            
            # Note: EventService is not used in worker for now to avoid circular dependencies
            # The shared event logger is used instead
            
            # Get NATS connection and JetStream context
            config = Config(servers=[self._nats_url])
            self._nc = await nats.connect(
                servers=config.nats.servers,
                name=config.nats.client_name,
                max_reconnect_attempts=config.nats.max_reconnect_attempts,
                reconnect_time_wait=config.nats.reconnect_time_wait,
            )
            self._js = self._nc.jetstream()
            logger.info(f"Worker '{self.worker_id}' connected to NATS and JetStream.")

            # Initialize component managers
            await self._worker_status_manager.start_heartbeat_loop()
            await self._job_status_manager.initialize(self._js)
            await self._failed_job_handler.initialize(self._js)

    def _resolve_ack_wait_seconds(self, queue_name: str) -> int:
        """
        Resolve ack_wait seconds for a given queue based on precedence:
        1) Per-queue value provided via ack_wait dict argument
        2) Single ack_wait int argument
        3) Environment-driven per-queue settings.ACK_WAIT_PER_QUEUE
        4) settings.DEFAULT_ACK_WAIT_SECONDS
        Ensures a positive integer, falling back to default if invalid.
        """
        try:
            # 1) per-queue dict from constructor
            if (
                isinstance(self._ack_wait_arg, dict)
                and queue_name in self._ack_wait_arg
            ):
                v = int(self._ack_wait_arg[queue_name])
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
            # 2) single int from constructor
            if isinstance(self._ack_wait_arg, int) and self._ack_wait_arg > 0:
                return int(self._ack_wait_arg)
            # 3) env per-queue
            if queue_name in ACK_WAIT_PER_QUEUE:
                v = int(ACK_WAIT_PER_QUEUE[queue_name])
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
        except Exception:
            pass
        # 4) default
        return DEFAULT_ACK_WAIT_SECONDS

    async def _subscribe_to_queue(self, queue_name: str) -> None:
        """Creates a durable consumer and starts fetching messages for a queue."""
        if not self._js:
            raise NaqException("JetStream context not available.")

        subject = f"{NAQ_PREFIX}.queue.{queue_name}"
        durable_name = f"{self.consumer_prefix}-{queue_name}"
        logger.info(
            f"Setting up consumer for queue '{queue_name}' (subject: {subject}, durable: {durable_name})"
        )

        try:
            # Resolve ack_wait seconds for this queue
            ack_wait_seconds = self._resolve_ack_wait_seconds(queue_name)
            logger.info(
                f"Creating consumer for queue '{queue_name}' with ack_wait={ack_wait_seconds}s"
            )
            psub = await self._js.pull_subscribe(
                subject=subject,
                durable=durable_name,
                config=ConsumerConfig(
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=ack_wait_seconds,
                    max_ack_pending=self._concurrency * 2,
                ),
            )
            self._consumers[queue_name] = psub
            logger.info(
                f"Pull consumer '{durable_name}' created for subject '{subject}'."
            )

            while self._running:
                if self._semaphore.locked():  # Check semaphore before fetching
                    await asyncio.sleep(0.1)  # Wait if concurrency limit reached
                    continue

                try:
                    # Calculate how many messages we can fetch based on available concurrency slots
                    available_slots = self._concurrency - (
                        self._concurrency - self._semaphore._value
                    )
                    if available_slots <= 0:
                        await asyncio.sleep(0.1)  # Wait if no slots free
                        continue

                    # Fetch up to the number of available slots, with a timeout
                    msgs = await psub.fetch(batch=available_slots, timeout=1)
                    if msgs:
                        logger.debug(
                            f"Fetched {len(msgs)} messages from '{durable_name}'"
                        )

                    for msg in msgs:
                        # Acquire semaphore before starting processing task
                        await self._semaphore.acquire()
                        # Create task to process the message
                        task = asyncio.create_task(self.process_message(msg))
                        # Add a callback to release the semaphore when the task completes (success or failure)
                        task.add_done_callback(lambda t: self._semaphore.release())
                        # Keep track of tasks (optional, for clean shutdown)
                        self._tasks.append(task)
                        self._tasks = [
                            t for t in self._tasks if not t.done()
                        ]  # Basic cleanup

                except TimeoutError:
                    # No messages available, or timeout hit, loop continues
                    await asyncio.sleep(0.1)  # Small sleep to prevent busy-wait
                    continue
                except Exception as e:
                    if "ConsumerNotFound" in str(e) or "consumer not found" in str(e).lower():
                        logger.warning(
                            f"Consumer '{durable_name}' not found. Stopping fetch loop."
                        )
                        break
                    logger.error(
                        f"Error fetching from consumer '{durable_name}': {e}",
                        exc_info=True,
                    )
                    await asyncio.sleep(1)  # Wait before retrying fetch
                    logger.warning(
                        f"Consumer '{durable_name}' not found. Stopping fetch loop."
                    )
                    break

        except Exception as e:
            logger.error(
                f"Failed to subscribe or run fetch loop for queue '{queue_name}': {e}",
                exc_info=True,
            )

    async def process_message(self, msg: Any) -> None:
        """Process a received job message."""
        job = None
        try:
            # Deserialize the job from the message data
            if hasattr(msg, "data"):
                job = Job.deserialize(msg.data)
            else:
                # For testing where msg might be a Job directly
                job = msg

            if self._shutdown_event.is_set():
                logger.info(
                    f"Shutdown in progress. Job {job.job_id if job else 'unknown'} will not be processed."
                )
                if hasattr(msg, "nak"):  # NAK the message so it can be re-queued
                    await msg.nak()
                return  # Do not process if shutdown is initiated

            # Log job started event
            logger_instance = await self._get_event_logger()
            if logger_instance:
                await logger_instance.log_job_started(
                    job_id=job.job_id,
                    worker_id=self.worker_id,
                    queue_name=job.queue_name,
                    nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                    nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                    details={
                        "function_name": getattr(job.function, "__name__", str(job.function)),
                        "job_timeout": job.timeout,
                        "max_retries": job.max_retries,
                        "job_kwargs": job.kwargs,
                        "dependency_ids": job.dependency_ids,
                    }
                )

            # Update worker status to busy with this job
            await self._worker_status_manager.update_status(
                WORKER_STATUS.BUSY, job_id=job.job_id
            )

            # Execute the job
            try:
                if job.timeout is not None and job.timeout > 0:
                    await asyncio.wait_for(job.execute(), timeout=job.timeout)
                else:
                    await job.execute()
            except asyncio.TimeoutError:
                job.error = f"Job timed out after {job.timeout} seconds"
                job.traceback = traceback.format_exc()

            # Calculate execution duration
            import time
            duration_ms = (time.time() - job._start_time) * 1000 if job._start_time else 0

            # Store result (which includes error/traceback if any)
            await self._job_status_manager.store_result(job)

            # Log job completed event
            logger_instance = await self._get_event_logger()
            if logger_instance:
                await logger_instance.log_job_completed(
                    job_id=job.job_id,
                    worker_id=self.worker_id,
                    duration_ms=duration_ms,
                    queue_name=job.queue_name,
                    nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                    nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                    details={
                        "function_name": getattr(job.function, "__name__", str(job.function)),
                        "job_timeout": job.timeout,
                        "max_retries": job.max_retries,
                        "job_kwargs": job.kwargs,
                        "dependency_ids": job.dependency_ids,
                        "result_type": type(job.result).__name__ if job.result is not None else None,
                    }
                )

            # Handle failure if needed (e.g., publish to dead-letter queue)
            if (
                job.status == JOB_STATUS.FAILED
            ):  # status is a property derived from job.error
                await self._failed_job_handler.handle_failed_job(job)

            # Acknowledge message processing complete
            if hasattr(msg, "ack"):
                await msg.ack()

        except Exception as e:
            logger.error(
                f"Error processing job {job.job_id if job else 'unknown'}: {e}",
                exc_info=True,
            )
            
            # Log job failed event
            if job:
                import time
                duration_ms = (time.time() - job._start_time) * 1000 if job._start_time else 0
                logger_instance = await self._get_event_logger()
                if logger_instance:
                    await logger_instance.log_job_failed(
                        job_id=job.job_id,
                        worker_id=self.worker_id,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        duration_ms=duration_ms,
                        queue_name=job.queue_name,
                        nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                        nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                        details={
                            "function_name": getattr(job.function, "__name__", str(job.function)),
                            "job_timeout": job.timeout,
                            "max_retries": job.max_retries,
                            "job_kwargs": job.kwargs,
                            "dependency_ids": job.dependency_ids,
                            "traceback": job.traceback,
                        }
                    )
            
            # If we have a NATS message and it has a term() method, terminate it
            if hasattr(msg, "term"):
                await msg.term()
        finally:
            # Update worker status back to idle
            await self._worker_status_manager.update_status(WORKER_STATUS.IDLE)

    async def _handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        if job is None:
            logger.error(
                "Job object is None after JobExecutionError, cannot handle retry/failure."
            )
            await msg.term()
            return

        logger.warning(f"Job {job.job_id} failed execution: {job.error}")

        # --- Retry Logic ---
        attempt = msg.metadata.num_delivered
        max_retries = job.max_retries if job.max_retries is not None else 0

        if attempt <= max_retries:
            delay = job.get_next_retry_delay()
            logger.info(
                f"Job {job.job_id} failed, scheduling retry {attempt}/{max_retries} "
                f"after {delay:.2f}s delay."
            )
            
            # Log retry scheduled event
            logger_instance = await self._get_event_logger()
            if logger_instance:
                await logger_instance.log_job_retry_scheduled(
                    job_id=job.job_id,
                    worker_id=self.worker_id,
                    delay_seconds=delay,
                    queue_name=job.queue_name,
                    nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                    nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                    details={
                        "function_name": getattr(job.function, "__name__", str(job.function)),
                        "job_timeout": job.timeout,
                        "max_retries": job.max_retries,
                        "job_kwargs": job.kwargs,
                        "dependency_ids": job.dependency_ids,
                        "retry_attempt": attempt,
                        "retry_delay": delay,
                        "error_message": job.error,
                    }
                )
            
            try:
                await msg.nak(delay=delay)
                logger.debug(f"Message Nak'd for retry: Sid='{msg.sid}'")
            except Exception as nak_e:
                logger.error(
                    f"Failed to NAK message Sid='{msg.sid}' for retry: {nak_e}",
                    exc_info=True,
                )
                await msg.term()  # Terminate if NAK fails
        else:
            # --- Terminal Failure ---
            logger.error(
                f"Job {job.job_id} failed after {attempt - 1} retries. Moving to failed queue."
            )
            
            # Log job failed event for terminal failure
            duration_ms = (time.time() - job._start_time) * 1000 if job._start_time else 0
            logger_instance = await self._get_event_logger()
            if logger_instance:
                await logger_instance.log_job_failed(
                    job_id=job.job_id,
                    worker_id=self.worker_id,
                    error_type="MaxRetriesExceeded",
                    error_message=job.error or "Job failed after maximum retries",
                    duration_ms=duration_ms,
                    queue_name=job.queue_name,
                    nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                    nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                    details={
                        "function_name": getattr(job.function, "__name__", str(job.function)),
                        "job_timeout": job.timeout,
                        "max_retries": job.max_retries,
                        "job_kwargs": job.kwargs,
                        "dependency_ids": job.dependency_ids,
                        "retry_attempt": attempt,
                        "max_retries_exceeded": True,
                    }
                )
            
            await self._job_status_manager.update_job_status(
                job.job_id, JOB_STATUS.FAILED
            )
            await self._job_status_manager.store_result(job)
            await self._failed_job_handler.publish_failed_job(job)
            try:
                await msg.ack()  # Ack original message after handling failure
                logger.debug(
                    f"Message acknowledged after moving to failed queue: Sid='{msg.sid}'"
                )
            except Exception as ack_e:
                logger.error(
                    f"Failed to ACK message Sid='{msg.sid}' after moving to failed queue: {ack_e}",
                    exc_info=True,
                )

    async def _handle_unexpected_error(
        self, job: Optional[Job], msg: Msg, error: Exception
    ) -> None:
        """Handle unexpected errors during message processing."""
        logger.error(
            f"Unhandled error processing message (Sid='{msg.sid}', "
            f"JobId='{job.job_id if job else 'N/A'}'): {error}",
            exc_info=True,
        )
        try:
            # Update status to failed if possible, otherwise terminate
            if job:
                job.error = (
                    f"Worker processing error: {error}"  # Assign error for storage
                )
                job.traceback = traceback.format_exc()
                
                # Log job failed event for unexpected error
                duration_ms = (time.time() - job._start_time) * 1000 if job._start_time else 0
                logger_instance = await self._get_event_logger()
                if logger_instance:
                    await logger_instance.log_job_failed(
                        job_id=job.job_id,
                        worker_id=self.worker_id,
                        error_type=type(error).__name__,
                        error_message=str(error),
                        duration_ms=duration_ms,
                        queue_name=job.queue_name,
                        nats_subject=msg.subject if hasattr(msg, 'subject') else None,
                        nats_sequence=None,  # TODO: Extract proper sequence number from msg.metadata
                        details={
                            "function_name": getattr(job.function, "__name__", str(job.function)),
                            "job_timeout": job.timeout,
                            "max_retries": job.max_retries,
                            "job_kwargs": job.kwargs,
                            "dependency_ids": job.dependency_ids,
                            "unexpected_error": True,
                            "traceback": job.traceback,
                        }
                    )
                
                await self._job_status_manager.update_job_status(
                    job.job_id, JOB_STATUS.FAILED
                )
                await self._job_status_manager.store_result(job)
            await msg.term()
            logger.warning(
                f"Terminated message Sid='{msg.sid}' due to unexpected processing error."
            )
        except Exception as term_e:
            logger.error(
                f"Failed to Terminate message Sid='{msg.sid}': {term_e}", exc_info=True
            )

    async def run(self) -> None:
        """Starts the worker, connects to NATS, and begins processing jobs."""
        self._running = True
        self._shutdown_event.clear()

        try:
            await self._connect()

            # Start event logger
            logger_instance = await self._get_event_logger()
            if logger_instance:
                await logger_instance.start()  # This is now async

            # Register worker initially
            await self._worker_status_manager.update_status(
                status=WORKER_STATUS.STARTING
            )

            # Start heartbeat task
            await self._worker_status_manager.start_heartbeat_loop()

            # Ensure the main work stream exists
            await ensure_stream(
                js=self._js,
                stream_name=self.stream_name,
                subjects=[f"{NAQ_PREFIX}.queue.*"],
            )

            # Start subscription tasks for each queue
            subscription_tasks = [
                asyncio.create_task(self._subscribe_to_queue(q_name))
                for q_name in self.queue_names
            ]

            logger.info(
                f"Worker '{self.worker_id}' started. Listening on queues: {self.queue_names}. "
                f"Concurrency: {self._concurrency}"
            )

            # Set status to idle once subscriptions are ready
            await self._worker_status_manager.update_status(status=WORKER_STATUS.IDLE)

            await self._shutdown_event.wait()

            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            await self._worker_status_manager.update_status(
                status=WORKER_STATUS.STOPPING
            )

            # Stop heartbeat task
            await self._worker_status_manager.stop_heartbeat_loop()

            # Wait for active processing tasks (respecting semaphore)
            active_tasks = self._concurrency - self._semaphore._value
            if active_tasks > 0:
                logger.info(f"Waiting for {active_tasks} active job(s) to finish...")
                # Wait for semaphore to be fully released, with a timeout
                try:
                    await asyncio.wait_for(
                        self._wait_for_semaphore(), timeout=30.0
                    )  # Wait up to 30s
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for active jobs to finish.")

            # Cancel subscription loops
            for task in subscription_tasks:
                task.cancel()
            await asyncio.gather(*subscription_tasks, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Run task cancelled.")
        except Exception as e:
            logger.error(f"Worker run loop encountered an error: {e}", exc_info=True)
            await self._worker_status_manager.update_status(
                status=WORKER_STATUS.STOPPING
            )
        finally:
            logger.info("Worker shutting down...")
            await self._close()
            logger.info("Worker shutdown complete.")

    async def _wait_for_semaphore(self) -> None:
        """Helper to wait until the semaphore value reaches concurrency limit."""
        while self._semaphore._value < self._concurrency:
            await asyncio.sleep(0.1)

    async def _close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        # Set shutdown event first to prevent new message processing
        self._shutdown_event.set()
        self._running = False

        # Stop event logger
        try:
            if self._event_logger:
                await self._event_logger.stop()
        except Exception as e:
            logger.error(f"Error stopping event logger: {e}")

        # Stop heartbeat and update status first
        try:
            await self._worker_status_manager.stop_heartbeat_loop()
        except Exception as e:
            logger.error(f"Error stopping heartbeat: {e}")

        # Cleanup all consumers before unregistering worker
        for queue_name, consumer in self._consumers.items():
            try:
                logger.debug(f"Unsubscribing consumer for queue {queue_name}")
                await consumer.unsubscribe()
                logger.debug(f"Draining consumer for queue {queue_name}")
                await consumer.drain()
            except Exception as e:
                logger.warning(
                    f"Error cleaning up consumer for queue {queue_name}: {e}"
                )
        self._consumers.clear()

        # Unregister worker after cleanup
        try:
            await self._worker_status_manager.unregister_worker()
        except Exception as e:
            logger.error(f"Error unregistering worker: {e}")

        # Finally close NATS connection
        try:
            # Clean up services if we created them
            if self._services is not None:
                await self._services.cleanup_all()
            # Close NATS connection
            if self._nc:
                await self._nc.close()
        except Exception as e:
            logger.error(f"Error closing services or NATS connection: {e}")

        self._nc = None
        self._js = None

    def signal_handler(self, sig, frame) -> None:
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self) -> None:
        """Installs signal handlers for graceful shutdown.

        Notes:
            - signal.signal() may only be called from the main thread of the main
              interpreter. If not in the main thread, this becomes a no-op with a warning.
        """
        try:
            import threading

            if threading.current_thread() is not threading.main_thread():
                logger.warning(
                    "Skipping installation of signal handlers because we are not in the main thread."
                )
                return
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except ValueError as e:
            # This can happen in environments that disallow setting signals (e.g., some notebooks)
            logger.warning(f"Could not install signal handlers: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error installing signal handlers: {e}", exc_info=True
            )

    # --- Static methods for worker monitoring ---
    @staticmethod
    async def list_workers(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        return await WorkerStatusManager.list_workers(nats_url)

    @staticmethod
    def list_workers_sync(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        return run_async_from_sync(Worker.list_workers, nats_url=nats_url)

    # --- Sync interface for long-running worker using anyio.BlockingPortal ---
    def run_sync(self) -> None:
        """
        Start the async worker in a clean AnyIO event loop using a BlockingPortal.

        Rationale:
        - Avoids mixing with any possibly running event loop or asyncio.run() constraints.
        - Provides consistent behavior when called from synchronous contexts (CLI, scripts).
        """
        try:
            from anyio.from_thread import start_blocking_portal
        except Exception as e:
            # Keep import local to avoid introducing runtime dependency unless used
            raise RuntimeError(
                "anyio is required for Worker.run_sync(). Please ensure 'anyio' is installed."
            ) from e

        # Install signal handlers in the main thread before starting the event loop thread
        self.install_signal_handlers()

        # Use BlockingPortal to create and own the event loop for the duration of run()
        with start_blocking_portal() as portal:
            return portal.call(self.run)

    # --- Optional persistent lifecycle control for sync contexts ---
    class _Controller:
        """
        Controller to manage a Worker from synchronous code, keeping a BlockingPortal alive.

        Methods:
            stop(): request graceful stop and wait for shutdown.
            status(): returns current boolean running state.
        """

        def __init__(self, worker, portal_cm, portal):
            self._worker = worker
            self._portal_cm = portal_cm
            self._portal = portal
            self._closed = False

        def stop(self) -> None:
            if self._closed:
                return

            # Signal shutdown in the worker's event loop
            def _signal():
                self._worker._running = False
                self._worker._shutdown_event.set()
                return None

            self._portal.call(_signal)
            # allow worker.run to finish, then close portal
            self._portal_cm.__exit__(None, None, None)
            self._closed = True

        def status(self) -> bool:
            # Check running flag via portal to avoid races
            def _get():
                return bool(self._worker._running)

            return self._portal.call(_get)

    def start_sync(self) -> "_Controller":
        """
        Start the worker asynchronously and return a synchronous Controller.

        Usage:
            ctl = worker.start_sync()
            # ... later
            ctl.stop()
        """
        try:
            from anyio.from_thread import start_blocking_portal
        except Exception as e:
            raise RuntimeError(
                "anyio is required for Worker.start_sync(). Please ensure 'anyio' is installed."
            ) from e

        # Install signal handlers in the main thread before starting the event loop thread
        self.install_signal_handlers()

        portal_cm = start_blocking_portal()
        portal = portal_cm.__enter__()

        # schedule the worker run in background; it exits when shutdown_event is set
        def _start() -> None:
            # fire-and-forget task
            async def _runner():
                await self.run()

            # run as a task; no result awaited here
            import anyio

            anyio.create_task_group().start_soon  # no-op reference to satisfy linters
            # simplest is to call soon
            asyncio.create_task(_runner())

        portal.call(_start)

        return Worker._Controller(self, portal_cm, portal)

    def stop_sync(self) -> None:
        """
        Convenience synchronous stop for a worker that was started via start_sync(),
        if a controller is not retained. This method is a no-op unless start_sync()
        was used and a controller stored on the instance.
        """
        ctl = getattr(self, "_sync_controller", None)
        if ctl:
            ctl.stop()