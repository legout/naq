# src/naq/worker/core.py
"""
Core Worker implementation for NAQ.

This module contains the main Worker class for processing jobs from NATS queues,
coordinating with the various manager components for status tracking, job management,
and failed job handling.
"""

import asyncio
import os
import signal
import socket
import sys
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional, Sequence

import nats
from loguru import logger
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig

from ..services import ServiceManager
from ..services.connection import ConnectionService
from ..services.streams import StreamService
from ..services.events import EventService
from ..services.jobs import JobService
from ..exceptions import NaqConnectionError, NaqException
from ..models import Job, JOB_STATUS
from ..settings import (
    ACK_WAIT_PER_QUEUE,
    DEFAULT_ACK_WAIT_SECONDS,
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    NAQ_PREFIX,
    WORKER_STATUS,
)
from ..utils import run_async_from_sync, setup_logging
from .status import WorkerStatusManager
from .jobs import JobStatusManager
from .failed import FailedJobHandler


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs. Handles retries, dependencies,
    and reports its status via heartbeats.
    """

    def __init__(
        self,
        queues: Optional[Sequence[str] | str] = None,
        config: Optional[Any] = None,
        nats_url: Optional[str] = None,  # Legacy parameter
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
        ack_wait: Optional[
            int | Dict[str, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
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
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._consumers: Dict[
            str, nats.js.api.PullSubscribe
        ] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Handle configuration - prefer passed config over legacy parameters
        if config is not None:
            self._config = config
        else:
            # Fallback to dictionary config for backward compatibility
            self._config = {
                'nats_url': nats_url or DEFAULT_NATS_URL,
                'concurrency': concurrency,
                'heartbeat_interval': heartbeat_interval,
                'worker_ttl': worker_ttl,
            }
        
        # Initialize service manager
        self._service_manager: Optional[ServiceManager] = None
        self._initialized = False

        # Create component managers
        self._worker_status_manager = WorkerStatusManager(self)
        self._job_status_manager = JobStatusManager(self)
        self._failed_job_handler = FailedJobHandler(self)

        setup_logging()  # Setup logging

    async def _get_services(self) -> ServiceManager:
        """Get or create service manager."""
        if self._service_manager is None:
            self._service_manager = ServiceManager(self._config)
            await self._service_manager.initialize_all()
        return self._service_manager

    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        if not self._initialized:
            # Initialize services
            services = await self._get_services()
            connection_service = await services.get_service(ConnectionService)
            event_service = await services.get_service(EventService)
            
            logger.info(f"Worker '{self.worker_id}' connected to NATS and JetStream via services.")

            # Initialize component managers
            await self._worker_status_manager.start_heartbeat_loop()
            await self._job_status_manager.initialize()
            await self._failed_job_handler.initialize()
            
            self._initialized = True

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
        services = await self._get_services()
        connection_service = await services.get_service(ConnectionService)
        
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
            
            async with connection_service.jetstream_scope() as js:
                psub = await js.pull_subscribe(
                    subject=subject,
                    durable=durable_name,
                    config=ConsumerConfig(
                        ack_policy=nats.js.api.AckPolicy.EXPLICIT,
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

                except nats.errors.TimeoutError:
                    # No messages available, or timeout hit, loop continues
                    await asyncio.sleep(0.1)  # Small sleep to prevent busy-wait
                    continue
                except nats.js.errors.ConsumerNotFoundError:
                    logger.warning(
                        f"Consumer '{durable_name}' not found. Stopping fetch loop."
                    )
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching from consumer '{durable_name}': {e}",
                        exc_info=True,
                    )
                    await asyncio.sleep(1)  # Wait before retrying fetch

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

            # Update worker status to busy with this job
            await self._worker_status_manager.update_status(
                WORKER_STATUS.BUSY, job_id=job.job_id
            )

            # Log worker busy event
            services = await self._get_services()
            event_service = await services.get_service(EventService)
            try:
                await event_service.log_worker_busy(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    current_job_id=job.job_id
                )
            except Exception as e:
                logger.warning(f"Failed to log worker busy event: {e}")

            # Use JobService for execution - it handles logging, timing, and result storage
            job_service = await services.get_service(JobService)
            
            # JobService.execute_job handles all the logic: logging start/completion,
            # execution with timeout, result storage, and error handling
            result = await job_service.execute_job(job, worker_id=self.worker_id)

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
            # If we have a NATS message and it has a term() method, terminate it
            if hasattr(msg, "term"):
                await msg.term()
        finally:
            # Update worker status back to idle
            await self._worker_status_manager.update_status(WORKER_STATUS.IDLE)
            
            # Log worker idle event
            try:
                services = await self._get_services()
                event_service = await services.get_service(EventService)
                await event_service.log_worker_idle(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid()
                )
            except Exception as e:
                logger.warning(f"Failed to log worker idle event: {e}")

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
            delay = job.get_retry_delay(attempt)
            logger.info(
                f"Job {job.job_id} failed, scheduling retry {attempt}/{max_retries} "
                f"after {delay:.2f}s delay."
            )
            
            # Log retry scheduled event using service
            event_service = await self._service_manager.get_service(EventService)
            await event_service.log_job_retry_scheduled(
                job_id=job.job_id,
                worker_id=self.worker_id,
                queue_name=job.queue_name,
                retry_count=attempt,
                retry_delay=delay,
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

            # Register worker initially
            await self._worker_status_manager.update_status(
                status=WORKER_STATUS.STARTING
            )

            # Log worker started event
            services = await self._get_services()
            event_service = await services.get_service(EventService)
            try:
                await event_service.log_worker_started(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    queue_names=self.queue_names,
                    concurrency_limit=self._concurrency
                )
            except Exception as e:
                logger.warning(f"Failed to log worker started event: {e}")

            # Start heartbeat task
            await self._worker_status_manager.start_heartbeat_loop()

            # Ensure the main work stream exists using service
            services = await self._get_services()
            stream_service = await services.get_service(StreamService)
            await stream_service.ensure_stream(
                name=self.stream_name,
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

        # Log worker stopped event before unregistering
        if self._service_manager is not None:
            try:
                event_service = await self._service_manager.get_service(EventService)
                await event_service.log_worker_stopped(
                    worker_id=self.worker_id,
                    hostname=socket.gethostname(),
                    pid=os.getpid()
                )
            except Exception as e:
                logger.warning(f"Failed to log worker stopped event: {e}")

        # Unregister worker after cleanup
        try:
            await self._worker_status_manager.unregister_worker()
        except Exception as e:
            logger.error(f"Error unregistering worker: {e}")

        # Close services
        if self._service_manager is not None:
            try:
                await self._service_manager.cleanup_all()
                self._service_manager = None
            except Exception as e:
                logger.error(f"Error closing services: {e}")
        
        self._initialized = False

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
            return asyncio.create_task(_runner())

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