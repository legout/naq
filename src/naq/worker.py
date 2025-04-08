# src/naq/worker.py
import asyncio
import os
import signal
import socket
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional, Sequence

import cloudpickle
import nats
from loguru import logger
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from .connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from .exceptions import NaqException, SerializationError
from .job import Job, JobExecutionError
from .settings import (
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    DEPENDENCY_CHECK_DELAY_SECONDS,
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_KV_NAME,
    JOB_STATUS_TTL_SECONDS,
    NAQ_PREFIX,
    RESULT_KV_NAME,
    WORKER_KV_NAME,
    WORKER_STATUS_BUSY,
    WORKER_STATUS_IDLE,
    WORKER_STATUS_STARTING,
    WORKER_STATUS_STOPPING,
)
from .utils import run_async_from_sync, setup_logging


class WorkerStatusManager:
    """
    Manages worker status reporting, heartbeats, and monitoring.
    """
    
    def __init__(
        self,
        worker_id: str,
        queue_names: List[str],
        heartbeat_interval: int,
        worker_ttl: int,
        concurrency: int,
        semaphore: asyncio.Semaphore,
    ):
        self.worker_id = worker_id
        self.queue_names = queue_names
        self._heartbeat_interval = heartbeat_interval
        self._worker_ttl = worker_ttl
        self._concurrency = concurrency
        self._semaphore = semaphore
        self._current_status = WORKER_STATUS_STARTING
        self._current_job_id: Optional[str] = None
        self._worker_kv: Optional[KeyValue] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = True
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self, js: JetStreamContext) -> None:
        """Initialize the worker status manager with a JetStream context."""
        # Connect to Worker KV Store
        try:
            self._worker_kv = await js.key_value(bucket=WORKER_KV_NAME)
            logger.info(f"Bound to worker status KV store: '{WORKER_KV_NAME}'")
            return
        except BucketNotFoundError:
            logger.warning(f"Worker status KV store '{WORKER_KV_NAME}' not found. Creating...")
            try:
                # Use integer seconds for TTL
                worker_ttl_seconds = int(self._worker_ttl) if self._worker_ttl > 0 else 0
                logger.info(
                    f"Creating worker status KV store '{WORKER_KV_NAME}' with default TTL: {worker_ttl_seconds}s"
                )
                self._worker_kv = await js.create_key_value(
                    bucket=WORKER_KV_NAME,
                    ttl=worker_ttl_seconds,
                    description="Stores naq worker status and heartbeats",
                )
                logger.info(f"Created worker status KV store: '{WORKER_KV_NAME}'")
            except Exception as create_e:
                logger.error(
                    f"Failed to create worker status KV store '{WORKER_KV_NAME}': {create_e}",
                    exc_info=True,
                )
                self._worker_kv = None  # Continue without worker monitoring if creation fails
        except Exception as e:
            logger.error(
                f"Failed to bind to worker status KV store '{WORKER_KV_NAME}': {e}",
                exc_info=True,
            )
            self._worker_kv = None
    
    async def update_status(
        self, status: Optional[str] = None, job_id: Optional[str] = None
    ) -> None:
        """Updates the worker's status in the KV store."""
        if not self._worker_kv:
            return  # Cannot update status if KV is not available

        if status:
            self._current_status = status
            
        # If status is busy, use provided job_id, otherwise clear it
        self._current_job_id = job_id if self._current_status == WORKER_STATUS_BUSY else None

        key = self.worker_id.encode("utf-8")
        worker_data = {
            "worker_id": self.worker_id,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "queues": self.queue_names,
            "status": self._current_status,
            "current_job_id": self._current_job_id,
            "last_heartbeat_utc": time.time(),
            "concurrency": self._concurrency,
            "active_tasks": self._concurrency - self._semaphore._value,  # How many tasks are active
        }
        
        try:
            payload = cloudpickle.dumps(worker_data)
            await self._worker_kv.put(key, payload)
            logger.debug(f"Worker {self.worker_id} heartbeat sent. Status: {self._current_status}")
        except Exception as e:
            logger.warning(f"Failed to update worker status/heartbeat for {self.worker_id}: {e}")
    
    async def start_heartbeat_loop(self) -> None:
        """Start the heartbeat loop to periodically update worker status."""
        self._shutdown_event.clear()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def _heartbeat_loop(self) -> None:
        """Periodically sends heartbeat updates."""
        while self._running:
            try:
                # Update status without changing it, just refresh TTL and timestamp
                await self.update_status()
                # Wait for interval or shutdown
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._heartbeat_interval
                )
                # If wait finished without timeout, shutdown was triggered
                break
            except asyncio.TimeoutError:
                continue  # Expected timeout, continue loop
            except asyncio.CancelledError:
                logger.info("Heartbeat loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop for {self.worker_id}: {e}", exc_info=True)
                # Wait a bit before retrying after an error
                await asyncio.sleep(self._heartbeat_interval)
    
    async def stop_heartbeat_loop(self) -> None:
        """Stop the heartbeat loop."""
        self._running = False
        self._shutdown_event.set()
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass  # Expected
    
    async def unregister_worker(self) -> None:
        """Removes the worker's status entry from the KV store."""
        if not self._worker_kv:
            return
            
        logger.info(f"Unregistering worker {self.worker_id}...")
        try:
            await self._worker_kv.delete(self.worker_id.encode("utf-8"))
            logger.debug(f"Worker {self.worker_id} status deleted from KV.")
        except Exception as e:
            logger.warning(f"Failed to delete worker status for {self.worker_id}: {e}")
    
    @staticmethod
    async def list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lists active workers by querying the worker status KV store.

        Args:
            nats_url: NATS server URL (if not using default).

        Returns:
            A list of dictionaries, each containing information about a worker.

        Raises:
            NaqConnectionError: If connection fails.
            NaqException: For other errors.
        """
        from .connection import (
            close_nats_connection,
            get_jetstream_context,
            get_nats_connection,
        )
        from .exceptions import NaqConnectionError, NaqException
        from .settings import WORKER_KV_NAME

        workers = []
        nc = None
        kv = None
        try:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)
            try:
                kv = await js.key_value(bucket=WORKER_KV_NAME)
            except Exception as e:
                logger.warning(f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}")
                return []  # Return empty list if store doesn't exist

            keys = await kv.keys()
            for key_bytes in keys:
                try:
                    entry = await kv.get(key_bytes)
                    if entry:
                        worker_data = cloudpickle.loads(entry.value)
                        workers.append(worker_data)
                except KeyNotFoundError:
                    continue  # Key might have expired between keys() and get()
                except Exception as e:
                    logger.error(f"Error reading worker data for key '{key_bytes.decode()}': {e}")

            return workers

        except NaqConnectionError:
            raise
        except Exception as e:
            raise NaqException(f"Error listing workers: {e}") from e
        finally:
            if nc:
                await close_nats_connection()


class JobStatusManager:
    """
    Manages job status tracking and dependency resolution.
    """
    
    def __init__(self):
        self._status_kv: Optional[KeyValue] = None
        self._result_kv: Optional[KeyValue] = None
    
    async def initialize(self, js: JetStreamContext) -> None:
        """Initialize the job status manager with a JetStream context."""
        await self._initialize_status_kv(js)
        await self._initialize_result_kv(js)
    
    async def _initialize_status_kv(self, js: JetStreamContext) -> None:
        """Initialize the job status KV store."""
        try:
            self._status_kv = await js.key_value(bucket=JOB_STATUS_KV_NAME)
            logger.info(f"Bound to job status KV store: '{JOB_STATUS_KV_NAME}'")
        except BucketNotFoundError:
            logger.warning(f"Job status KV store '{JOB_STATUS_KV_NAME}' not found. Creating...")
            try:
                # Use integer seconds for TTL
                status_ttl_seconds = int(JOB_STATUS_TTL_SECONDS) if JOB_STATUS_TTL_SECONDS > 0 else 0
                logger.info(
                    f"Creating job status KV store '{JOB_STATUS_KV_NAME}' with default TTL: {status_ttl_seconds}s"
                )
                self._status_kv = await js.create_key_value(
                    bucket=JOB_STATUS_KV_NAME,
                    ttl=status_ttl_seconds,
                    description="Stores naq job completion status for dependencies",
                )
                logger.info(f"Created job status KV store: '{JOB_STATUS_KV_NAME}'")
            except Exception as create_e:
                logger.error(
                    f"Failed to create job status KV store '{JOB_STATUS_KV_NAME}': {create_e}",
                    exc_info=True,
                )
                # Worker might still function but dependencies won't work reliably
                self._status_kv = None
        except Exception as e:
            logger.error(
                f"Failed to bind to job status KV store '{JOB_STATUS_KV_NAME}': {e}",
                exc_info=True,
            )
            self._status_kv = None
    
    async def _initialize_result_kv(self, js: JetStreamContext) -> None:
        """Initialize the result KV store."""
        try:
            self._result_kv = await js.key_value(bucket=RESULT_KV_NAME)
            logger.info(f"Bound to result KV store: '{RESULT_KV_NAME}'")
        except BucketNotFoundError:
            logger.warning(f"Result KV store '{RESULT_KV_NAME}' not found. Creating...")
            try:
                # Use integer seconds for TTL
                default_ttl_seconds = int(DEFAULT_RESULT_TTL_SECONDS) if DEFAULT_RESULT_TTL_SECONDS > 0 else 0
                logger.info(
                    f"Creating result KV store '{RESULT_KV_NAME}' with default TTL: {default_ttl_seconds}s"
                )
                self._result_kv = await js.create_key_value(
                    bucket=RESULT_KV_NAME,
                    ttl=default_ttl_seconds,
                    description="Stores naq job results and errors",
                )
                logger.info(f"Created result KV store: '{RESULT_KV_NAME}'")
            except Exception as create_e:
                logger.error(
                    f"Failed to create result KV store '{RESULT_KV_NAME}': {create_e}",
                    exc_info=True,
                )
                self._result_kv = None  # Continue without result backend if creation fails
        except Exception as e:
            logger.error(
                f"Failed to bind to result KV store '{RESULT_KV_NAME}': {e}",
                exc_info=True,
            )
            self._result_kv = None
    
    async def check_dependencies(self, job: Job) -> bool:
        """Checks if all dependencies for the job are met."""
        if not job.dependency_ids:
            return True  # No dependencies

        if not self._status_kv:
            logger.warning(
                f"Job status KV store not available. Cannot check dependencies for job {job.job_id}. Assuming met."
            )
            return True

        logger.debug(f"Checking dependencies for job {job.job_id}: {job.dependency_ids}")
        try:
            for dep_id in job.dependency_ids:
                try:
                    entry = await self._status_kv.get(dep_id.encode("utf-8"))
                    status = entry.value.decode("utf-8")
                    if status == JOB_STATUS_COMPLETED:
                        logger.debug(f"Dependency {dep_id} for job {job.job_id} is completed.")
                        continue  # Dependency met
                    elif status == JOB_STATUS_FAILED:
                        logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} failed. Job {job.job_id} will not run."
                        )
                        return False
                    else:
                        # Unknown status? Treat as unmet for safety.
                        logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} has unknown status '{status}'. Treating as unmet."
                        )
                        return False
                except KeyNotFoundError:
                    # Dependency status not found, means it hasn't completed yet
                    logger.debug(f"Dependency {dep_id} for job {job.job_id} not found in status KV. Not met yet.")
                    return False
            # If loop completes, all dependencies were found and completed
            logger.debug(f"All dependencies met for job {job.job_id}.")
            return True
        except Exception as e:
            logger.error(f"Error checking dependencies for job {job.job_id}: {e}", exc_info=True)
            return False  # Assume dependencies not met on error
    
    async def update_job_status(self, job_id: str, status: str) -> None:
        """Updates the job status in the KV store."""
        if not self._status_kv:
            logger.warning(f"Job status KV store not available. Cannot update status for job {job_id}.")
            return

        logger.debug(f"Updating status for job {job_id} to '{status}'")
        try:
            await self._status_kv.put(job_id.encode("utf-8"), status.encode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to update status for job {job_id} to '{status}': {e}", exc_info=True)
    
    async def store_result(self, job: Job) -> None:
        """Stores the job result or failure info in the result KV store."""
        if not self._result_kv:
            logger.debug(f"Result KV store not available. Skipping result storage for job {job.job_id}.")
            return

        key = job.job_id.encode("utf-8")
        
        try:
            if job.error:
                # Store failure information
                result_data = {
                    "status": JOB_STATUS_FAILED,
                    "error": job.error,
                    "traceback": job.traceback,
                }
                logger.debug(f"Storing failure info for job {job.job_id}")
            else:
                # Store successful result
                result_data = {
                    "status": JOB_STATUS_COMPLETED,
                    "result": job.result,
                }
                logger.debug(f"Storing result for job {job.job_id}")

            payload = cloudpickle.dumps(result_data)
            await self._result_kv.put(key, payload)

        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            logger.error(f"Failed to store result/failure info for job {job.job_id}: {e}", exc_info=True)


class FailedJobHandler:
    """
    Handles failed job processing and storage.
    """
    
    def __init__(self):
        self._js: Optional[JetStreamContext] = None
    
    async def initialize(self, js: JetStreamContext) -> None:
        """Initialize the failed job handler with a JetStream context."""
        self._js = js
        await self._ensure_failed_stream()
    
    async def _ensure_failed_stream(self) -> None:
        """Ensures the stream for failed jobs exists."""
        if not self._js:
            logger.error("JetStream context not available, cannot ensure failed stream.")
            return
            
        try:
            await ensure_stream(
                js=self._js,
                stream_name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
            )
        except Exception as e:
            # Log the error but allow the worker to continue if possible
            logger.error(
                f"Failed to ensure failed jobs stream '{FAILED_JOB_STREAM_NAME}': {e}",
                exc_info=True,
            )
    
    async def publish_failed_job(self, job: Job) -> None:
        """Publishes failed job details to the failed job subject."""
        if not self._js:
            logger.error(f"Cannot publish failed job {job.job_id}, JetStream context not available.")
            return

        failed_subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name or 'unknown'}"
        try:
            payload = job.serialize_failed_job()
            await self._js.publish(failed_subject, payload)
            logger.info(f"Published failed job {job.job_id} details to subject '{failed_subject}'.")
        except SerializationError as e:
            logger.error(f"Could not serialize failed job {job.job_id} details: {e}", exc_info=True)
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id} to subject '{failed_subject}': {e}",
                exc_info=True,
            )


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs. Handles retries, dependencies,
    and reports its status via heartbeats.
    """

    def __init__(
        self,
        queues: Sequence[str] | str,
        nats_url: Optional[str] = None,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
    ):
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            raise ValueError("Worker must listen to at least one queue.")

        self.queue_names: List[str] = list(set(queues))  # Ensure unique names
        self.subjects: List[str] = [f"{NAQ_PREFIX}.queue.{name}" for name in self.queue_names]
        self._nats_url = nats_url
        self._concurrency = concurrency
        
        # Generate a unique ID if name is not provided, otherwise use name as base
        base_name = worker_name or f"naq-worker-{socket.gethostname()}"
        self.worker_id = f"{base_name}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        
        self._heartbeat_interval = heartbeat_interval
        self._worker_ttl = worker_ttl

        # Connection and state variables
        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Create component managers
        self._status_manager = WorkerStatusManager(
            worker_id=self.worker_id,
            queue_names=self.queue_names,
            heartbeat_interval=heartbeat_interval,
            worker_ttl=worker_ttl,
            concurrency=concurrency,
            semaphore=self._semaphore,
        )
        self._job_status_manager = JobStatusManager()
        self._failed_job_handler = FailedJobHandler()

        setup_logging()  # Setup logging

    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            logger.info(f"Worker '{self.worker_id}' connected to NATS and JetStream.")

            # Initialize component managers
            await self._status_manager.initialize(self._js)
            await self._job_status_manager.initialize(self._js)
            await self._failed_job_handler.initialize(self._js)

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
            psub = await self._js.pull_subscribe(
                subject=subject,
                durable=durable_name,
                config=ConsumerConfig(
                    ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                    ack_wait=30,  # TODO: Make configurable, relate to job timeout
                    max_ack_pending=self._concurrency * 2,
                ),
            )
            logger.info(f"Pull consumer '{durable_name}' created for subject '{subject}'.")

            while self._running:
                if self._semaphore.locked():  # Check semaphore before fetching
                    await asyncio.sleep(0.1)  # Wait if concurrency limit reached
                    continue

                try:
                    # Calculate how many messages we can fetch based on available concurrency slots
                    available_slots = self._concurrency - (self._concurrency - self._semaphore._value)
                    if available_slots <= 0:
                        await asyncio.sleep(0.1)  # Wait if no slots free
                        continue

                    # Fetch up to the number of available slots, with a timeout
                    msgs = await psub.fetch(batch=available_slots, timeout=1)
                    if msgs:
                        logger.debug(f"Fetched {len(msgs)} messages from '{durable_name}'")

                    for msg in msgs:
                        # Acquire semaphore before starting processing task
                        await self._semaphore.acquire()
                        # Create task to process the message
                        task = asyncio.create_task(self.process_message(msg))
                        # Add a callback to release the semaphore when the task completes (success or failure)
                        task.add_done_callback(lambda t: self._semaphore.release())
                        # Keep track of tasks (optional, for clean shutdown)
                        self._tasks.append(task)
                        self._tasks = [t for t in self._tasks if not t.done()]  # Basic cleanup

                except nats.errors.TimeoutError:
                    # No messages available, or timeout hit, loop continues
                    await asyncio.sleep(0.1)  # Small sleep to prevent busy-wait
                    continue
                except nats.js.errors.ConsumerNotFoundError:
                    logger.warning(f"Consumer '{durable_name}' not found. Stopping fetch loop.")
                    break
                except Exception as e:
                    logger.error(f"Error fetching from consumer '{durable_name}': {e}", exc_info=True)
                    await asyncio.sleep(1)  # Wait before retrying fetch

        except Exception as e:
            logger.error(
                f"Failed to subscribe or run fetch loop for queue '{queue_name}': {e}",
                exc_info=True,
            )

    async def process_message(self, msg: Msg) -> None:
        """Deserializes and executes a job, handling retries, failures, and dependencies."""
        job: Optional[Job] = None
        try:
            logger.info(
                f"Received message: Subject='{msg.subject}', Sid='{msg.sid}', "
                f"Seq={msg.metadata.sequence.stream}, Delivered={msg.metadata.num_delivered}"
            )
            job = Job.deserialize(msg.data)

            # --- Dependency Check ---
            dependencies_met = await self._job_status_manager.check_dependencies(job)
            if not dependencies_met:
                logger.info(
                    f"Dependencies not met for job {job.job_id}. "
                    f"Re-queueing with delay {DEPENDENCY_CHECK_DELAY_SECONDS}s."
                )
                await msg.nak(delay=DEPENDENCY_CHECK_DELAY_SECONDS)
                return  # Stop processing this message for now

            # --- Update Status to Busy ---
            await self._status_manager.update_status(status=WORKER_STATUS_BUSY, job_id=job.job_id)

            # --- Execute Job ---
            logger.info(
                f"Processing job {job.job_id} ({getattr(job.function, '__name__', 'unknown')}) "
                f"attempt {msg.metadata.num_delivered}"
            )
            await asyncio.to_thread(job.execute)  # Run synchronous function in thread pool

            # --- Success ---
            logger.info(f"Job {job.job_id} completed successfully.")
            await self._job_status_manager.update_job_status(job.job_id, JOB_STATUS_COMPLETED)
            await self._job_status_manager.store_result(job)
            await msg.ack()
            logger.debug(f"Message acknowledged: Sid='{msg.sid}'")

        except JobExecutionError as e:  # Job function raised an exception
            await self._handle_job_execution_error(job, msg)
        except SerializationError as e:
            logger.error(f"Failed to deserialize job data: {e}. Terminating message.", exc_info=True)
            await msg.term()  # Terminate poison pill messages
        except Exception as e:
            # Catch-all for unexpected errors during processing
            await self._handle_unexpected_error(job, msg, e)
        finally:
            # --- Update Status back to Idle (or stopping if shutdown happened) ---
            final_status = WORKER_STATUS_IDLE if self._running else WORKER_STATUS_STOPPING
            await self._status_manager.update_status(status=final_status, job_id=None)

    async def _handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        if job is None:
            logger.error("Job object is None after JobExecutionError, cannot handle retry/failure.")
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
            try:
                await msg.nak(delay=delay)
                logger.debug(f"Message Nak'd for retry: Sid='{msg.sid}'")
            except Exception as nak_e:
                logger.error(f"Failed to NAK message Sid='{msg.sid}' for retry: {nak_e}", exc_info=True)
                await msg.term()  # Terminate if NAK fails
        else:
            # --- Terminal Failure ---
            logger.error(f"Job {job.job_id} failed after {attempt - 1} retries. Moving to failed queue.")
            await self._job_status_manager.update_job_status(job.job_id, JOB_STATUS_FAILED)
            await self._job_status_manager.store_result(job)
            await self._failed_job_handler.publish_failed_job(job)
            try:
                await msg.ack()  # Ack original message after handling failure
                logger.debug(f"Message acknowledged after moving to failed queue: Sid='{msg.sid}'")
            except Exception as ack_e:
                logger.error(
                    f"Failed to ACK message Sid='{msg.sid}' after moving to failed queue: {ack_e}",
                    exc_info=True,
                )

    async def _handle_unexpected_error(self, job: Optional[Job], msg: Msg, error: Exception) -> None:
        """Handle unexpected errors during message processing."""
        logger.error(
            f"Unhandled error processing message (Sid='{msg.sid}', "
            f"JobId='{job.job_id if job else 'N/A'}'): {error}",
            exc_info=True,
        )
        try:
            # Update status to failed if possible, otherwise terminate
            if job:
                job.error = f"Worker processing error: {error}"  # Assign error for storage
                job.traceback = traceback.format_exc()
                await self._job_status_manager.update_job_status(job.job_id, JOB_STATUS_FAILED)
                await self._job_status_manager.store_result(job)
            await msg.term()
            logger.warning(f"Terminated message Sid='{msg.sid}' due to unexpected processing error.")
        except Exception as term_e:
            logger.error(f"Failed to Terminate message Sid='{msg.sid}': {term_e}", exc_info=True)

    async def run(self) -> None:
        """Starts the worker, connects to NATS, and begins processing jobs."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()
            
            # Register worker initially
            await self._status_manager.update_status(status=WORKER_STATUS_STARTING)

            # Start heartbeat task
            await self._status_manager.start_heartbeat_loop()

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
            await self._status_manager.update_status(status=WORKER_STATUS_IDLE)

            await self._shutdown_event.wait()

            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            await self._status_manager.update_status(status=WORKER_STATUS_STOPPING)

            # Stop heartbeat task
            await self._status_manager.stop_heartbeat_loop()

            # Wait for active processing tasks (respecting semaphore)
            active_tasks = self._concurrency - self._semaphore._value
            if active_tasks > 0:
                logger.info(f"Waiting for {active_tasks} active job(s) to finish...")
                # Wait for semaphore to be fully released, with a timeout
                try:
                    await asyncio.wait_for(self._wait_for_semaphore(), timeout=30.0)  # Wait up to 30s
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
            await self._status_manager.update_status(status=WORKER_STATUS_STOPPING)
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
        # Unregister worker first
        await self._status_manager.unregister_worker()
        # Close NATS connection
        await close_nats_connection()
        self._nc = None
        self._js = None

    def signal_handler(self, sig, frame) -> None:
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self) -> None:
        """Installs signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    # --- Static methods for worker monitoring ---
    @staticmethod
    async def list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        return await WorkerStatusManager.list_workers(nats_url)

    @staticmethod
    def list_workers_sync(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        return run_async_from_sync(Worker.list_workers(nats_url=nats_url))
