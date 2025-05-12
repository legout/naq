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
from .job import Job, JobStatus
from .settings import (
    DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
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
    
    def __init__(self, worker):
        """Initialize the worker status manager."""
        self.worker = worker
        self._current_status = WORKER_STATUS_STARTING
        self._kv_store = None
        self._heartbeat_task = None
    
    async def _get_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return the NATS Key-Value store for worker statuses."""
        if self._kv_store is None:
            if not self.worker._js:
                logger.error("JetStream context not available")
                return None
            try:
                self._kv_store = await self.worker._js.key_value(bucket=WORKER_KV_NAME)
            except BucketNotFoundError:
                try:
                    self._kv_store = await self.worker._js.create_key_value(
                        bucket=WORKER_KV_NAME,
                        ttl=self.worker._worker_ttl if self.worker._worker_ttl > 0 else 0,
                        description="Stores naq worker status and heartbeats"
                    )
                except Exception as e:
                    logger.error(f"Failed to create worker status KV store: {e}")
                    self._kv_store = None
            except Exception as e:
                logger.error(f"Failed to get worker status KV store: {e}")
                self._kv_store = None
        return self._kv_store
    
    async def update_status(self, status: str, job_id: Optional[str] = None) -> None:
        """Updates the worker's status in the KV store."""
        self._current_status = status
        kv_store = await self._get_kv_store()
        if not kv_store:
            return

        payload = {
            "worker_id": self.worker.worker_id,
            "status": status,
            "timestamp": time.time(),
            "hostname": socket.gethostname(),
            "pid": os.getpid()
        }
        if job_id:
            payload["job_id"] = job_id

        try:
            await kv_store.put(self.worker.worker_id.encode(), cloudpickle.dumps(payload))
        except Exception as e:
            logger.error(f"Failed to update worker status: {e}")
    
    async def _heartbeat(self) -> None:
        """Sends periodic heartbeat updates."""
        while True:
            await self.update_status(self._current_status)
            await asyncio.sleep(DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS)

    async def start_heartbeat_loop(self) -> None:
        """Start the heartbeat loop."""
        if not self._heartbeat_task:
            self._heartbeat_task = asyncio.create_task(self._heartbeat())
            await self.update_status(WORKER_STATUS_IDLE)

    async def stop_heartbeat_loop(self) -> None:
        """Stop the heartbeat loop."""
        if not self._heartbeat_task or self._heartbeat_task.done():
            return

        # Update status before canceling task to ensure it's captured
        try:
            await self.update_status(WORKER_STATUS_STOPPING)
        except Exception as e:
            logger.error(f"Error updating status during shutdown: {e}")

        # Cancel and wait for task with proper exception handling
        self._heartbeat_task.cancel()
        try:
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error during heartbeat task shutdown: {e}")

    async def unregister_worker(self) -> None:
        """Delete the worker's status entry from the KV store."""
        kv_store = await self._get_kv_store()
        if not kv_store:
            logger.warning(f"Worker status KV store not available. Cannot unregister worker {self.worker.worker_id}")
            return

        try:
            await kv_store.delete(self.worker.worker_id.encode('utf-8'))
            logger.info(f"Unregistered worker {self.worker.worker_id}")
        except Exception as e:
            logger.error(f"Failed to unregister worker {self.worker.worker_id}: {e}")
    
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
    
    def __init__(self, worker):
        self.worker = worker
        self._result_kv_store = None

    async def _get_result_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return NATS KV store for results."""
        if self._result_kv_store is None:
            if not self.worker._js:
                logger.error("JetStream context not available")
                return None
            try:
                self._result_kv_store = await self.worker._js.key_value(bucket=RESULT_KV_NAME)
            except BucketNotFoundError:
                try:
                    self._result_kv_store = await self.worker._js.create_key_value(
                        bucket=RESULT_KV_NAME,
                        description="Stores job results and errors"
                    )
                except Exception as e:
                    logger.error(f"Failed to create result KV store: {e}")
                    self._result_kv_store = None
            except Exception as e:
                logger.error(f"Failed to get result KV store: {e}")
                self._result_kv_store = None
        return self._result_kv_store

    async def update_job(self, job: Job) -> None:
        """Update job status and result in KV store."""
        kv_store = await self._get_result_kv_store()
        if not kv_store:
            logger.warning(f"Result KV store not available. Cannot update status for job {job.job_id}")
            return

        try:
            payload = {
                'status': job.status.value,
                'result': job.result if hasattr(job, 'result') else None,
                'error': str(job.error) if job.error else None,
                'traceback': job.traceback,
                'job_id': job.job_id,
                'queue_name': job.queue_name,
                'started_at': job.started_at,
                'finished_at': job.finished_at
            }
            serialized_payload = cloudpickle.dumps(payload)
            await kv_store.put(job.job_id.encode('utf-8'), serialized_payload)
            logger.debug(f"Updated status for job {job.job_id} to {job.status.value}")
        except Exception as e:
            logger.error(f"Failed to update job status: {e}")
    
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
            self._result_kv_store = await js.key_value(bucket=RESULT_KV_NAME)
            logger.info(f"Bound to result KV store: '{RESULT_KV_NAME}'")
        except BucketNotFoundError:
            logger.warning(f"Result KV store '{RESULT_KV_NAME}' not found. Creating...")
            try:
                # Use integer seconds for TTL
                default_ttl_seconds = int(DEFAULT_RESULT_TTL_SECONDS) if DEFAULT_RESULT_TTL_SECONDS > 0 else 0
                logger.info(
                    f"Creating result KV store '{RESULT_KV_NAME}' with default TTL: {default_ttl_seconds}s"
                )
                self._result_kv_store = await js.create_key_value(
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
                self._result_kv_store = None  # Continue without result backend if creation fails
        except Exception as e:
            logger.error(
                f"Failed to bind to result KV store '{RESULT_KV_NAME}': {e}",
                exc_info=True,
            )
            self._result_kv_store = None
    
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
        if not self._result_kv_store: # Directly use the initialized attribute
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
            await self._result_kv_store.put(key, payload) # Use the direct attribute

        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            logger.error(f"Failed to store result/failure info for job {job.job_id}: {e}", exc_info=True)


class FailedJobHandler:
    """
    Handles failed job processing and storage.
    """
    
    def __init__(self, worker):
        self.worker = worker

    async def handle_failed_job(self, job: Job) -> None:
        """Handle a failed job by publishing it to the failed job stream."""
        if not self.worker._js:
            logger.error(f"Cannot handle failed job {job.job_id}, JetStream context not available")
            return

        subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name}"
        try:
            # Ensure the failed job stream exists
            await ensure_stream(
                js=self.worker._js,
                stream_name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
            )
            
            # Publish the failed job
            payload = job.serialize_failed_job()
            await self.worker._js.publish(subject, payload)
            logger.info(f"Published failed job {job.job_id} to {subject}")
        except Exception as e:
            logger.error(f"Failed to publish failed job {job.job_id}: {e}", exc_info=True)
    
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

        # Preserve order while ensuring uniqueness using dict.fromkeys()
        self.queue_names: List[str] = list(dict.fromkeys(queues))
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
        self._consumers: Dict[str, nats.js.api.PullSubscribe] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Create component managers
        self._worker_status_manager = WorkerStatusManager(self)
        self._job_status_manager = JobStatusManager(self)
        self._failed_job_handler = FailedJobHandler(self)

        setup_logging()  # Setup logging

    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            logger.info(f"Worker '{self.worker_id}' connected to NATS and JetStream.")

            # Initialize component managers
            await self._worker_status_manager.start_heartbeat_loop()
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
            self._consumers[queue_name] = psub
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

    async def process_message(self, msg: Any) -> None:
        """Process a received job message."""
        job = None
        try:
            # Deserialize the job from the message data
            if hasattr(msg, 'data'):
                job = Job.deserialize(msg.data)
            else:
                # For testing where msg might be a Job directly
                job = msg

            if self._shutdown_event.is_set():
                logger.info(f"Shutdown in progress. Job {job.job_id if job else 'unknown'} will not be processed.")
                if hasattr(msg, 'nak'): # NAK the message so it can be re-queued
                    await msg.nak()
                return # Do not process if shutdown is initiated
    
            # Update worker status to busy with this job
            await self._worker_status_manager.update_status(WORKER_STATUS_BUSY, job_id=job.job_id)
    
            # Execute the job
            await job.execute() # This will set job.error and job.traceback on failure
    
            # Store result (which includes error/traceback if any)
            await self._job_status_manager.store_result(job)
    
            # Handle failure if needed (e.g., publish to dead-letter queue)
            if job.status == JobStatus.FAILED: # status is a property derived from job.error
                await self._failed_job_handler.handle_failed_job(job)

            # Acknowledge message processing complete
            if hasattr(msg, 'ack'):
                await msg.ack()

        except Exception as e:
            logger.error(f"Error processing job {job.job_id if job else 'unknown'}: {e}", exc_info=True)
            # If we have a NATS message and it has a term() method, terminate it
            if hasattr(msg, 'term'):
                await msg.term()
        finally:
            # Update worker status back to idle
            await self._worker_status_manager.update_status(WORKER_STATUS_IDLE)

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
            await self._worker_status_manager.update_status(status=WORKER_STATUS_STARTING)

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
            await self._worker_status_manager.update_status(status=WORKER_STATUS_IDLE)

            await self._shutdown_event.wait()

            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            await self._worker_status_manager.update_status(status=WORKER_STATUS_STOPPING)

            # Stop heartbeat task
            await self._worker_status_manager.stop_heartbeat_loop()

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
            await self._worker_status_manager.update_status(status=WORKER_STATUS_STOPPING)
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
                logger.warning(f"Error cleaning up consumer for queue {queue_name}: {e}")
        self._consumers.clear()

        # Unregister worker after cleanup
        try:
            await self._worker_status_manager.unregister_worker()
        except Exception as e:
            logger.error(f"Error unregistering worker: {e}")

        # Finally close NATS connection
        try:
            await close_nats_connection()
        except Exception as e:
            logger.error(f"Error closing NATS connection: {e}")
        
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
