# src/naq/worker.py
import asyncio
import signal
import logging
import os # Import os
from typing import Optional, List, Sequence

import nats
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.aio.msg import Msg

from .settings import DEFAULT_QUEUE_NAME, NAQ_PREFIX
from .job import Job, JobExecutionError # Import JobExecutionError
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection, ensure_stream # Import ensure_stream
from .exceptions import NaqException, SerializationError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Define subject for failed jobs
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
# Define stream name for failed jobs (could be same or different)
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"

class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs.
    """
    def __init__(
        self,
        queues: Sequence[str] | str,
        nats_url: Optional[str] = None,
        concurrency: int = 10, # Max concurrent jobs
        worker_name: Optional[str] = None, # For durable consumer names
    ):
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            raise ValueError("Worker must listen to at least one queue.")

        self.queue_names: List[str] = list(set(queues)) # Ensure unique names
        self.subjects: List[str] = [f"{NAQ_PREFIX}.queue.{name}" for name in self.queue_names]
        self._nats_url = nats_url
        self._concurrency = concurrency
        self._worker_name = worker_name or f"naq-worker-{os.urandom(4).hex()}" # Unique-ish name

        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency) # Add semaphore for concurrency control

        # JetStream stream name (assuming one stream for all queues for now)
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"


    async def _connect(self):
        """Establish NATS connection and JetStream context."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            logger.info(f"Worker '{self._worker_name}' connected to NATS and JetStream.")

    async def _ensure_failed_stream(self):
        """Ensures the stream for storing failed jobs exists."""
        if not self._js:
            logger.warning("Cannot ensure failed stream, JetStream context not available.")
            return
        try:
            # Use InterestPolicy to retain messages even without consumers
            await ensure_stream(
                js=self._js,
                stream_name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
                retention=nats.js.api.RetentionPolicy.INTEREST, # Keep if interest (consumers) or limits
                storage=nats.js.api.StorageType.FILE, # Use File storage
            )
            logger.info(f"Ensured failed jobs stream '{FAILED_JOB_STREAM_NAME}' exists.")
        except Exception as e:
            logger.error(f"Failed to ensure failed jobs stream '{FAILED_JOB_STREAM_NAME}': {e}", exc_info=True)


    async def _subscribe_to_queue(self, queue_name: str):
        """Creates a durable consumer and starts fetching messages for a queue."""
        if not self._js:
            raise NaqException("JetStream context not available.")

        subject = f"{NAQ_PREFIX}.queue.{queue_name}"
        durable_name = f"{self.consumer_prefix}-{queue_name}"
        logger.info(f"Setting up consumer for queue '{queue_name}' (subject: {subject}, durable: {durable_name})")

        try:
            psub = await self._js.pull_subscribe(
                subject=subject,
                durable=durable_name,
                config=ConsumerConfig(
                    ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                    ack_wait=30, # TODO: Make configurable, relate to job timeout
                    max_ack_pending=self._concurrency * 2,
                )
            )
            logger.info(f"Pull consumer '{durable_name}' created for subject '{subject}'.")

            while self._running:
                if self._semaphore.locked(): # Check semaphore before fetching
                    await asyncio.sleep(0.1) # Wait if concurrency limit reached
                    continue

                try:
                    # Calculate how many messages we can fetch based on available concurrency slots
                    available_slots = self._concurrency - (self._concurrency - self._semaphore._value)
                    if available_slots <= 0:
                        await asyncio.sleep(0.1) # Wait if no slots free
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
                        self._tasks = [t for t in self._tasks if not t.done()] # Basic cleanup

                except nats.errors.TimeoutError:
                    # No messages available, or timeout hit, loop continues
                    await asyncio.sleep(0.1) # Small sleep prevent busy-wait if fetch always times out quickly
                    continue
                except nats.js.errors.ConsumerNotFoundError:
                    logger.warning(f"Consumer '{durable_name}' not found. Stopping fetch loop.")
                    break
                except Exception as e:
                    logger.error(f"Error fetching from consumer '{durable_name}': {e}", exc_info=True)
                    await asyncio.sleep(1) # Wait before retrying fetch

        except Exception as e:
            logger.error(f"Failed to subscribe or run fetch loop for queue '{queue_name}': {e}", exc_info=True)


    async def process_message(self, msg: Msg):
        """Deserializes and executes a job, handling retries and failures."""
        job: Optional[Job] = None
        try:
            logger.info(f"Received message: Subject='{msg.subject}', Sid='{msg.sid}', Seq={msg.metadata.sequence.stream}, Delivered={msg.metadata.num_delivered}")
            job = Job.deserialize(msg.data)
            logger.info(f"Processing job {job.job_id} ({getattr(job.function, '__name__', 'unknown')}) attempt {msg.metadata.num_delivered}")

            # Execute the job function (synchronous functions run in thread pool)
            await asyncio.to_thread(job.execute)

            # --- Success ---
            logger.info(f"Job {job.job_id} completed successfully.")
            await msg.ack()
            logger.debug(f"Message acknowledged: Sid='{msg.sid}'")

        except JobExecutionError as e: # Job function raised an exception
            logger.warning(f"Job {job.job_id} failed execution: {job.error}")
            if job is None: # Should not happen if JobExecutionError is raised
                 logger.error("Job object is None after JobExecutionError, cannot handle retry/failure.")
                 await msg.term() # Terminate if we can't process it
                 return

            # --- Retry Logic ---
            # NATS delivery count starts at 1
            attempt = msg.metadata.num_delivered
            max_retries = job.max_retries if job.max_retries is not None else 0

            if attempt <= max_retries:
                delay = job.get_retry_delay(attempt)
                logger.info(f"Job {job.job_id} failed, scheduling retry {attempt}/{max_retries} after {delay:.2f}s delay.")
                try:
                    await msg.nak(delay=delay)
                    logger.debug(f"Message Nak'd for retry: Sid='{msg.sid}'")
                except Exception as nak_e:
                    logger.error(f"Failed to NAK message Sid='{msg.sid}' for retry: {nak_e}", exc_info=True)
                    # What to do now? Maybe terminate to avoid loops?
                    await msg.term()

            # --- Terminal Failure ---
            else:
                logger.error(f"Job {job.job_id} failed after {attempt-1} retries. Moving to failed queue.")
                await self.publish_failed_job(job)
                try:
                    # Acknowledge the original message after moving to failed queue
                    await msg.ack()
                    logger.debug(f"Message acknowledged after moving to failed queue: Sid='{msg.sid}'")
                except Exception as ack_e:
                     logger.error(f"Failed to ACK message Sid='{msg.sid}' after moving to failed queue: {ack_e}", exc_info=True)


        except SerializationError as e:
            logger.error(f"Failed to deserialize job data: {e}. Terminating message.", exc_info=True)
            # Terminate poison pill messages that cannot be deserialized
            await msg.term()
        except Exception as e:
            # Catch-all for unexpected errors during processing (not job execution)
            logger.error(f"Unhandled error processing message (Sid='{msg.sid}', JobId='{job.job_id if job else 'N/A'}'): {e}", exc_info=True)
            # Terminate the message to prevent potential loops if it's not a job error
            try:
                await msg.term()
                logger.warning(f"Terminated message Sid='{msg.sid}' due to unexpected processing error.")
            except Exception as term_e:
                logger.error(f"Failed to Terminate message Sid='{msg.sid}': {term_e}", exc_info=True)
        # Semaphore is released via task.add_done_callback in _subscribe_to_queue


    async def publish_failed_job(self, job: Job):
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
            logger.error(f"Failed to publish failed job {job.job_id} to subject '{failed_subject}': {e}", exc_info=True)


    async def run(self):
        """Starts the worker, connects to NATS, and begins processing jobs."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()
            # Ensure the main work stream exists
            await ensure_stream(js=self._js, stream_name=self.stream_name, subjects=[f"{NAQ_PREFIX}.queue.*"])
            # Ensure the stream for failed jobs exists
            await self._ensure_failed_stream()

            # Start subscription tasks for each queue
            subscription_tasks = [
                asyncio.create_task(self._subscribe_to_queue(q_name))
                for q_name in self.queue_names
            ]

            logger.info(f"Worker '{self._worker_name}' started. Listening on queues: {self.queue_names}. Concurrency: {self._concurrency}")
            await self._shutdown_event.wait()

            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            # Wait for active processing tasks (respecting semaphore)
            # Wait for all semaphore slots to be released
            active_tasks = self._concurrency - self._semaphore._value
            if active_tasks > 0:
                 logger.info(f"Waiting for {active_tasks} active job(s) to finish...")
                 # Wait for semaphore to be fully released, with a timeout
                 try:
                     await asyncio.wait_for(self._wait_for_semaphore(), timeout=30.0) # Wait up to 30s
                 except asyncio.TimeoutError:
                     logger.warning("Timeout waiting for active jobs to finish.")

            # Cancel subscription loops (they should exit gracefully based on self._running)
            for task in subscription_tasks:
                task.cancel()
            await asyncio.gather(*subscription_tasks, return_exceptions=True) # Wait for cancellation

        except asyncio.CancelledError:
             logger.info("Run task cancelled.")
        except Exception as e:
            logger.error(f"Worker run loop encountered an error: {e}", exc_info=True)
        finally:
            logger.info("Worker shutting down...")
            await self._close()
            logger.info("Worker shutdown complete.")

    async def _wait_for_semaphore(self):
         """Helper to wait until the semaphore value reaches concurrency limit."""
         while self._semaphore._value < self._concurrency:
             await asyncio.sleep(0.1)


    async def _close(self):
        """Closes NATS connection and cleans up resources."""
        # Close NATS connection (this should ideally stop subscriptions)
        await close_nats_connection() # Use the shared close function
        self._nc = None
        self._js = None


    def signal_handler(self, sig, frame):
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()


    def install_signal_handlers(self):
        """Installs signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
