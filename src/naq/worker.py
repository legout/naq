# src/naq/worker.py
import asyncio
import signal
import logging
from typing import Optional, List, Sequence

import nats
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.aio.msg import Msg

from .settings import DEFAULT_QUEUE_NAME, NAQ_PREFIX
from .job import Job
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .exceptions import NaqException, JobExecutionError, SerializationError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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

    async def _subscribe_to_queue(self, queue_name: str):
        """Creates a durable consumer and starts fetching messages for a queue."""
        if not self._js:
            raise NaqException("JetStream context not available.")

        subject = f"{NAQ_PREFIX}.queue.{queue_name}"
        # Create a durable consumer name specific to the queue and worker group/type
        # This allows multiple worker instances to share the load for a queue.
        durable_name = f"{self.consumer_prefix}-{queue_name}"

        logger.info(f"Setting up consumer for queue '{queue_name}' (subject: {subject}, durable: {durable_name})")

        try:
            # Create a pull consumer (durable)
            # If multiple workers use the same durable name, they form a consumer group.
            psub = await self._js.pull_subscribe(
                subject=subject,
                durable=durable_name,
                config=ConsumerConfig(
                    ack_policy=nats.js.api.AckPolicy.EXPLICIT, # Must ack/nak messages
                    ack_wait=30, # 30 seconds to ack before redelivery
                    max_ack_pending=self._concurrency * 2, # Allow fetching more than concurrency limit
                    # deliver_policy=DeliverPolicy.ALL # Start from beginning (or NEW)
                )
            )
            logger.info(f"Pull consumer '{durable_name}' created for subject '{subject}'.")

            # Start a task to continuously fetch and process messages
            while self._running:
                try:
                    # Fetch a batch of messages, non-blocking after first fetch
                    msgs = await psub.fetch(batch=self._concurrency, timeout=10) # Timeout helps loop check self._running
                    logger.debug(f"Fetched {len(msgs)} messages from '{durable_name}'")
                    for msg in msgs:
                        # Don't wait for process_message to finish here,
                        # schedule it to run concurrently up to the limit.
                        # Need a mechanism to limit concurrency properly.
                        # Using asyncio.Semaphore is a good way.
                        # For now, simple task creation (might exceed concurrency)
                        task = asyncio.create_task(self.process_message(msg))
                        self._tasks.append(task)
                        # Optional: Clean up completed tasks periodically
                        self._tasks = [t for t in self._tasks if not t.done()]

                except nats.errors.TimeoutError:
                    # No messages available, continue loop
                    await asyncio.sleep(0.1) # Small sleep to prevent busy-waiting
                    continue
                except nats.js.errors.ConsumerNotFoundError:
                     logger.warning(f"Consumer '{durable_name}' not found. It might have been deleted. Stopping fetch loop.")
                     break # Exit loop if consumer is gone
                except Exception as e:
                    logger.error(f"Error fetching from consumer '{durable_name}': {e}", exc_info=True)
                    await asyncio.sleep(1) # Wait before retrying fetch

        except Exception as e:
            logger.error(f"Failed to subscribe or run fetch loop for queue '{queue_name}': {e}", exc_info=True)


    async def process_message(self, msg: Msg):
        """Deserializes and executes a job from a NATS message."""
        job: Optional[Job] = None
        try:
            logger.info(f"Received message: Subject='{msg.subject}', Sid='{msg.sid}', Seq={msg.metadata.sequence.stream}")
            job = Job.deserialize(msg.data)
            logger.info(f"Processing job {job.job_id} ({getattr(job.function, '__name__', 'unknown')})")

            # Execute the job function
            await asyncio.to_thread(job.execute) # Run synchronous function in thread pool

            if job.error:
                logger.error(f"Job {job.job_id} failed: {job.error}")
                # Decide on ack policy for failures:
                # - ack() -> remove from queue
                # - nak() -> redeliver immediately (dangerous for persistent errors)
                # - nak(delay=...) -> redeliver after delay
                # - term() -> mark as terminally failed (requires NATS 2.10+)
                # For now, log error and ack to prevent infinite loops on bad jobs.
                # RQ has retry mechanisms, which would require more state/logic here.
                await msg.ack()
                logger.warning(f"Acknowledged failed job {job.job_id} to prevent redelivery loop.")
            else:
                logger.info(f"Job {job.job_id} completed successfully. Result: {job.result}")
                await msg.ack() # Acknowledge successful processing
                logger.debug(f"Message acknowledged: Sid='{msg.sid}'")

        except SerializationError as e:
            logger.error(f"Failed to deserialize job data: {e}. Acknowledging to prevent redelivery.", exc_info=True)
            await msg.ack() # Ack poison pill messages
        except Exception as e:
            logger.error(f"Unhandled error processing message (Sid='{msg.sid}', JobId='{job.job_id if job else 'N/A'}'): {e}", exc_info=True)
            # Nak to signal processing failure, potentially with delay
            try:
                await msg.nak(delay=10) # Redeliver after 10 seconds
                logger.warning(f"Nak'd message Sid='{msg.sid}' due to processing error.")
            except Exception as nak_e:
                logger.error(f"Failed to NAK message Sid='{msg.sid}': {nak_e}", exc_info=True)
        finally:
            # Clean up task reference if needed (depends on how tasks are managed)
            pass


    async def run(self):
        """Starts the worker, connects to NATS, and begins processing jobs."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()

            # Ensure the main stream exists before starting consumers
            await ensure_stream(js=self._js, stream_name=self.stream_name, subjects=[f"{NAQ_PREFIX}.queue.*"])

            # Start subscription tasks for each queue
            subscription_tasks = [
                asyncio.create_task(self._subscribe_to_queue(q_name))
                for q_name in self.queue_names
            ]

            logger.info(f"Worker '{self._worker_name}' started. Listening on queues: {self.queue_names}. Concurrency: {self._concurrency}")

            # Keep running until shutdown signal
            await self._shutdown_event.wait()

            # Wait for subscription tasks and processing tasks to finish
            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            # Give tasks some time to finish gracefully
            # Cancelling pull consumers directly isn't straightforward, stopping the fetch loop is better.
            # Wait for active processing tasks
            if self._tasks:
                await asyncio.wait(self._tasks, timeout=10) # Wait max 10s for jobs to finish

            # Wait for subscription loops to exit
            if subscription_tasks:
                 await asyncio.wait(subscription_tasks, timeout=5)


        except Exception as e:
            logger.error(f"Worker run loop encountered an error: {e}", exc_info=True)
        finally:
            logger.info("Worker shutting down...")
            await self._close()
            logger.info("Worker shutdown complete.")


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
