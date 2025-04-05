# src/naq/queue.py
import asyncio
from typing import Optional, Callable, Any, Tuple, Dict

from .settings import DEFAULT_QUEUE_NAME, NAQ_PREFIX
from .job import Job
from .connection import get_jetstream_context, ensure_stream
from .exceptions import NaqException

class Queue:
    """Represents a job queue backed by a NATS JetStream stream."""

    def __init__(
        self,
        name: str = DEFAULT_QUEUE_NAME,
        nats_url: Optional[str] = None, # Allow overriding NATS URL per queue
        default_timeout: Optional[int] = None, # Placeholder for job timeout
    ):
        self.name = name
        # Use a NATS subject derived from the queue name
        # Example: Queue 'high' -> subject 'naq.queue.high'
        self.subject = f"{NAQ_PREFIX}.queue.{self.name}"
        # The stream name could be shared or queue-specific
        # Using a single stream 'naq_jobs' for all queues for simplicity now
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        self._nats_url = nats_url # Store potential override
        self._js: Optional[nats.js.JetStreamContext] = None
        self._default_timeout = default_timeout

    async def _get_js(self) -> nats.js.JetStreamContext:
        """Gets the JetStream context, initializing if needed."""
        if self._js is None:
            self._js = await get_jetstream_context(url=self._nats_url) # Pass URL if overridden
            # Ensure the stream exists when the queue is first used
            # The subject this queue publishes to must be bound to the stream
            await ensure_stream(js=self._js, stream_name=self.stream_name, subjects=[f"{NAQ_PREFIX}.queue.*"])
        return self._js

    async def enqueue(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Job:
        """
        Creates a job from a function call and enqueues it.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            The enqueued Job instance.
        """
        job = Job(function=func, args=args, kwargs=kwargs)
        print(f"Enqueueing job {job.job_id} ({func.__name__}) to queue '{self.name}' (subject: {self.subject})")

        try:
            js = await self._get_js()
            serialized_job = job.serialize()

            # Publish the job to the specific subject for this queue
            # JetStream will capture this message in the configured stream
            ack = await js.publish(
                subject=self.subject,
                payload=serialized_job,
                # timeout=... # Optional publish timeout
            )
            print(f"Job {job.job_id} published successfully. Stream: {ack.stream}, Seq: {ack.seq}")
            return job
        except Exception as e:
            # Log or handle the error appropriately
            print(f"Error enqueueing job {job.job_id}: {e}")
            raise NaqException(f"Failed to enqueue job: {e}") from e

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"

# Helper function for simple enqueue cases
async def enqueue(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    **kwargs: Any,
) -> Job:
    """Helper to enqueue a job onto a specific queue."""
    q = Queue(name=queue_name)
    return await q.enqueue(func, *args, **kwargs)
