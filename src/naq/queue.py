# src/naq/queue.py
import asyncio
import time
import datetime
from typing import Optional, Callable, Any, Tuple, Dict, Union
from datetime import timedelta, timezone

import nats
from nats.js.kv import KeyValue

from .settings import DEFAULT_QUEUE_NAME, NAQ_PREFIX, JOB_SERIALIZER
from .job import Job, RetryDelayType
from .connection import get_jetstream_context, ensure_stream, close_nats_connection, get_nats_connection
from .exceptions import NaqException, ConnectionError as NaqConnectionError, ConfigurationError
from .utils import run_async_from_sync

# Define KV bucket name for scheduled jobs
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"

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

    async def _get_scheduled_kv(self) -> KeyValue:
        """Gets the KeyValue store for scheduled jobs."""
        nc = await get_nats_connection(url=self._nats_url) # Need plain NATS connection for KV
        js = await get_jetstream_context(nc=nc) # Get JS from the same connection
        try:
            # Create or bind to the KV store
            kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            return kv
        except Exception as e:
            raise NaqConnectionError(f"Failed to access KV store '{SCHEDULED_JOBS_KV_NAME}': {e}") from e

    async def enqueue(
        self,
        func: Callable,
        *args: Any,
        # Add retry parameters
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        **kwargs: Any,
    ) -> Job:
        """
        Creates a job from a function call and enqueues it.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            max_retries: Maximum number of retries allowed.
            retry_delay: Delay between retries (seconds).
                         Can be a single number or a sequence for backoff.
            **kwargs: Keyword arguments for the function.

        Returns:
            The enqueued Job instance.
        """
        # Pass retry params and queue name to Job constructor
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name, # Pass queue name
        )
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

    async def enqueue_at(
        self,
        dt: datetime.datetime,
        func: Callable,
        *args: Any,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        **kwargs: Any,
    ) -> Job:
        """
        Schedules a job to be enqueued at a specific datetime.

        Args:
            dt: The datetime (timezone-aware recommended) when the job should be enqueued.
            func: The function to execute.
            *args: Positional arguments for the function.
            max_retries: Maximum number of retries allowed for the job execution.
            retry_delay: Delay between retries (seconds).
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance (note: job_id is assigned now, but execution is deferred).
        """
        if dt.tzinfo is None:
            # If datetime is naive, assume local timezone, but warn user.
            # It's generally better to use timezone-aware datetimes.
            # Convert to UTC for internal storage.
            # print("Warning: Enqueuing with naive datetime. Assuming local timezone.") # Consider logging
            scheduled_timestamp = dt.astimezone(timezone.utc).timestamp()
        else:
            # Convert timezone-aware datetime to UTC timestamp
            scheduled_timestamp = dt.astimezone(timezone.utc).timestamp()

        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name,
        )

        await self._schedule_job(job, scheduled_timestamp)
        print(f"Scheduled job {job.job_id} ({func.__name__}) to run at {dt} on queue '{self.name}'")
        return job

    async def enqueue_in(
        self,
        delta: timedelta,
        func: Callable,
        *args: Any,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        **kwargs: Any,
    ) -> Job:
        """
        Schedules a job to be enqueued after a specific time delta.

        Args:
            delta: The timedelta after which the job should be enqueued.
            func: The function to execute.
            *args: Positional arguments for the function.
            max_retries: Maximum number of retries allowed for the job execution.
            retry_delay: Delay between retries (seconds).
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
        """
        now_utc = datetime.datetime.now(timezone.utc)
        scheduled_time_utc = now_utc + delta
        return await self.enqueue_at(scheduled_time_utc, func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs)

    async def schedule(
        self,
        func: Callable,
        *args: Any,
        cron: Optional[str] = None,
        interval: Optional[Union[timedelta, float, int]] = None,
        repeat: Optional[int] = None, # None for infinite, 0/1 for once (use enqueue_*), >1 for limited repeats
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        **kwargs: Any,
    ) -> Job:
        """
        Schedules a job to run repeatedly based on cron or interval.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            cron: A cron string (e.g., '*/5 * * * *') defining the schedule.
            interval: A timedelta or seconds defining the interval between runs.
            repeat: Number of times to repeat (None for indefinitely).
            max_retries: Max retries for each job execution.
            retry_delay: Delay between execution retries.
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance (representing the first scheduled run).
        """
        if not cron and not interval:
            raise ConfigurationError("Either 'cron' or 'interval' must be provided for schedule()")
        if cron and interval:
            raise ConfigurationError("Provide either 'cron' or 'interval', not both.")

        # Need croniter if cron is used
        if cron:
            try:
                from croniter import croniter
            except ImportError:
                raise ImportError("Please install 'croniter' to use cron scheduling: pip install croniter") from None

        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name,
        )

        now_utc = datetime.datetime.now(timezone.utc)
        first_run_ts: float

        if cron:
            # Calculate the first run time based on the cron expression
            cron_iter = croniter(cron, now_utc)
            first_run_ts = cron_iter.get_next(datetime.datetime).timestamp()
        elif interval:
            if isinstance(interval, (int, float)):
                interval = timedelta(seconds=interval)
            # First run is one interval from now
            first_run_ts = (now_utc + interval).timestamp()
        else:
             # Should not happen due to initial check, but satisfy type checker
             raise ConfigurationError("Invalid schedule configuration.")


        await self._schedule_job(
            job,
            scheduled_timestamp=first_run_ts,
            cron=cron,
            interval_seconds=interval.total_seconds() if interval else None,
            repeat=repeat
        )
        print(f"Scheduled recurring job {job.job_id} ({func.__name__}) starting at {datetime.datetime.fromtimestamp(first_run_ts, timezone.utc)} on queue '{self.name}'")
        return job


    async def _schedule_job(
        self,
        job: Job,
        scheduled_timestamp: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ):
        """Internal helper to store job details in the KV store."""
        kv = await self._get_scheduled_kv()
        # Serialize the original job payload separately
        original_job_payload = job.serialize()

        # Store metadata alongside the original payload
        schedule_data = {
            'job_id': job.job_id,
            'scheduled_timestamp_utc': scheduled_timestamp,
            'queue_name': job.queue_name,
            'cron': cron,
            'interval_seconds': interval_seconds,
            'repeat': repeat,
            '_orig_job_payload': original_job_payload, # Store the bytes
            '_serializer': JOB_SERIALIZER, # Store serializer used
        }

        try:
            # Use cloudpickle to serialize the schedule_data dictionary
            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            # Store using job_id as the key
            await kv.put(job.job_id.encode('utf-8'), serialized_schedule_data)
        except Exception as e:
            raise NaqException(f"Failed to store scheduled job {job.job_id} in KV store: {e}") from e


    async def purge(self) -> int:
        """
        Removes all jobs from this queue by purging messages
        with the queue's subject from the underlying JetStream stream.

        Returns:
            The number of purged messages.

        Raises:
            NaqConnectionError: If connection to NATS fails.
            NaqException: For other errors during purging.
        """
        print(f"Purging queue '{self.name}' (subject: {self.subject} in stream: {self.stream_name})")
        try:
            js = await self._get_js()
            # Ensure the stream exists first (purge fails if stream doesn't exist)
            await ensure_stream(js=js, stream_name=self.stream_name, subjects=[f"{NAQ_PREFIX}.queue.*"])

            # Purge messages specifically matching this queue's subject
            resp = await js.purge_stream(
                name=self.stream_name,
                filter=self.subject # Only purge messages for this specific queue's subject
            )
            print(f"Purge successful for queue '{self.name}'. Purged {resp.purged} messages.")
            return resp.purged
        except nats.js.errors.StreamNotFoundError:
             print(f"Stream '{self.stream_name}' not found. Nothing to purge for queue '{self.name}'.")
             return 0 # Stream doesn't exist, so 0 messages purged
        except nats.errors.NatsError as e:
             print(f"NATS error purging queue '{self.name}': {e}")
             raise NaqConnectionError(f"NATS error during purge: {e}") from e
        except Exception as e:
            print(f"Error purging queue '{self.name}': {e}")
            raise NaqException(f"Failed to purge queue: {e}") from e

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"

# --- Async Helper Functions ---

async def enqueue(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None, # Allow passing nats_url
    max_retries: Optional[int] = 0, # Add retry params
    retry_delay: RetryDelayType = 0, # Add retry params
    **kwargs: Any,
) -> Job:
    """Helper to enqueue a job onto a specific queue (async)."""
    # Pass nats_url if provided
    q = Queue(name=queue_name, nats_url=nats_url)
    # Pass retry params to queue's enqueue method
    job = await q.enqueue(func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs)
    # Decide if the helper should close the connection. Generally no.
    # await close_nats_connection() # Avoid closing here, let user manage connection
    return job

async def enqueue_at(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job for a specific time (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    return await q.enqueue_at(dt, func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs)

async def enqueue_in(
    delta: timedelta,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job after a delay (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    return await q.enqueue_in(delta, func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs)

async def schedule(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    cron: Optional[str] = None,
    interval: Optional[Union[timedelta, float, int]] = None,
    repeat: Optional[int] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a recurring job (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    return await q.schedule(
        func, *args, cron=cron, interval=interval, repeat=repeat,
        max_retries=max_retries, retry_delay=retry_delay, **kwargs
    )

async def purge_queue(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
) -> int:
    """Helper to purge jobs from a specific queue (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    purged_count = await q.purge()
    # await close_nats_connection() # Avoid closing here
    return purged_count


# --- Sync Helper Functions ---

def enqueue_sync(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0, # Add retry params
    retry_delay: RetryDelayType = 0, # Add retry params
    **kwargs: Any,
) -> Job:
    """
    Helper to enqueue a job onto a specific queue (synchronous).

    Note: This will block until the enqueue operation is complete.
    It manages its own NATS connection lifecycle for the operation.
    """
    async def _main():
        # Pass retry params to async enqueue helper
        job = await enqueue(
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs
        )
        await close_nats_connection() # Close connection after sync op
        return job
    return run_async_from_sync(_main())

def enqueue_at_sync(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job for a specific time (sync)."""
    async def _main():
        job = await enqueue_at(
            dt, func, *args, queue_name=queue_name, nats_url=nats_url,
            max_retries=max_retries, retry_delay=retry_delay, **kwargs
        )
        await close_nats_connection()
        return job
    return run_async_from_sync(_main())

def enqueue_in_sync(
    delta: timedelta,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job after a delay (sync)."""
    async def _main():
        job = await enqueue_in(
            delta, func, *args, queue_name=queue_name, nats_url=nats_url,
            max_retries=max_retries, retry_delay=retry_delay, **kwargs
        )
        await close_nats_connection()
        return job
    return run_async_from_sync(_main())

def schedule_sync(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    cron: Optional[str] = None,
    interval: Optional[Union[timedelta, float, int]] = None,
    repeat: Optional[int] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a recurring job (sync)."""
    async def _main():
        job = await schedule(
            func, *args, queue_name=queue_name, nats_url=nats_url,
            cron=cron, interval=interval, repeat=repeat,
            max_retries=max_retries, retry_delay=retry_delay, **kwargs
        )
        await close_nats_connection()
        return job
    return run_async_from_sync(_main())

def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
) -> int:
    """
    Helper to purge jobs from a specific queue (synchronous).

    Note: This will block until the purge operation is complete.
    It manages its own NATS connection lifecycle for the operation.
    """
    async def _main():
        count = await purge_queue(queue_name=queue_name, nats_url=nats_url)
        await close_nats_connection() # Close connection after sync op
        return count
    return run_async_from_sync(_main())
