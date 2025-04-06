# src/naq/queue.py
import asyncio
import time
import datetime
import cloudpickle # Ensure cloudpickle is imported
from typing import Optional, Callable, Any, Tuple, Dict, Union
from datetime import timedelta, timezone

import nats
from nats.js.kv import KeyValue, KeyValueEntry
from nats.js.errors import KeyNotFoundError # Import KeyNotFoundError

from .settings import (
    DEFAULT_QUEUE_NAME, NAQ_PREFIX, JOB_SERIALIZER,
    SCHEDULED_JOBS_KV_NAME, SCHEDULED_JOB_STATUS_ACTIVE,
    SCHEDULED_JOB_STATUS_PAUSED, SCHEDULED_JOB_STATUS_FAILED, # Import statuses
)
from .job import Job, RetryDelayType
from .connection import get_jetstream_context, ensure_stream, close_nats_connection, get_nats_connection
from .exceptions import NaqException, ConnectionError as NaqConnectionError, ConfigurationError, JobNotFoundError # Add JobNotFoundError
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
        # Ensure the KV store is created if it doesn't exist
        nc = await get_nats_connection(url=self._nats_url)
        js = await get_jetstream_context(nc=nc)
        try:
            # Create or bind to the KV store with a TTL (optional, but good practice)
            # TTL here applies to the bucket itself, not individual keys
            kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            logger.debug(f"Connected to KV store '{SCHEDULED_JOBS_KV_NAME}'")
            return kv
        except Exception as e:
            # Attempt to create if not found (basic creation)
            try:
                 logger.info(f"KV store '{SCHEDULED_JOBS_KV_NAME}' not found, attempting creation...")
                 kv = await js.create_key_value(bucket=SCHEDULED_JOBS_KV_NAME, description="Stores naq scheduled job details")
                 logger.info(f"KV store '{SCHEDULED_JOBS_KV_NAME}' created.")
                 return kv
            except Exception as create_e:
                 raise NaqConnectionError(f"Failed to access or create KV store '{SCHEDULED_JOBS_KV_NAME}': {create_e}") from create_e

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
        original_job_payload = job.serialize()

        schedule_data = {
            'job_id': job.job_id,
            'scheduled_timestamp_utc': scheduled_timestamp,
            'queue_name': job.queue_name,
            'cron': cron,
            'interval_seconds': interval_seconds,
            'repeat': repeat,
            '_orig_job_payload': original_job_payload,
            '_serializer': JOB_SERIALIZER,
            'status': SCHEDULED_JOB_STATUS_ACTIVE, # Initial status
            'schedule_failure_count': 0, # Initial failure count
            'last_enqueued_utc': None, # Track last enqueue time
            'next_run_utc': scheduled_timestamp, # Explicitly store next run time
        }

        try:
            serialized_schedule_data = cloudpickle.dumps(schedule_data)
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

    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancels a scheduled job by deleting it from the KV store.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            True if the job was found and deleted, False otherwise.

        Raises:
            NaqConnectionError: If connection to NATS fails.
            NaqException: For other errors during deletion.
        """
        print(f"Attempting to cancel scheduled job '{job_id}'")
        kv = await self._get_scheduled_kv()
        try:
            # Use delete with purge=True to ensure it's fully removed
            await kv.delete(job_id.encode('utf-8'), purge=True)
            print(f"Scheduled job '{job_id}' cancelled successfully.")
            return True
        except KeyNotFoundError:
            print(f"Scheduled job '{job_id}' not found. Cannot cancel.")
            return False
        except Exception as e:
            print(f"Error cancelling scheduled job '{job_id}': {e}")
            raise NaqException(f"Failed to cancel scheduled job: {e}") from e

    async def _update_scheduled_job_status(self, job_id: str, status: str) -> bool:
        """Internal helper to update the status of a scheduled job."""
        kv = await self._get_scheduled_kv()
        try:
            entry = await kv.get(job_id.encode('utf-8'))
            if not entry:
                 raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

            schedule_data = cloudpickle.loads(entry.value)
            if schedule_data.get('status') == status:
                print(f"Scheduled job '{job_id}' already has status '{status}'.")
                return True # No change needed

            schedule_data['status'] = status
            serialized_schedule_data = cloudpickle.dumps(schedule_data)

            # Use update with revision check for optimistic concurrency control
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            print(f"Scheduled job '{job_id}' status updated to '{status}'.")
            return True
        except KeyNotFoundError:
             raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
            # Handle potential revision mismatch (another process updated it)
            if "wrong last sequence" in str(e).lower():
                 print(f"Warning: Concurrent modification detected for job '{job_id}'. Update failed. Please retry.")
                 # Depending on requirements, could retry automatically here
                 return False # Indicate update failed due to concurrency
            else:
                 print(f"NATS API error updating status for job '{job_id}': {e}")
                 raise NaqException(f"Failed to update job status: {e}") from e
        except Exception as e:
            print(f"Error updating status for job '{job_id}': {e}")
            raise NaqException(f"Failed to update job status: {e}") from e

    async def pause_scheduled_job(self, job_id: str) -> bool:
        """Pauses a scheduled job."""
        print(f"Attempting to pause scheduled job '{job_id}'")
        return await self._update_scheduled_job_status(job_id, SCHEDULED_JOB_STATUS_PAUSED)

    async def resume_scheduled_job(self, job_id: str) -> bool:
        """Resumes a paused scheduled job."""
        print(f"Attempting to resume scheduled job '{job_id}'")
        return await self._update_scheduled_job_status(job_id, SCHEDULED_JOB_STATUS_ACTIVE)

    async def modify_scheduled_job(self, job_id: str, **updates: Any) -> bool:
        """
        Modifies parameters of a scheduled job (e.g., cron, interval, repeat).
        Currently supports modifying schedule parameters only.

        Args:
            job_id: The ID of the job to modify.
            **updates: Keyword arguments for parameters to update.
                       Supported: 'cron', 'interval', 'repeat', 'scheduled_timestamp_utc'.

        Returns:
            True if modification was successful.

        Raises:
            JobNotFoundError: If the job doesn't exist.
            ConfigurationError: If invalid update parameters are provided.
            NaqException: For other errors.
        """
        print(f"Attempting to modify scheduled job '{job_id}' with updates: {updates}")
        kv = await self._get_scheduled_kv()
        supported_keys = {'cron', 'interval', 'repeat', 'scheduled_timestamp_utc'}
        update_keys = set(updates.keys())

        if not update_keys.issubset(supported_keys):
            raise ConfigurationError(f"Unsupported modification keys: {update_keys - supported_keys}. Supported: {supported_keys}")

        try:
            entry = await kv.get(job_id.encode('utf-8'))
            if not entry:
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

            schedule_data = cloudpickle.loads(entry.value)

            # Apply updates
            needs_next_run_recalc = False
            if 'cron' in updates:
                schedule_data['cron'] = updates['cron']
                schedule_data['interval_seconds'] = None # Clear interval if cron is set
                needs_next_run_recalc = True
            if 'interval' in updates:
                interval = updates['interval']
                if isinstance(interval, (int, float)):
                    interval = timedelta(seconds=interval)
                if isinstance(interval, timedelta):
                    schedule_data['interval_seconds'] = interval.total_seconds()
                    schedule_data['cron'] = None # Clear cron if interval is set
                    needs_next_run_recalc = True
                else:
                    raise ConfigurationError("'interval' must be timedelta or numeric seconds.")
            if 'repeat' in updates:
                schedule_data['repeat'] = updates['repeat']
            if 'scheduled_timestamp_utc' in updates:
                 # Allow explicitly setting the next run time
                 schedule_data['scheduled_timestamp_utc'] = updates['scheduled_timestamp_utc']
                 schedule_data['next_run_utc'] = updates['scheduled_timestamp_utc']
                 needs_next_run_recalc = False # Explicitly set, no recalc needed now

            # Recalculate next run time if cron/interval changed and not explicitly set
            if needs_next_run_recalc:
                 now_utc = datetime.datetime.now(timezone.utc)
                 next_run_ts: Optional[float] = None
                 if schedule_data['cron']:
                     try:
                         from croniter import croniter
                         cron_iter = croniter(schedule_data['cron'], now_utc)
                         next_run_ts = cron_iter.get_next(datetime.datetime).timestamp()
                     except ImportError:
                         raise ImportError("Please install 'croniter' to use cron scheduling.")
                     except Exception as e:
                         raise ConfigurationError(f"Invalid cron format '{schedule_data['cron']}': {e}")
                 elif schedule_data['interval_seconds']:
                     # Base next run on the *original* scheduled time or last run if available?
                     # Let's base it on 'now' for simplicity when modifying.
                     next_run_ts = (now_utc + timedelta(seconds=schedule_data['interval_seconds'])).timestamp()

                 if next_run_ts is not None:
                     schedule_data['scheduled_timestamp_utc'] = next_run_ts
                     schedule_data['next_run_utc'] = next_run_ts
                 else:
                      # This case might occur if a one-off job's time is modified without providing a new time
                      print(f"Warning: Could not determine next run time for job '{job_id}' after modification. Check parameters.")


            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            print(f"Scheduled job '{job_id}' modified successfully.")
            return True

        except KeyNotFoundError:
             raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
            if "wrong last sequence" in str(e).lower():
                 print(f"Warning: Concurrent modification detected for job '{job_id}'. Update failed. Please retry.")
                 return False
            else:
                 print(f"NATS API error modifying job '{job_id}': {e}")
                 raise NaqException(f"Failed to modify job: {e}") from e
        except Exception as e:
            print(f"Error modifying job '{job_id}': {e}")
            raise NaqException(f"Failed to modify job: {e}") from e

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

async def cancel_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to cancel a scheduled job (async)."""
    # Need a queue instance mainly to get connection/KV config easily
    q = Queue(nats_url=nats_url) # Queue name doesn't matter here
    return await q.cancel_scheduled_job(job_id)

async def pause_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to pause a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.pause_scheduled_job(job_id)

async def resume_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to resume a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.resume_scheduled_job(job_id)

async def modify_scheduled_job(job_id: str, nats_url: Optional[str] = None, **updates: Any) -> bool:
    """Helper to modify a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.modify_scheduled_job(job_id, **updates)


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

def cancel_scheduled_job_sync(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to cancel a scheduled job (sync)."""
    async def _main():
        res = await cancel_scheduled_job(job_id, nats_url=nats_url)
        await close_nats_connection()
        return res
    return run_async_from_sync(_main())

def pause_scheduled_job_sync(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to pause a scheduled job (sync)."""
    async def _main():
        res = await pause_scheduled_job(job_id, nats_url=nats_url)
        await close_nats_connection()
        return res
    return run_async_from_sync(_main())

def resume_scheduled_job_sync(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to resume a scheduled job (sync)."""
    async def _main():
        res = await resume_scheduled_job(job_id, nats_url=nats_url)
        await close_nats_connection()
        return res
    return run_async_from_sync(_main())

def modify_scheduled_job_sync(job_id: str, nats_url: Optional[str] = None, **updates: Any) -> bool:
    """Helper to modify a scheduled job (sync)."""
    async def _main():
        res = await modify_scheduled_job(job_id, nats_url=nats_url, **updates)
        await close_nats_connection()
        return res
    return run_async_from_sync(_main())
