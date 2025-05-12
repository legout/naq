# src/naq/queue.py
import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import cloudpickle
import nats
from loguru import logger
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from .connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from .exceptions import ConfigurationError
from .exceptions import ConnectionError as NaqConnectionError
from .exceptions import JobNotFoundError, NaqException
from .job import Job, RetryDelayType
from .settings import (
    DEFAULT_QUEUE_NAME,
    JOB_SERIALIZER,
    NAQ_PREFIX,
    SCHEDULED_JOB_STATUS_ACTIVE,
    SCHEDULED_JOB_STATUS_PAUSED,
    SCHEDULED_JOBS_KV_NAME,
)
from .utils import run_async_from_sync, setup_logging


class ScheduledJobManager:
    """
    Manager for scheduled jobs within a Queue.
    Handles storing, retrieving, and managing scheduled jobs in the NATS KV store.
    """

    def __init__(self, queue_name: str, nats_url: Optional[str] = None):
        self.queue_name = queue_name
        self._nats_url = nats_url
        self._kv: Optional[KeyValue] = None

    async def get_kv(self) -> KeyValue:
        """Gets the KeyValue store for scheduled jobs, creating it if needed."""
        if self._kv is not None:
            return self._kv

        # Ensure the KV store is created if it doesn't exist
        nc = await get_nats_connection(url=self._nats_url)
        js = await get_jetstream_context(nc=nc)
        try:
            # Try to connect to existing KV store
            self._kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            logger.debug(f"Connected to KV store '{SCHEDULED_JOBS_KV_NAME}'")
            return self._kv
        except Exception:
            # Attempt to create if not found
            try:
                logger.info(
                    f"KV store '{SCHEDULED_JOBS_KV_NAME}' not found, creating..."
                )
                self._kv = await js.create_key_value(
                    bucket=SCHEDULED_JOBS_KV_NAME,
                    description="Stores naq scheduled job details",
                )
                logger.info(f"KV store '{SCHEDULED_JOBS_KV_NAME}' created.")
                return self._kv
            except Exception as create_e:
                raise NaqConnectionError(
                    f"Failed to access or create KV store '{SCHEDULED_JOBS_KV_NAME}': {create_e}"
                ) from create_e

    async def store_job(
        self,
        job: Job,
        scheduled_timestamp: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ) -> None:
        """
        Stores a job in the scheduled jobs KV store.
        
        Args:
            job: The job to schedule
            scheduled_timestamp: When the job should run (UTC timestamp)
            cron: Optional cron expression for recurring jobs
            interval_seconds: Optional interval in seconds for recurring jobs
            repeat: Optional number of times to repeat (None=infinite)
        
        Raises:
            NaqException: If storing the job fails
        """
        kv = await self.get_kv()
        original_job_payload = job.serialize()

        schedule_data = {
            "job_id": job.job_id,
            "scheduled_timestamp_utc": scheduled_timestamp,
            "queue_name": job.queue_name,
            "cron": cron,
            "interval_seconds": interval_seconds,
            "repeat": repeat,
            "_orig_job_payload": original_job_payload,
            "_serializer": JOB_SERIALIZER,
            "status": SCHEDULED_JOB_STATUS_ACTIVE,  # Initial status
            "schedule_failure_count": 0,  # Initial failure count
            "last_enqueued_utc": None,  # Track last enqueue time
            "next_run_utc": scheduled_timestamp,  # Explicitly store next run time
        }

        try:
            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.put(job.job_id.encode("utf-8"), serialized_schedule_data)
        except Exception as e:
            raise NaqException(
                f"Failed to store scheduled job {job.job_id} in KV store: {e}"
            ) from e

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancels a scheduled job by deleting it from the KV store.
        
        Args:
            job_id: ID of the job to cancel
            
        Returns:
            True if job was found and canceled, False if not found
            
        Raises:
            NaqException: For errors other than job not found
        """
        logger.info(f"Attempting to cancel scheduled job '{job_id}'")
        kv = await self.get_kv()
        try:
            # Use delete with purge=True to ensure it's fully removed
            await kv.delete(job_id.encode("utf-8"), purge=True)
            logger.info(f"Scheduled job '{job_id}' cancelled successfully.")
            return True
        except KeyNotFoundError:
            logger.warning(f"Scheduled job '{job_id}' not found. Cannot cancel.")
            return False
        except Exception as e:
            logger.error(f"Error cancelling scheduled job '{job_id}': {e}")
            raise NaqException(f"Failed to cancel scheduled job: {e}") from e

    async def update_job_status(self, job_id: str, status: str) -> bool:
        """
        Updates the status of a scheduled job.
        
        Args:
            job_id: ID of the job to update
            status: New status (ACTIVE, PAUSED, etc.)
            
        Returns:
            True if update was successful, False on concurrency conflict
            
        Raises:
            JobNotFoundError: If job doesn't exist
            NaqException: For other errors
        """
        kv = await self.get_kv()
        try:
            entry = await kv.get(job_id.encode("utf-8"))
            if not entry:
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

            schedule_data = cloudpickle.loads(entry.value)
            if schedule_data.get("status") == status:
                logger.info(f"Scheduled job '{job_id}' already has status '{status}'.")
                return True  # No change needed

            schedule_data["status"] = status
            serialized_schedule_data = cloudpickle.dumps(schedule_data)

            # Use update with revision check for optimistic concurrency control
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            logger.info(f"Scheduled job '{job_id}' status updated to '{status}'.")
            return True
        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
            # Handle potential revision mismatch (another process updated it)
            if "wrong last sequence" in str(e).lower():
                logger.warning(
                    f"Concurrent modification detected for job '{job_id}'. Update failed. Please retry."
                )
                return False  # Indicate update failed due to concurrency
            else:
                logger.error(f"NATS API error updating status for job '{job_id}': {e}")
                raise NaqException(f"Failed to update job status: {e}") from e
        except Exception as e:
            logger.error(f"Error updating status for job '{job_id}': {e}")
            raise NaqException(f"Failed to update job status: {e}") from e

    async def modify_job(self, job_id: str, **updates: Any) -> bool:
        """
        Modifies parameters of a scheduled job.
        
        Args:
            job_id: ID of the job to modify
            **updates: Parameters to update (cron, interval, repeat, etc.)
            
        Returns:
            True if modification was successful, False on concurrency conflict
            
        Raises:
            JobNotFoundError: If job doesn't exist
            ConfigurationError: If invalid parameters are provided
            NaqException: For other errors
        """
        logger.info(f"Attempting to modify scheduled job '{job_id}' with updates: {updates}")
        kv = await self.get_kv()
        supported_keys = {"cron", "interval", "repeat", "scheduled_timestamp_utc"}
        update_keys = set(updates.keys())

        if not update_keys.issubset(supported_keys):
            raise ConfigurationError(
                f"Unsupported modification keys: {update_keys - supported_keys}. Supported: {supported_keys}"
            )

        try:
            entry = await kv.get(job_id.encode("utf-8"))
            if not entry:
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

            schedule_data = cloudpickle.loads(entry.value)

            # Apply updates
            needs_next_run_recalc = False
            if "cron" in updates:
                schedule_data["cron"] = updates["cron"]
                schedule_data["interval_seconds"] = None  # Clear interval if cron is set
                needs_next_run_recalc = True
                
            if "interval" in updates:
                interval = updates["interval"]
                if isinstance(interval, (int, float)):
                    interval = timedelta(seconds=interval)
                if isinstance(interval, timedelta):
                    schedule_data["interval_seconds"] = interval.total_seconds()
                    schedule_data["cron"] = None  # Clear cron if interval is set
                    needs_next_run_recalc = True
                else:
                    raise ConfigurationError(
                        "'interval' must be timedelta or numeric seconds."
                    )
                    
            if "repeat" in updates:
                schedule_data["repeat"] = updates["repeat"]
                
            if "scheduled_timestamp_utc" in updates:
                # Allow explicitly setting the next run time
                schedule_data["scheduled_timestamp_utc"] = updates["scheduled_timestamp_utc"]
                schedule_data["next_run_utc"] = updates["scheduled_timestamp_utc"]
                needs_next_run_recalc = False  # Explicitly set, no recalc needed now

            # Recalculate next run time if cron/interval changed and not explicitly set
            if needs_next_run_recalc:
                next_run_ts = self._calculate_next_run_time(schedule_data)
                
                if next_run_ts is not None:
                    schedule_data["scheduled_timestamp_utc"] = next_run_ts
                    schedule_data["next_run_utc"] = next_run_ts
                else:
                    # This case might occur if a one-off job's time is modified without providing a new time
                    logger.warning(
                        f"Could not determine next run time for job '{job_id}' after modification. Check parameters."
                    )

            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            logger.info(f"Scheduled job '{job_id}' modified successfully.")
            return True

        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
            if "wrong last sequence" in str(e).lower():
                logger.warning(
                    f"Concurrent modification detected for job '{job_id}'. Update failed. Please retry."
                )
                return False
            else:
                logger.error(f"NATS API error modifying job '{job_id}': {e}")
                raise NaqException(f"Failed to modify job: {e}") from e
        except Exception as e:
            logger.error(f"Error modifying job '{job_id}': {e}")
            raise NaqException(f"Failed to modify job: {e}") from e

    def _calculate_next_run_time(self, schedule_data: Dict[str, Any]) -> Optional[float]:
        """
        Calculates the next run time based on cron or interval.
        
        Args:
            schedule_data: The scheduled job data
            
        Returns:
            Next run timestamp or None if it couldn't be calculated
        """
        now_utc = datetime.datetime.now(timezone.utc)
        
        if schedule_data.get("cron"):
            try:
                from croniter import croniter
                cron_iter = croniter(schedule_data["cron"], now_utc)
                return cron_iter.get_next(datetime.datetime).timestamp()
            except ImportError:
                raise ImportError("Please install 'croniter' to use cron scheduling.")
            except Exception as e:
                raise ConfigurationError(f"Invalid cron format '{schedule_data['cron']}': {e}")
                
        elif schedule_data.get("interval_seconds"):
            # Base next run on 'now' for simplicity when modifying
            return (now_utc + timedelta(seconds=schedule_data["interval_seconds"])).timestamp()
            
        return None


class Queue:
    """Represents a job queue backed by a NATS JetStream stream."""

    # Add regex for valid queue names (alphanumeric, underscore, hyphen)
    _VALID_QUEUE_NAME = re.compile(r'^[a-zA-Z0-9_.-]+$')

    def __init__(
        self,
        name: str = DEFAULT_QUEUE_NAME,
        nats_url: Optional[str] = None,
        default_timeout: Optional[int] = None,
    ):
        """
        Initialize a Queue instance.
        
        Args:
            name: The name of the queue. Must be non-empty and contain only
                alphanumeric characters, underscores, or hyphens.
            nats_url: Optional NATS server URL override
            default_timeout: Optional default job timeout in seconds
            
        Raises:
            ValueError: If queue name is empty or contains invalid characters
        """
        if not name:
            raise ValueError("Queue name cannot be empty")
        if not self._VALID_QUEUE_NAME.match(name):
            raise ValueError(
                f"Queue name '{name}' contains invalid characters. "
                "Only alphanumeric, underscore, hyphen, and dot are allowed."
            )
            
        self.name = name
        self.subject = f"{NAQ_PREFIX}.queue.{self.name}"
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        self._nats_url = nats_url
        self._js: Optional[nats.js.JetStreamContext] = None
        self._default_timeout = default_timeout
        self._scheduled_job_manager = ScheduledJobManager(name, nats_url)
        
        setup_logging()  # Ensure logging is set up

    async def _get_js(self) -> nats.js.JetStreamContext:
        """Gets the JetStream context, initializing if needed."""
        if self._js is None:
            # First, get the NATS connection
            nc = await get_nats_connection(url=self._nats_url)
            # Then, get the JetStream context using the connection
            self._js = await get_jetstream_context(nc=nc)
            # Ensure the stream exists when the queue is first used
            await ensure_stream(
                js=self._js,
                stream_name=self.stream_name,
                subjects=[f"{NAQ_PREFIX}.queue.*"],
            )
        return self._js

    async def enqueue(
        self,
        func: Callable,
        *args: Any,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
        **kwargs: Any,
    ) -> Job:
        """
        Creates a job from a function call and enqueues it.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            max_retries: Maximum number of retries allowed. Must be non-negative.
            retry_delay: Delay between retries (seconds). Must be non-negative.
            depends_on: A job ID, Job instance, or list of IDs/instances this job depends on.
            **kwargs: Keyword arguments for the function.

        Returns:
            The enqueued Job instance.
            
        Raises:
            ValueError: If max_retries or retry_delay is negative
            NaqException: If enqueuing fails
        """
        # Validate retry parameters
        if max_retries is not None and max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if not isinstance(retry_delay, ( int, float, list, tuple)):
            raise TypeError("retry_delay must be a number (int or float or list of them)")
        #if retry_delay < 0:
        #    raise ValueError("retry_delay cannot be negative")
        # Create the job object
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name,
            depends_on=depends_on,
            retry_strategy=kwargs.get("retry_strategy", "linear"),
            retry_on=kwargs.get("retry_on"),
            ignore_on=kwargs.get("ignore_on"),
            result_ttl=kwargs.get("result_ttl"),
        )
        
        logger.info(
            f"Enqueueing job {job.job_id} ({func.__name__}) to queue '{self.name}' (subject: {self.subject})"
        )
        if job.dependency_ids:
            logger.info(f"Job {job.job_id} depends on: {job.dependency_ids}")

        try:
            js = await self._get_js()
            serialized_job = job.serialize()

            # Publish the job to the specific subject for this queue
            ack = await js.publish(
                subject=self.subject,
                payload=serialized_job,
            )
            logger.info(
                f"Job {job.job_id} published successfully. Stream: {ack.stream}, Seq: {ack.seq}"
            )
            return job
        except Exception as e:
            logger.error(f"Error enqueueing job {job.job_id}: {e}", exc_info=True)
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
            dt: The datetime when the job should be enqueued.
            func: The function to execute.
            *args: Positional arguments for the function.
            max_retries: Maximum number of retries allowed.
            retry_delay: Delay between retries (seconds).
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
            
        Raises:
            NaqException: If scheduling fails
        """
        # Convert datetime to UTC timestamp
        if dt.tzinfo is None:
            # If datetime is naive, assume local timezone
            scheduled_timestamp = dt.astimezone(timezone.utc).timestamp()
        else:
            # Convert timezone-aware datetime to UTC timestamp
            scheduled_timestamp = dt.astimezone(timezone.utc).timestamp()

        # Create the job
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name,
        )

        # Store in scheduled jobs KV
        await self._scheduled_job_manager.store_job(job, scheduled_timestamp)
        
        logger.info(
            f"Scheduled job {job.job_id} ({func.__name__}) to run at {dt} on queue '{self.name}'"
        )
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
            max_retries: Maximum number of retries allowed.
            retry_delay: Delay between retries (seconds).
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
            
        Raises:
            NaqException: If scheduling fails
        """
        now_utc = datetime.datetime.now(timezone.utc)
        scheduled_time_utc = now_utc + delta
        return await self.enqueue_at(
            scheduled_time_utc,
            func,
            *args,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs,
        )

    async def schedule(
        self,
        func: Callable,
        *args: Any,
        cron: Optional[str] = None,
        interval: Optional[Union[timedelta, float, int]] = None,
        repeat: Optional[int] = None,
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
            
        Raises:
            ConfigurationError: If schedule configuration is invalid
            NaqException: If scheduling fails
        """
        # Validate schedule parameters
        if not cron and not interval:
            raise ConfigurationError(
                "Either 'cron' or 'interval' must be provided for schedule()"
            )
        if cron and interval:
            raise ConfigurationError("Provide either 'cron' or 'interval', not both.")

        # Check for croniter if cron is used
        if cron:
            try:
                from croniter import croniter
            except ImportError:
                raise ImportError(
                    "Please install 'croniter' to use cron scheduling: pip install croniter"
                ) from None

        # Create the job object
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay,
            queue_name=self.name,
        )

        # Calculate first run time
        now_utc = datetime.datetime.now(timezone.utc)
        first_run_ts: float

        if cron:
            # Calculate the first run time based on the cron expression
            cron_iter = croniter(cron, now_utc)
            first_run_ts = cron_iter.get_next(datetime.datetime).timestamp()
        elif interval:
            # Convert to timedelta if seconds were provided
            if isinstance(interval, (int, float)):
                interval = timedelta(seconds=interval)
            # First run is one interval from now
            first_run_ts = (now_utc + interval).timestamp()
        else:
            # Should not happen due to initial check
            raise ConfigurationError("Invalid schedule configuration.")

        # Extract interval seconds if interval was provided
        interval_seconds = interval.total_seconds() if isinstance(interval, timedelta) else None
        if isinstance(interval, (int, float)):
            interval_seconds = float(interval)

        # Store in scheduled jobs KV
        await self._scheduled_job_manager.store_job(
            job,
            scheduled_timestamp=first_run_ts,
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        )
        
        logger.info(
            f"Scheduled recurring job {job.job_id} ({func.__name__}) starting at "
            f"{datetime.datetime.fromtimestamp(first_run_ts, timezone.utc)} on queue '{self.name}'"
        )
        return job

    async def purge(self) -> int:
        """
        Removes all jobs from this queue by purging messages.

        Returns:
            The number of purged messages.

        Raises:
            NaqConnectionError: If connection to NATS fails.
            NaqException: For other errors during purging.
        """
        logger.info(
            f"Purging queue '{self.name}' (subject: {self.subject} in stream: {self.stream_name})"
        )
        try:
            js = await self._get_js()
            # Ensure the stream exists first
            await ensure_stream(
                js=js, stream_name=self.stream_name, subjects=[f"{NAQ_PREFIX}.queue.*"]
            )

            # Purge messages for this queue's subject
            resp = await js.purge_stream(
                name=self.stream_name,
                subject=self.subject,
            )
            logger.info(f"Purge successful for queue '{self.name}'.")
            return resp
        except nats.js.errors.NotFoundError:
            logger.warning(
                f"Stream '{self.stream_name}' not found. Nothing to purge for queue '{self.name}'."
            )
            return 0  # Stream doesn't exist, so 0 messages purged
        except nats.errors.Error as e:
            logger.error(f"NATS error purging queue '{self.name}': {e}")
            raise NaqConnectionError(f"NATS error during purge: {e}") from e
        except Exception as e:
            logger.error(f"Error purging queue '{self.name}': {e}")
            raise NaqException(f"Failed to purge queue: {e}") from e

    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancels a scheduled job by deleting it from the KV store.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            True if the job was found and deleted, False otherwise.

        Raises:
            NaqException: For errors during deletion.
        """
        return await self._scheduled_job_manager.cancel_job(job_id)

    async def pause_scheduled_job(self, job_id: str) -> bool:
        """
        Pauses a scheduled job.
        
        Args:
            job_id: The ID of the job to pause
            
        Returns:
            True if successful, False on concurrency conflict
            
        Raises:
            JobNotFoundError: If job doesn't exist
            NaqException: For other errors
        """
        logger.info(f"Attempting to pause scheduled job '{job_id}'")
        return await self._scheduled_job_manager.update_job_status(
            job_id, SCHEDULED_JOB_STATUS_PAUSED
        )

    async def resume_scheduled_job(self, job_id: str) -> bool:
        """
        Resumes a paused scheduled job.
        
        Args:
            job_id: The ID of the job to resume
            
        Returns:
            True if successful, False on concurrency conflict
            
        Raises:
            JobNotFoundError: If job doesn't exist
            NaqException: For other errors
        """
        logger.info(f"Attempting to resume scheduled job '{job_id}'")
        return await self._scheduled_job_manager.update_job_status(
            job_id, SCHEDULED_JOB_STATUS_ACTIVE
        )

    async def modify_scheduled_job(self, job_id: str, **updates: Any) -> bool:
        """
        Modifies parameters of a scheduled job.
        
        Args:
            job_id: The ID of the job to modify
            **updates: Parameters to update (cron, interval, repeat, etc.)
            
        Returns:
            True if successful, False on concurrency conflict
            
        Raises:
            JobNotFoundError: If job doesn't exist
            ConfigurationError: If invalid parameters are provided
            NaqException: For other errors
        """
        return await self._scheduled_job_manager.modify_job(job_id, **updates)

    async def close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        await close_nats_connection()
        self._js = None

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"


# --- Async Helper Functions ---

async def enqueue(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    **kwargs: Any,
) -> Job:
    """Helper to enqueue a job onto a specific queue (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    job = await q.enqueue(
        func,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        depends_on=depends_on,
        **kwargs,
    )
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
    return await q.enqueue_at(
        dt, func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs
    )


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
    return await q.enqueue_in(
        delta, func, *args, max_retries=max_retries, retry_delay=retry_delay, **kwargs
    )


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
        func,
        *args,
        cron=cron,
        interval=interval,
        repeat=repeat,
        max_retries=max_retries,
        retry_delay=retry_delay,
        **kwargs,
    )


async def purge_queue(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
) -> int:
    """Helper to purge jobs from a specific queue (async)."""
    q = Queue(name=queue_name, nats_url=nats_url)
    return await q.purge()


async def cancel_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to cancel a scheduled job (async)."""
    q = Queue(nats_url=nats_url)  # Queue name doesn't matter here
    return await q.cancel_scheduled_job(job_id)


async def pause_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to pause a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.pause_scheduled_job(job_id)


async def resume_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool:
    """Helper to resume a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.resume_scheduled_job(job_id)


async def modify_scheduled_job(
    job_id: str, nats_url: Optional[str] = None, **updates: Any
) -> bool:
    """Helper to modify a scheduled job (async)."""
    q = Queue(nats_url=nats_url)
    return await q.modify_scheduled_job(job_id, **updates)


# --- Sync Helper Functions ---

def enqueue_sync(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    **kwargs: Any,
) -> Job:
    """Helper to enqueue a job onto a specific queue (synchronous)."""
    async def _main():
        job = await enqueue(
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            depends_on=depends_on,
            **kwargs,
        )
        await close_nats_connection()
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
            dt,
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs,
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
            delta,
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs,
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
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            cron=cron,
            interval=interval,
            repeat=repeat,
            max_retries=max_retries,
            retry_delay=retry_delay,
            **kwargs,
        )
        await close_nats_connection()
        return job

    return run_async_from_sync(_main())


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: Optional[str] = None,
) -> int:
    """Helper to purge jobs from a specific queue (synchronous)."""
    async def _main():
        count = await purge_queue(queue_name=queue_name, nats_url=nats_url)
        await close_nats_connection()
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


def modify_scheduled_job_sync(
    job_id: str, nats_url: Optional[str] = None, **updates: Any
) -> bool:
    """Helper to modify a scheduled job (sync)."""
    async def _main():
        res = await modify_scheduled_job(job_id, nats_url=nats_url, **updates)
        await close_nats_connection()
        return res

    return run_async_from_sync(_main())
