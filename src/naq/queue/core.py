"""Core queue functionality.

This module contains the base Queue class and core queue operations.
"""

import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import cloudpickle
import nats
from loguru import logger
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from ..connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from ..exceptions import ConfigurationError
from ..exceptions import NaqConnectionError
from ..exceptions import JobNotFoundError, NaqException
from ..models.jobs import Job, RetryDelayType
from .scheduled import ScheduledJobManager
from ..models.enums import SCHEDULED_JOB_STATUS
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
    JOB_SERIALIZER,
    NAQ_PREFIX,
    SCHEDULED_JOBS_KV_NAME
)
from ..utils import run_async_from_sync, setup_logging


class Queue:
    """Represents a job queue backed by a NATS JetStream stream."""

    # Add regex for valid queue names (alphanumeric, underscore, hyphen)
    _VALID_QUEUE_NAME = re.compile(r"^[a-zA-Z0-9_.-]+$")

    def __init__(
        self,
        name: str = DEFAULT_QUEUE_NAME,
        nats_url: str = DEFAULT_NATS_URL,
        default_timeout: Optional[int] = None,
        prefer_thread_local: bool = False,
    ):
        """
        Initialize a Queue instance.

        Args:
            name: The name of the queue. Must be non-empty and contain only
                alphanumeric characters, underscores, or hyphens.
            nats_url: Optional NATS server URL override
            default_timeout: Optional default job timeout in seconds
            prefer_thread_local: When True, reuse a thread-local connection/JS context.

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
        self._prefer_thread_local = prefer_thread_local

        setup_logging()  # Ensure logging is set up

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _get_js(self) -> nats.js.JetStreamContext:
        """Gets the JetStream context, initializing if needed."""
        if self._js is None:
            # First, get the NATS connection
            nc = await get_nats_connection(
                url=self._nats_url, prefer_thread_local=self._prefer_thread_local
            )
            # Then, get the JetStream context using the connection
            self._js = await get_jetstream_context(
                nc=nc, prefer_thread_local=self._prefer_thread_local
            )
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
        timeout: Optional[int] = None,
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
        if not isinstance(retry_delay, (int, float, list, tuple)):
            raise TypeError(
                "retry_delay must be a number (int or float or list of them)"
            )
        # if retry_delay < 0:
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
            timeout=timeout,
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
        timeout: Optional[int] = None,
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
            timeout=timeout,
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
        timeout: Optional[int] = None,
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
        timeout: Optional[int] = None,
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
            timeout=timeout,
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
        interval_seconds = (
            interval.total_seconds() if isinstance(interval, timedelta) else None
        )
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
            job_id, SCHEDULED_JOB_STATUS.PAUSED
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
            job_id, SCHEDULED_JOB_STATUS.ACTIVE
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
