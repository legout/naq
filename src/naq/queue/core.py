# src/naq/queue/core.py
"""
Core queue implementation for naq.

This module provides the main Queue class which represents a job queue
backed by a NATS JetStream stream.
"""

import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Callable, List, Optional, Union

import cloudpickle
import nats
from loguru import logger

from ..connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from ..exceptions import NaqConnectionError, NaqException
from ..models import Job, RetryDelayType
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    NAQ_PREFIX,
)
from ..utils import run_async_from_sync, setup_logging
from .scheduled import ScheduledJobManager


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
        service_manager = None,
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
        self._default_max_retries = 0
        self._default_result_ttl = None
        self._scheduled_job_manager = ScheduledJobManager(name, nats_url, service_manager)
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
        # Try to use service layer if available
        if self._scheduled_job_manager._service_manager is not None:
            try:
                # Get connection service
                connection_service = await self._scheduled_job_manager._service_manager.get_service("connection")
                # Get connection and JetStream context through service
                nc = await connection_service.get_connection()
                js = await connection_service.get_jetstream()
                # Ensure the stream exists when the queue is first used
                await ensure_stream(
                    js=js,
                    stream_name=self.stream_name,
                    subjects=[f"{NAQ_PREFIX}.queue.*"],
                )
                return js
            except Exception as e:
                logger.warning(f"Failed to use service layer for queue connection, falling back to direct connection: {e}")
        
        # Fallback to direct connection
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
        *args,
        **kwargs,
    ) -> Job:
        """
        Enqueue a job to be executed by a worker.

        Args:
            func: The function to execute.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The enqueued Job instance.
        """
        # Create job instance
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            queue_name=self.name,
            timeout=self._default_timeout,
            max_retries=self._default_max_retries,
            result_ttl=self._default_result_ttl,
        )

        logger.debug(f"Enqueuing job {job.job_id} to queue {self.name}")
        # Serialize and publish job
        job_data = job.serialize()
        subject = f"{NAQ_PREFIX}.queue.{self.name}"
        logger.debug(f"Publishing job {job.job_id} to subject {subject}")
        
        # Try to use service layer if available
        if self._scheduled_job_manager._service_manager is not None:
            try:
                logger.debug(f"Using service manager to publish job {job.job_id}")
                # Get connection service
                connection_service = await self._scheduled_job_manager._service_manager.get_service("connection")
                # Get connection and JetStream context through service
                nc = await connection_service.get_connection()
                js = await connection_service.get_jetstream()
                await js.publish(subject=subject, payload=job_data)
                logger.debug(f"Successfully published job {job.job_id} using service layer")
                return job
            except Exception as e:
                # Fall back to direct connection if service layer fails
                logger.warning(f"Failed to use service layer for enqueueing job, falling back to direct connection: {e}", exc_info=True)
        
        # Fallback to direct connection
        logger.debug(f"Using direct connection to publish job {job.job_id}")
        nc = None
        try:
            nc = await get_nats_connection(url=self._nats_url)
            js = await get_jetstream_context(nc=nc)
            await js.publish(subject=subject, payload=job_data)
            logger.debug(f"Successfully published job {job.job_id} using direct connection")
        except Exception as e:
            logger.error(f"Error enqueueing job {job.job_id}: {e}", exc_info=True)
            raise NaqException(f"Failed to enqueue job: {e}") from e
        finally:
            if nc:
                await close_nats_connection()
        return job

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
            dt: The datetime when the job should be enqueued (timezone-aware preferred).
            func: The function to execute.
            *args: Positional arguments to pass to the function.
            max_retries: Optional number of retry attempts on failure (default: 0).
            retry_delay: Delay between retries, either seconds (int/float) or callable.
            timeout: Optional timeout for job execution in seconds.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The scheduled Job instance.

        Raises:
            NaqException: If scheduling fails.
        """
        # Ensure timezone-aware datetime
        if dt.tzinfo is None:
            logger.warning(
                "enqueue_at() called with naive datetime. Assuming UTC."
            )
            dt = dt.replace(tzinfo=timezone.utc)
        elif dt.tzinfo != timezone.utc:
            # Convert to UTC if in different timezone
            dt = dt.astimezone(timezone.utc)

        # Create job instance with scheduling info
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            queue_name=self.name,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            result_ttl=self._default_result_ttl,
        )

        # Store in scheduled jobs KV store
        scheduled_ts = dt.timestamp()
        await self._scheduled_job_manager.store_job(
            job=job,
            scheduled_timestamp=scheduled_ts,
        )

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
            *args: Positional arguments to pass to the function.
            max_retries: Optional number of retry attempts on failure (default: 0).
            retry_delay: Delay between retries, either seconds (int/float) or callable.
            timeout: Optional timeout for job execution in seconds.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The scheduled Job instance.

        Raises:
            NaqException: If scheduling fails.
        """
        if not isinstance(delta, timedelta):
            raise ValueError("'delta' must be a timedelta instance")

        # Calculate the future datetime
        run_at = datetime.datetime.now(timezone.utc) + delta

        # Create job instance with scheduling info
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            queue_name=self.name,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            result_ttl=self._default_result_ttl,
        )

        # Store in scheduled jobs KV store
        scheduled_ts = run_at.timestamp()
        await self._scheduled_job_manager.store_job(
            job=job,
            scheduled_timestamp=scheduled_ts,
        )

        logger.info(
            f"Scheduled job {job.job_id} ({func.__name__}) to run in {delta} on queue '{self.name}'"
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
            purged_count = await js.purge_stream(
                stream=self.stream_name, filter=self.subject
            )
            logger.info(f"Purged {purged_count} messages from queue '{self.name}'")
            return purged_count
        except nats.errors.ConnectionClosedError:
            raise NaqConnectionError("NATS connection is closed.")
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
        from ..exceptions import JobNotFoundError
        
        logger.info(f"Attempting to pause scheduled job '{job_id}'")
        return await self._scheduled_job_manager.update_job_status(
            job_id, "PAUSED"  # Using string literal to avoid import issues
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
        from ..exceptions import JobNotFoundError
        
        logger.info(f"Attempting to resume scheduled job '{job_id}'")
        return await self._scheduled_job_manager.update_job_status(
            job_id, "ACTIVE"  # Using string literal to avoid import issues
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
            *args: Positional arguments to pass to the function.
            cron: Optional cron expression for scheduling (e.g., "0 0 * * *" for daily at midnight).
            interval: Optional interval for scheduling, either as timedelta or seconds.
            repeat: Optional number of times to repeat (None=infinite).
            max_retries: Optional number of retry attempts on failure (default: 0).
            retry_delay: Delay between retries, either seconds (int/float) or callable.
            timeout: Optional timeout for job execution in seconds.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The scheduled Job instance.

        Raises:
            NaqException: If scheduling fails.
            ValueError: If neither cron nor interval is provided, or if both are provided.
        """
        # Validate scheduling parameters
        if (cron is None) == (interval is None):
            raise ValueError("Exactly one of 'cron' or 'interval' must be provided.")

        # Create job instance with scheduling info
        job = Job(
            function=func,
            args=args,
            kwargs=kwargs,
            queue_name=self.name,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            result_ttl=self._default_result_ttl,
        )

        # Calculate first run time
        now_utc = datetime.datetime.now(timezone.utc)
        if cron is not None:
            try:
                from croniter import croniter

                cron_iter = croniter(cron, now_utc)
                first_run_ts = cron_iter.get_next(datetime.datetime).timestamp()
            except ImportError:
                raise ImportError("Please install 'croniter' to use cron scheduling.")
            except Exception as e:
                from ..exceptions import ConfigurationError
                raise ConfigurationError(
                    f"Invalid cron format '{cron}': {e}"
                )
        elif interval is not None:
            if isinstance(interval, (int, float)):
                interval = timedelta(seconds=interval)
            if not isinstance(interval, timedelta):
                from ..exceptions import ConfigurationError
                raise ConfigurationError(
                    "'interval' must be timedelta or numeric seconds."
                )
            first_run_ts = (now_utc + interval).timestamp()
        else:
            # This should not happen due to validation above, but for safety
            from ..exceptions import ConfigurationError
            raise ConfigurationError("Either 'cron' or 'interval' must be provided.")

        # Store in scheduled jobs KV store
        await self._scheduled_job_manager.store_job(
            job=job,
            scheduled_timestamp=first_run_ts,
            cron=cron,
            interval_seconds=interval.total_seconds() if isinstance(interval, timedelta) else interval,
            repeat=repeat,
        )

        logger.info(
            f"Scheduled recurring job {job.job_id} ({func.__name__}) starting at "
            f"{datetime.datetime.fromtimestamp(first_run_ts, timezone.utc)} on queue '{self.name}'"
        )
        return job

    async def close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        await close_nats_connection()
        self._js = None

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"