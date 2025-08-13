# src/naq/queue/core.py
"""
Core Queue implementation for NAQ.

This module contains the main Queue class for managing NATS JetStream-backed job queues.
"""

import datetime
from datetime import timedelta, timezone
from typing import Any, Callable, List, Optional, Union

import nats
from loguru import logger

from ..services import ServiceManager
from ..services.connection import ConnectionService
from ..services.streams import StreamService
from ..services.events import EventService
from ..services.jobs import JobService
from ..services.scheduler import SchedulerService
from ..exceptions import ConfigurationError, NaqException
from ..models import Job, RetryDelayType
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    NAQ_PREFIX,
    SCHEDULED_JOB_STATUS,
)
from ..utils.decorators import retry, timing, log_errors
from ..utils.error_handling import async_error_handler_context, get_global_error_handler
from ..utils.validation import validate_config, VALID_QUEUE_NAME
from ..utils.nats_helpers import create_subject
from ..utils.logging import StructuredLogger, setup_structured_logging as setup_logging
from ..utils.context_managers import performance_context


class Queue:
    """Represents a job queue backed by a NATS JetStream stream."""

    def __init__(
        self,
        name: str = DEFAULT_QUEUE_NAME,
        config: Optional[Any] = None,
        nats_url: Optional[str] = None,
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
        # Validate queue name using utility
        if not name:
            raise ValueError("Queue name cannot be empty")
        if not VALID_QUEUE_NAME.validator(name):
            raise ValueError(
                f"Queue name '{name}' contains invalid characters. "
                "Only alphanumeric, underscore, hyphen, and dot are allowed."
            )

        self.name = name
        self.subject = create_subject(NAQ_PREFIX, "queue", self.name)
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        self._nats_url = nats_url
        self._default_timeout = default_timeout
        self._prefer_thread_local = prefer_thread_local
        
        # Handle configuration - prefer passed config over legacy parameters
        if config is not None:
            self._config = config
        else:
            # Fallback to dictionary config for backward compatibility
            self._config = {
                'nats_url': nats_url or DEFAULT_NATS_URL,
                'prefer_thread_local': prefer_thread_local,
            }
            
            # Validate configuration
            try:
                validate_config(self._config)
            except Exception as e:
                logger.warning(f"Configuration validation warning: {e}")
        
        # Initialize service manager with configuration
        self._service_manager: Optional[ServiceManager] = None
        self._initialized = False
        self._error_handler = get_global_error_handler()
        self._logger = StructuredLogger(f"queue.{name}")

        setup_logging()  # Ensure logging is set up

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _get_services(self) -> ServiceManager:
        """Get or create service manager."""
        if self._service_manager is None:
            self._service_manager = ServiceManager(self._config)
            await self._service_manager.initialize_all()
        return self._service_manager
    
    @retry(max_attempts=3, delay=1.0, backoff="exponential")
    @timing(threshold_ms=1000)
    @log_errors(reraise=True)
    async def _ensure_initialized(self) -> None:
        """Ensure services are initialized with retry and timing."""
        if not self._initialized:
            async with self._logger.operation_context("initialize_queue_services"):
                # Get services and ensure stream exists
                services = await self._get_services()
                stream_service = await services.get_service(StreamService)
                await stream_service.ensure_stream(
                    self.stream_name,
                    [create_subject(NAQ_PREFIX, "queue", "*")]
                )
                self._initialized = True

    async def _get_connection_service(self) -> ConnectionService:
        """Get the connection service, initializing if needed."""
        async with async_error_handler_context(
            self._error_handler, 
            "get_connection_service"
        ):
            await self._ensure_initialized()
            services = await self._get_services()
            return await services.get_service(ConnectionService)

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

        # Use structured logging with context
        self._logger.info(
            f"Enqueueing job {job.job_id} ({func.__name__}) to queue '{self.name}'",
            job_id=job.job_id,
            function_name=func.__name__,
            queue_name=self.name,
            subject=self.subject
        )
        
        if job.dependency_ids:
            self._logger.info(
                f"Job {job.job_id} has dependencies",
                job_id=job.job_id,
                dependency_ids=job.dependency_ids
            )

        # Use error handler context and performance tracking
        async with async_error_handler_context(
            self._error_handler, 
            f"enqueue_job_{job.job_id}",
            reraise=True
        ):
            async with performance_context("enqueue_job", self._logger) as perf:
                await self._ensure_initialized()
                services = await self._get_services()
                job_service = await services.get_service(JobService)
                
                # Use JobService to enqueue the job - this handles serialization,
                # publishing, and event logging
                await job_service.enqueue_job(job, self.name)
                
                self._logger.info(
                    f"Job {job.job_id} enqueued successfully via JobService",
                    job_id=job.job_id,
                    queue_name=self.name,
                    function_name=func.__name__
                )
                
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

        # Use scheduler service to store job
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        await scheduler_service.schedule_job(job, scheduled_timestamp)

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

        # Use scheduler service to store recurring job
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        await scheduler_service.schedule_job(
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
            await self._ensure_initialized()
            stream_service = await self._service_manager.get_service(StreamService)

            # Purge messages for this queue's subject
            purged_count = await stream_service.purge_stream(
                name=self.stream_name,
                subject=self.subject,
            )
            logger.info(f"Purge successful for queue '{self.name}'. Purged {purged_count} messages.")
            return purged_count
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
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        return await scheduler_service.cancel_scheduled_job(job_id)

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
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        return await scheduler_service.pause_scheduled_job(job_id)

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
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        return await scheduler_service.resume_scheduled_job(job_id)

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
        await self._ensure_initialized()
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        return await scheduler_service.modify_scheduled_job(job_id, **updates)

    async def close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        if self._service_manager is not None:
            await self._service_manager.cleanup_all()
            self._service_manager = None
        self._initialized = False

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"