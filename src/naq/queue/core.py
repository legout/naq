"""Core queue functionality.

This module contains the base Queue class and core queue operations.
"""

import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Callable, List, Optional, Union

from loguru import logger

from ..connection.context_managers import nats_jetstream
from ..exceptions import ConfigurationError
from ..models.jobs import Job, RetryDelayType
from .scheduled import ScheduledJobManager
from ..models.enums import SCHEDULED_JOB_STATUS
from ..services import ServiceManager, ConnectionService, StreamService
from ..services.config import create_global_config, GlobalServiceConfig
from ..settings import DEFAULT_QUEUE_NAME, DEFAULT_NATS_URL, NAQ_PREFIX
from ..utils import setup_logging
from ..utils.decorators import retry
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter, ensure_type


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
        service_manager: Optional[ServiceManager] = None,
        config: Optional[GlobalServiceConfig] = None,
    ):
        """
        Initialize a Queue instance.

        Args:
            name: The name of the queue. Must be non-empty and contain only
                alphanumeric characters, underscores, or hyphens.
            nats_url: Optional NATS server URL override
            default_timeout: Optional default job timeout in seconds
            prefer_thread_local: When True, reuse a thread-local connection/JS context.
            service_manager: Optional ServiceManager instance for managing services.
                           If not provided, services will be created directly.
            config: Optional GlobalServiceConfig for connection configuration.

        Raises:
            ValueError: If queue name is empty or contains invalid characters
        """
        # Validate parameters
        try:
            validate_parameter(name, "name", not_none=True, pattern=self._VALID_QUEUE_NAME)
        except ValueError as e:
            if "does not match required pattern" in str(e):
                if not name:
                    raise ValueError(f"Queue name cannot be empty")
                else:
                    raise ValueError(f"Queue name contains invalid characters: '{name}'")
            raise
            
        validate_parameter(nats_url, "nats_url", not_none=True)
        if default_timeout is not None:
            validate_parameter(default_timeout, "default_timeout", min_value=0)

        self.name = name
        self.subject = f"{NAQ_PREFIX}.queue.{self.name}"
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        self._nats_url = nats_url
        self._js: Optional[object] = None  # Will be JetStreamContext
        self._default_timeout = default_timeout
        self._scheduled_job_manager = ScheduledJobManager(name, nats_url, config=config)
        self._prefer_thread_local = prefer_thread_local
        self._service_manager = service_manager
        self._connection_service: Optional[ConnectionService] = None
        self._stream_service: Optional[StreamService] = None
        self._config = config or create_global_config()

        setup_logging()  # Ensure logging is set up

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
    async def _ensure_services(self) -> None:
        """Ensure that ConnectionService and StreamService are available."""
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "ensure_services",
            queue_name=self.name,
            has_service_manager=self._service_manager is not None
        ):
            try:
                if self._connection_service is None or self._stream_service is None:
                    if self._service_manager:
                        # Get services from ServiceManager
                        if self._service_manager.has_service("connection"):
                            self._connection_service = await self._service_manager.get_service(
                                "connection", ConnectionService
                            )
                        else:
                            # Register ConnectionService if not available
                            self._connection_service = (
                                await self._service_manager.register_service(
                                    "connection", ConnectionService
                                )
                            )

                        if self._service_manager.has_service("stream"):
                            self._stream_service = await self._service_manager.get_service(
                                "stream", StreamService
                            )
                        else:
                            # Register StreamService if not available
                            self._stream_service = await self._service_manager.register_service(
                                "stream", StreamService
                            )
                    else:
                        # Create services directly if no ServiceManager
                        from ..services import ServiceConfig

                        config = ServiceConfig(nats_url=self._nats_url)

                        if self._connection_service is None:
                            self._connection_service = ConnectionService(config)
                            await self._connection_service.initialize()

                        if self._stream_service is None:
                            self._stream_service = StreamService(
                                config, self._connection_service
                            )
                            await self._stream_service.initialize()
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="ensure_services operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name})
                raise

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
    async def _get_js(self):
        """Gets the JetStream context, initializing if needed."""
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "get_jetstream_context",
            queue_name=self.name,
            stream_name=self.stream_name
        ):
            try:
                if self._js is None:
                    # Create config with the specific NATS URL
                    config = create_global_config()
                    config.nats_url = self._nats_url

                    # Use the context manager to get JetStream context
                    async with nats_jetstream(config) as (nc, js):
                        self._js = js

                        # Ensure the stream exists when the queue is first used
                        try:
                            await js.stream_info(self.stream_name)
                            logger.debug(f"Stream '{self.stream_name}' already exists")
                        except Exception:
                            # Stream doesn't exist, create it
                            await js.add_stream(
                                name=self.stream_name,
                                subjects=[f"{NAQ_PREFIX}.queue.*"],
                            )
                            logger.info(f"Stream '{self.stream_name}' created")
                return self._js
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="get_jetstream_context operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "stream_name": self.stream_name})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
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
        # Validate parameters
        validate_parameter(func, "func", not_none=True)
        if max_retries is not None:
            validate_parameter(max_retries, "max_retries", min_value=0)
        ensure_type(retry_delay, (int, float, list, tuple), "retry_delay")
        
        # Validate retry_delay is non-negative for numeric types
        if isinstance(retry_delay, (int, float)) and retry_delay < 0:
            raise ValueError(f"retry_delay cannot be negative")
            
        if timeout is not None:
            validate_parameter(timeout, "timeout", min_value=0)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "enqueue_job",
            queue_name=self.name,
            function_name=func.__name__,
            job_id=None  # Will be set after job creation
        ):
            try:
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

                # Use the context manager for JetStream operations
                config = create_global_config()
                config.nats_url = self._nats_url

                async with nats_jetstream(config) as (nc, js):
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
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="enqueue operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "function": func.__name__})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
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
        # Validate parameters
        validate_parameter(dt, "dt", not_none=True)
        validate_parameter(func, "func", not_none=True)
        if max_retries is not None:
            validate_parameter(max_retries, "max_retries", min_value=0)
        ensure_type(retry_delay, (int, float, list, tuple), "retry_delay")
        if timeout is not None:
            validate_parameter(timeout, "timeout", min_value=0)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "enqueue_at",
            queue_name=self.name,
            function_name=func.__name__,
            scheduled_time=dt.isoformat()
        ):
            try:
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
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="enqueue_at operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "function": func.__name__})
                raise

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

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
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
        # Validate parameters
        validate_parameter(func, "func", not_none=True)
        if max_retries is not None:
            validate_parameter(max_retries, "max_retries", min_value=0)
        ensure_type(retry_delay, (int, float, list, tuple), "retry_delay")
        
        # Validate retry_delay is non-negative for numeric types
        if isinstance(retry_delay, (int, float)) and retry_delay < 0:
            raise ValueError(f"retry_delay cannot be negative")
            
        if timeout is not None:
            validate_parameter(timeout, "timeout", min_value=0)
        if repeat is not None:
            validate_parameter(repeat, "repeat", min_value=1)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "schedule_job",
            queue_name=self.name,
            function_name=func.__name__,
            cron=cron,
            interval_seconds=interval.total_seconds() if isinstance(interval, timedelta) else interval,
            repeat=repeat
        ):
            try:
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
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="schedule operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "function": func.__name__})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
    async def purge(self) -> int:
        """
        Removes all jobs from this queue by purging messages.

        Returns:
            The number of purged messages.

        Raises:
            NaqConnectionError: If connection to NATS fails.
            NaqException: For other errors during purging.
        """
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "purge_queue",
            queue_name=self.name,
            subject=self.subject,
            stream_name=self.stream_name
        ):
            try:
                logger.info(
                    f"Purging queue '{self.name}' (subject: {self.subject} in stream: {self.stream_name})"
                )
                # Use the context manager for JetStream operations
                config = create_global_config()
                config.nats_url = self._nats_url

                async with nats_jetstream(config) as (nc, js):
                    # Purge messages for this queue's stream
                    try:
                        await js.purge_stream(self.stream_name)
                        logger.info(f"Purge successful for queue '{self.name}'.")
                        return 1
                    except Exception as purge_error:
                        logger.error(
                            f"Error purging stream '{self.stream_name}': {purge_error}"
                        )
                        return 0
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="purge operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
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
        # Validate parameters
        validate_parameter(job_id, "job_id", not_none=True)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "cancel_scheduled_job",
            queue_name=self.name,
            job_id=job_id
        ):
            try:
                return await self._scheduled_job_manager.cancel_job(job_id)
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="cancel_scheduled_job operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "job_id": job_id})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
    )
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
        # Validate parameters
        validate_parameter(job_id, "job_id", not_none=True)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "pause_scheduled_job",
            queue_name=self.name,
            job_id=job_id
        ):
            try:
                logger.info(f"Attempting to pause scheduled job '{job_id}'")
                return await self._scheduled_job_manager.update_job_status(
                    job_id, SCHEDULED_JOB_STATUS.PAUSED
                )
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="pause_scheduled_job operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "job_id": job_id})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
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
        # Validate parameters
        validate_parameter(job_id, "job_id", not_none=True)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "resume_scheduled_job",
            queue_name=self.name,
            job_id=job_id
        ):
            try:
                logger.info(f"Attempting to resume scheduled job '{job_id}'")
                return await self._scheduled_job_manager.update_job_status(
                    job_id, SCHEDULED_JOB_STATUS.ACTIVE
                )
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="resume_scheduled_job operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "job_id": job_id})
                raise

    @retry(
        max_attempts=3,
        delay=1.0,
        exceptions=(ConnectionError, TimeoutError)
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
        # Validate parameters
        validate_parameter(job_id, "job_id", not_none=True)
        
        structured_logger = StructuredLogger("naq.queue.core")
        
        with structured_logger.operation_context(
            "modify_scheduled_job",
            queue_name=self.name,
            job_id=job_id,
            update_keys=list(updates.keys())
        ):
            try:
                return await self._scheduled_job_manager.modify_job(job_id, **updates)
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="modify_scheduled_job operation")
                error_handler.handle_error(wrapped_error, context={"queue_name": self.name, "job_id": job_id, "updates": updates})
                raise

    async def close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        # With context managers, connections are automatically closed
        # Just reset our reference
        self._js = None

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"
