"""Core queue functionality.

This module contains the base Queue class and core queue operations.
"""

import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Callable, List, Optional, Union, TYPE_CHECKING

from loguru import logger

from ..exceptions import ConfigurationError
from ..models.jobs import Job, RetryDelayType
from .scheduled import ScheduledJobManager
from ..models.enums import SCHEDULED_JOB_STATUS
from ..config import DEFAULT_QUEUE_NAME, DEFAULT_NATS_URL, NAQ_PREFIX
from ..utils import setup_logging
from ..utils.decorators import retry
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter, ensure_type

if TYPE_CHECKING:
    from ..nats_client import NatsClient


class Queue:
    
    """
    Represents a job queue backed by a NATS JetStream stream.

    This class provides a high-level interface for interacting with job queues.
    It uses the unified NatsClient for all NATS operations, replacing the previous
    service layer approach. The Queue class supports both synchronous and
    asynchronous operations for job submission, retrieval, and management.

    Key features:
    - Job submission with various options (delay, retry, timeout)
    - Job retrieval and status tracking
    - Scheduled job management
    - Integration with the new configuration system

    Examples:
        >>> # Create a queue with default settings
        >>> queue = Queue()
        >>> 
        >>> # Submit a job
        >>> job = await queue.submit(my_function, arg1, arg2)
        >>> 
        >>> # Create a queue with a custom name
        >>> queue = Queue(name="my_custom_queue")
        >>> 
        >>> # Use with a custom NatsClient
        >>> client = NatsClient()
        >>> queue = Queue(nats_client=client)
    """

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
        # Validate parameters
        self._validate_queue_name(name)
        validate_parameter(nats_url, "nats_url", not_none=True)
        if default_timeout is not None:
            validate_parameter(default_timeout, "default_timeout", min_value=0)

        self.name = name
        self.subject = f"{NAQ_PREFIX}.queue.{self.name}"
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        self._nats_url = nats_url
        self._default_timeout = default_timeout
        self._prefer_thread_local = prefer_thread_local
        self._client: Optional["NatsClient"] = None
        self._scheduled_job_manager = ScheduledJobManager(
            name, nats_url
        )

        setup_logging()  # Ensure logging is set up

    def _validate_queue_name(self, name: str) -> None:
        """Validate that the queue name is valid.

        Args:
            name: The queue name to validate

        Raises:
            ValueError: If queue name is empty or contains invalid characters
        """
        if not name:
            raise ValueError("Queue name cannot be empty")
        if not self._VALID_QUEUE_NAME.match(name):
            raise ValueError(
                f"Invalid queue name '{name}'. Queue names must contain only "
                f"alphanumeric characters, underscores, hyphens, or periods."
            )

    def _validate_job_parameters(
        self,
        func: Callable,
        max_retries: Optional[int],
        retry_delay: RetryDelayType,
        timeout: Optional[int],
    ) -> None:
        """Validate job parameters.

        Args:
            func: The function to validate
            max_retries: Maximum number of retries
            retry_delay: Delay between retries
            timeout: Job timeout

        Raises:
            ValueError: If parameters are invalid
        """
        # Validate function is callable
        if not callable(func):
            raise ValueError("Job function must be callable")

        # Validate max_retries
        if max_retries is not None and max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        # Validate retry_delay
        if isinstance(retry_delay, (int, float)) and retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")

        # Validate timeout
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
    async def _ensure_client(self) -> None:
        """Ensure that the NATS client is available."""
        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "ensure_client",
            queue_name=self.name,
        ):
            try:
                if self._client is None:
                    logger.debug("Creating NATS client")
                    self._client = NatsClient(
                        nats_url=self._nats_url,
                        prefer_thread_local=self._prefer_thread_local,
                    )
                    await self._client.connect()
                    logger.debug("Successfully created NATS client")
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(
                    e, context="ensure_client operation"
                )
                error_handler.handle_error(
                    wrapped_error, context={"queue_name": self.name}
                )
                raise

    async def __aenter__(self):
        """Async context manager entry."""
        # Use long-lived service context for queue lifecycle
        if self._service_manager:
            self._service_context = long_lived_service_context(
                self._service_manager, logger_name=f"naq.queue.core.{self.name}"
            )
            await self._service_context.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if hasattr(self, "_service_context"):
            await self._service_context.__aexit__(exc_type, exc_val, exc_tb)
        await self.close()

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_parameters(func, max_retries, retry_delay, timeout)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "enqueue_job",
            queue_name=self.name,
            function_name=func.__name__,
            job_id=None,  # Will be set after job creation
        ):
            try:
                await self._ensure_services()
                # DEBUG: Log kwargs before job creation
                import asyncio

                logger.debug(
                    "Creating job with kwargs",
                    kwargs_keys=list(kwargs.keys()),
                    kwargs_types={k: type(v).__name__ for k, v in kwargs.items()},
                )

                # Check for asyncio.Task objects in kwargs
                task_objects = []
                for key, value in kwargs.items():
                    if isinstance(value, asyncio.Task):
                        task_objects.append(
                            {
                                "key": key,
                                "task_id": id(value),
                                "task_state": value._state
                                if hasattr(value, "_state")
                                else "unknown",
                                "task_done": value.done()
                                if hasattr(value, "done")
                                else "unknown",
                            }
                        )

                if task_objects:
                    logger.error(
                        "Found asyncio.Task objects in kwargs before job creation",
                        task_objects=task_objects,
                    )

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

                # DEBUG: Log job object after creation
                logger.debug(
                    "Job object created",
                    job_id=job.job_id,
                    job_kwargs_keys=list(job.kwargs.keys()),
                    job_kwargs_types={
                        k: type(v).__name__ for k, v in job.kwargs.items()
                    },
                )

                logger.info(
                    f"Enqueueing job {job.job_id} ({func.__name__}) to queue '{self.name}' (subject: {self.subject})"
                )
                if job.dependency_ids:
                    logger.info(f"Job {job.job_id} depends on: {job.dependency_ids}")

                await self._job_service.enqueue_job(job, self.subject)
                logger.info(f"Job {job.job_id} published successfully.")
                return job
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(e, context="enqueue operation")
                error_handler.handle_error(
                    wrapped_error,
                    context={"queue_name": self.name, "function": func.__name__},
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_parameters(func, max_retries, retry_delay, timeout)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "enqueue_at",
            queue_name=self.name,
            function_name=func.__name__,
            scheduled_time=dt.isoformat(),
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
                error_handler.handle_error(
                    wrapped_error,
                    context={"queue_name": self.name, "function": func.__name__},
                )
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

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_parameters(func, max_retries, retry_delay, timeout)
        if repeat is not None:
            validate_parameter(repeat, "repeat", min_value=1)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "schedule_job",
            queue_name=self.name,
            function_name=func.__name__,
            cron=cron,
            interval_seconds=interval.total_seconds()
            if isinstance(interval, timedelta)
            else interval,
            repeat=repeat,
        ):
            try:
                # Validate schedule parameters
                if not cron and not interval:
                    raise ConfigurationError(
                        "Either 'cron' or 'interval' must be provided for schedule()"
                    )
                if cron and interval:
                    raise ConfigurationError(
                        "Provide either 'cron' or 'interval', not both."
                    )

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
                    interval.total_seconds()
                    if isinstance(interval, timedelta)
                    else None
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
                error_handler.handle_error(
                    wrapped_error,
                    context={"queue_name": self.name, "function": func.__name__},
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
            stream_name=self.stream_name,
        ):
            try:
                await self._ensure_services()
                logger.info(
                    f"Purging queue '{self.name}' (subject: {self.subject} in stream: {self.stream_name})"
                )
                # Purge messages for this queue's stream
                try:
                    await self._stream_service.purge_stream(
                        self.stream_name, self.subject
                    )
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
                error_handler.handle_error(
                    wrapped_error, context={"queue_name": self.name}
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
            "cancel_scheduled_job", queue_name=self.name, job_id=job_id
        ):
            try:
                return await self._scheduled_job_manager.cancel_job(job_id)
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(
                    e, context="cancel_scheduled_job operation"
                )
                error_handler.handle_error(
                    wrapped_error, context={"queue_name": self.name, "job_id": job_id}
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_id(job_id)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "pause_scheduled_job", queue_name=self.name, job_id=job_id
        ):
            try:
                logger.info(f"Attempting to pause scheduled job '{job_id}'")
                return await self._scheduled_job_manager.update_job_status(
                    job_id, SCHEDULED_JOB_STATUS.PAUSED
                )
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(
                    e, context="pause_scheduled_job operation"
                )
                error_handler.handle_error(
                    wrapped_error, context={"queue_name": self.name, "job_id": job_id}
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_id(job_id)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "resume_scheduled_job", queue_name=self.name, job_id=job_id
        ):
            try:
                logger.info(f"Attempting to resume scheduled job '{job_id}'")
                return await self._scheduled_job_manager.update_job_status(
                    job_id, SCHEDULED_JOB_STATUS.ACTIVE
                )
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(
                    e, context="resume_scheduled_job operation"
                )
                error_handler.handle_error(
                    wrapped_error, context={"queue_name": self.name, "job_id": job_id}
                )
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
        self._validate_job_id(job_id)

        structured_logger = StructuredLogger("naq.queue.core")

        with structured_logger.operation_context(
            "modify_scheduled_job",
            queue_name=self.name,
            job_id=job_id,
            update_keys=list(updates.keys()),
        ):
            try:
                return await self._scheduled_job_manager.modify_job(job_id, **updates)
            except Exception as e:
                error_handler = ErrorHandler()
                wrapped_error = wrap_naq_exception(
                    e, context="modify_scheduled_job operation"
                )
                error_handler.handle_error(
                    wrapped_error,
                    context={
                        "queue_name": self.name,
                        "job_id": job_id,
                        "updates": updates,
                    },
                )
                raise

    async def close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        # With service managers, connections are automatically closed
        # Just reset our service references
        self._connection_service = None
        self._stream_service = None
        self._job_service = None
        self._event_service = None
        self._kv_store_service = None

    def __repr__(self) -> str:
        return f"Queue('{self.name}')"
