# src/naq/models.py
import asyncio
import time
import traceback
import uuid
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import msgspec
from nats.js import JetStreamContext

from .exceptions import (
    JobExecutionError,
    JobNotFoundError,
    NaqConnectionError,
    NaqException,
    SerializationError,
)
from .results import Results
from .settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
)


class JOB_STATUS(Enum):
    """Enum representing the possible states of a job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    SCHEDULED = "scheduled"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class JobEventType(str, Enum):
    """Enum representing the types of job events."""

    ENQUEUED = "enqueued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    SCHEDULED = "scheduled"
    SCHEDULE_TRIGGERED = "schedule_triggered"
    CANCELLED = "cancelled"
    STATUS_CHANGED = "status_changed"


class JobEvent(msgspec.Struct):
    """
    Represents an event in the lifecycle of a job.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    job_id: str
    event_type: JobEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    worker_id: Optional[str] = None
    queue_name: Optional[str] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[float] = None
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None

    @classmethod
    def enqueued(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create an ENQUEUED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.ENQUEUED,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def started(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a STARTED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STARTED,
            worker_id=worker_id,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def completed(
        cls,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a COMPLETED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.COMPLETED,
            worker_id=worker_id,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details={"duration_ms": duration_ms, **(details or {})},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def failed(
        cls,
        job_id: str,
        worker_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a FAILED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.FAILED,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details={
                "duration_ms": duration_ms,
                "error_type": error_type,
                "error_message": error_message,
                **(details or {}),
            },
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def retry_scheduled(
        cls,
        job_id: str,
        worker_id: str,
        delay_seconds: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a RETRY_SCHEDULED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.RETRY_SCHEDULED,
            worker_id=worker_id,
            message=f"Retry scheduled in {delay_seconds} seconds",
            queue_name=queue_name,
            details={"delay_seconds": delay_seconds, **(details or {})},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def scheduled(
        cls,
        job_id: str,
        queue_name: str,
        scheduled_timestamp_utc: float,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a SCHEDULED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULED,
            queue_name=queue_name,
            worker_id=worker_id,
            details={
                "scheduled_timestamp_utc": scheduled_timestamp_utc,
                **(details or {}),
            },
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def schedule_triggered(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a SCHEDULE_TRIGGERED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_TRIGGERED,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def cancelled(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a CANCELLED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.CANCELLED,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            message=f"Job cancelled",
            details=details or {},
        )

    @classmethod
    def status_changed(
        cls,
        job_id: str,
        queue_name: str,
        old_status: str,
        new_status: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a STATUS_CHANGED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STATUS_CHANGED,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            message=f"Job status changed from {old_status} to {new_status}",
            details={
                "old_status": old_status,
                "new_status": new_status,
                **(details or {}),
            },
        )


class JobResult(msgspec.Struct):
    """
    Represents the result of a job execution.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    job_id: str
    status: str
    result: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    start_time: float = 0.0
    finish_time: float = 0.0

    @property
    def duration_ms(self) -> Optional[float]:
        """Calculate duration in milliseconds if start and finish times are available."""
        if self.start_time > 0 and self.finish_time > 0:
            return (self.finish_time - self.start_time) * 1000
        return None

    @classmethod
    def from_job(cls, job: "Job") -> "JobResult":
        """Create a JobResult from a Job object."""
        return cls(
            job_id=job.job_id,
            status=job.status.value
            if isinstance(job.status, JOB_STATUS)
            else str(job.status),
            result=job.result,
            error=job.error,
            traceback=job.traceback,
            start_time=job._start_time or 0.0,
            finish_time=job._finish_time or 0.0,
        )


class Schedule(msgspec.Struct):
    """
    Represents a scheduled job configuration.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    job_id: str
    scheduled_timestamp_utc: float
    _orig_job_payload: bytes
    cron: Optional[str] = None
    interval_seconds: Optional[float] = None
    repeat: Optional[int] = None
    status: str = "active"
    last_enqueued_utc: Optional[float] = None
    schedule_failure_count: int = 0

    @property
    def is_recurring(self) -> bool:
        """Check if this is a recurring schedule."""
        return self.cron is not None or self.interval_seconds is not None

    @property
    def is_infinite_repeat(self) -> bool:
        """Check if this schedule repeats infinitely."""
        return self.repeat is None

    def should_retry_schedule(self) -> bool:
        """Check if the schedule should be retried after a failure."""
        from .settings import MAX_SCHEDULE_FAILURES

        if MAX_SCHEDULE_FAILURES is None:
            return True
        return self.schedule_failure_count < MAX_SCHEDULE_FAILURES

    def increment_failure_count(self) -> None:
        """Increment the schedule failure count."""
        self.schedule_failure_count += 1

    def reset_failure_count(self) -> None:
        """Reset the schedule failure count."""
        self.schedule_failure_count = 0

    def update_last_enqueued(self) -> None:
        """Update the last enqueued timestamp to now."""
        self.last_enqueued_utc = time.time()

    @classmethod
    def from_job(
        cls,
        job: "Job",
        scheduled_timestamp_utc: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ) -> "Schedule":
        """Create a Schedule from a Job object."""
        return cls(
            job_id=job.job_id,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            _orig_job_payload=job.serialize(),
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        )


# Define retry strategies
from .settings import RETRY_STRATEGY

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}

# Define a type hint for retry delays (same as in job.py)
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


class Job(msgspec.Struct):
    """
    Represents a job to be executed.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    # Core job attributes
    job_id: str = msgspec.field(
        default_factory=lambda: str(uuid.uuid4()).replace("-", "")
    )
    function: Callable = msgspec.field(default=None)
    args: Tuple = msgspec.field(default_factory=tuple)
    kwargs: Dict = msgspec.field(default_factory=dict)
    queue_name: str = msgspec.field(default=DEFAULT_QUEUE_NAME)
    max_retries: int = msgspec.field(default=0)
    retry_delay: RetryDelayType = msgspec.field(default=0)
    retry_strategy: str = msgspec.field(default="linear")
    retry_on: Optional[Tuple[type, ...]] = msgspec.field(default=None)
    ignore_on: Optional[Tuple[type, ...]] = msgspec.field(default=None)
    depends_on: Optional[Union[str, List[str], "Job", List["Job"]]] = msgspec.field(
        default=None
    )
    result_ttl: Optional[int] = msgspec.field(default=None)
    timeout: Optional[int] = msgspec.field(default=None)

    # Runtime attributes (not serialized)
    enqueue_time: float = msgspec.field(default_factory=time.time)
    error: Optional[str] = msgspec.field(default=None)
    traceback: Optional[str] = msgspec.field(default=None)
    _retry_count: int = msgspec.field(default=0, name="retry_count")
    _start_time: Optional[float] = msgspec.field(default=None, name="start_time")
    _finish_time: Optional[float] = msgspec.field(default=None, name="finish_time")
    result: Optional[Any] = msgspec.field(default=None)

    def __post_init__(self) -> None:
        """
        Post-initialization hook for validating and processing job parameters.

        This method is called automatically by msgspec.Struct after field initialization.

        Raises:
            ValueError: If retry_strategy is invalid
        """
        # Normalize enum or other inputs to a canonical lowercase string
        if hasattr(self.retry_strategy, "value"):
            self.retry_strategy = self.retry_strategy.value
        else:
            self.retry_strategy = str(self.retry_strategy).lower()

        if self.retry_strategy not in VALID_RETRY_STRATEGIES:
            raise ValueError(
                f"Invalid retry strategy '{self.retry_strategy}'. "
                f"Must be one of: {', '.join(sorted(VALID_RETRY_STRATEGIES))}"
            )

        # Ensure args and kwargs are properly initialized
        if self.args is None:
            self.args = ()
        if self.kwargs is None:
            self.kwargs = {}

    @property
    def status(self) -> JOB_STATUS:
        """
        Return the current status of the job.

        Returns:
            JOB_STATUS: The current status of the job based on its execution state.

        The status is determined as follows:
        - PENDING: Job has not started execution (start_time is None)
        - RUNNING: Job is currently executing (start_time set but finish_time is None)
        - COMPLETED: Job finished successfully (finish_time set and no error)
        - FAILED: Job finished with an error (finish_time set and error is present)
        """
        if self._start_time is None:
            return JOB_STATUS.PENDING
        if self._finish_time is None:
            return JOB_STATUS.RUNNING
        return JOB_STATUS.FAILED if self.error is not None else JOB_STATUS.COMPLETED

    @property
    def dependency_ids(self) -> List[str]:
        """
        Return a list of dependency job IDs.

        Returns:
            List[str]: A list of job IDs that this job depends on.

        This property normalizes various dependency input formats into a consistent
        list of job ID strings. It handles:
        - Single string (job ID)
        - Single Job object
        - List of strings (job IDs)
        - List of Job objects
        - Mixed list of strings and Job objects
        """
        if not self.depends_on:
            return []
        if isinstance(self.depends_on, str):
            return [self.depends_on]
        if isinstance(self.depends_on, Job):
            return [self.depends_on.job_id]
        ids = []
        for dep in self.depends_on:
            if isinstance(dep, Job):
                ids.append(dep.job_id)
            else:
                ids.append(str(dep))
        return ids

    @property
    def retry_count(self) -> int:
        """
        Get the current retry count.

        Returns:
            int: The number of times this job has been retried.
        """
        return self._retry_count

    def increment_retry_count(self) -> None:
        """
        Increment the retry count.

        This method is called when a job is being retried after a failure.
        """
        self._retry_count += 1

    def should_retry(self, exc: Exception) -> bool:
        """
        Determine if the job should be retried based on the exception.

        Args:
            exc: The exception that caused the job to fail

        Returns:
            bool: True if the job should be retried, False otherwise

        The retry logic follows this precedence:
        1. Never retry if max_retries is exceeded
        2. Never retry if the exception is in ignore_on list
        3. Only retry if the exception is in retry_on list (if specified)
        4. Retry all exceptions if retries are configured and no specific lists are set
        """
        # Don't retry if max retries exceeded
        if self.retry_count >= self.max_retries:
            return False

        # If ignore_on is specified, check if exception matches any ignored types
        if self.ignore_on and any(
            isinstance(exc, exc_type) for exc_type in self.ignore_on
        ):
            return False

        # If retry_on is specified, only retry on those exceptions
        if self.retry_on:
            return any(isinstance(exc, exc_type) for exc_type in self.retry_on)

        # By default, retry on all exceptions if retries are configured
        return self.max_retries > 0

    def get_next_retry_delay(self) -> float:
        """
        Calculate the delay for the next retry attempt.

        Returns:
            float: Delay in seconds before the next retry

        The delay calculation depends on the retry_strategy:
        - Linear: Always returns the base delay
        - Exponential: Returns base_delay * (2^retry_count)
        - Sequence: Uses the retry_delay as a lookup table, taking the value at index
          retry_count (or the last value if retry_count exceeds the sequence length)
        """
        if isinstance(self.retry_delay, (int, float)):
            base_delay = float(self.retry_delay)
            if self.retry_strategy == "linear":
                return base_delay
            else:  # exponential
                return base_delay * (2**self.retry_count)

        # If retry_delay is a sequence, use it as a lookup table
        if isinstance(self.retry_delay, Sequence):
            idx = min(self.retry_count, len(self.retry_delay) - 1)
            return float(self.retry_delay[idx])

        return 0.0

    async def execute(self) -> Any:
        """
        Execute the job's function and manage its state.

        Returns:
            Any: The function's return value.

        Raises:
            Exception: Any exception that the function raises and isn't handled by retry logic.

        This method handles both synchronous and asynchronous functions:
        - Async functions are awaited directly
        - Sync functions are run in a separate thread to avoid blocking the event loop

        The method manages the job's execution state by setting start_time and finish_time,
        and capturing any errors or tracebacks that occur during execution.
        """
        self._start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(self.function):
                # For async functions, use await as before
                self.result = await self.function(*self.args, **self.kwargs)
            else:
                # For sync functions, run them in a separate thread to avoid blocking
                # the event loop. This allows true parallel execution when
                # multiple sync jobs are processed concurrently.
                self.result = await asyncio.to_thread(
                    self.function, *self.args, **self.kwargs
                )
            self._finish_time = time.time()
            return self.result
        except Exception as e:
            self.error = str(e)
            self.traceback = traceback.format_exc()
            # self.status is a property derived from self.error, no direct assignment needed
            # Do not re-raise, allow Worker.process_message to handle storing result/error
        finally:
            self._finish_time = time.time()

    def serialize(self) -> bytes:
        """
        Serializes the job data for sending over NATS.

        Returns:
            bytes: Serialized job data suitable for transmission over NATS.

        This method uses the configured serializer (pickle or JSON) to convert
        the job object into a byte representation that can be sent over NATS.
        """
        from .serializers import get_serializer

        serializer = get_serializer()
        return serializer.serialize_job(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "Job":
        """
        Deserializes job data received from NATS.

        Args:
            data: Byte data containing the serialized job information.

        Returns:
            Job: A fully reconstructed Job object.

        This method uses the configured serializer to reconstruct a Job object
        from its serialized byte representation.
        """
        from .serializers import get_serializer

        serializer = get_serializer()
        # Simplified per improvement plan: trust serializer to return a Job
        return serializer.deserialize_job(data)

    def serialize_failed_job(self) -> bytes:
        """
        Serializes job data including error info for the failed queue.

        Returns:
            bytes: Serialized job data including error information.

        This method is used when a job fails and needs to be sent to the
        failed job queue for later analysis or retry.
        """
        from .serializers import get_serializer

        serializer = get_serializer()
        return serializer.serialize_failed_job(self)

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        """
        Serializes job result data.

        Args:
            result: The result value from the job execution.
            status: The final status of the job.
            error: Optional error message if the job failed.
            traceback_str: Optional traceback string if the job failed.

        Returns:
            bytes: Serialized result data.

        This method serializes the result of job execution, including any
        error information, for storage in the result backend.
        """
        from .serializers import get_serializer

        serializer = get_serializer()
        return serializer.serialize_result(result, status.value, error, traceback_str)

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """
        Deserializes job result data.

        Args:
            data: Byte data containing the serialized result information.

        Returns:
            Dict[str, Any]: A dictionary containing the result data including
            status, result value, error message, and traceback if applicable.

        This method reconstructs result data from its serialized representation
        for use by clients fetching job results.
        """
        from .serializers import get_serializer

        serializer = get_serializer()
        return serializer.deserialize_result(data)

    def __repr__(self) -> str:
        """
        Basic string representation without fetching status.

        Returns:
            str: A string representation of the job including its ID and function name.

        This provides a concise, readable representation of the job for
        debugging and logging purposes.
        """
        func_name = getattr(self.function, "__name__", repr(self.function))
        return f"<Job {self.job_id}: {func_name}(...)>"
