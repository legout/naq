# src/naq/models/jobs.py
"""
Job model definitions for the NAQ job queue system.

This module contains the core job model classes that represent jobs and their results
in the NAQ system. These classes use msgspec.Struct for efficient serialization
and provide comprehensive functionality for job management and execution.
"""

import asyncio
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import msgspec

from .enums import JOB_STATUS
from ..exceptions import (
    JobExecutionError,
    JobNotFoundError,
    NaqConnectionError,
    NaqException,
    SerializationError,
)
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
)


# Define retry strategies
# RETRY_STRATEGY is now defined in enums.py to avoid circular imports

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


class JobResult(msgspec.Struct):
    """
    Represents the result of a job execution.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.

    The JobResult class captures the outcome of job execution, including status,
    result data, error information, and timing details. It provides methods to
    calculate execution duration and can be created from a Job object.

    Attributes:
        job_id: Unique identifier for the job
        status: Final status of the job execution
        result: The return value from the job function (if successful)
        error: Error message if the job failed
        traceback: Full traceback if the job failed
        start_time: When the job started executing (Unix timestamp)
        finish_time: When the job finished executing (Unix timestamp)

    Example:
        ```python
        from naq.models.jobs import JobResult
        from naq.models.enums import JOB_STATUS

        # Create a job result
        result = JobResult(
            job_id="job-123",
            status=JOB_STATUS.COMPLETED,
            result="Job completed successfully",
            start_time=1625097600.0,
            finish_time=1625097601.5
        )

        # Calculate duration
        print(f"Job took {result.duration_ms}ms")
        ```
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
        """
        Calculate duration in milliseconds if start and finish times are available.

        Returns:
            Optional[float]: Duration in milliseconds, or None if times are not available

        Example:
            ```python
            if result.duration_ms:
                print(f"Job executed in {result.duration_ms:.2f}ms")
            ```
        """
        if self.start_time > 0 and self.finish_time > 0:
            return (self.finish_time - self.start_time) * 1000
        return None

    @classmethod
    def from_job(cls, job: "Job") -> "JobResult":
        """
        Create a JobResult from a Job object.

        This method extracts the relevant information from a Job object to create
        a JobResult that represents the final state of the job execution.

        Args:
            job: The Job object to create a result from

        Returns:
            JobResult: A new JobResult instance containing job execution information

        Example:
            ```python
            # After executing a job
            result = JobResult.from_job(job)
            print(f"Job {result.job_id} finished with status: {result.status}")
            ```
        """
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


class Job(msgspec.Struct):
    """
    Represents a job to be executed.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.

    The Job class encapsulates all information needed to execute a function,
    including the function itself, arguments, retry configuration, and execution
    state. It provides comprehensive functionality for job lifecycle management,
    serialization, and retry logic.

    Attributes:
        job_id: Unique identifier for the job (auto-generated if not provided)
        function: The callable to execute
        args: Positional arguments to pass to the function
        kwargs: Keyword arguments to pass to the function
        queue_name: Name of the queue to process this job
        max_retries: Maximum number of retry attempts (default: 0)
        retry_delay: Delay between retry attempts (default: 0)
        retry_strategy: Strategy for calculating retry delays (default: "linear")
        retry_on: Exception types that should trigger retries
        ignore_on: Exception types that should NOT trigger retries
        depends_on: Job IDs that must complete before this job can run
        result_ttl: Time-to-live for job results in seconds
        timeout: Maximum execution time in seconds
        enqueue_time: When the job was created (Unix timestamp)
        error: Error message if the job failed
        traceback: Full traceback if the job failed
        _retry_count: Internal counter for retry attempts
        _start_time: When the job started executing (Unix timestamp)
        _finish_time: When the job finished executing (Unix timestamp)
        result: The return value from the job function (if successful)

    Example:
        ```python
        from naq.models.jobs import Job
        from naq.models.enums import JOB_STATUS

        # Create a simple job
        job = Job(
            function=lambda x, y: x + y,
            args=(2, 3),
            queue_name="math",
            max_retries=3
        )

        # Check job status
        print(f"Job status: {job.status}")
        ```
    """

    # Core job attributes
    function: Callable
    job_id: str = msgspec.field(
        default_factory=lambda: str(uuid.uuid4()).replace("-", "")
    )
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
        It normalizes the retry strategy to a lowercase string and validates it
        against the available strategies. It also ensures that args and kwargs
        are properly initialized.

        Raises:
            ValueError: If retry_strategy is invalid

        Example:
            ```python
            # This method is called automatically when creating a Job
            job = Job(retry_strategy="EXPONENTIAL")  # Will be normalized to "exponential"
            ```
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

        Example:
            ```python
            if job.status == JOB_STATUS.COMPLETED:
                print("Job finished successfully")
            elif job.status == JOB_STATUS.FAILED:
                print(f"Job failed: {job.error}")
            ```
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

        Example:
            ```python
            # If job depends on other jobs
            for dep_id in job.dependency_ids:
                print(f"Waiting for job {dep_id} to complete")
            ```
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

        Example:
            ```python
            if job.retry_count > 0:
                print(f"Job has been retried {job.retry_count} times")
            ```
        """
        return self._retry_count

    def increment_retry_count(self) -> None:
        """
        Increment the retry count.

        This method is called when a job is being retried after a failure.

        Example:
            ```python
            # Before retrying a failed job
            job.increment_retry_count()
            print(f"Retry attempt #{job.retry_count}")
            ```
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

        Example:
            ```python
            try:
                result = await job.execute()
            except Exception as e:
                if job.should_retry(e):
                    print("Will retry this job")
                else:
                    print("Will not retry this job")
            ```
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

        Example:
            ```python
            delay = job.get_next_retry_delay()
            print(f"Next retry in {delay} seconds")
            ```
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

        Example:
            ```python
            # Execute a job and handle the result
            try:
                result = await job.execute()
                print(f"Job completed with result: {result}")
            except Exception as e:
                print(f"Job failed: {e}")
            ```
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
            # Re-raise the exception for the caller to handle
            raise
        finally:
            self._finish_time = time.time()

    def serialize(self) -> bytes:
        """
        Serializes the job data for sending over NATS.

        Returns:
            bytes: Serialized job data suitable for transmission over NATS.

        This method uses the configured serializer (pickle or JSON) to convert
        the job object into a byte representation that can be sent over NATS.

        Example:
            ```python
            # Serialize a job for sending to a worker
            job_data = job.serialize()
            # Send job_data to NATS...
            ```
        """
        from ..serializers import get_serializer

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

        Example:
            ```python
            # Deserialize a job received from NATS
            job = Job.deserialize(received_data)
            print(f"Deserialized job: {job.job_id}")
            ```
        """
        from ..serializers import get_serializer

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

        Example:
            ```python
            # After a job fails
            failed_data = job.serialize_failed_job()
            # Send failed_data to failed job queue...
            ```
        """
        from ..serializers import get_serializer

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

        Example:
            ```python
            # Serialize a job result
            result_data = Job.serialize_result(
                result="Success",
                status=JOB_STATUS.COMPLETED
            )
            # Store result_data in result backend...
            ```
        """
        from ..serializers import get_serializer

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

        Example:
            ```python
            # Deserialize a job result
            result_info = Job.deserialize_result(stored_data)
            print(f"Job status: {result_info['status']}")
            ```
        """
        from ..serializers import get_serializer

        serializer = get_serializer()
        return serializer.deserialize_result(data)

    def __repr__(self) -> str:
        """
        Basic string representation without fetching status.

        Returns:
            str: A string representation of the job including its ID and function name.

        This provides a concise, readable representation of the job for
        debugging and logging purposes.

        Example:
            ```python
            print(job)  # Uses __repr__ to display job information
            ```
        """
        func_name = getattr(self.function, "__name__", repr(self.function))
        return f"<Job {self.job_id}: {func_name}(...)>"
