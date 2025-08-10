# src/naq/job.py
import asyncio
import time
import traceback
import uuid
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import msgspec
from nats.js import JetStreamContext

from .exceptions import JobExecutionError, JobNotFoundError, NaqConnectionError, NaqException, SerializationError
from .results import Results
from .settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
)
from .serializers import get_serializer

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


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


# Define retry strategies
from .settings import RETRY_STRATEGY

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}


class Job(msgspec.Struct):
    """
    Represents a job to be executed.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    # Core job attributes
    job_id: str = msgspec.field(default_factory=lambda: str(uuid.uuid4()).replace("-", ""))
    function: Callable = msgspec.field(default=None)
    args: Tuple = msgspec.field(default_factory=tuple)
    kwargs: Dict = msgspec.field(default_factory=dict)
    queue_name: str = msgspec.field(default=DEFAULT_QUEUE_NAME)
    max_retries: int = msgspec.field(default=0)
    retry_delay: RetryDelayType = msgspec.field(default=0)
    retry_strategy: str = msgspec.field(default="linear")
    retry_on: Optional[Tuple[Exception, ...]] = msgspec.field(default=None)
    ignore_on: Optional[Tuple[Exception, ...]] = msgspec.field(default=None)
    depends_on: Optional[Union[str, List[str], "Job", List["Job"]]] = msgspec.field(default=None)
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
        if self.ignore_on and isinstance(exc, self.ignore_on):
            return False

        # If retry_on is specified, only retry on those exceptions
        if self.retry_on:
            return isinstance(exc, self.retry_on)

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
        serializer = get_serializer()
        return serializer.serialize_result(result, status, error, traceback_str)

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

    def get_results_manager(self, nats_url: str = DEFAULT_NATS_URL) -> Results:
        """
        Get a Results instance for managing job results.

        Args:
            nats_url: NATS server URL. Defaults to DEFAULT_NATS_URL.

        Returns:
            Results: A Results instance configured with the specified NATS URL.
            
        This method provides a convenient way to obtain a Results manager
        for interacting with job results stored in the NATS backend.
        """
        return Results(nats_url=nats_url)

    @staticmethod
    async def fetch_result(
        job_id: str,
        nats_url: str = DEFAULT_NATS_URL,
    ) -> Any:
        """
        Fetches the result or error information for a completed job from the result backend.

        Args:
            job_id: The ID of the job.
            nats_url: NATS server URL (if not using default).

        Returns:
            Any: The job's result if successful.

        Raises:
            JobNotFoundError: If the job result is not found (or expired).
            JobExecutionError: If the job failed, raises an error containing failure details.
            NaqConnectionError: If connection fails.
            SerializationError: If the stored result cannot be deserialized.
            NaqException: For other errors.
            
        This static method provides a convenient way to fetch job results without
        needing to instantiate a Job object. It handles both successful results
        and errors, raising appropriate exceptions for failed jobs.
        """
        from .connection import close_nats_connection, get_nats_connection

        results_manager = Results(nats_url=nats_url)
        nc = None
        try:
            result_data = await results_manager.fetch_job_result(job_id)

            if result_data.get("status") == JOB_STATUS.FAILED.value:
                error_str = result_data.get("error", "Unknown error")
                traceback_str = result_data.get("traceback")
                err_msg = f"Job {job_id} failed: {error_str}"
                if traceback_str:
                    err_msg += f"\nTraceback:\n{traceback_str}"
                # Raise an exception containing the failure info
                raise JobExecutionError(err_msg)
            elif result_data.get("status") == JOB_STATUS.COMPLETED.value:
                return result_data.get("result")
            else:
                # Should not happen if worker stores status correctly
                raise NaqException(
                    f"Job {job_id} found in result store but has unexpected status: {result_data.get('status')}"
                )

        except (
            JobNotFoundError,
            JobExecutionError,
            NaqConnectionError,
            SerializationError,
        ):
            # Re-raise these specific errors without wrapping
            raise
        except Exception as e:
            raise NaqException(f"Error fetching result for job {job_id}: {e}") from e

    @staticmethod
    def fetch_result_sync(job_id: str, nats_url: str = DEFAULT_NATS_URL) -> Any:
        """
        DEPRECATED: Use fetch_result instead.

        Args:
            job_id: The ID of the job.
            nats_url: NATS server URL (if not using default).

        Returns:
            Any: The job's result if successful.

        Note:
            This synchronous version can cause issues with asyncio event loops.
            Use the async version with proper connection management instead.
        """
        from warnings import warn

        warn(
            "fetch_result_sync is deprecated. Use fetch_result instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        from .utils import run_async_from_sync

        return run_async_from_sync(Job.fetch_result, job_id, nats_url=nats_url)