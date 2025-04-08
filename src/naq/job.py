# src/naq/job.py
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, Protocol

import cloudpickle

from .exceptions import JobExecutionError, SerializationError
from .settings import JOB_SERIALIZER, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


class Serializer(Protocol):
    """Protocol defining the interface for job serializers."""
    
    @staticmethod
    def serialize_job(job: 'Job') -> bytes:
        """Serialize a job to bytes."""
        ...
    
    @staticmethod
    def deserialize_job(data: bytes) -> 'Job':
        """Deserialize bytes to a job."""
        ...
    
    @staticmethod
    def serialize_failed_job(job: 'Job') -> bytes:
        """Serialize a failed job to bytes."""
        ...
    
    @staticmethod
    def serialize_result(result: Any, status: str, error: Optional[str] = None, 
                       traceback_str: Optional[str] = None) -> bytes:
        """Serialize a job result to bytes."""
        ...
    
    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserialize bytes to a result dictionary."""
        ...


class PickleSerializer:
    """Serializes jobs and results using cloudpickle."""
    
    @staticmethod
    def serialize_job(job: 'Job') -> bytes:
        """Serialize a job to bytes using cloudpickle."""
        try:
            payload = {
                "job_id": job.job_id,
                "enqueue_time": job.enqueue_time,
                "function": cloudpickle.dumps(job.function),
                "args": cloudpickle.dumps(job.args),
                "kwargs": cloudpickle.dumps(job.kwargs),
                "max_retries": job.max_retries,
                "retry_delay": job.retry_delay,
                "queue_name": job.queue_name,
                "dependency_ids": job.dependency_ids,
                "result_ttl": job.result_ttl,
            }
            return cloudpickle.dumps(payload)
        except Exception as e:
            raise SerializationError(f"Failed to pickle job: {e}") from e
    
    @staticmethod
    def deserialize_job(data: bytes) -> 'Job':
        """Deserialize bytes to a job using cloudpickle."""
        try:
            payload = cloudpickle.loads(data)
            function = cloudpickle.loads(payload["function"])
            args = cloudpickle.loads(payload["args"])
            kwargs = cloudpickle.loads(payload["kwargs"])

            # Create the job with all the saved attributes
            job = Job(
                function=function,
                args=args,
                kwargs=kwargs,
                job_id=payload.get("job_id"),
                enqueue_time=payload.get("enqueue_time"),
                max_retries=payload.get("max_retries", 0),
                retry_delay=payload.get("retry_delay", 0),
                queue_name=payload.get("queue_name"),
                result_ttl=payload.get("result_ttl"),
            )

            # Restore dependency IDs if present
            if "dependency_ids" in payload:
                job.dependency_ids = payload["dependency_ids"]

            return job
        except Exception as e:
            raise SerializationError(f"Failed to unpickle job: {e}") from e
    
    @staticmethod
    def serialize_failed_job(job: 'Job') -> bytes:
        """Serialize a failed job to bytes using cloudpickle."""
        try:
            payload = {
                "job_id": job.job_id,
                "enqueue_time": job.enqueue_time,
                "function_str": getattr(job.function, "__name__", repr(job.function)),
                "args_repr": repr(job.args),
                "kwargs_repr": repr(job.kwargs),
                "max_retries": job.max_retries,
                "retry_delay": job.retry_delay,
                "queue_name": job.queue_name,
                "error": job.error,
                "traceback": job.traceback,
            }
            return cloudpickle.dumps(payload)
        except Exception as e:
            raise SerializationError(f"Failed to pickle failed job details: {e}") from e
    
    @staticmethod
    def serialize_result(result: Any, status: str, error: Optional[str] = None, 
                       traceback_str: Optional[str] = None) -> bytes:
        """Serialize a job result to bytes using cloudpickle."""
        try:
            payload = {
                "status": status,
                "result": result if status == JOB_STATUS_COMPLETED else None,
                "error": error,
                "traceback": traceback_str,
            }
            return cloudpickle.dumps(payload)
        except Exception as e:
            raise SerializationError(f"Failed to pickle result data: {e}") from e
    
    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserialize bytes to a result dictionary using cloudpickle."""
        try:
            return cloudpickle.loads(data)
        except Exception as e:
            raise SerializationError(f"Failed to unpickle result data: {e}") from e


class JsonSerializer:
    """
    JSON serialization placeholder.
    
    This is not fully implemented yet, but the class structure allows for
    easy implementation in the future.
    """
    
    @staticmethod
    def serialize_job(job: 'Job') -> bytes:
        raise NotImplementedError("JSON serialization not fully implemented yet")
    
    @staticmethod
    def deserialize_job(data: bytes) -> 'Job':
        raise NotImplementedError("JSON serialization not fully implemented yet")
    
    @staticmethod
    def serialize_failed_job(job: 'Job') -> bytes:
        raise NotImplementedError("JSON serialization not fully implemented yet")
    
    @staticmethod
    def serialize_result(result: Any, status: str, error: Optional[str] = None, 
                       traceback_str: Optional[str] = None) -> bytes:
        raise NotImplementedError("JSON serialization not fully implemented yet")
    
    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        raise NotImplementedError("JSON serialization not fully implemented yet")


# Factory function to get the appropriate serializer
def get_serializer() -> Serializer:
    """Returns the appropriate serializer based on JOB_SERIALIZER setting."""
    if JOB_SERIALIZER == "pickle":
        return PickleSerializer
    elif JOB_SERIALIZER == "json":
        return JsonSerializer
    else:
        raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")


class Job:
    """
    Represents a job to be executed.
    Includes function, args, kwargs, metadata, retry, and dependency configuration.
    """

    def __init__(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        enqueue_time: Optional[float] = None,
        max_retries: Optional[int] = 0,  # Default: no retries
        retry_delay: RetryDelayType = 0,  # Default: immediate retry (if max_retries > 0)
        queue_name: Optional[str] = None,  # Store the original queue name
        depends_on: Optional[
            Union[str, List[str], "Job", List["Job"]]
        ] = None,  # Job dependencies
        result_ttl: Optional[int] = None,  # TTL for the result in seconds
    ):
        self.job_id: str = job_id or uuid.uuid4().hex
        self.function: Callable = function
        self.args: Tuple[Any, ...] = args or ()
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.enqueue_time: float = enqueue_time or time.time()
        self.max_retries: Optional[int] = max_retries
        self.retry_delay: RetryDelayType = retry_delay
        self.queue_name: Optional[str] = queue_name  # Store the queue name
        self.result_ttl: Optional[int] = result_ttl  # Store result TTL

        # Dependencies tracking
        self.dependency_ids: List[str] = []
        if depends_on is not None:
            if isinstance(depends_on, (str, Job)):
                depends_on = [depends_on]  # Convert single item to list

            for dep in depends_on:
                if isinstance(dep, str):
                    self.dependency_ids.append(dep)  # It's already a job_id
                elif isinstance(dep, Job):
                    self.dependency_ids.append(
                        dep.job_id
                    )  # Extract job_id from Job instance
                else:
                    raise ValueError(
                        f"Invalid dependency type: {type(dep)}. Must be job_id string or Job instance."
                    )

        # Execution result/error state
        self.result: Any = None
        self.error: Optional[str] = None
        self.traceback: Optional[str] = None  # Store traceback on failure

    def execute(self) -> Any:
        """Executes the job, capturing error and traceback."""
        self.error = None
        self.traceback = None
        try:
            self.result = self.function(*self.args, **self.kwargs)
            return self.result
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            self.traceback = traceback.format_exc()  # Capture traceback
            # Re-raise the exception so the worker knows it failed
            raise JobExecutionError(f"Job {self.job_id} failed: {self.error}") from e

    def get_retry_delay(self, attempt: int) -> float:
        """Calculates the retry delay based on the configured strategy."""
        if isinstance(self.retry_delay, (int, float)):
            return float(self.retry_delay)
        elif isinstance(self.retry_delay, Sequence):
            if attempt > 0 and attempt <= len(self.retry_delay):
                # Use the delay specified for this attempt (1-based index)
                return float(self.retry_delay[attempt - 1])
            elif self.retry_delay:
                # If attempt exceeds sequence length, use the last value
                return float(self.retry_delay[-1])
        # Default to 0 if misconfigured or empty sequence
        return 0.0

    def serialize(self) -> bytes:
        """Serializes the job data for sending over NATS."""
        serializer = get_serializer()
        return serializer.serialize_job(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "Job":
        """Deserializes job data received from NATS."""
        serializer = get_serializer()
        return serializer.deserialize_job(data)

    def serialize_failed_job(self) -> bytes:
        """Serializes job data including error info for the failed queue."""
        serializer = get_serializer()
        return serializer.serialize_failed_job(self)
    
    @staticmethod
    def serialize_result(result: Any, status: str, error: Optional[str] = None, 
                       traceback_str: Optional[str] = None) -> bytes:
        """Serializes job result data."""
        serializer = get_serializer()
        return serializer.serialize_result(result, status, error, traceback_str)
    
    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserializes job result data."""
        serializer = get_serializer()
        return serializer.deserialize_result(data)

    def __repr__(self) -> str:
        func_name = getattr(self.function, "__name__", repr(self.function))
        status = "pending"
        if self.error:
            status = "failed"
        elif self.result is not None:  # Crude check for completion
            status = "finished"
        return f"<Job {self.job_id}: {func_name}(...) status={status}>"

    # --- Static method to fetch result ---
    @staticmethod
    async def fetch_result(
        job_id: str,
        nats_url: Optional[str] = None,
    ) -> Any:
        """
        Fetches the result or error information for a completed job from the result backend.

        Args:
            job_id: The ID of the job.
            nats_url: NATS server URL (if not using default).

        Returns:
            The job's result if successful.

        Raises:
            JobNotFoundError: If the job result is not found (or expired).
            JobExecutionError: If the job failed, raises an error containing failure details.
            NaqConnectionError: If connection fails.
            SerializationError: If the stored result cannot be deserialized.
            NaqException: For other errors.
        """
        from nats.js.errors import KeyNotFoundError

        from .connection import (
            close_nats_connection,
            get_jetstream_context,
            get_nats_connection,
        )
        from .exceptions import (
            JobExecutionError,
            JobNotFoundError,
            NaqConnectionError,
            NaqException,
            SerializationError,
        )
        from .settings import RESULT_KV_NAME

        nc = None
        try:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)
            
            try:
                kv = await js.key_value(bucket=RESULT_KV_NAME)
            except Exception as e:
                raise NaqException(
                    f"Result backend KV store '{RESULT_KV_NAME}' not accessible: {e}"
                ) from e

            try:
                entry = await kv.get(job_id.encode("utf-8"))
                result_data = Job.deserialize_result(entry.value)
            except KeyNotFoundError:
                raise JobNotFoundError(
                    f"Result for job {job_id} not found. It may not have completed, failed, or the result expired."
                ) from None

            if result_data.get("status") == JOB_STATUS_FAILED:
                error_str = result_data.get("error", "Unknown error")
                traceback_str = result_data.get("traceback")
                err_msg = f"Job {job_id} failed: {error_str}"
                if traceback_str:
                    err_msg += f"\nTraceback:\n{traceback_str}"
                # Raise an exception containing the failure info
                raise JobExecutionError(err_msg)
            elif result_data.get("status") == JOB_STATUS_COMPLETED:
                return result_data.get("result")
            else:
                # Should not happen if worker stores status correctly
                raise NaqException(
                    f"Job {job_id} found in result store but has unexpected status: {result_data.get('status')}"
                )

        except (JobNotFoundError, JobExecutionError, NaqConnectionError, SerializationError):
            # Re-raise these specific errors without wrapping
            raise
        except Exception as e:
            raise NaqException(f"Error fetching result for job {job_id}: {e}") from e
        finally:
            # Close connection if we opened it here
            if nc:
                await close_nats_connection()  # Use shared close

    @staticmethod
    def fetch_result_sync(job_id: str, nats_url: Optional[str] = None) -> Any:
        """Synchronous version of fetch_result."""
        from .utils import run_async_from_sync

        return run_async_from_sync(Job.fetch_result(job_id, nats_url=nats_url))
