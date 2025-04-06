# src/naq/job.py
import uuid
import time
import cloudpickle
#import json
import traceback # Import traceback
from typing import Optional, Any, Callable, Tuple, Dict, Union, Sequence, List

from .settings import JOB_SERIALIZER, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED
from .exceptions import SerializationError, JobExecutionError
# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]

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
        max_retries: Optional[int] = 0, # Default: no retries
        retry_delay: RetryDelayType = 0, # Default: immediate retry (if max_retries > 0)
        queue_name: Optional[str] = None, # Store the original queue name
        depends_on: Optional[Union[str, List[str], 'Job', List['Job']]] = None, # Job dependencies
        result_ttl: Optional[int] = None, # TTL for the result in seconds
    ):
        self.job_id: str = job_id or uuid.uuid4().hex
        self.function: Callable = function
        self.args: Tuple[Any, ...] = args or ()
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.enqueue_time: float = enqueue_time or time.time()
        self.max_retries: Optional[int] = max_retries
        self.retry_delay: RetryDelayType = retry_delay
        self.queue_name: Optional[str] = queue_name # Store the queue name
        self.result_ttl: Optional[int] = result_ttl # Store result TTL

        # Dependencies tracking
        self.dependency_ids: List[str] = []
        if depends_on is not None:
            if isinstance(depends_on, (str, Job)):
                depends_on = [depends_on]  # Convert single item to list
            
            for dep in depends_on:
                if isinstance(dep, str):
                    self.dependency_ids.append(dep)  # It's already a job_id
                elif isinstance(dep, Job):
                    self.dependency_ids.append(dep.job_id)  # Extract job_id from Job instance
                else:
                    raise ValueError(f"Invalid dependency type: {type(dep)}. Must be job_id string or Job instance.")

        # Execution result/error state
        self.result: Any = None
        self.error: Optional[str] = None
        self.traceback: Optional[str] = None # Store traceback on failure

    def execute(self) -> Any:
        """Executes the job, capturing error and traceback."""
        self.error = None
        self.traceback = None
        try:
            self.result = self.function(*self.args, **self.kwargs)
            return self.result
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            self.traceback = traceback.format_exc() # Capture traceback
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
        if JOB_SERIALIZER == 'pickle':
            try:
                payload = {
                    'job_id': self.job_id,
                    'enqueue_time': self.enqueue_time,
                    'function': cloudpickle.dumps(self.function),
                    'args': cloudpickle.dumps(self.args),
                    'kwargs': cloudpickle.dumps(self.kwargs),
                    'max_retries': self.max_retries,
                    'retry_delay': self.retry_delay,
                    'queue_name': self.queue_name,
                    'dependency_ids': self.dependency_ids,  # Include dependencies
                    'result_ttl': self.result_ttl, # Include result_ttl
                    # Do not serialize result/error/traceback for initial enqueue
                }
                return cloudpickle.dumps(payload)
            except Exception as e:
                raise SerializationError(f"Failed to pickle job: {e}") from e
        elif JOB_SERIALIZER == 'json':
             # JSON serialization requires function path, not the function object itself
             # This is more complex to implement robustly, requires import mechanisms
             # Sticking to pickle for the prototype for simplicity like RQ does.
             raise NotImplementedError("JSON serialization not fully implemented yet.")
        else:
            raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")

    @classmethod
    def deserialize(cls, data: bytes) -> 'Job':
        """Deserializes job data received from NATS."""
        if JOB_SERIALIZER == 'pickle':
            try:
                payload = cloudpickle.loads(data)
                function = cloudpickle.loads(payload['function'])
                args = cloudpickle.loads(payload['args'])
                kwargs = cloudpickle.loads(payload['kwargs'])
                
                # Create the job with all the saved attributes, including dependencies
                job = cls(
                    function=function,
                    args=args,
                    kwargs=kwargs,
                    job_id=payload.get('job_id'),
                    enqueue_time=payload.get('enqueue_time'),
                    max_retries=payload.get('max_retries', 0),
                    retry_delay=payload.get('retry_delay', 0),
                    queue_name=payload.get('queue_name'),
                    result_ttl=payload.get('result_ttl'), # Deserialize result_ttl
                )
                
                # Restore dependency IDs if present
                if 'dependency_ids' in payload:
                    job.dependency_ids = payload['dependency_ids']
                    
                return job
            except Exception as e:
                raise SerializationError(f"Failed to unpickle job: {e}") from e
        elif JOB_SERIALIZER == 'json':
            raise NotImplementedError("JSON serialization not fully implemented yet.")
        else:
            raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")

    def serialize_failed_job(self) -> bytes:
        """Serializes job data including error info for the failed queue."""
        if JOB_SERIALIZER == 'pickle':
            try:
                # Serialize function/args/kwargs again for the failed record
                payload = {
                    'job_id': self.job_id,
                    'enqueue_time': self.enqueue_time,
                    'function_str': getattr(self.function, '__name__', repr(self.function)), # Store func name
                    'args_repr': repr(self.args), # Store reprs for inspection
                    'kwargs_repr': repr(self.kwargs),
                    'max_retries': self.max_retries,
                    'retry_delay': self.retry_delay,
                    'queue_name': self.queue_name,
                    'error': self.error,
                    'traceback': self.traceback,
                    # Optionally include pickled versions if re-queueing from failed is needed
                    # 'function': cloudpickle.dumps(self.function),
                    # 'args': cloudpickle.dumps(self.args),
                    # 'kwargs': cloudpickle.dumps(self.kwargs),
                }
                return cloudpickle.dumps(payload)
            except Exception as e:
                raise SerializationError(f"Failed to pickle failed job details: {e}") from e
        else:
             raise NotImplementedError("JSON serialization not implemented.")

    def __repr__(self) -> str:
        func_name = getattr(self.function, '__name__', repr(self.function))
        status = "pending"
        if self.error:
            status = "failed"
        elif self.result is not None: # Crude check for completion
             status = "finished"
        return f"<Job {self.job_id}: {func_name}(...) status={status}>"

    # --- Static method to fetch result ---
    @staticmethod
    async def fetch_result(
        job_id: str,
        nats_url: Optional[str] = None,
        # Allow specifying connection/js context directly?
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
        from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
        from .settings import RESULT_KV_NAME
        from .exceptions import JobNotFoundError, JobExecutionError, NaqConnectionError, SerializationError, NaqException
        from nats.js.errors import KeyNotFoundError

        nc = None
        kv = None
        try:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)
            try:
                kv = await js.key_value(bucket=RESULT_KV_NAME)
            except Exception as e:
                 raise NaqException(f"Result backend KV store '{RESULT_KV_NAME}' not accessible: {e}") from e

            entry = await kv.get(job_id.encode('utf-8'))
            result_data = cloudpickle.loads(entry.value)

            if result_data.get('status') == JOB_STATUS_FAILED:
                error_str = result_data.get('error', 'Unknown error')
                traceback_str = result_data.get('traceback')
                err_msg = f"Job {job_id} failed: {error_str}"
                if traceback_str:
                    err_msg += f"\nTraceback:\n{traceback_str}"
                # Raise an exception containing the failure info
                raise JobExecutionError(err_msg)
            elif result_data.get('status') == JOB_STATUS_COMPLETED:
                return result_data.get('result')
            else:
                # Should not happen if worker stores status correctly
                raise NaqException(f"Job {job_id} found in result store but has unexpected status: {result_data.get('status')}")

        except KeyNotFoundError:
            raise JobNotFoundError(f"Result for job {job_id} not found. It may not have completed, failed, or the result expired.") from None
        except SerializationError: # Re-raise specific error
             raise
        except NaqConnectionError: # Re-raise specific error
             raise
        except JobExecutionError: # Re-raise specific error
             raise
        except Exception as e:
            raise NaqException(f"Error fetching result for job {job_id}: {e}") from e
        finally:
            # Close connection if we opened it here
            if nc:
                 await close_nats_connection() # Use shared close

    @staticmethod
    def fetch_result_sync(job_id: str, nats_url: Optional[str] = None) -> Any:
        """Synchronous version of fetch_result."""
        from .utils import run_async_from_sync
        return run_async_from_sync(Job.fetch_result(job_id, nats_url=nats_url))
