# src/naq/job.py
import uuid
import time
import cloudpickle
import json
import traceback # Import traceback
from typing import Optional, Any, Callable, Tuple, Dict, Union, Sequence

from .settings import JOB_SERIALIZER
from .exceptions import SerializationError

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]

class Job:
    """
    Represents a job to be executed.
    Includes function, args, kwargs, metadata, and retry configuration.
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
    ):
        self.job_id: str = job_id or uuid.uuid4().hex
        self.function: Callable = function
        self.args: Tuple[Any, ...] = args or ()
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.enqueue_time: float = enqueue_time or time.time()
        self.max_retries: Optional[int] = max_retries
        self.retry_delay: RetryDelayType = retry_delay
        self.queue_name: Optional[str] = queue_name # Store the queue name

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
                return cls(
                    function=function,
                    args=args,
                    kwargs=kwargs,
                    job_id=payload.get('job_id'),
                    enqueue_time=payload.get('enqueue_time'),
                    max_retries=payload.get('max_retries', 0),
                    retry_delay=payload.get('retry_delay', 0),
                    queue_name=payload.get('queue_name'),
                )
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
