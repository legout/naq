# src/naq/job.py
import uuid
import time
import cloudpickle
import json
from typing import Optional, Any, Callable, Tuple, Dict

from .settings import JOB_SERIALIZER
from .exceptions import SerializationError

class Job:
    """
    Represents a job to be executed.
    Includes function, args, kwargs, and metadata.
    """
    def __init__(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        enqueue_time: Optional[float] = None,
    ):
        self.job_id: str = job_id or uuid.uuid4().hex
        self.function: Callable = function
        self.args: Tuple[Any, ...] = args or ()
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.enqueue_time: float = enqueue_time or time.time()
        self.result: Any = None
        self.error: Optional[str] = None

    def execute(self) -> Any:
        """Executes the job."""
        try:
            self.result = self.function(*self.args, **self.kwargs)
            return self.result
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            # Potentially re-raise or handle differently
            raise

    def serialize(self) -> bytes:
        """Serializes the job data for sending over NATS."""
        if JOB_SERIALIZER == 'pickle':
            try:
                # Serialize function, args, kwargs separately for potential future flexibility
                payload = {
                    'job_id': self.job_id,
                    'enqueue_time': self.enqueue_time,
                    'function': cloudpickle.dumps(self.function),
                    'args': cloudpickle.dumps(self.args),
                    'kwargs': cloudpickle.dumps(self.kwargs),
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
                )
            except Exception as e:
                raise SerializationError(f"Failed to unpickle job: {e}") from e
        elif JOB_SERIALIZER == 'json':
            raise NotImplementedError("JSON serialization not fully implemented yet.")
        else:
            raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")

    def __repr__(self) -> str:
        func_name = getattr(self.function, '__name__', repr(self.function))
        return f"<Job {self.job_id}: {func_name}(...)>"
