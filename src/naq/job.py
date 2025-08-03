# src/naq/job.py
import asyncio
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, Protocol

from nats.js import JetStreamContext

# src/naq/job_status.py
import cloudpickle
import json
import importlib
from dataclasses import asdict, is_dataclass

from .exceptions import SerializationError
from .settings import JOB_SERIALIZER, JOB_STATUS, JSON_ENCODER, JSON_DECODER

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


class Serializer(Protocol):
    """Protocol defining the interface for job serializers."""

    @staticmethod
    def serialize_job(job: "Job") -> bytes:
        """Serialize a job to bytes."""
        ...

    @staticmethod
    def deserialize_job(data: bytes) -> "Job":
        """Deserialize bytes to a job."""
        ...

    @staticmethod
    def serialize_failed_job(job: "Job") -> bytes:
        """Serialize a failed job to bytes."""
        ...

    @staticmethod
    def serialize_result(
        result: Any,
        status: str,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        """Serialize a job result to bytes."""
        ...

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserialize bytes to a result dictionary."""
        ...


class PickleSerializer:
    """Serializes jobs and results using cloudpickle."""

    @staticmethod
    def serialize_job(job: "Job") -> bytes:
        """Serialize a job to bytes using cloudpickle."""
        try:
            # Ensure retry_strategy is stored as a plain string
            rs = getattr(job, "retry_strategy", None)
            if rs is not None and hasattr(rs, "value"):
                rs = rs.value
            elif rs is not None:
                rs = str(rs)

            payload = {
                "job_id": job.job_id,
                "enqueue_time": job.enqueue_time,
                "function": cloudpickle.dumps(job.function),
                "args": cloudpickle.dumps(job.args),
                "kwargs": cloudpickle.dumps(job.kwargs),
                "max_retries": job.max_retries,
                "retry_delay": job.retry_delay,
                "queue_name": job.queue_name,
                # "dependency_ids": job.dependency_ids,
                "depends_on": job.depends_on,
                "result_ttl": job.result_ttl,
                "timeout": job.timeout,
                "retry_strategy": rs,
                "retry_on": [
                    exc.__name__ if isinstance(exc, type) else str(exc)
                    for exc in getattr(job, "retry_on", []) or []
                ],
                "ignore_on": [
                    exc.__name__ if isinstance(exc, type) else str(exc)
                    for exc in getattr(job, "ignore_on", []) or []
                ],
            }
            return cloudpickle.dumps(payload)
        except Exception as e:
            raise SerializationError(f"Failed to pickle job: {e}") from e

    @staticmethod
    def deserialize_job(data: bytes) -> "Job":
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
                queue_name=payload.get("queue_name"),
                max_retries=payload.get("max_retries", 0),
                retry_delay=payload.get("retry_delay", 0),
                retry_strategy=payload.get("retry_strategy"),
                retry_on=payload.get("retry_on"),
                ignore_on=payload.get("ignore_on"),
                depends_on=payload.get("depends_on"),
                result_ttl=payload.get("result_ttl"),
                timeout=payload.get("timeout"),
            )

            # Restore dependency IDs if present
            # if "dependency_ids" in payload:
            #    job.dependency_ids = payload["dependency_ids"]

            return job
        except Exception as e:
            raise SerializationError(f"Failed to unpickle job: {e}") from e

    @staticmethod
    def serialize_failed_job(job: "Job") -> bytes:
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
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        """Serialize a job result to bytes using cloudpickle."""

        try:
            payload = {
                "status": status,
                "result": result if status == JOB_STATUS.COMPLETED else None,
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
    Secure JSON serializer.

    - Functions and exception classes are encoded as import paths (module:qualname).
    - Only JSON-safe structures are stored; no code execution on (de)serialization.
    """

    @staticmethod
    def _resolve_dotted_path(path: str) -> Any:
        try:
            module_path, attr = path.split(":", 1)
        except ValueError:
            # backwards compatibility if dot-only: module.attr
            parts = path.rsplit(".", 1)
            if len(parts) != 2:
                raise SerializationError(f"Invalid import path: {path}")
            module_path, attr = parts
        try:
            module = importlib.import_module(module_path)
            obj = module
            for part in attr.split("."):
                obj = getattr(obj, part)
            return obj
        except Exception as e:
            raise SerializationError(f"Could not import '{path}': {e}") from e

    @staticmethod
    def _qualname(obj: Any) -> str:
        module = getattr(obj, "__module__", None)
        qualname = getattr(obj, "__qualname__", getattr(obj, "__name__", None))
        if not module or not qualname:
            raise SerializationError(f"Object is not importable: {obj!r}")
        return f"{module}:{qualname}"

    @staticmethod
    def _encode_args_kwargs(
        args: Tuple, kwargs: Dict
    ) -> Tuple[List[Any], Dict[str, Any]]:
        def make_jsonable(x: Any) -> Any:
            if is_dataclass(x):
                return asdict(x)
            if isinstance(x, (str, int, float, bool)) or x is None:
                return x
            if isinstance(x, (list, tuple)):
                return [make_jsonable(i) for i in x]
            if isinstance(x, dict):
                return {str(k): make_jsonable(v) for k, v in x.items()}
            # Fallback: repr to avoid unsafe serialization
            return {"__repr__": repr(x)}

        return make_jsonable(args), make_jsonable(kwargs)

    @staticmethod
    def _encode_exceptions(
        exc_tuple: Optional[Tuple[Exception, ...]],
    ) -> Optional[List[str]]:
        if not exc_tuple:
            return None
        paths: List[str] = []
        for exc in exc_tuple:
            if not isinstance(exc, type) or not issubclass(exc, BaseException):
                raise SerializationError(
                    "retry_on/ignore_on must be exception classes when using JSON serializer"
                )
            paths.append(JsonSerializer._qualname(exc))
        return paths

    @staticmethod
    def _decode_exceptions(
        exc_paths: Optional[List[str]],
    ) -> Optional[Tuple[type, ...]]:
        if not exc_paths:
            return None
        types: List[type] = []
        for path in exc_paths:
            exc = JsonSerializer._resolve_dotted_path(path)
            if not isinstance(exc, type) or not issubclass(exc, BaseException):
                raise SerializationError(f"Imported '{path}' is not an Exception type")
            types.append(exc)
        return tuple(types)

    @staticmethod
    def _get_json_hooks():
        # Resolve encoder/decoder classes from settings; fallback to stdlib
        try:
            enc = JsonSerializer._resolve_dotted_path(JSON_ENCODER)
        except Exception:
            import json as _json

            enc = _json.JSONEncoder
        try:
            dec = JsonSerializer._resolve_dotted_path(JSON_DECODER)
        except Exception:
            import json as _json

            dec = _json.JSONDecoder
        return enc, dec

    @staticmethod
    def serialize_job(job: "Job") -> bytes:
        try:
            func_path = JsonSerializer._qualname(job.function)
        except SerializationError as e:
            # Do not allow pickling fallback for security
            raise SerializationError(
                f"JSON serializer requires importable function: {e}"
            ) from e

        args_json, kwargs_json = JsonSerializer._encode_args_kwargs(
            job.args, job.kwargs
        )

        # Ensure retry_strategy is a simple string
        rs = job.retry_strategy
        if rs is not None and hasattr(rs, "value"):
            rs = rs.value
        elif rs is not None:
            rs = str(rs)

        payload = {
            "job_id": job.job_id,
            "enqueue_time": job.enqueue_time,
            "function": func_path,
            "args": args_json,
            "kwargs": kwargs_json,
            "max_retries": job.max_retries,
            "retry_delay": job.retry_delay,
            "queue_name": job.queue_name,
            "depends_on": job.dependency_ids,  # store as list of IDs
            "result_ttl": job.result_ttl,
            "timeout": job.timeout,
            "retry_strategy": rs,
            "retry_on": JsonSerializer._encode_exceptions(job.retry_on),
            "ignore_on": JsonSerializer._encode_exceptions(job.ignore_on),
        }

        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except Exception as e:
            raise SerializationError(f"Failed to JSON-serialize job: {e}") from e

    @staticmethod
    def deserialize_job(data: bytes) -> "Job":
        _Encoder, Decoder = JsonSerializer._get_json_hooks()
        try:
            payload = json.loads(data.decode("utf-8"), cls=Decoder)
        except Exception as e:
            raise SerializationError(f"Failed to parse JSON job: {e}") from e

        try:
            function = JsonSerializer._resolve_dotted_path(payload["function"])
        except Exception as e:
            raise SerializationError(f"Failed to resolve function: {e}") from e

        args = tuple(payload.get("args", []) or [])
        kwargs = payload.get("kwargs", {}) or {}

        retry_on = JsonSerializer._decode_exceptions(payload.get("retry_on"))
        ignore_on = JsonSerializer._decode_exceptions(payload.get("ignore_on"))

        # depends_on is a list of job IDs (strings)
        depends_on = payload.get("depends_on")

        from .settings import RETRY_STRATEGY  # local import to avoid cycles

        retry_strategy = payload.get("retry_strategy", RETRY_STRATEGY.LINEAR)
        # Normalize to string for Job construction
        if retry_strategy is None:
            retry_strategy = "linear"
        elif hasattr(retry_strategy, "value"):
            retry_strategy = retry_strategy.value
        else:
            retry_strategy = str(retry_strategy)

        job = Job(
            function=function,
            args=args,
            kwargs=kwargs,
            job_id=payload.get("job_id"),
            queue_name=payload.get("queue_name") or "default",
            max_retries=payload.get("max_retries", 0),
            retry_delay=payload.get("retry_delay", 0),
            retry_strategy=retry_strategy,
            retry_on=retry_on,
            ignore_on=ignore_on,
            depends_on=depends_on,
            result_ttl=payload.get("result_ttl"),
            timeout=payload.get("timeout"),
        )
        return job

    @staticmethod
    def serialize_failed_job(job: "Job") -> bytes:
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
        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except Exception as e:
            raise SerializationError(f"Failed to JSON-serialize failed job: {e}") from e

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        # status to value for storage
        payload = {
            "status": status.value if hasattr(status, "value") else str(status),
            "result": result
            if (hasattr(status, "value") and status.value == JOB_STATUS.COMPLETED.value)
            else None,
            "error": error,
            "traceback": traceback_str,
        }
        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except Exception as e:
            raise SerializationError(f"Failed to JSON-serialize result: {e}") from e

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        _Encoder, Decoder = JsonSerializer._get_json_hooks()
        try:
            obj = json.loads(data.decode("utf-8"), cls=Decoder)
            return obj
        except Exception as e:
            raise SerializationError(f"Failed to parse JSON result: {e}") from e


# Factory function to get the appropriate serializer
def get_serializer() -> Serializer:
    """Returns the appropriate serializer based on JOB_SERIALIZER setting."""
    if JOB_SERIALIZER == "pickle":
        return PickleSerializer
    elif JOB_SERIALIZER == "json":
        return JsonSerializer
    else:
        raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")


# Define retry strategies
from .settings import RETRY_STRATEGY

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}


class Job:
    """Represents a job to be executed."""

    def __init__(
        self,
        function: Callable,
        args: Tuple = (),
        kwargs: Dict = None,
        job_id: Optional[str] = None,
        queue_name: str = "default",
        max_retries: int = 0,
        retry_delay: RetryDelayType = 0,
        retry_strategy: str = RETRY_STRATEGY.LINEAR,
        retry_on: Optional[Tuple[Exception, ...]] = None,
        ignore_on: Optional[Tuple[Exception, ...]] = None,
        depends_on: Optional[Union[str, List[str], "Job", List["Job"]]] = None,
        result_ttl: Optional[int] = None,
        timeout: Optional[int] = None,
    ):
        """
        Initialize a Job.

        Args:
            function: The function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            job_id: Optional job ID (generated if not provided)
            queue_name: Name of the queue this job belongs to
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries (seconds)
            retry_strategy: Strategy for retry delays ('linear' or 'exponential')
            retry_on: Tuple of exception types to retry on
            ignore_on: Tuple of exception types to not retry on
            depends_on: Job ID(s) that must complete before this one

        Raises:
            ValueError: If retry_strategy is invalid
        """
        # Normalize enum or other inputs to a canonical lowercase string
        if hasattr(retry_strategy, "value"):
            retry_strategy = retry_strategy.value
        else:
            retry_strategy = str(retry_strategy).lower()

        if retry_strategy not in VALID_RETRY_STRATEGIES:
            raise ValueError(
                f"Invalid retry strategy '{retry_strategy}'. "
                f"Must be one of: {', '.join(sorted(VALID_RETRY_STRATEGIES))}"
            )

        self.job_id = job_id or str(uuid.uuid4()).replace("-", "")
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.queue_name = queue_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_strategy = retry_strategy
        self.retry_on = retry_on
        self.ignore_on = ignore_on
        self.depends_on = depends_on
        self.result_ttl = result_ttl
        self.timeout = timeout

        self.enqueue_time = time.time()
        self.error = None
        self.traceback = None
        self._retry_count = 0
        self._start_time: Optional[float] = None
        self._finish_time: Optional[float] = None

    @property
    def status(self) -> JOB_STATUS:
        """Return the current status of the job."""
        if self._start_time is None:
            return JOB_STATUS.PENDING
        if self._finish_time is None:
            return JOB_STATUS.RUNNING
        return JOB_STATUS.FAILED if self.error is not None else JOB_STATUS.COMPLETED

    @property
    def dependency_ids(self) -> List[str]:
        """Return a list of dependency job IDs."""
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
        """Get the current retry count."""
        return self._retry_count

    def increment_retry_count(self) -> None:
        """Increment the retry count."""
        self._retry_count += 1

    def should_retry(self, exc: Exception) -> bool:
        """
        Determine if the job should be retried based on the exception.

        Args:
            exc: The exception that caused the job to fail

        Returns:
            True if the job should be retried, False otherwise
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
            Delay in seconds before the next retry
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
        """Execute the job's function and manage its state.

        Returns:
            The function's return value.

        Raises:
            Any exception that the function raises and isn't handled by retry logic.
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
        """Serializes the job data for sending over NATS."""
        serializer = get_serializer()
        return serializer.serialize_job(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "Job":
        """Deserializes job data received from NATS."""
        serializer = get_serializer()
        # Simplified per improvement plan: trust serializer to return a Job
        return serializer.deserialize_job(data)

    def serialize_failed_job(self) -> bytes:
        """Serializes job data including error info for the failed queue."""
        serializer = get_serializer()
        return serializer.serialize_failed_job(self)

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        """Serializes job result data."""
        serializer = get_serializer()
        return serializer.serialize_result(result, status, error, traceback_str)

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserializes job result data."""
        serializer = get_serializer()
        return serializer.deserialize_result(data)

    def __repr__(self) -> str:
        """Basic string representation without fetching status."""
        func_name = getattr(self.function, "__name__", repr(self.function))
        return f"<Job {self.job_id}: {func_name}(...)>"

    # --- Static method to fetch result ---
    @staticmethod
    async def _fetch_result_data(
        job_id: str,
        js: Optional[JetStreamContext] = None,
        nats_url: Optional[str] = None,
    ) -> Any:
        """Fetches the result or error information for a completed job from the result backend."""
        from nats.js.errors import KeyNotFoundError

        from .connection import (
            close_nats_connection,
            get_jetstream_context,
            get_nats_connection,
        )
        from .exceptions import (
            JobNotFoundError,
            NaqException,
        )
        from .settings import RESULT_KV_NAME

        nc = None
        should_close = False
        if not js:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)
            should_close = True

        try:
            kv = await js.key_value(bucket=RESULT_KV_NAME)
        except Exception as e:
            if should_close and nc:
                await close_nats_connection()
            raise NaqException(
                f"Result backend KV store '{RESULT_KV_NAME}' not accessible: {e}"
            ) from e

        try:
            # Keys in NATS KV APIs should be str, not bytes
            entry = await kv.get(job_id)
            result_data = Job.deserialize_result(entry.value)
            return result_data
        except KeyNotFoundError:
            raise JobNotFoundError(
                f"Result for job {job_id} not found. It may not have completed, failed, or the result expired."
            ) from None
        finally:
            if should_close and nc:
                await close_nats_connection()

    @staticmethod
    def _fetch_result_data_sync(
        job_id: str,
        nats_url: Optional[str] = None,
    ) -> Any:
        """
        DEPRECATED: Use _fetch_result_data instead.

        This synchronous version can cause issues with asyncio event loops.
        Use the async version with proper connection management instead.
        """
        from warnings import warn

        warn(
            "_fetch_result_data_sync is deprecated. Use _fetch_result_data instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        from .utils import run_async_from_sync

        return run_async_from_sync(Job._fetch_result_data, job_id, nats_url=nats_url)

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
        # from nats.js.errors import KeyNotFoundError

        from .connection import (
            close_nats_connection,
            get_nats_connection,
        )
        from .exceptions import (
            JobExecutionError,
            JobNotFoundError,
            NaqConnectionError,
            NaqException,
            SerializationError,
        )

        nc = None
        try:
            nc = await get_nats_connection(url=nats_url)
            result_data = await Job._fetch_result_data(
                job_id=job_id,
                nats_url=nats_url,
            )

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
        finally:
            # Close connection if we opened it here
            if nc:
                await close_nats_connection()  # Use shared close

    @staticmethod
    def fetch_result_sync(job_id: str, nats_url: Optional[str] = None) -> Any:
        """
        DEPRECATED: Use fetch_result instead.

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
