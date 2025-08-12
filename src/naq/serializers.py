# src/naq/serializers.py
import cloudpickle
import json
import importlib
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, Protocol

from .exceptions import SerializationError
from .models import JOB_STATUS, Job
from .settings import (
    DEFAULT_QUEUE_NAME,
    JOB_SERIALIZER,
    JSON_ENCODER,
    JSON_DECODER,
)


def _normalize_retry_strategy(retry_strategy: Any) -> str:
    """Normalize retry_strategy to a simple string value."""
    if retry_strategy is None:
        return "linear"
    if hasattr(retry_strategy, "value"):
        return retry_strategy.value
    return str(retry_strategy)


class Serializer(Protocol):
    """Protocol defining the interface for job serializers."""

    @staticmethod
    def serialize_job(job: Job) -> bytes:
        """Serialize a job to bytes."""
        ...

    @staticmethod
    def deserialize_job(data: bytes) -> Job:
        """Deserialize bytes to a job."""
        ...

    @staticmethod
    def serialize_failed_job(job: Job) -> bytes:
        """Serialize a failed job to bytes."""
        ...

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
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
            payload = {
                "job_id": job.job_id,
                "enqueue_time": job.enqueue_time,
                "function": cloudpickle.dumps(job.function),
                "args": cloudpickle.dumps(job.args),
                "kwargs": cloudpickle.dumps(job.kwargs),
                "max_retries": job.max_retries,
                "retry_delay": job.retry_delay,
                "queue_name": job.queue_name,
                "depends_on": job.depends_on,
                "result_ttl": job.result_ttl,
                "timeout": job.timeout,
                "retry_strategy": _normalize_retry_strategy(job.retry_strategy),
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
            from .job import Job  # Import here to avoid circular imports

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
            "retry_strategy": _normalize_retry_strategy(job.retry_strategy),
            "retry_on": JsonSerializer._encode_exceptions(job.retry_on),
            "ignore_on": JsonSerializer._encode_exceptions(job.ignore_on),
        }

        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to JSON-serialize job: {e}") from e
        except Exception as e:
            raise SerializationError(f"Unexpected error during JSON serialization: {e}") from e

    @staticmethod
    def deserialize_job(data: bytes) -> "Job":
        _Encoder, Decoder = JsonSerializer._get_json_hooks()
        try:
            payload = json.loads(data.decode("utf-8"), cls=Decoder)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Failed to parse JSON job: {e}") from e
        except Exception as e:
            raise SerializationError(f"Unexpected error during JSON parsing: {e}") from e

        try:
            function = JsonSerializer._resolve_dotted_path(payload["function"])
        except KeyError:
            raise SerializationError("Job data missing required 'function' field") from None
        except Exception as e:
            raise SerializationError(f"Failed to resolve function: {e}") from e

        args = tuple(payload.get("args", []) or [])
        kwargs = payload.get("kwargs", {}) or {}

        retry_on = JsonSerializer._decode_exceptions(payload.get("retry_on"))
        ignore_on = JsonSerializer._decode_exceptions(payload.get("ignore_on"))

        # depends_on is a list of job IDs (strings)
        depends_on = payload.get("depends_on")

        from .settings import RETRY_STRATEGY  # local import to avoid cycles

        retry_strategy = _normalize_retry_strategy(
            payload.get("retry_strategy", RETRY_STRATEGY.LINEAR)
        )

        from .job import Job  # Import here to avoid circular imports

        job = Job(
            function=function,
            args=args,
            kwargs=kwargs,
            job_id=payload.get("job_id"),
            queue_name=payload.get("queue_name") or DEFAULT_QUEUE_NAME,
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
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to JSON-serialize failed job: {e}") from e
        except Exception as e:
            raise SerializationError(f"Unexpected error during JSON failed job serialization: {e}") from e

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        # status to value for storage
        status_value = status.value if hasattr(status, "value") else str(status)
        is_completed = hasattr(status, "value") and status.value == JOB_STATUS.COMPLETED.value
        
        payload = {
            "status": status_value,
            "result": result if is_completed else None,
            "error": error,
            "traceback": traceback_str,
        }
        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to JSON-serialize result: {e}") from e
        except Exception as e:
            raise SerializationError(f"Unexpected error during JSON result serialization: {e}") from e

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        _Encoder, Decoder = JsonSerializer._get_json_hooks()
        try:
            obj = json.loads(data.decode("utf-8"), cls=Decoder)
            return obj
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Failed to parse JSON result: {e}") from e
        except Exception as e:
            raise SerializationError(f"Unexpected error during JSON result parsing: {e}") from e


# Factory function to get the appropriate serializer
def get_serializer() -> Serializer:
    """Returns the appropriate serializer based on JOB_SERIALIZER setting."""
    if JOB_SERIALIZER == "pickle":
        return PickleSerializer
    elif JOB_SERIALIZER == "json":
        return JsonSerializer
    else:
        raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")