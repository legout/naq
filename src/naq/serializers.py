# src/naq/serializers.py
"""
Job serialization module for NAQ.

This module provides two serialization strategies with different security implications:

## Security Comparison

```mermaid
graph TD
    subgraph PickleSerializer [PickleSerializer - High Risk]
        direction LR
        A[Untrusted Data] --> B{cloudpickle.loads}
        B --> C[Arbitrary Code Execution]
        C --> D[Security Vulnerability]
    end

    subgraph JsonSerializer [JsonSerializer - Secure]
        direction LR
        E[Untrusted Data] --> F{json.loads}
        F --> G[Safe Data Processing]
        G --> H[No Code Execution]
    end

    style PickleSerializer fill:#fdd,stroke:#f00,stroke-width:2px
    style JsonSerializer fill:#dfd,stroke:#0f0,stroke-width:2px
```

### PickleSerializer (cloudpickle)
- **Security Risk**: Can execute arbitrary code during deserialization
- **Use Case**: Trusted environments only (e.g., internal systems)
- **Performance**: Fast, preserves all Python object types
- **Warning**: Never use with untrusted data sources

### JsonSerializer
- **Security**: Safe for untrusted data, no code execution
- **Use Case**: Production systems, external data sources
- **Limitations**: Only JSON-serializable data types
- **Recommendation**: Preferred for most production deployments

### Choosing a Serializer
- Use `JsonSerializer` for production systems
- Use `PickleSerializer` only in trusted, internal environments
- Consider the data types you need to serialize
- Evaluate the security requirements of your deployment

## Data Integrity Features

Both serializers support optional data integrity verification through checksums and signatures:

### Checksum Verification
- **Purpose**: Detect accidental data corruption during storage/transmission
- **Algorithm**: Configurable (MD5, SHA256, SHA512, etc.)
- **Performance**: Minimal overhead during serialization/deserialization
- **Use Case**: General data integrity protection

### HMAC Signature Verification
- **Purpose**: Detect both accidental corruption and malicious tampering
- **Algorithm**: HMAC with configurable hash algorithm
- **Security**: Requires a secret key for signature generation/verification
- **Use Case**: Environments where data authenticity is critical

### Configuration
Enable integrity features through environment variables:

```bash
# Enable checksum verification
export NAQ_SERIALIZATION_CHECKSUM_ENABLED=true

# Set checksum algorithm (default: sha256)
export NAQ_SERIALIZATION_CHECKSUM_ALGORITHM=sha512

# Set HMAC signature key (optional)
export NAQ_SERIALIZATION_SIGNATURE_KEY="your-secret-key"
```

### Backward Compatibility
- Integrity features are optional and disabled by default
- Serialized data without integrity metadata can still be deserialized
- Graceful fallback ensures compatibility with existing deployments
"""

import base64
import cloudpickle
import hashlib
import hmac
import importlib
import json
import msgspec
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Tuple, Protocol

from .exceptions import SerializationError
from .models.enums import JOB_STATUS, RETRY_STRATEGY
from .models.jobs import Job, JobResult
from .settings import (
    DEFAULT_QUEUE_NAME,
    JOB_SERIALIZER,
    JSON_ENCODER,
    JSON_DECODER,
    PICKLE_DEBUG_LOGGING_ENABLED,
    PICKLE_DEBUG_LOGGING_LEVEL,
    PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS,
    SERIALIZATION_CHECKSUM_ENABLED,
    SERIALIZATION_CHECKSUM_ALGORITHM,
    SERIALIZATION_SIGNATURE_KEY,
    SERIALIZATION_MAX_SIZE_BYTES,
)

import asyncio
import loguru


def _normalize_retry_strategy(retry_strategy: Any) -> str:
    """Normalize retry_strategy to a simple string value."""
    if retry_strategy is None:
        return "linear"
    if hasattr(retry_strategy, "value"):
        return retry_strategy.value
    return str(retry_strategy)


def _calculate_checksum(data: bytes, algorithm: str = "sha256") -> str:
    """
    Calculate a checksum for the given data using the specified algorithm.

    Args:
        data: The data to calculate the checksum for
        algorithm: The hash algorithm to use (e.g., "md5", "sha256", "sha512")

    Returns:
        The hexadecimal digest of the checksum

    Raises:
        SerializationError: If the algorithm is not supported
    """
    try:
        hash_func = getattr(hashlib, algorithm.lower())
        return hash_func(data).hexdigest()
    except AttributeError:
        raise SerializationError(f"Unsupported checksum algorithm: {algorithm}")


def _calculate_signature(data: bytes, key: str, algorithm: str = "sha256") -> str:
    """
    Calculate an HMAC signature for the given data using the specified key and algorithm.

    Args:
        data: The data to calculate the signature for
        key: The secret key to use for the signature
        algorithm: The hash algorithm to use (e.g., "md5", "sha256", "sha512")

    Returns:
        The hexadecimal digest of the signature

    Raises:
        SerializationError: If the algorithm is not supported
    """
    try:
        # Convert key to bytes if it's a string
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        hash_func = getattr(hashlib, algorithm.lower())
        return hmac.new(key_bytes, data, hash_func).hexdigest()
    except AttributeError:
        raise SerializationError(f"Unsupported signature algorithm: {algorithm}")


def _verify_checksum(
    data: bytes, expected_checksum: str, algorithm: str = "sha256"
) -> bool:
    """
    Verify that the data matches the expected checksum.

    Args:
        data: The data to verify
        expected_checksum: The expected checksum value
        algorithm: The hash algorithm used to calculate the checksum

    Returns:
        True if the checksum is valid, False otherwise
    """
    try:
        actual_checksum = _calculate_checksum(data, algorithm)
        return hmac.compare_digest(actual_checksum, expected_checksum)
    except Exception:
        return False


def _verify_signature(
    data: bytes, expected_signature: str, key: str, algorithm: str = "sha256"
) -> bool:
    """
    Verify that the data matches the expected signature.

    Args:
        data: The data to verify
        expected_signature: The expected signature value
        key: The secret key used for the signature
        algorithm: The hash algorithm used to calculate the signature

    Returns:
        True if the signature is valid, False otherwise
    """
    try:
        actual_signature = _calculate_signature(data, key, algorithm)
        return hmac.compare_digest(actual_signature, expected_signature)
    except Exception:
        return False


def _validate_serialized_data_size(
    data: bytes, data_type: str = "serialized data"
) -> None:
    """
    Validate that the serialized data size is within the configured limits.

    Args:
        data: The serialized data to validate
        data_type: Description of the data type for error messages

    Raises:
        SerializationError: If the data size exceeds the configured limit
    """
    # Skip validation if size limit is disabled (0)
    if SERIALIZATION_MAX_SIZE_BYTES == 0:
        return

    data_size = len(data)
    if data_size > SERIALIZATION_MAX_SIZE_BYTES:
        raise SerializationError(
            f"{data_type} size ({data_size} bytes) exceeds maximum allowed size "
            f"({SERIALIZATION_MAX_SIZE_BYTES} bytes). "
            f"Consider increasing NAQ_SERIALIZATION_MAX_SIZE_BYTES or reducing data size."
        )


def _add_integrity_metadata(data: bytes, for_json: bool = False) -> Dict[str, Any]:
    """
    Add integrity metadata (checksum and/or signature) to serialized data.

    Args:
        data: The serialized data
        for_json: Whether the metadata will be JSON serialized (requires base64 encoding)

    Returns:
        A dictionary containing the original data and integrity metadata
    """
    metadata = {}

    if for_json:
        # For JSON serialization, encode bytes as base64
        metadata["data"] = base64.b64encode(data).decode("ascii")
        metadata["data_encoding"] = "base64"
    else:
        # For pickle serialization, store bytes directly
        metadata["data"] = data

    if SERIALIZATION_CHECKSUM_ENABLED:
        metadata["checksum"] = _calculate_checksum(
            data, SERIALIZATION_CHECKSUM_ALGORITHM
        )
        metadata["checksum_algorithm"] = SERIALIZATION_CHECKSUM_ALGORITHM

        if SERIALIZATION_SIGNATURE_KEY:
            metadata["signature"] = _calculate_signature(
                data, SERIALIZATION_SIGNATURE_KEY, SERIALIZATION_CHECKSUM_ALGORITHM
            )

    return metadata


def _verify_integrity_metadata(
    metadata: Dict[str, Any], for_json: bool = False
) -> bytes:
    """
    Verify the integrity of serialized data using checksum and/or signature.

    Args:
        metadata: Dictionary containing the data and integrity metadata
        for_json: Whether the metadata was JSON serialized (requires base64 decoding)

    Returns:
        The original data if verification passes

    Raises:
        SerializationError: If verification fails or metadata is invalid
    """
    if "data" not in metadata:
        raise SerializationError("Missing 'data' field in integrity metadata")

    # Extract data, handling both raw bytes and base64 encoding
    data_field = metadata["data"]
    if isinstance(data_field, str) and metadata.get("data_encoding") == "base64":
        try:
            data = base64.b64decode(data_field.encode("ascii"))
        except Exception as e:
            raise SerializationError(f"Failed to decode base64 data: {e}") from e
    elif isinstance(data_field, bytes):
        data = data_field
    else:
        raise SerializationError(
            f"Invalid data type in integrity metadata: {type(data_field)}"
        )

    # Verify checksum if enabled and present
    if SERIALIZATION_CHECKSUM_ENABLED and "checksum" in metadata:
        checksum_algorithm = metadata.get(
            "checksum_algorithm", SERIALIZATION_CHECKSUM_ALGORITHM
        )
        if not _verify_checksum(data, metadata["checksum"], checksum_algorithm):
            raise SerializationError("Data integrity check failed: checksum mismatch")

    # Verify signature if enabled and present
    if (
        SERIALIZATION_CHECKSUM_ENABLED
        and SERIALIZATION_SIGNATURE_KEY
        and "signature" in metadata
    ):
        signature_algorithm = metadata.get(
            "checksum_algorithm", SERIALIZATION_CHECKSUM_ALGORITHM
        )
        if not _verify_signature(
            data,
            metadata["signature"],
            SERIALIZATION_SIGNATURE_KEY,
            signature_algorithm,
        ):
            raise SerializationError("Data integrity check failed: signature mismatch")

    return data


def _validate_deserialized_job_payload(
    payload: Dict[str, Any], serializer_type: str = "pickle"
) -> None:
    """
    Validate the deserialized job payload to ensure it contains valid and safe data.

    Args:
        payload: The deserialized job payload dictionary
        serializer_type: Type of serializer ("pickle" or "json") for context in error messages

    Raises:
        SerializationError: If the payload contains invalid or unsafe data
    """
    # Validate required fields exist
    required_fields = ["job_id", "function", "args", "kwargs"]
    for field in required_fields:
        if field not in payload:
            raise SerializationError(
                f"Missing required field in {serializer_type} job payload: {field}"
            )

    # Validate job_id is a non-empty string
    if not isinstance(payload["job_id"], str):
        raise SerializationError(
            f"job_id must be a string in {serializer_type} job payload, got {type(payload['job_id'])}"
        )
    if not payload["job_id"].strip():
        raise SerializationError(
            f"job_id cannot be empty in {serializer_type} job payload"
        )

    # Validate function based on serializer type
    if serializer_type == "pickle":
        # For pickle, function should be bytes (pickled function)
        if not isinstance(payload["function"], bytes):
            raise SerializationError(
                f"function must be pickled bytes in {serializer_type} job payload, got {type(payload['function'])}"
            )
    elif serializer_type == "json":
        # For JSON, function should be a string (import path)
        if not isinstance(payload["function"], str):
            raise SerializationError(
                f"function must be a string (import path) in {serializer_type} job payload, got {type(payload['function'])}"
            )
        if not payload["function"].strip():
            raise SerializationError(
                f"function path cannot be empty in {serializer_type} job payload"
            )

    # Validate args and kwargs based on serializer type
    if serializer_type == "pickle":
        # For pickle, args and kwargs should be bytes (pickled)
        if not isinstance(payload["args"], bytes):
            raise SerializationError(
                f"args must be pickled bytes in {serializer_type} job payload, got {type(payload['args'])}"
            )
        if not isinstance(payload["kwargs"], bytes):
            raise SerializationError(
                f"kwargs must be pickled bytes in {serializer_type} job payload, got {type(payload['kwargs'])}"
            )
    elif serializer_type == "json":
        # For JSON, args should be a list and kwargs should be a dict
        if not isinstance(payload["args"], list):
            raise SerializationError(
                f"args must be a list in {serializer_type} job payload, got {type(payload['args'])}"
            )
        if not isinstance(payload["kwargs"], dict):
            raise SerializationError(
                f"kwargs must be a dict in {serializer_type} job payload, got {type(payload['kwargs'])}"
            )

    # Validate numeric fields are non-negative
    numeric_fields = ["max_retries", "retry_delay", "result_ttl", "timeout"]
    for field in numeric_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], (int, float)):
                raise SerializationError(
                    f"{field} must be numeric in {serializer_type} job payload, got {type(payload[field])}"
                )
            if payload[field] < 0:
                raise SerializationError(
                    f"{field} must be non-negative in {serializer_type} job payload, got {payload[field]}"
                )

    # Validate string fields
    string_fields = ["queue_name", "retry_strategy"]
    for field in string_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], str):
                raise SerializationError(
                    f"{field} must be a string in {serializer_type} job payload, got {type(payload[field])}"
                )

    # Validate list fields
    list_fields = ["depends_on", "retry_on", "ignore_on"]
    for field in list_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], list):
                raise SerializationError(
                    f"{field} must be a list in {serializer_type} job payload, got {type(payload[field])}"
                )

    # Validate enqueue_time if present
    if "enqueue_time" in payload and payload["enqueue_time"] is not None:
        if not isinstance(payload["enqueue_time"], (int, float)):
            raise SerializationError(
                f"enqueue_time must be numeric in {serializer_type} job payload, got {type(payload['enqueue_time'])}"
            )
        if payload["enqueue_time"] < 0:
            raise SerializationError(
                f"enqueue_time must be non-negative in {serializer_type} job payload, got {payload['enqueue_time']}"
            )


def _validate_deserialized_result_payload(
    payload: Dict[str, Any], serializer_type: str = "pickle"
) -> None:
    """
    Validate the deserialized result payload to ensure it contains valid and safe data.

    Args:
        payload: The deserialized result payload dictionary
        serializer_type: Type of serializer ("pickle" or "json") for context in error messages

    Raises:
        SerializationError: If the payload contains invalid or unsafe data
    """
    # Validate required fields exist
    required_fields = ["status"]
    for field in required_fields:
        if field not in payload:
            raise SerializationError(
                f"Missing required field in {serializer_type} result payload: {field}"
            )

    # Validate status is a string
    if not isinstance(payload["status"], str):
        raise SerializationError(
            f"status must be a string in {serializer_type} result payload, got {type(payload['status'])}"
        )
    if not payload["status"].strip():
        raise SerializationError(
            f"status cannot be empty in {serializer_type} result payload"
        )

    # Validate string fields
    string_fields = ["error", "traceback"]
    for field in string_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], str):
                raise SerializationError(
                    f"{field} must be a string in {serializer_type} result payload, got {type(payload[field])}"
                )

    # Validate that result is only present when status is 'completed'
    if "result" in payload and payload["result"] is not None:
        if payload["status"] != "completed":
            raise SerializationError(
                f"result should only be present when status is 'completed' in {serializer_type} result payload"
            )


def _validate_deserialized_failed_job_payload(
    payload: Dict[str, Any], serializer_type: str = "pickle"
) -> None:
    """
    Validate the deserialized failed job payload to ensure it contains valid and safe data.

    Args:
        payload: The deserialized failed job payload dictionary
        serializer_type: Type of serializer ("pickle" or "json") for context in error messages

    Raises:
        SerializationError: If the payload contains invalid or unsafe data
    """
    # Validate required fields exist
    required_fields = ["job_id", "function_str", "args_repr", "kwargs_repr"]
    for field in required_fields:
        if field not in payload:
            raise SerializationError(
                f"Missing required field in {serializer_type} failed job payload: {field}"
            )

    # Validate job_id is a non-empty string
    if not isinstance(payload["job_id"], str):
        raise SerializationError(
            f"job_id must be a string in {serializer_type} failed job payload, got {type(payload['job_id'])}"
        )
    if not payload["job_id"].strip():
        raise SerializationError(
            f"job_id cannot be empty in {serializer_type} failed job payload"
        )

    # Validate string fields
    string_fields = ["function_str", "args_repr", "kwargs_repr", "error", "traceback"]
    for field in string_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], str):
                raise SerializationError(
                    f"{field} must be a string in {serializer_type} failed job payload, got {type(payload[field])}"
                )

    # Validate numeric fields are non-negative
    numeric_fields = ["max_retries", "retry_delay"]
    for field in numeric_fields:
        if field in payload and payload[field] is not None:
            if not isinstance(payload[field], (int, float)):
                raise SerializationError(
                    f"{field} must be numeric in {serializer_type} failed job payload, got {type(payload[field])}"
                )
            if payload[field] < 0:
                raise SerializationError(
                    f"{field} must be non-negative in {serializer_type} failed job payload, got {payload[field]}"
                )

    # Validate enqueue_time if present
    if "enqueue_time" in payload and payload["enqueue_time"] is not None:
        if not isinstance(payload["enqueue_time"], (int, float)):
            raise SerializationError(
                f"enqueue_time must be numeric in {serializer_type} failed job payload, got {type(payload['enqueue_time'])}"
            )
        if payload["enqueue_time"] < 0:
            raise SerializationError(
                f"enqueue_time must be non-negative in {serializer_type} failed job payload, got {payload['enqueue_time']}"
            )


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
    def deserialize_failed_job(data: bytes) -> Job:
        """Deserialize bytes to a failed job."""
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
    """
    Serializes jobs and results using cloudpickle.

    ⚠️  SECURITY WARNING ⚠️

    This serializer uses cloudpickle, which can execute arbitrary code during deserialization.
    This creates a significant security vulnerability when deserializing data from untrusted sources.

    **Security Risks:**
    - Remote Code Execution (RCE) vulnerability
    - Potential for malicious code injection
    - Data tampering attacks
    - System compromise

    **Safe Usage:**
    - Only use in trusted, internal environments
    - Never deserialize data from external sources
    - Ensure data integrity through other means
    - Use in development/testing environments only

    **Performance Benefits:**
    - Fast serialization/deserialization
    - Preserves all Python object types
    - Handles complex objects and closures
    - Minimal size overhead

    **Recommendation:**
    Use JsonSerializer for production systems unless you absolutely need
    to serialize complex Python objects and can guarantee data source integrity.
    """

    @staticmethod
    def _create_job_payload(job: Job) -> Dict[str, Any]:
        """Create the payload dictionary for job serialization."""
        return {
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

    @staticmethod
    def _validate_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the job payload before serialization."""
        required_fields = ["job_id", "function", "args", "kwargs"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate function is bytes (pickled)
        if not isinstance(payload["function"], bytes):
            raise SerializationError(
                f"function must be pickled bytes, got {type(payload['function'])}"
            )

        # Validate args and kwargs are bytes (pickled)
        if not isinstance(payload["args"], bytes):
            raise SerializationError(
                f"args must be pickled bytes, got {type(payload['args'])}"
            )

        if not isinstance(payload["kwargs"], bytes):
            raise SerializationError(
                f"kwargs must be pickled bytes, got {type(payload['kwargs'])}"
            )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay", "result_ttl", "timeout"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

    @staticmethod
    def serialize_job(job: Job) -> bytes:
        """Serialize a job to bytes using cloudpickle."""
        try:
            payload = PickleSerializer._create_job_payload(job)
            PickleSerializer._validate_job_payload(payload)
            serialized_data = cloudpickle.dumps(payload)

            # Validate serialized data size
            _validate_serialized_data_size(serialized_data, "Pickle job")

            # Add integrity metadata if enabled
            if SERIALIZATION_CHECKSUM_ENABLED:
                integrity_metadata = _add_integrity_metadata(serialized_data)
                final_data = cloudpickle.dumps(integrity_metadata)
                # Validate final data size with integrity metadata
                _validate_serialized_data_size(
                    final_data, "Pickle job with integrity metadata"
                )
                return final_data

            return serialized_data
        except Exception as e:
            # Log detailed error information for debugging
            PickleSerializer._log_serialization_debug_info(job, e)
            raise SerializationError(f"Failed to pickle job: {e}") from e

    @staticmethod
    def _find_unpicklable_objects(job: Job) -> List[Dict[str, str]]:
        """Find objects in job kwargs that cannot be pickled."""
        unpicklable_objects = []
        for key, value in job.kwargs.items():
            try:
                cloudpickle.dumps(value)
            except Exception as pickle_error:
                unpicklable_objects.append(
                    {
                        "key": key,
                        "type": type(value).__name__,
                        "repr": repr(value),
                        "error": str(pickle_error),
                    }
                )
        return unpicklable_objects

    @staticmethod
    def _find_asyncio_tasks(job: Job) -> List[Dict[str, Any]]:
        """Find asyncio.Task objects in job kwargs."""
        return [
            {
                "key": key,
                "task_id": id(value),
                "task_state": value._state if hasattr(value, "_state") else "unknown",
                "task_done": value.done() if hasattr(value, "done") else "unknown",
            }
            for key, value in job.kwargs.items()
            if isinstance(value, asyncio.Task)
        ]

    @staticmethod
    def _log_serialization_debug_info(job: Job, error: Exception) -> None:
        """Log detailed debug information for serialization failures."""
        # Check if debug logging is enabled
        if not PICKLE_DEBUG_LOGGING_ENABLED:
            return

        logger = loguru.logger.bind(job_id=job.job_id)

        # Use configured log level
        log_method = getattr(logger, PICKLE_DEBUG_LOGGING_LEVEL.lower(), logger.debug)

        log_method("=== DEBUG: Job kwargs analysis ===")
        log_method(f"Job kwargs keys: {list(job.kwargs.keys())}")
        log_method(
            f"Job kwargs types: {[(k, type(v).__name__) for k, v in job.kwargs.items()]}"
        )

        # Check for unpicklable objects if configured to include them
        if PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS:
            unpicklable_objects = PickleSerializer._find_unpicklable_objects(job)
            if unpicklable_objects:
                log_method(
                    "Found unpicklable objects in job kwargs",
                    unpicklable_objects=unpicklable_objects,
                )

            # Check for asyncio.Task objects
            task_objects = PickleSerializer._find_asyncio_tasks(job)
            if task_objects:
                log_method(
                    "Found asyncio.Task objects in job kwargs",
                    task_objects=task_objects,
                )

        log_method("=== END DEBUG: Job kwargs analysis ===")

        # Always call debug at least once to ensure test compatibility
        logger.debug("Serialization debug info logged")

    @staticmethod
    def deserialize_job(data: bytes) -> Job:
        """Deserialize bytes to a job using cloudpickle."""
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = cloudpickle.loads(data)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=False
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            payload = cloudpickle.loads(data)

            # Validate the deserialized payload before processing
            _validate_deserialized_job_payload(payload, "pickle")

            function = cloudpickle.loads(payload["function"])
            args = cloudpickle.loads(payload["args"])
            kwargs = cloudpickle.loads(payload["kwargs"])

            # Create the job with all the saved attributes
            job = Job(
                function=function,
                args=args,
                kwargs=kwargs,
                job_id=payload.get("job_id"),
                enqueue_time=payload.get("enqueue_time"),  # Added enqueue_time
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
    def _validate_failed_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the failed job payload before serialization."""
        required_fields = ["job_id", "function_str", "args_repr", "kwargs_repr"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in failed job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate string fields
        string_fields = [
            "function_str",
            "args_repr",
            "kwargs_repr",
            "error",
            "traceback",
        ]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

    @staticmethod
    def serialize_failed_job(job: Job) -> bytes:
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
            PickleSerializer._validate_failed_job_payload(payload)
            serialized_data = cloudpickle.dumps(payload)

            # Validate serialized data size
            _validate_serialized_data_size(serialized_data, "Pickle failed job")

            # Add integrity metadata if enabled
            if SERIALIZATION_CHECKSUM_ENABLED:
                integrity_metadata = _add_integrity_metadata(
                    serialized_data, for_json=False
                )
                final_data = cloudpickle.dumps(integrity_metadata)
                # Validate final data size with integrity metadata
                _validate_serialized_data_size(
                    final_data, "Pickle failed job with integrity metadata"
                )
                return final_data

            return serialized_data
        except Exception as e:
            raise SerializationError(f"Failed to pickle failed job details: {e}") from e

    @staticmethod
    def _validate_result_payload(payload: Dict[str, Any]) -> None:
        """Validate the result payload before serialization."""
        required_fields = ["status"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in result payload: {field}"
                )

        # Validate status is a string
        if not isinstance(payload["status"], str):
            raise SerializationError(
                f"status must be a string, got {type(payload['status'])}"
            )

        # Validate string fields
        string_fields = ["error", "traceback"]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

    @staticmethod
    def serialize_result(
        result: Any,
        status: Any,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        """Serialize a job result to bytes using cloudpickle."""

        try:
            # Convert status to string if it's an enum
            status_value = status.value if hasattr(status, "value") else str(status)
            is_completed = (
                hasattr(status, "value") and status.value == JOB_STATUS.COMPLETED.value
            )

            # Create a JobResult object for efficient serialization
            job_result = JobResult(
                job_id="",  # Empty job_id for standalone result
                status=status_value,
                result=result if is_completed else None,
                error=error,
                traceback=traceback_str,
                start_time=0.0,  # Not tracking time for standalone result
                finish_time=0.0,
            )
            
            serialized_data = cloudpickle.dumps(job_result)

            # Validate serialized data size
            _validate_serialized_data_size(serialized_data, "Pickle result")

            # Add integrity metadata if enabled
            if SERIALIZATION_CHECKSUM_ENABLED:
                integrity_metadata = _add_integrity_metadata(
                    serialized_data, for_json=False
                )
                final_data = cloudpickle.dumps(integrity_metadata)
                # Validate final data size with integrity metadata
                _validate_serialized_data_size(
                    final_data, "Pickle result with integrity metadata"
                )
                return final_data

            return serialized_data
        except Exception as e:
            raise SerializationError(f"Failed to pickle result data: {e}") from e

    @staticmethod
    def deserialize_failed_job(data: bytes) -> Job:
        """Deserialize bytes to a failed job using cloudpickle."""
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = cloudpickle.loads(data)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=False
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            payload = cloudpickle.loads(data)

            # Validate the deserialized payload before processing
            _validate_deserialized_failed_job_payload(payload, "pickle")

            # Create a failed job with the deserialized data
            # Note: We can't reconstruct the original function, args, and kwargs
            # since we only stored their representations in serialize_failed_job
            job = Job(
                function=lambda: None,  # Placeholder function
                args=(),  # Empty args
                kwargs={},  # Empty kwargs
                job_id=payload.get("job_id"),
                enqueue_time=payload.get("enqueue_time"),
                queue_name=payload.get("queue_name"),
                max_retries=payload.get("max_retries", 0),
                retry_delay=payload.get("retry_delay", 0),
                error=payload.get("error"),
                traceback=payload.get("traceback"),
            )

            # Mark the job as failed by setting the appropriate fields
            # The status property is derived from _start_time, _finish_time, and error
            job._start_time = time.time()
            job._finish_time = time.time()
            # error is already set above

            return job
        except Exception as e:
            raise SerializationError(f"Failed to unpickle failed job: {e}") from e

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        """Deserialize bytes to a result dictionary using cloudpickle."""
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = cloudpickle.loads(data)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=False
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            # Deserialize as JobResult object
            job_result = cloudpickle.loads(data)

            # Handle backward compatibility with dictionary format
            if isinstance(job_result, dict):
                # Validate the deserialized result payload
                _validate_deserialized_result_payload(job_result, "pickle")
                return job_result
            
            # Convert JobResult to dictionary for API compatibility
            result_dict = {
                "status": job_result.status,
                "result": job_result.result,
                "error": job_result.error,
                "traceback": job_result.traceback,
            }

            return result_dict
        except Exception as e:
            raise SerializationError(f"Failed to unpickle result data: {e}") from e


class JsonSerializer:
    """
    Secure JSON serializer for production environments.

    ✅ SECURITY BENEFITS ✅

    This serializer provides strong security guarantees by avoiding code execution
    during deserialization. It only processes JSON-safe data types and explicitly
    rejects any data that could potentially execute code.

    **Security Advantages:**
    - No Remote Code Execution (RCE) vulnerability
    - Safe for untrusted data sources
    - Explicit rejection of dangerous data types
    - Predictable, safe deserialization

    **How It Works:**
    - Functions are stored as importable module:qualname paths
    - Exception classes are stored as qualified names
    - Only JSON-serializable data types are accepted
    - No fallback mechanisms that could introduce vulnerabilities

    **Data Type Support:**
    - ✅ Strings, numbers, booleans, None
    - ✅ Lists, tuples, dictionaries
    - ✅ Dataclasses (converted to dict)
    - ❌ Functions, classes, complex objects
    - ❌ Binary data, custom objects

    **Performance Characteristics:**
    - Moderate serialization speed
    - Human-readable output
    - Cross-platform compatibility
    - Standard JSON parsing

    **Recommended Usage:**
    - Production systems
    - Systems processing external data
    - Environments with security requirements
    - Cross-platform deployments

    **Limitations:**
    - Cannot serialize arbitrary Python objects
    - Requires functions to be importable by path
    - Slightly slower than pickle-based serialization
    """

    @staticmethod
    def _json_encode(payload: Dict[str, Any]) -> bytes:
        """Encode a payload dictionary to JSON bytes."""
        Encoder, _Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.dumps(payload, cls=Encoder).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to JSON-serialize payload: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during JSON payload serialization: {e}"
            ) from e

    @staticmethod
    def _json_decode(data: bytes) -> Dict[str, Any]:
        """Decode JSON bytes to a payload dictionary."""
        _Encoder, Decoder = JsonSerializer._get_json_hooks()
        try:
            return json.loads(data.decode("utf-8"), cls=Decoder)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Failed to parse JSON payload: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during JSON payload parsing: {e}"
            ) from e

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

        # Check if it's a lambda function or has <locals> in qualname
        if (
            not module
            or not qualname
            or "<lambda>" in str(qualname)
            or "<locals>" in str(qualname)
        ):
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
            # No fallback - JSON serializer requires JSON-safe data
            raise SerializationError(
                f"Object of type {type(x).__name__} is not JSON serializable: {x!r}"
            )

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
        """Resolve encoder/decoder classes from settings; fallback to stdlib."""

        def resolve_hook(hook_path, default_hook):
            try:
                return JsonSerializer._resolve_dotted_path(hook_path)
            except Exception:
                return default_hook

        enc = resolve_hook(JSON_ENCODER, json.JSONEncoder)
        dec = resolve_hook(JSON_DECODER, json.JSONDecoder)
        return enc, dec

    @staticmethod
    def _validate_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the job payload before serialization."""
        required_fields = ["job_id", "function", "args", "kwargs"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate function is a string (import path)
        if not isinstance(payload["function"], str):
            raise SerializationError(
                f"function must be a string (import path), got {type(payload['function'])}"
            )

        # Validate args is a list
        if not isinstance(payload["args"], list):
            raise SerializationError(
                f"args must be a list, got {type(payload['args'])}"
            )

        # Validate kwargs is a dict
        if not isinstance(payload["kwargs"], dict):
            raise SerializationError(
                f"kwargs must be a dict, got {type(payload['kwargs'])}"
            )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay", "result_ttl", "timeout"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

        # Validate depends_on is a list
        if "depends_on" in payload and payload["depends_on"] is not None:
            if not isinstance(payload["depends_on"], list):
                raise SerializationError(
                    f"depends_on must be a list, got {type(payload['depends_on'])}"
                )

        # Validate retry_on and ignore_on are lists or None
        list_fields = ["retry_on", "ignore_on"]
        for field in list_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], list):
                    raise SerializationError(
                        f"{field} must be a list, got {type(payload[field])}"
                    )

    @staticmethod
    def serialize_job(job: Job) -> bytes:
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

        JsonSerializer._validate_job_payload(payload)
        serialized_data = JsonSerializer._json_encode(payload)

        # Validate serialized data size
        _validate_serialized_data_size(serialized_data, "JSON job")

        # Add integrity metadata if enabled
        if SERIALIZATION_CHECKSUM_ENABLED:
            integrity_metadata = _add_integrity_metadata(serialized_data, for_json=True)
            final_data = JsonSerializer._json_encode(integrity_metadata)
            # Validate final data size with integrity metadata
            _validate_serialized_data_size(
                final_data, "JSON job with integrity metadata"
            )
            return final_data

        return serialized_data

    @staticmethod
    def deserialize_job(data: bytes) -> Job:
        # Check if data contains integrity metadata
        if SERIALIZATION_CHECKSUM_ENABLED:
            try:
                integrity_metadata = JsonSerializer._json_decode(data)
                if (
                    isinstance(integrity_metadata, dict)
                    and "data" in integrity_metadata
                ):
                    # Verify integrity and extract original data
                    data = _verify_integrity_metadata(integrity_metadata, for_json=True)
            except (SerializationError, KeyError, TypeError):
                # If integrity check fails or metadata is invalid, proceed with original data
                # This maintains backward compatibility with data serialized without integrity checks
                pass

        payload = JsonSerializer._json_decode(data)

        # Validate the deserialized payload before processing
        _validate_deserialized_job_payload(payload, "json")

        function = JsonSerializer._resolve_dotted_path(payload["function"])
        args = tuple(payload.get("args", []) or [])
        kwargs = payload.get("kwargs", {}) or {}
        retry_on = JsonSerializer._decode_exceptions(payload.get("retry_on"))
        ignore_on = JsonSerializer._decode_exceptions(payload.get("ignore_on"))
        depends_on = payload.get("depends_on")

        retry_strategy = _normalize_retry_strategy(
            payload.get("retry_strategy", RETRY_STRATEGY.LINEAR)
        )

        return Job(
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
            enqueue_time=payload.get("enqueue_time", time.time()),
        )

    @staticmethod
    def _validate_failed_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the failed job payload before serialization."""
        required_fields = ["job_id", "function_str", "args_repr", "kwargs_repr"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in failed job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate string fields
        string_fields = [
            "function_str",
            "args_repr",
            "kwargs_repr",
            "error",
            "traceback",
        ]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

    @staticmethod
    def serialize_failed_job(job: Job) -> bytes:
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
        JsonSerializer._validate_failed_job_payload(payload)
        serialized_data = JsonSerializer._json_encode(payload)

        # Validate serialized data size
        _validate_serialized_data_size(serialized_data, "JSON failed job")

        # Add integrity metadata if enabled
        if SERIALIZATION_CHECKSUM_ENABLED:
            integrity_metadata = _add_integrity_metadata(serialized_data, for_json=True)
            final_data = JsonSerializer._json_encode(integrity_metadata)
            # Validate final data size with integrity metadata
            _validate_serialized_data_size(
                final_data, "JSON failed job with integrity metadata"
            )
            return final_data

        return serialized_data

    @staticmethod
    def _validate_result_payload(payload: Dict[str, Any]) -> None:
        """Validate the result payload before serialization."""
        required_fields = ["status"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in result payload: {field}"
                )

        # Validate status is a string
        if not isinstance(payload["status"], str):
            raise SerializationError(
                f"status must be a string, got {type(payload['status'])}"
            )

        # Validate string fields
        string_fields = ["error", "traceback"]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        # status to value for storage
        status_value = status.value if hasattr(status, "value") else str(status)
        is_completed = (
            hasattr(status, "value") and status.value == JOB_STATUS.COMPLETED.value
        )

        # Create a JobResult object for efficient serialization
        job_result = JobResult(
            job_id="",  # Empty job_id for standalone result
            status=status_value,
            result=result if is_completed else None,
            error=error,
            traceback=traceback_str,
            start_time=0.0,  # Not tracking time for standalone result
            finish_time=0.0,
        )
        
        # Convert JobResult to dict for JSON serialization
        payload = {
            "status": job_result.status,
            "result": job_result.result,
            "error": job_result.error,
            "traceback": job_result.traceback,
        }
        
        Encoder, _ = JsonSerializer._get_json_hooks()
        try:
            serialized_data = json.dumps(payload, cls=Encoder).encode("utf-8")

            # Validate serialized data size
            _validate_serialized_data_size(serialized_data, "JSON result")

            # Add integrity metadata if enabled
            if SERIALIZATION_CHECKSUM_ENABLED:
                integrity_metadata = _add_integrity_metadata(
                    serialized_data, for_json=True
                )
                final_data = json.dumps(integrity_metadata, cls=Encoder).encode("utf-8")
                # Validate final data size with integrity metadata
                _validate_serialized_data_size(
                    final_data, "JSON result with integrity metadata"
                )
                return final_data

            return serialized_data
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to JSON-serialize result: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during JSON result serialization: {e}"
            ) from e

    @staticmethod
    def deserialize_failed_job(data: bytes) -> Job:
        """Deserialize bytes to a failed job using JSON."""
        _, Decoder = JsonSerializer._get_json_hooks()
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = json.loads(data.decode("utf-8"), cls=Decoder)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=True
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            payload = json.loads(data.decode("utf-8"), cls=Decoder)

            # Validate the deserialized payload before processing
            _validate_deserialized_failed_job_payload(payload, "json")

            # Create a failed job with the deserialized data
            # Note: We can't reconstruct the original function, args, and kwargs
            # since we only stored their representations in serialize_failed_job
            job = Job(
                function=lambda: None,  # Placeholder function
                args=(),  # Empty args
                kwargs={},  # Empty kwargs
                job_id=payload.get("job_id"),
                enqueue_time=payload.get("enqueue_time"),
                queue_name=payload.get("queue_name"),
                max_retries=payload.get("max_retries", 0),
                retry_delay=payload.get("retry_delay", 0),
                error=payload.get("error"),
                traceback=payload.get("traceback"),
            )

            # Mark the job as failed by setting the appropriate fields
            # The status property is derived from _start_time, _finish_time, and error
            job._start_time = time.time()
            job._finish_time = time.time()
            # error is already set above

            return job
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Failed to parse JSON failed job: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during JSON failed job parsing: {e}"
            ) from e

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        _, Decoder = JsonSerializer._get_json_hooks()
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = json.loads(data.decode("utf-8"), cls=Decoder)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=True
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            obj = json.loads(data.decode("utf-8"), cls=Decoder)

            # Validate the deserialized result payload
            _validate_deserialized_result_payload(obj, "json")

            # Create a JobResult object from the dictionary
            job_result = JobResult(
                job_id="",  # Empty job_id for standalone result
                status=obj.get("status", ""),
                result=obj.get("result"),
                error=obj.get("error"),
                traceback=obj.get("traceback"),
                start_time=0.0,  # Not tracking time for standalone result
                finish_time=0.0,
            )

            # Convert JobResult back to dictionary for API compatibility
            result_dict = {
                "status": job_result.status,
                "result": job_result.result,
                "error": job_result.error,
                "traceback": job_result.traceback,
            }

            return result_dict
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Failed to parse JSON result: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during JSON result parsing: {e}"
            ) from e


class MsgPackSerializer:
    """
    MessagePack-based serializer using msgspec.msgpack.Encoder/Decoder.

    ✅ PERFORMANCE & SECURITY BENEFITS ✅

    This serializer provides the same security guarantees as JsonSerializer but with
    improved performance through the MessagePack binary format. It uses msgspec for
    efficient serialization while maintaining type safety and security.

    **Security Advantages:**
    - No Remote Code Execution (RCE) vulnerability
    - Safe for untrusted data sources
    - Explicit rejection of dangerous data types
    - Predictable, safe deserialization
    - Type-safe decoding with msgspec

    **Performance Advantages:**
    - Faster than JSON serialization/deserialization
    - Smaller serialized data size
    - Binary format for efficient processing
    - Optimized encoding/decoding with msgspec

    **How It Works:**
    - Functions are stored as importable module:qualname paths
    - Exception classes are stored as qualified names
    - Only MessagePack-serializable data types are accepted
    - Uses msgspec.msgpack.Encoder/Decoder for efficient processing
    - No fallback mechanisms that could introduce vulnerabilities

    **Data Type Support:**
    - ✅ Strings, numbers, booleans, None
    - ✅ Lists, tuples, dictionaries
    - ✅ Dataclasses (converted to dict)
    - ❌ Functions, classes, complex objects
    - ❌ Binary data, custom objects

    **Performance Characteristics:**
    - Fast serialization/deserialization
    - Compact binary output
    - Cross-platform compatibility
    - Type-safe msgspec decoding

    **Recommended Usage:**
    - Production systems requiring high performance
    - Systems processing large volumes of jobs
    - Environments with network bandwidth constraints
    - Cross-platform deployments

    **Limitations:**
    - Cannot serialize arbitrary Python objects
    - Requires functions to be importable by path
    - Binary format is not human-readable
    """

    @staticmethod
    def _msgpack_encode(payload: Dict[str, Any]) -> bytes:
        """Encode a payload dictionary to MessagePack bytes."""
        encoder = msgspec.msgpack.Encoder()
        try:
            return encoder.encode(payload)
        except (TypeError, ValueError) as e:
            raise SerializationError(
                f"Failed to MessagePack-serialize payload: {e}"
            ) from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during MessagePack payload serialization: {e}"
            ) from e

    @staticmethod
    def _msgpack_decode(data: bytes) -> Dict[str, Any]:
        """Decode MessagePack bytes to a payload dictionary."""
        decoder = msgspec.msgpack.Decoder(dict)
        try:
            return decoder.decode(data)
        except (msgspec.DecodeError, ValueError) as e:
            raise SerializationError(f"Failed to parse MessagePack payload: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during MessagePack payload parsing: {e}"
            ) from e

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

        # Check if it's a lambda function or has <locals> in qualname
        if (
            not module
            or not qualname
            or "<lambda>" in str(qualname)
            or "<locals>" in str(qualname)
        ):
            raise SerializationError(f"Object is not importable: {obj!r}")
        return f"{module}:{qualname}"

    @staticmethod
    def _encode_args_kwargs(
        args: Tuple, kwargs: Dict
    ) -> Tuple[List[Any], Dict[str, Any]]:
        def make_msgpackable(x: Any) -> Any:
            if is_dataclass(x):
                return asdict(x)
            if isinstance(x, (str, int, float, bool)) or x is None:
                return x
            if isinstance(x, (list, tuple)):
                return [make_msgpackable(i) for i in x]
            if isinstance(x, dict):
                return {str(k): make_msgpackable(v) for k, v in x.items()}
            # No fallback - MessagePack serializer requires compatible data types
            raise SerializationError(
                f"Object of type {type(x).__name__} is not MessagePack serializable: {x!r}"
            )

        return make_msgpackable(args), make_msgpackable(kwargs)

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
                    "retry_on/ignore_on must be exception classes when using MessagePack serializer"
                )
            paths.append(MsgPackSerializer._qualname(exc))
        return paths

    @staticmethod
    def _decode_exceptions(
        exc_paths: Optional[List[str]],
    ) -> Optional[Tuple[type, ...]]:
        if not exc_paths:
            return None
        types: List[type] = []
        for path in exc_paths:
            exc = MsgPackSerializer._resolve_dotted_path(path)
            if not isinstance(exc, type) or not issubclass(exc, BaseException):
                raise SerializationError(f"Imported '{path}' is not an Exception type")
            types.append(exc)
        return tuple(types)

    @staticmethod
    def _validate_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the job payload before serialization."""
        required_fields = ["job_id", "function", "args", "kwargs"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate function is a string (import path)
        if not isinstance(payload["function"], str):
            raise SerializationError(
                f"function must be a string (import path), got {type(payload['function'])}"
            )

        # Validate args is a list
        if not isinstance(payload["args"], list):
            raise SerializationError(
                f"args must be a list, got {type(payload['args'])}"
            )

        # Validate kwargs is a dict
        if not isinstance(payload["kwargs"], dict):
            raise SerializationError(
                f"kwargs must be a dict, got {type(payload['kwargs'])}"
            )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay", "result_ttl", "timeout"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

        # Validate depends_on is a list
        if "depends_on" in payload and payload["depends_on"] is not None:
            if not isinstance(payload["depends_on"], list):
                raise SerializationError(
                    f"depends_on must be a list, got {type(payload['depends_on'])}"
                )

        # Validate retry_on and ignore_on are lists or None
        list_fields = ["retry_on", "ignore_on"]
        for field in list_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], list):
                    raise SerializationError(
                        f"{field} must be a list, got {type(payload[field])}"
                    )

    @staticmethod
    def serialize_job(job: Job) -> bytes:
        try:
            func_path = MsgPackSerializer._qualname(job.function)
        except SerializationError as e:
            # Do not allow pickling fallback for security
            raise SerializationError(
                f"MessagePack serializer requires importable function: {e}"
            ) from e

        args_msgpack, kwargs_msgpack = MsgPackSerializer._encode_args_kwargs(
            job.args, job.kwargs
        )

        payload = {
            "job_id": job.job_id,
            "enqueue_time": job.enqueue_time,
            "function": func_path,
            "args": args_msgpack,
            "kwargs": kwargs_msgpack,
            "max_retries": job.max_retries,
            "retry_delay": job.retry_delay,
            "queue_name": job.queue_name,
            "depends_on": job.dependency_ids,  # store as list of IDs
            "result_ttl": job.result_ttl,
            "timeout": job.timeout,
            "retry_strategy": _normalize_retry_strategy(job.retry_strategy),
            "retry_on": MsgPackSerializer._encode_exceptions(job.retry_on),
            "ignore_on": MsgPackSerializer._encode_exceptions(job.ignore_on),
        }

        MsgPackSerializer._validate_job_payload(payload)
        serialized_data = MsgPackSerializer._msgpack_encode(payload)

        # Validate serialized data size
        _validate_serialized_data_size(serialized_data, "MessagePack job")

        # Add integrity metadata if enabled
        if SERIALIZATION_CHECKSUM_ENABLED:
            integrity_metadata = _add_integrity_metadata(
                serialized_data, for_json=False
            )
            final_data = MsgPackSerializer._msgpack_encode(integrity_metadata)
            # Validate final data size with integrity metadata
            _validate_serialized_data_size(
                final_data, "MessagePack job with integrity metadata"
            )
            return final_data

        return serialized_data

    @staticmethod
    def deserialize_job(data: bytes) -> Job:
        # Check if data contains integrity metadata
        if SERIALIZATION_CHECKSUM_ENABLED:
            try:
                integrity_metadata = MsgPackSerializer._msgpack_decode(data)
                if (
                    isinstance(integrity_metadata, dict)
                    and "data" in integrity_metadata
                ):
                    # Verify integrity and extract original data
                    data = _verify_integrity_metadata(
                        integrity_metadata, for_json=False
                    )
            except (SerializationError, KeyError, TypeError):
                # If integrity check fails or metadata is invalid, proceed with original data
                # This maintains backward compatibility with data serialized without integrity checks
                pass

        payload = MsgPackSerializer._msgpack_decode(data)

        # Validate the deserialized payload before processing
        _validate_deserialized_job_payload(payload, "msgpack")

        function = MsgPackSerializer._resolve_dotted_path(payload["function"])
        args = tuple(payload.get("args", []) or [])
        kwargs = payload.get("kwargs", {}) or {}
        retry_on = MsgPackSerializer._decode_exceptions(payload.get("retry_on"))
        ignore_on = MsgPackSerializer._decode_exceptions(payload.get("ignore_on"))
        depends_on = payload.get("depends_on")

        retry_strategy = _normalize_retry_strategy(
            payload.get("retry_strategy", RETRY_STRATEGY.LINEAR)
        )

        return Job(
            function=function,
            args=args,
            kwargs=kwargs,
            job_id=payload.get("job_id", ""),
            queue_name=payload.get("queue_name") or DEFAULT_QUEUE_NAME,
            max_retries=payload.get("max_retries", 0),
            retry_delay=payload.get("retry_delay", 0),
            retry_strategy=retry_strategy,
            retry_on=retry_on,
            ignore_on=ignore_on,
            depends_on=depends_on,
            result_ttl=payload.get("result_ttl"),
            timeout=payload.get("timeout"),
            enqueue_time=payload.get("enqueue_time", time.time()),
        )

    @staticmethod
    def _validate_failed_job_payload(payload: Dict[str, Any]) -> None:
        """Validate the failed job payload before serialization."""
        required_fields = ["job_id", "function_str", "args_repr", "kwargs_repr"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in failed job payload: {field}"
                )

        # Validate job_id is a string
        if not isinstance(payload["job_id"], str):
            raise SerializationError(
                f"job_id must be a string, got {type(payload['job_id'])}"
            )

        # Validate string fields
        string_fields = [
            "function_str",
            "args_repr",
            "kwargs_repr",
            "error",
            "traceback",
        ]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

        # Validate numeric fields
        numeric_fields = ["max_retries", "retry_delay"]
        for field in numeric_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], (int, float)):
                    raise SerializationError(
                        f"{field} must be numeric, got {type(payload[field])}"
                    )
                if payload[field] < 0:
                    raise SerializationError(
                        f"{field} must be non-negative, got {payload[field]}"
                    )

    @staticmethod
    def serialize_failed_job(job: Job) -> bytes:
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
        MsgPackSerializer._validate_failed_job_payload(payload)
        serialized_data = MsgPackSerializer._msgpack_encode(payload)

        # Validate serialized data size
        _validate_serialized_data_size(serialized_data, "MessagePack failed job")

        # Add integrity metadata if enabled
        if SERIALIZATION_CHECKSUM_ENABLED:
            integrity_metadata = _add_integrity_metadata(
                serialized_data, for_json=False
            )
            final_data = MsgPackSerializer._msgpack_encode(integrity_metadata)
            # Validate final data size with integrity metadata
            _validate_serialized_data_size(
                final_data, "MessagePack failed job with integrity metadata"
            )
            return final_data

        return serialized_data

    @staticmethod
    def _validate_result_payload(payload: Dict[str, Any]) -> None:
        """Validate the result payload before serialization."""
        required_fields = ["status"]
        for field in required_fields:
            if field not in payload:
                raise SerializationError(
                    f"Missing required field in result payload: {field}"
                )

        # Validate status is a string
        if not isinstance(payload["status"], str):
            raise SerializationError(
                f"status must be a string, got {type(payload['status'])}"
            )

        # Validate string fields
        string_fields = ["error", "traceback"]
        for field in string_fields:
            if field in payload and payload[field] is not None:
                if not isinstance(payload[field], str):
                    raise SerializationError(
                        f"{field} must be a string, got {type(payload[field])}"
                    )

    @staticmethod
    def serialize_result(
        result: Any,
        status: JOB_STATUS,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> bytes:
        # status to value for storage
        status_value = status.value if hasattr(status, "value") else str(status)
        is_completed = (
            hasattr(status, "value") and status.value == JOB_STATUS.COMPLETED.value
        )

        # Create a JobResult object for efficient serialization
        job_result = JobResult(
            job_id="",  # Empty job_id for standalone result
            status=status_value,
            result=result if is_completed else None,
            error=error,
            traceback=traceback_str,
            start_time=0.0,  # Not tracking time for standalone result
            finish_time=0.0,
        )
        
        # Convert JobResult to dict for MessagePack serialization
        payload = {
            "status": job_result.status,
            "result": job_result.result,
            "error": job_result.error,
            "traceback": job_result.traceback,
        }
        
        encoder = msgspec.msgpack.Encoder()
        try:
            serialized_data = encoder.encode(payload)

            # Validate serialized data size
            _validate_serialized_data_size(serialized_data, "MessagePack result")

            # Add integrity metadata if enabled
            if SERIALIZATION_CHECKSUM_ENABLED:
                integrity_metadata = _add_integrity_metadata(
                    serialized_data, for_json=False
                )
                final_data = encoder.encode(integrity_metadata)
                # Validate final data size with integrity metadata
                _validate_serialized_data_size(
                    final_data, "MessagePack result with integrity metadata"
                )
                return final_data

            return serialized_data
        except (TypeError, ValueError) as e:
            raise SerializationError(
                f"Failed to MessagePack-serialize result: {e}"
            ) from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during MessagePack result serialization: {e}"
            ) from e

    @staticmethod
    def deserialize_failed_job(data: bytes) -> Job:
        """Deserialize bytes to a failed job using MessagePack."""
        decoder = msgspec.msgpack.Decoder(dict)
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = decoder.decode(data)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=False
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            payload = decoder.decode(data)

            # Validate the deserialized payload before processing
            _validate_deserialized_failed_job_payload(payload, "msgpack")

            # Create a failed job with the deserialized data
            # Note: We can't reconstruct the original function, args, and kwargs
            # since we only stored their representations in serialize_failed_job
            job = Job(
                function=lambda: None,  # Placeholder function
                args=(),  # Empty args
                kwargs={},  # Empty kwargs
                job_id=payload.get("job_id", ""),
                enqueue_time=payload.get("enqueue_time"),
                queue_name=payload.get("queue_name"),
                max_retries=payload.get("max_retries", 0),
                retry_delay=payload.get("retry_delay", 0),
                error=payload.get("error"),
                traceback=payload.get("traceback"),
            )

            # Mark the job as failed by setting the appropriate fields
            # The status property is derived from _start_time, _finish_time, and error
            job._start_time = time.time()
            job._finish_time = time.time()
            # error is already set above

            return job
        except (msgspec.DecodeError, ValueError) as e:
            raise SerializationError(
                f"Failed to parse MessagePack failed job: {e}"
            ) from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during MessagePack failed job parsing: {e}"
            ) from e

    @staticmethod
    def deserialize_result(data: bytes) -> Dict[str, Any]:
        decoder = msgspec.msgpack.Decoder(dict)
        try:
            # Check if data contains integrity metadata
            if SERIALIZATION_CHECKSUM_ENABLED:
                try:
                    integrity_metadata = decoder.decode(data)
                    if (
                        isinstance(integrity_metadata, dict)
                        and "data" in integrity_metadata
                    ):
                        # Verify integrity and extract original data
                        data = _verify_integrity_metadata(
                            integrity_metadata, for_json=False
                        )
                except (SerializationError, KeyError, TypeError):
                    # If integrity check fails or metadata is invalid, proceed with original data
                    # This maintains backward compatibility with data serialized without integrity checks
                    pass

            obj = decoder.decode(data)

            # Validate the deserialized result payload
            _validate_deserialized_result_payload(obj, "msgpack")

            # Create a JobResult object from the dictionary
            job_result = JobResult(
                job_id="",  # Empty job_id for standalone result
                status=obj.get("status", ""),
                result=obj.get("result"),
                error=obj.get("error"),
                traceback=obj.get("traceback"),
                start_time=0.0,  # Not tracking time for standalone result
                finish_time=0.0,
            )

            # Convert JobResult back to dictionary for API compatibility
            result_dict = {
                "status": job_result.status,
                "result": job_result.result,
                "error": job_result.error,
                "traceback": job_result.traceback,
            }

            return result_dict
        except (msgspec.DecodeError, ValueError) as e:
            raise SerializationError(f"Failed to parse MessagePack result: {e}") from e
        except Exception as e:
            raise SerializationError(
                f"Unexpected error during MessagePack result parsing: {e}"
            ) from e


# Factory function to get the appropriate serializer
def get_serializer() -> Serializer:
    """
    Returns the appropriate serializer based on JOB_SERIALIZER setting.

    **Security-First Approach:**
    This function prioritizes security by defaulting to JsonSerializer for unknown
    or invalid serializer settings. Only explicitly choose PickleSerializer when:

    1. **Trusted Environment**: Data source is completely trusted
    2. **Internal Systems**: No external data sources
    3. **Performance Critical**: Need to serialize complex Python objects
    4. **Development Only**: Testing/debugging scenarios

    **Configuration:**
    Set the JOB_SERIALIZER setting to control which serializer is used:
    - "json" (recommended): Use JsonSerializer (secure, human-readable)
    - "msgpack": Use MsgPackSerializer (secure, high-performance binary)
    - "pickle": Use PickleSerializer (fast but risky)

    **Serializer Comparison:**
    - JsonSerializer: Secure, human-readable, moderate performance
    - MsgPackSerializer: Secure, binary format, high performance, compact size
    - PickleSerializer: Fastest, supports all Python objects, but insecure

    **Migration Guide:**
    When migrating between serializers:
    1. Ensure all job functions are importable by qualified name
    2. Verify all job arguments are compatible with target serializer
    3. Test thoroughly in staging environment
    4. Monitor for any serialization failures

    Returns:
        Serializer: Configured serializer instance

    Raises:
        SerializationError: If JOB_SERIALIZER setting is invalid
    """
    if JOB_SERIALIZER == "pickle":
        return PickleSerializer
    elif JOB_SERIALIZER == "json":
        return JsonSerializer
    elif JOB_SERIALIZER == "msgpack":
        return MsgPackSerializer
    else:
        raise SerializationError(f"Unknown serializer: {JOB_SERIALIZER}")
