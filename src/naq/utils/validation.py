"""
Validation utilities for NAQ

This module provides common validation functions used throughout the NAQ library.
"""

import re
from typing import Any, Type, Union, Tuple, Optional, List, TypeVar

from ..exceptions import ValidationError

T = TypeVar('T')


def validate_parameter(
    value: Any,
    param_name: str,
    expected_type: Union[Type, Tuple[Type, ...]],
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None,
    allowed_values: Optional[List[Any]] = None,
    required: bool = True,
) -> None:
    """
    Validate a parameter value.
    
    Args:
        value: The value to validate.
        param_name: Name of the parameter for error messages.
        expected_type: Expected type(s) for the value.
        min_value: Minimum value for numeric types.
        max_value: Maximum value for numeric types.
        min_length: Minimum length for strings, lists, etc.
        max_length: Maximum length for strings, lists, etc.
        pattern: Regular expression pattern for string validation.
        allowed_values: List of allowed values.
        required: Whether the value is required (not None).
        
    Raises:
        ValidationError: If validation fails.
    """
    # Check if required
    if required and value is None:
        raise ValidationError(f"Parameter '{param_name}' is required")
    
    # Skip further validation if not required and value is None
    if not required and value is None:
        return
    
    # Check type
    if not isinstance(value, expected_type):
        if isinstance(expected_type, tuple):
            type_names = [t.__name__ for t in expected_type]
            expected_str = " or ".join(type_names)
        else:
            expected_str = expected_type.__name__
        
        raise ValidationError(
            f"Parameter '{param_name}' must be of type {expected_str}, "
            f"got {type(value).__name__}"
        )
    
    # Check numeric range
    if isinstance(value, (int, float)):
        if min_value is not None and value < min_value:
            raise ValidationError(
                f"Parameter '{param_name}' must be >= {min_value}, got {value}"
            )
        
        if max_value is not None and value > max_value:
            raise ValidationError(
                f"Parameter '{param_name}' must be <= {max_value}, got {value}"
            )
    
    # Check length
    if hasattr(value, '__len__'):
        length = len(value)
        
        if min_length is not None and length < min_length:
            raise ValidationError(
                f"Parameter '{param_name}' must have length >= {min_length}, "
                f"got {length}"
            )
        
        if max_length is not None and length > max_length:
            raise ValidationError(
                f"Parameter '{param_name}' must have length <= {max_length}, "
                f"got {length}"
            )
    
    # Check pattern for strings
    if isinstance(value, str) and pattern:
        if not re.match(pattern, value):
            raise ValidationError(
                f"Parameter '{param_name}' must match pattern '{pattern}', "
                f"got '{value}'"
            )
    
    # Check allowed values
    if allowed_values is not None and value not in allowed_values:
        raise ValidationError(
            f"Parameter '{param_name}' must be one of {allowed_values}, "
            f"got {value}"
        )


def validate_nats_url(url: str) -> None:
    """
    Validate a NATS URL.
    
    Args:
        url: NATS URL to validate.
        
    Raises:
        ValidationError: If URL is invalid.
    """
    validate_parameter(url, "url", str, required=True)
    
    # Basic NATS URL validation
    nats_pattern = r'^nats://(?:[^:@]+(?::[^@]+)?@)?[^:]+:\d+$'
    if not re.match(nats_pattern, url):
        raise ValidationError(
            f"Invalid NATS URL: '{url}'. Expected format: nats://[user:password@]host:port"
        )


def validate_subject(subject: str) -> None:
    """
    Validate a NATS subject.
    
    Args:
        subject: NATS subject to validate.
        
    Raises:
        ValidationError: If subject is invalid.
    """
    validate_parameter(subject, "subject", str, required=True, min_length=1)
    
    # Basic subject validation (no spaces, valid characters)
    if ' ' in subject or '\t' in subject or '\n' in subject or '\r' in subject:
        raise ValidationError(
            f"Invalid subject '{subject}'. Subject cannot contain whitespace"
        )


def validate_stream_name(stream_name: str) -> None:
    """
    Validate a JetStream stream name.
    
    Args:
        stream_name: Stream name to validate.
        
    Raises:
        ValidationError: If stream name is invalid.
    """
    validate_parameter(stream_name, "stream_name", str, required=True, min_length=1)
    
    # Stream name validation (alphanumeric, dots, dashes, underscores)
    if not re.match(r'^[a-zA-Z0-9_.-]+$', stream_name):
        raise ValidationError(
            f"Invalid stream name '{stream_name}'. "
            "Stream names can only contain alphanumeric characters, dots, dashes, and underscores"
        )


def validate_queue_name(queue_name: str) -> None:
    """
    Validate a queue name.
    
    Args:
        queue_name: Queue name to validate.
        
    Raises:
        ValidationError: If queue name is invalid.
    """
    validate_parameter(queue_name, "queue_name", str, required=True, min_length=1)
    
    # Queue name validation (similar to stream name)
    if not re.match(r'^[a-zA-Z0-9_.-]+$', queue_name):
        raise ValidationError(
            f"Invalid queue name '{queue_name}'. "
            "Queue names can only contain alphanumeric characters, dots, dashes, and underscores"
        )


def validate_job_id(job_id: str) -> None:
    """
    Validate a job ID.
    
    Args:
        job_id: Job ID to validate.
        
    Raises:
        ValidationError: If job ID is invalid.
    """
    validate_parameter(job_id, "job_id", str, required=True, min_length=1)
    
    # Job ID validation (UUID format or similar)
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if not re.match(uuid_pattern, job_id):
        raise ValidationError(
            f"Invalid job ID '{job_id}'. Expected UUID format"
        )


def validate_timeout(timeout: float) -> None:
    """
    Validate a timeout value.
    
    Args:
        timeout: Timeout value in seconds.
        
    Raises:
        ValidationError: If timeout is invalid.
    """
    validate_parameter(timeout, "timeout", (int, float), min_value=0.1)


def validate_concurrency(concurrency: int) -> None:
    """
    Validate a concurrency value.
    
    Args:
        concurrency: Concurrency level.
        
    Raises:
        ValidationError: If concurrency is invalid.
    """
    validate_parameter(concurrency, "concurrency", int, min_value=1, max_value=1000)


def validate_batch_size(batch_size: int) -> None:
    """
    Validate a batch size value.
    
    Args:
        batch_size: Batch size.
        
    Raises:
        ValidationError: If batch size is invalid.
    """
    validate_parameter(batch_size, "batch_size", int, min_value=1, max_value=10000)


def ensure_type(value: Any, expected_type: Type[T], param_name: str = "value") -> T:
    """
    Ensure that a value is of the expected type.
    
    Args:
        value: The value to check.
        expected_type: The expected type.
        param_name: Name of the parameter for error messages.
        
    Returns:
        The value with the correct type.
        
    Raises:
        ValidationError: If the value is not of the expected type.
    """
    if not isinstance(value, expected_type):
        raise ValidationError(
            f"Parameter '{param_name}' must be of type {expected_type.__name__}, "
            f"got {type(value).__name__}"
        )
    return value