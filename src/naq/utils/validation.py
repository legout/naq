"""
Validation utilities for NAQ.

This module provides comprehensive validation utilities for data models,
configuration, and other common validation patterns used throughout the codebase.
"""

import re
from typing import Any, Dict, List, Optional, Union, get_type_hints
from dataclasses import is_dataclass
from enum import Enum

from ..exceptions import NaqException


class ValidationError(NaqException):
    """Raised when validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value


def validate_type(value: Any, expected_type: type, field_name: str = "field") -> None:
    """
    Validate that a value is of the expected type.
    
    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: Name of the field being validated (for error messages)
        
    Raises:
        ValidationError: If the value is not of the expected type
    """
    if not isinstance(value, expected_type):
        raise ValidationError(
            f"Expected {field_name} to be of type {expected_type.__name__}, "
            f"got {type(value).__name__}",
            field=field_name,
            value=value
        )


def validate_range(
    value: Union[int, float],
    min_val: Optional[Union[int, float]] = None,
    max_val: Optional[Union[int, float]] = None,
    field_name: str = "field"
) -> None:
    """
    Validate that a numeric value is within the specified range.
    
    Args:
        value: The value to validate
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the value is outside the specified range
    """
    if min_val is not None and value < min_val:
        raise ValidationError(
            f"{field_name} must be at least {min_val}, got {value}",
            field=field_name,
            value=value
        )
    
    if max_val is not None and value > max_val:
        raise ValidationError(
            f"{field_name} must be at most {max_val}, got {value}",
            field=field_name,
            value=value
        )


def validate_choice(
    value: Any,
    choices: List[Any],
    field_name: str = "field"
) -> None:
    """
    Validate that a value is one of the allowed choices.
    
    Args:
        value: The value to validate
        choices: List of allowed values
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the value is not in the allowed choices
    """
    if value not in choices:
        raise ValidationError(
            f"{field_name} must be one of {choices}, got {value}",
            field=field_name,
            value=value
        )


def validate_required(value: Any, field_name: str = "field") -> None:
    """
    Validate that a required value is not None or empty.
    
    Args:
        value: The value to validate
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the value is None or empty
    """
    if value is None:
        raise ValidationError(
            f"{field_name} is required",
            field=field_name,
            value=value
        )
    
    if isinstance(value, (str, list, dict)) and len(value) == 0:
        raise ValidationError(
            f"{field_name} cannot be empty",
            field=field_name,
            value=value
        )


def validate_string(
    value: Any,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None,
    field_name: str = "field"
) -> None:
    """
    Validate a string value.
    
    Args:
        value: The value to validate
        min_length: Minimum string length
        max_length: Maximum string length
        pattern: Regular expression pattern to match
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the string validation fails
    """
    validate_type(value, str, field_name)
    
    if min_length is not None and len(value) < min_length:
        raise ValidationError(
            f"{field_name} must be at least {min_length} characters long, "
            f"got {len(value)} characters",
            field=field_name,
            value=value
        )
    
    if max_length is not None and len(value) > max_length:
        raise ValidationError(
            f"{field_name} must be at most {max_length} characters long, "
            f"got {len(value)} characters",
            field=field_name,
            value=value
        )
    
    if pattern is not None and not re.match(pattern, value):
        raise ValidationError(
            f"{field_name} does not match required pattern '{pattern}'",
            field=field_name,
            value=value
        )


def validate_url(value: str, field_name: str = "url") -> None:
    """
    Validate that a string is a valid URL.
    
    Args:
        value: The URL to validate
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the URL is invalid
    """
    validate_string(value, field_name=field_name)
    
    # Simple URL validation regex
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP address
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )
    
    if not url_pattern.match(value):
        raise ValidationError(
            f"{field_name} must be a valid URL, got '{value}'",
            field=field_name,
            value=value
        )


def validate_email(value: str, field_name: str = "email") -> None:
    """
    Validate that a string is a valid email address.
    
    Args:
        value: The email to validate
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the email is invalid
    """
    validate_string(value, field_name=field_name)
    
    # Simple email validation regex
    email_pattern = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    
    if not email_pattern.match(value):
        raise ValidationError(
            f"{field_name} must be a valid email address, got '{value}'",
            field=field_name,
            value=value
        )


def validate_dict(
    value: Any,
    required_keys: Optional[List[str]] = None,
    optional_keys: Optional[List[str]] = None,
    field_name: str = "dict"
) -> None:
    """
    Validate a dictionary value.
    
    Args:
        value: The dictionary to validate
        required_keys: Keys that must be present
        optional_keys: Keys that are allowed but not required
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the dictionary validation fails
    """
    validate_type(value, dict, field_name)
    
    if required_keys:
        missing_keys = [key for key in required_keys if key not in value]
        if missing_keys:
            raise ValidationError(
                f"{field_name} is missing required keys: {missing_keys}",
                field=field_name,
                value=value
            )
    
    if optional_keys:
        all_keys = set(required_keys or []) | set(optional_keys)
        unknown_keys = [key for key in value if key not in all_keys]
        if unknown_keys:
            raise ValidationError(
                f"{field_name} contains unknown keys: {unknown_keys}",
                field=field_name,
                value=value
            )


def validate_job_config(config: Dict[str, Any]) -> None:
    """
    Validate job configuration.
    
    Args:
        config: Job configuration dictionary
        
    Raises:
        ValidationError: If the configuration is invalid
    """
    validate_type(config, dict, "job_config")
    
    # Validate required fields
    validate_required(config.get("function"), "function")
    
    # Validate numeric fields
    if "max_retries" in config:
        validate_type(config["max_retries"], int, "max_retries")
        validate_range(config["max_retries"], min_val=0, field_name="max_retries")
    
    if "retry_delay" in config:
        validate_type(config["retry_delay"], (int, float), "retry_delay")
        validate_range(config["retry_delay"], min_val=0, field_name="retry_delay")
    
    if "timeout" in config:
        validate_type(config["timeout"], int, "timeout")
        validate_range(config["timeout"], min_val=1, field_name="timeout")
    
    # Validate string fields
    if "queue_name" in config:
        validate_string(config["queue_name"], min_length=1, field_name="queue_name")
    
    if "retry_strategy" in config:
        validate_choice(
            config["retry_strategy"],
            ["linear", "exponential"],
            "retry_strategy"
        )


def validate_connection_config(config: Dict[str, Any]) -> None:
    """
    Validate connection configuration.
    
    Args:
        config: Connection configuration dictionary
        
    Raises:
        ValidationError: If the configuration is invalid
    """
    validate_type(config, dict, "connection_config")
    
    # Validate servers
    if "servers" in config:
        validate_type(config["servers"], list, "servers")
        for i, server in enumerate(config["servers"]):
            validate_url(server, f"servers[{i}]")
    
    # Validate numeric fields
    if "max_reconnect_attempts" in config:
        validate_type(config["max_reconnect_attempts"], int, "max_reconnect_attempts")
        validate_range(config["max_reconnect_attempts"], min_val=0, field_name="max_reconnect_attempts")
    
    if "reconnect_time_wait" in config:
        validate_type(config["reconnect_time_wait"], (int, float), "reconnect_time_wait")
        validate_range(config["reconnect_time_wait"], min_val=0, field_name="reconnect_time_wait")
    
    # Validate string fields
    if "client_name" in config:
        validate_string(config["client_name"], min_length=1, field_name="client_name")


def validate_event_config(config: Dict[str, Any]) -> None:
    """
    Validate event configuration.
    
    Args:
        config: Event configuration dictionary
        
    Raises:
        ValidationError: If the configuration is invalid
    """
    validate_type(config, dict, "event_config")
    
    # Validate boolean fields
    if "enabled" in config:
        validate_type(config["enabled"], bool, "enabled")
    
    # Validate numeric fields
    if "batch_size" in config:
        validate_type(config["batch_size"], int, "batch_size")
        validate_range(config["batch_size"], min_val=1, field_name="batch_size")
    
    if "flush_interval" in config:
        validate_type(config["flush_interval"], (int, float), "flush_interval")
        validate_range(config["flush_interval"], min_val=0.1, field_name="flush_interval")
    
    if "max_buffer_size" in config:
        validate_type(config["max_buffer_size"], int, "max_buffer_size")
        validate_range(config["max_buffer_size"], min_val=1, field_name="max_buffer_size")
    
    # Validate string fields
    if "storage_type" in config:
        validate_choice(
            config["storage_type"],
            ["nats", "memory", "file"],
            "storage_type"
        )
    
    if "storage_url" in config:
        validate_url(config["storage_url"], "storage_url")


def validate_dataclass(obj: Any, field_name: str = "object") -> None:
    """
    Validate a dataclass object.
    
    Args:
        obj: The dataclass object to validate
        field_name: Name of the field being validated
        
    Raises:
        ValidationError: If the dataclass validation fails
    """
    if not is_dataclass(obj):
        raise ValidationError(
            f"{field_name} must be a dataclass instance",
            field=field_name,
            value=obj
        )
    
    # Get type hints for the dataclass
    type_hints = get_type_hints(type(obj))
    
    # Validate each field
    for field_name, field_type in type_hints.items():
        if hasattr(obj, field_name):
            field_value = getattr(obj, field_name)
            
            # Skip None values for optional fields
            if field_value is None and getattr(type(obj).__annotations__.get(field_name), None) is None:
                continue
            
            # Basic type validation
            if not isinstance(field_value, field_type):
                # Handle Union types (Optional[X] is Union[X, None])
                if hasattr(field_type, "__origin__") and field_type.__origin__ is Union:
                    # Check if any of the Union types match
                    if not any(isinstance(field_value, t) for t in field_type.__args__ if t is not type(None)):
                        raise ValidationError(
                            f"Field {field_name} must be one of types {field_type.__args__}, "
                            f"got {type(field_value).__name__}",
                            field=field_name,
                            value=field_value
                        )
                else:
                    raise ValidationError(
                        f"Field {field_name} must be of type {field_type.__name__}, "
                        f"got {type(field_value).__name__}",
                        field=field_name,
                        value=field_value
                    )


class Validator:
    """
    A configurable validator for complex objects.
    """
    
    def __init__(self, rules: Optional[Dict[str, callable]] = None):
        """
        Initialize the validator.
        
        Args:
            rules: Dictionary mapping field names to validation functions
        """
        self.rules = rules or {}
    
    def add_rule(self, field_name: str, validation_func: callable) -> None:
        """
        Add a validation rule.
        
        Args:
            field_name: Name of the field to validate
            validation_func: Function that validates the field value
        """
        self.rules[field_name] = validation_func
    
    def validate(self, obj: Any) -> List[ValidationError]:
        """
        Validate an object against all rules.
        
        Args:
            obj: Object to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for field_name, validation_func in self.rules.items():
            try:
                if hasattr(obj, field_name):
                    field_value = getattr(obj, field_name)
                    validation_func(field_value)
            except ValidationError as e:
                errors.append(e)
        
        return errors
    
    def validate_or_raise(self, obj: Any) -> None:
        """
        Validate an object and raise an exception if validation fails.
        
        Args:
            obj: Object to validate
            
        Raises:
            ValidationError: If validation fails
        """
        errors = self.validate(obj)
        if errors:
            error_messages = [str(error) for error in errors]
            raise ValidationError(
                f"Validation failed: {'; '.join(error_messages)}",
                field="multiple_fields"
            )