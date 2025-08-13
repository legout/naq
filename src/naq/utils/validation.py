# src/naq/utils/validation.py
"""
Validation utilities for NAQ.

This module provides parameter validation, configuration validation,
and schema validation utilities.
"""

import re
import jsonschema
from typing import Any, Dict, List, Optional, Union, Type, Callable
from dataclasses import dataclass, fields
from enum import Enum

from loguru import logger


class ValidationError(ValueError):
    """Validation error with detailed context."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value


@dataclass
class ValidationRule:
    """Validation rule definition."""
    name: str
    validator: Callable[[Any], bool]
    message: str
    required: bool = True


class ValidationHelper:
    """Centralized validation utilities."""
    
    def __init__(self):
        """Initialize validation helper."""
        self.rules: Dict[str, List[ValidationRule]] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
    
    def add_rule(self, field: str, rule: ValidationRule):
        """Add validation rule for a field."""
        if field not in self.rules:
            self.rules[field] = []
        self.rules[field].append(rule)
    
    def add_schema(self, name: str, schema: Dict[str, Any]):
        """Add JSON schema for validation."""
        self.schemas[name] = schema
    
    def validate_field(self, field: str, value: Any) -> None:
        """Validate a single field value."""
        if field not in self.rules:
            return
        
        for rule in self.rules[field]:
            if value is None and not rule.required:
                continue
                
            if not rule.validator(value):
                raise ValidationError(
                    rule.message,
                    field=field,
                    value=value
                )
    
    def validate_dict(self, data: Dict[str, Any], field_prefix: str = "") -> None:
        """Validate dictionary data against registered rules."""
        for field, value in data.items():
            full_field = f"{field_prefix}.{field}" if field_prefix else field
            self.validate_field(full_field, value)
    
    def validate_schema(self, schema_name: str, data: Any) -> None:
        """Validate data against JSON schema."""
        if schema_name not in self.schemas:
            raise ValidationError(f"Unknown schema: {schema_name}")
        
        try:
            jsonschema.validate(data, self.schemas[schema_name])
        except jsonschema.ValidationError as e:
            raise ValidationError(f"Schema validation failed: {e.message}") from e


# Common validation functions
def validate_string_length(min_length: int = 0, max_length: Optional[int] = None):
    """Create string length validator."""
    def validator(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        if len(value) < min_length:
            return False
        if max_length is not None and len(value) > max_length:
            return False
        return True
    return validator


def validate_regex(pattern: str, flags: int = 0):
    """Create regex validator."""
    compiled = re.compile(pattern, flags)
    
    def validator(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        return compiled.match(value) is not None
    return validator


def validate_numeric_range(min_val: Optional[Union[int, float]] = None, 
                          max_val: Optional[Union[int, float]] = None):
    """Create numeric range validator."""
    def validator(value: Any) -> bool:
        if not isinstance(value, (int, float)):
            return False
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True
    return validator


def validate_type(*types: Type):
    """Create type validator."""
    def validator(value: Any) -> bool:
        return isinstance(value, types)
    return validator


def validate_list_items(item_validator: Callable[[Any], bool]):
    """Create list items validator."""
    def validator(value: Any) -> bool:
        if not isinstance(value, list):
            return False
        return all(item_validator(item) for item in value)
    return validator


def validate_dict_keys(key_validator: Callable[[Any], bool]):
    """Create dict keys validator."""
    def validator(value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        return all(key_validator(key) for key in value.keys())
    return validator


def validate_enum(enum_class: Type[Enum]):
    """Create enum validator."""
    valid_values = {item.value for item in enum_class}
    
    def validator(value: Any) -> bool:
        return value in valid_values
    return validator


# Common validation rules
VALID_QUEUE_NAME = ValidationRule(
    "valid_queue_name",
    validate_regex(r"^[a-zA-Z0-9_.-]+$"),
    "Queue name must contain only alphanumeric characters, underscores, hyphens, and dots"
)

NON_EMPTY_STRING = ValidationRule(
    "non_empty_string",
    lambda x: isinstance(x, str) and len(x.strip()) > 0,
    "Value must be a non-empty string"
)

POSITIVE_INTEGER = ValidationRule(
    "positive_integer",
    lambda x: isinstance(x, int) and x > 0,
    "Value must be a positive integer"
)

NON_NEGATIVE_NUMBER = ValidationRule(
    "non_negative_number",
    lambda x: isinstance(x, (int, float)) and x >= 0,
    "Value must be a non-negative number"
)

VALID_URL = ValidationRule(
    "valid_url",
    validate_regex(r"^https?://[^\s/$.?#].[^\s]*$|^nats://[^\s/$.?#].[^\s]*$"),
    "Value must be a valid URL"
)


def validate_config(config: Dict[str, Any]) -> None:
    """Validate NAQ configuration dictionary."""
    helper = ValidationHelper()
    
    # Add validation rules for common config fields
    helper.add_rule("nats_url", VALID_URL)
    helper.add_rule("queue_name", VALID_QUEUE_NAME)
    helper.add_rule("max_retries", NON_NEGATIVE_NUMBER)
    helper.add_rule("retry_delay", NON_NEGATIVE_NUMBER)
    helper.add_rule("timeout", POSITIVE_INTEGER)
    helper.add_rule("concurrency", POSITIVE_INTEGER)
    
    # Validate nested configuration sections
    if "nats" in config:
        nats_config = config["nats"]
        if "servers" in nats_config:
            servers = nats_config["servers"]
            if isinstance(servers, list):
                for i, server in enumerate(servers):
                    helper.validate_field(f"nats.servers[{i}]", server)
    
    if "workers" in config:
        workers_config = config["workers"]
        helper.validate_dict(workers_config, "workers")
    
    if "events" in config:
        events_config = config["events"]
        helper.validate_dict(events_config, "events")
    
    # Validate top-level config
    helper.validate_dict(config)


def validate_json_schema(data: Any, schema: Dict[str, Any]) -> None:
    """Validate data against JSON schema."""
    try:
        jsonschema.validate(data, schema)
    except jsonschema.ValidationError as e:
        raise ValidationError(f"Schema validation failed: {e.message}") from e
    except jsonschema.SchemaError as e:
        raise ValidationError(f"Invalid schema: {e.message}") from e


def validate_dataclass_instance(instance: Any) -> None:
    """Validate dataclass instance against field annotations."""
    if not hasattr(instance, '__dataclass_fields__'):
        raise ValidationError("Object is not a dataclass instance")
    
    for field in fields(instance):
        value = getattr(instance, field.name)
        
        # Check required fields
        if value is None and field.default == field.default_factory == dataclass.MISSING:
            raise ValidationError(f"Required field '{field.name}' is missing")
        
        # Basic type checking (if type annotation is available)
        if value is not None and hasattr(field.type, '__origin__'):
            # Handle generic types like Optional, Union, List, etc.
            continue  # Skip complex type checking for now
        elif value is not None and isinstance(field.type, type):
            if not isinstance(value, field.type):
                raise ValidationError(
                    f"Field '{field.name}' should be {field.type.__name__}, got {type(value).__name__}",
                    field=field.name,
                    value=value
                )


class ParameterValidator:
    """Function parameter validator using decorators."""
    
    def __init__(self):
        self.validators = {}
    
    def add_validator(self, param_name: str, validator: Callable[[Any], bool], message: str):
        """Add parameter validator."""
        self.validators[param_name] = (validator, message)
    
    def validate_call(self, func_name: str, **kwargs):
        """Validate function call parameters."""
        for param_name, value in kwargs.items():
            if param_name in self.validators:
                validator, message = self.validators[param_name]
                if not validator(value):
                    raise ValidationError(
                        f"{func_name}: {message}",
                        field=param_name,
                        value=value
                    )


def validate_function_params(**param_validators):
    """Decorator to validate function parameters."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import inspect
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            for param_name, validator_info in param_validators.items():
                if param_name in bound.arguments:
                    value = bound.arguments[param_name]
                    
                    if isinstance(validator_info, tuple):
                        validator, message = validator_info
                    else:
                        validator, message = validator_info, f"Invalid value for {param_name}"
                    
                    if not validator(value):
                        raise ValidationError(
                            f"{func.__name__}: {message}",
                            field=param_name,
                            value=value
                        )
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Global validation helper
_global_helper = ValidationHelper()


def get_validation_helper() -> ValidationHelper:
    """Get the global validation helper."""
    return _global_helper


def validate_job_config(config: Dict[str, Any]) -> bool:
    """
    Validate job configuration for backward compatibility.
    
    Args:
        config: Job configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    # Basic validation - check required fields
    required_fields = ['function', 'queue_name']
    for field in required_fields:
        if field not in config:
            return False
    return True


def validate_queue_name(queue_name: str) -> bool:
    """
    Validate queue name for backward compatibility.
    
    Args:
        queue_name: Queue name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not queue_name or not isinstance(queue_name, str):
        return False
    # Basic validation - alphanumeric and underscores
    import re
    return bool(re.match(r'^[a-zA-Z0-9_-]+$', queue_name))


def validate_worker_config(config: Dict[str, Any]) -> bool:
    """
    Validate worker configuration for backward compatibility.
    
    Args:
        config: Worker configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    # Basic validation - check for required worker fields
    if not config or not isinstance(config, dict):
        return False
    
    # Check concurrency is positive
    if 'concurrency' in config:
        try:
            concurrency = int(config['concurrency'])
            if concurrency <= 0:
                return False
        except (ValueError, TypeError):
            return False
    
    return True