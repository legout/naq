"""Validation utilities for NAQ.

This module contains common validation utilities used throughout the NAQ codebase.
"""

import re
from typing import Any, Optional, Pattern, Type, Union

from ..exceptions import ValidationError, TypeConversionError


def validate_parameter(
    value: Any,
    param_name: str,
    not_none: bool = False,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    regex_pattern: Optional[Union[str, Pattern[str]]] = None,
    pattern: Optional[Union[str, Pattern[str]]] = None,  # Alias for regex_pattern for backward compatibility
    custom_validator: Optional[callable] = None,
    error_message: Optional[str] = None,
) -> None:
    r"""Validate a parameter against specified criteria.
    
    This function checks if a parameter meets various validation criteria and raises
    a ValidationError if any validation fails.
    
    Args:
        value: The value to validate.
        param_name: The name of the parameter being validated (used in error messages).
        not_none: If True, raises ValidationError if value is None.
        min_value: Minimum allowed value (for numeric types).
        max_value: Maximum allowed value (for numeric types).
        regex_pattern: Regular expression pattern that the value must match (for strings).
            Can be a string pattern or a compiled regex Pattern.
        custom_validator: Optional custom validation function that takes the value
            and returns True if valid, False otherwise.
        error_message: Custom error message to use if validation fails.
            If not provided, a default message will be generated.
            
    Raises:
        ValidationError: If any validation criterion fails.
        
    Examples:
        >>> validate_parameter("test", "param", not_none=True)
        # Passes validation
        
        >>> validate_parameter(None, "param", not_none=True)
        # Raises ValidationError: Parameter 'param' cannot be None
        
        >>> validate_parameter(5, "number", min_value=0, max_value=10)
        # Passes validation
        
        >>> validate_parameter(15, "number", max_value=10)
        # Raises ValidationError: Parameter 'number' must be less than or equal to 10
        
        >>> validate_parameter("abc123", "id", regex_pattern=r'^[a-z]+\d+$')
        # Passes validation
        
        >>> validate_parameter("123abc", "id", regex_pattern=r'^[a-z]+\d+$')
        # Raises ValidationError: Parameter 'id' does not match required pattern
    """
    # Check for None
    if not_none and value is None:
        error_msg = error_message or f"Parameter '{param_name}' cannot be None"
        raise ValueError(error_msg)
    
    # If value is None and not_none is False, skip other validations
    if value is None:
        return
    
    # Check min/max values for numeric types
    if min_value is not None or max_value is not None:
        if not isinstance(value, (int, float)):
            error_msg = error_message or f"Parameter '{param_name}' must be numeric for min/max validation"
            raise ValidationError(error_msg)
        
        if min_value is not None and value < min_value:
            if min_value == 0:
                error_msg = error_message or f"{param_name} cannot be negative"
            else:
                error_msg = error_message or f"Parameter '{param_name}' must be greater than or equal to {min_value}"
            raise ValueError(error_msg)

        if max_value is not None and value > max_value:
            error_msg = error_message or f"Parameter '{param_name}' must be less than or equal to {max_value}"
            raise ValueError(error_msg)
    
    # Check regex pattern for strings
    # Use pattern if provided (for backward compatibility), otherwise use regex_pattern
    actual_pattern = pattern or regex_pattern
    
    if actual_pattern is not None:
        if not isinstance(value, str):
            error_msg = error_message or f"Parameter '{param_name}' must be a string for regex validation"
            raise ValueError(error_msg)
        
        if isinstance(actual_pattern, str):
            compiled_pattern = re.compile(actual_pattern)
        else:
            compiled_pattern = actual_pattern
        
        if not compiled_pattern.match(value):
            error_msg = error_message or f"Parameter '{param_name}' does not match required pattern"
            raise ValueError(error_msg)
    
    # Check custom validator
    if custom_validator is not None:
        if not custom_validator(value):
            error_msg = error_message or f"Parameter '{param_name}' failed custom validation"
            raise ValidationError(error_msg)


def ensure_type(
    value: Any,
    expected_type: Union[Type, tuple[Type, ...]],
    param_name: str = "value",
    convert: bool = True,
    strict: bool = False,
) -> Any:
    """Ensure a value is of the expected type, optionally converting it.
    
    This function checks if a value matches the expected type(s). If convert is True,
    it will attempt to convert the value to the expected type. If strict is True,
    it will raise an error if the value is not already of the expected type.
    
    Args:
        value: The value to check and potentially convert.
        expected_type: The expected type or tuple of allowed types.
        param_name: The name of the parameter being validated (used in error messages).
        convert: If True, attempt to convert the value to the expected type.
            If False, only validate the type without conversion.
        strict: If True, raise an error if the value is not already of the
            expected type (no conversion attempted). This parameter is ignored
            if convert is False.
            
    Returns:
        The original value if it matches the expected type, or the converted
        value if conversion was successful.
            
    Raises:
        ValidationError: If the value is not of the expected type and conversion
            is disabled or fails.
        TypeConversionError: If type conversion fails when convert is True.
            
    Examples:
        >>> ensure_type("5", int, "number")
        # Returns 5 (converted from string to int)
        
        >>> ensure_type("5", int, "number", convert=False)
        # Raises ValidationError: Parameter 'number' must be of type <class 'int'>
        
        >>> ensure_type(5, int, "number", strict=True)
        # Returns 5 (already correct type)
        
        >>> ensure_type("5", int, "number", strict=True)
        # Raises ValidationError: Parameter 'number' must be of type <class 'int'>
        
        >>> ensure_type("5", (int, str), "number")
        # Returns "5" (already one of the allowed types)
        
        >>> ensure_type(None, int, "number")
        # Raises ValidationError: Parameter 'number' cannot be None
    """
    # Check for None
    if value is None:
        raise ValidationError(f"Parameter '{param_name}' cannot be None")
    
    # If already the correct type, return it
    if isinstance(value, expected_type):
        return value
    
    # If strict mode is enabled and convert is True, raise error
    if strict and convert:
        raise ValidationError(
            f"Parameter '{param_name}' must be of type {expected_type}, "
            f"got {type(value).__name__}"
        )
    
    # If convert is False, just validate the type
    if not convert:
        raise ValidationError(
            f"Parameter '{param_name}' must be of type {expected_type}, "
            f"got {type(value).__name__}"
        )
    
    # Attempt conversion
    try:
        # Handle tuple of types
        if isinstance(expected_type, tuple):
            # Try each type in order, return first successful conversion
            for type_option in expected_type:
                try:
                    return type_option(value)
                except (ValueError, TypeError):
                    continue
            # If all conversions failed
            raise TypeConversionError(
                f"Failed to convert parameter '{param_name}' from {type(value).__name__} "
                f"to any of the expected types: {expected_type}"
            )
        else:
            # Single type conversion
            return expected_type(value)
    except (ValueError, TypeError) as e:
        raise TypeConversionError(
            f"Failed to convert parameter '{param_name}' from {type(value).__name__} "
            f"to {expected_type}: {str(e)}"
        ) from e