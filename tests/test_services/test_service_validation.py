"""Unit tests for the validation utility functions."""

import re
import pytest

from naq.exceptions import ValidationError, TypeConversionError
from naq.utils.validation import validate_parameter, ensure_type


class TestValidateParameter:
    """Test cases for the validate_parameter function."""

    def test_validate_parameter_with_valid_value(self):
        """Test validate_parameter with a valid value."""
        # Should not raise any exception
        validate_parameter("test", "param")

    def test_validate_parameter_with_none_and_not_none_false(self):
        """Test validate_parameter with None value when not_none is False."""
        # Should not raise any exception
        validate_parameter(None, "param", not_none=False)

    def test_validate_parameter_with_none_and_not_none_true(self):
        """Test validate_parameter with None value when not_none is True."""
        with pytest.raises(ValidationError, match="Parameter 'param' cannot be None"):
            validate_parameter(None, "param", not_none=True)

    def test_validate_parameter_with_custom_error_message_for_none(self):
        """Test validate_parameter with custom error message for None validation."""
        with pytest.raises(ValidationError, match="Custom error message"):
            validate_parameter(None, "param", not_none=True, error_message="Custom error message")

    def test_validate_parameter_with_min_value_valid(self):
        """Test validate_parameter with valid min value."""
        validate_parameter(5, "param", min_value=0)

    def test_validate_parameter_with_min_value_invalid(self):
        """Test validate_parameter with invalid min value."""
        with pytest.raises(ValidationError, match="Parameter 'param' must be greater than or equal to 10"):
            validate_parameter(5, "param", min_value=10)

    def test_validate_parameter_with_max_value_valid(self):
        """Test validate_parameter with valid max value."""
        validate_parameter(5, "param", max_value=10)

    def test_validate_parameter_with_max_value_invalid(self):
        """Test validate_parameter with invalid max value."""
        with pytest.raises(ValidationError, match="Parameter 'param' must be less than or equal to 5"):
            validate_parameter(10, "param", max_value=5)

    def test_validate_parameter_with_min_max_value_valid(self):
        """Test validate_parameter with valid min and max values."""
        validate_parameter(5, "param", min_value=0, max_value=10)

    def test_validate_parameter_with_min_max_value_invalid(self):
        """Test validate_parameter with invalid min and max values."""
        with pytest.raises(ValidationError, match="Parameter 'param' must be greater than or equal to 0"):
            validate_parameter(-1, "param", min_value=0, max_value=10)

    def test_validate_parameter_with_non_numeric_value_and_min_max(self):
        """Test validate_parameter with non-numeric value when min/max validation is used."""
        with pytest.raises(ValidationError, match="Parameter 'param' must be numeric for min/max validation"):
            validate_parameter("not a number", "param", min_value=0, max_value=10)

    def test_validate_parameter_with_regex_pattern_valid(self):
        """Test validate_parameter with valid regex pattern."""
        validate_parameter("abc123", "param", regex_pattern=r'^[a-z]+\d+$')

    def test_validate_parameter_with_regex_pattern_invalid(self):
        """Test validate_parameter with invalid regex pattern."""
        with pytest.raises(ValidationError, match="Parameter 'param' does not match required pattern"):
            validate_parameter("123abc", "param", regex_pattern=r'^[a-z]+\d+$')

    def test_validate_parameter_with_compiled_regex_pattern_valid(self):
        """Test validate_parameter with valid compiled regex pattern."""
        pattern = re.compile(r'^[a-z]+\d+$')
        validate_parameter("abc123", "param", regex_pattern=pattern)

    def test_validate_parameter_with_compiled_regex_pattern_invalid(self):
        """Test validate_parameter with invalid compiled regex pattern."""
        pattern = re.compile(r'^[a-z]+\d+$')
        with pytest.raises(ValidationError, match="Parameter 'param' does not match required pattern"):
            validate_parameter("123abc", "param", regex_pattern=pattern)

    def test_validate_parameter_with_non_string_value_and_regex(self):
        """Test validate_parameter with non-string value when regex validation is used."""
        with pytest.raises(ValidationError, match="Parameter 'param' must be a string for regex validation"):
            validate_parameter(123, "param", regex_pattern=r'^\d+$')

    def test_validate_parameter_with_custom_validator_valid(self):
        """Test validate_parameter with valid custom validator."""
        def is_even(value):
            return value % 2 == 0
        
        validate_parameter(4, "param", custom_validator=is_even)

    def test_validate_parameter_with_custom_validator_invalid(self):
        """Test validate_parameter with invalid custom validator."""
        def is_even(value):
            return value % 2 == 0
        
        with pytest.raises(ValidationError, match="Parameter 'param' failed custom validation"):
            validate_parameter(5, "param", custom_validator=is_even)

    def test_validate_parameter_with_custom_error_message_for_custom_validator(self):
        """Test validate_parameter with custom error message for custom validator."""
        def is_even(value):
            return value % 2 == 0
        
        with pytest.raises(ValidationError, match="Custom validation error"):
            validate_parameter(5, "param", custom_validator=is_even, error_message="Custom validation error")

    def test_validate_parameter_with_none_skips_other_validations(self):
        """Test validate_parameter skips other validations when value is None and not_none is False."""
        # Should not raise any exception even though other validations would fail
        validate_parameter(None, "param", not_none=False, min_value=0, max_value=10, regex_pattern=r'^test$')

    def test_validate_parameter_with_multiple_validations_valid(self):
        """Test validate_parameter with multiple valid validations."""
        validate_parameter(
            "test123",
            "param",
            not_none=True,
            regex_pattern=r'^[a-z]+\d+$',
            custom_validator=lambda x: len(x) > 5
        )

    def test_validate_parameter_with_multiple_validations_invalid(self):
        """Test validate_parameter with multiple invalid validations."""
        with pytest.raises(ValidationError, match="Parameter 'param' cannot be None"):
            validate_parameter(
                None,
                "param",
                not_none=True,
                min_value=0,
                max_value=10,
                regex_pattern=r'^test$'
            )


class TestEnsureType:
    """Test cases for the ensure_type function."""

    def test_ensure_type_with_correct_type(self):
        """Test ensure_type with value of correct type."""
        result = ensure_type(5, int, "number")
        assert result == 5
        assert isinstance(result, int)

    def test_ensure_type_with_none_value(self):
        """Test ensure_type with None value."""
        with pytest.raises(ValidationError, match="Parameter 'number' cannot be None"):
            ensure_type(None, int, "number")

    def test_ensure_type_with_conversion_enabled(self):
        """Test ensure_type with type conversion enabled."""
        result = ensure_type("5", int, "number", convert=True)
        assert result == 5
        assert isinstance(result, int)

    def test_ensure_type_with_conversion_disabled(self):
        """Test ensure_type with type conversion disabled."""
        with pytest.raises(ValidationError, match="Parameter 'number' must be of type <class 'int'>"):
            ensure_type("5", int, "number", convert=False)

    def test_ensure_type_with_strict_mode_enabled(self):
        """Test ensure_type with strict mode enabled."""
        with pytest.raises(ValidationError, match="Parameter 'number' must be of type <class 'int'>"):
            ensure_type("5", int, "number", strict=True)

    def test_ensure_type_with_strict_mode_and_correct_type(self):
        """Test ensure_type with strict mode and correct type."""
        result = ensure_type(5, int, "number", strict=True)
        assert result == 5
        assert isinstance(result, int)

    def test_ensure_type_with_multiple_allowed_types(self):
        """Test ensure_type with multiple allowed types."""
        result = ensure_type("5", (int, str), "value")
        assert result == "5"
        assert isinstance(result, str)

    def test_ensure_type_with_multiple_allowed_types_and_conversion(self):
        """Test ensure_type with multiple allowed types and conversion."""
        result = ensure_type("5", (int, float), "value")
        assert result == 5
        assert isinstance(result, int)

    def test_ensure_type_with_failed_conversion(self):
        """Test ensure_type with failed type conversion."""
        with pytest.raises(TypeConversionError, match="Failed to convert parameter 'text' from <class 'int'> to <class 'str'>"):
            ensure_type(5, str, "text")

    def test_ensure_type_with_failed_conversion_to_multiple_types(self):
        """Test ensure_type with failed conversion to multiple types."""
        with pytest.raises(TypeConversionError, match="Failed to convert parameter 'value' from <class 'str'> to any of the expected types"):
            ensure_type("not a number", (int, float), "value")

    def test_ensure_type_with_custom_error_message_for_conversion_failure(self):
        """Test ensure_type with custom error message for conversion failure."""
        try:
            ensure_type("not a number", int, "value")
        except TypeConversionError as e:
            assert "Failed to convert parameter 'value'" in str(e)

    def test_ensure_type_with_string_to_int_conversion(self):
        """Test ensure_type with string to int conversion."""
        result = ensure_type("123", int, "number")
        assert result == 123
        assert isinstance(result, int)

    def test_ensure_type_with_string_to_float_conversion(self):
        """Test ensure_type with string to float conversion."""
        result = ensure_type("123.45", float, "number")
        assert result == 123.45
        assert isinstance(result, float)

    def test_ensure_type_with_int_to_string_conversion(self):
        """Test ensure_type with int to string conversion."""
        result = ensure_type(123, str, "text")
        assert result == "123"
        assert isinstance(result, str)

    def test_ensure_type_with_float_to_int_conversion(self):
        """Test ensure_type with float to int conversion."""
        result = ensure_type(123.7, int, "number")
        assert result == 123  # Truncated, not rounded
        assert isinstance(result, int)

    def test_ensure_type_with_bool_to_int_conversion(self):
        """Test ensure_type with bool to int conversion."""
        result = ensure_type(True, int, "flag")
        assert result == 1
        assert isinstance(result, int)

    def test_ensure_type_with_int_to_bool_conversion(self):
        """Test ensure_type with int to bool conversion."""
        result = ensure_type(1, bool, "flag")
        assert result is True
        assert isinstance(result, bool)

    def test_ensure_type_with_zero_to_bool_conversion(self):
        """Test ensure_type with zero to bool conversion."""
        result = ensure_type(0, bool, "flag")
        assert result is False
        assert isinstance(result, bool)

    def test_ensure_type_with_empty_string_to_bool_conversion(self):
        """Test ensure_type with empty string to bool conversion."""
        result = ensure_type("", bool, "flag")
        assert result is False
        assert isinstance(result, bool)

    def test_ensure_type_with_non_empty_string_to_bool_conversion(self):
        """Test ensure_type with non-empty string to bool conversion."""
        result = ensure_type("text", bool, "flag")
        assert result is True
        assert isinstance(result, bool)