"""Unit tests for the ErrorHandler class, create_error_context function, and wrap_naq_exception function."""

import asyncio
import json
import pickle
import threading
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.exceptions import NaqException, NaqConnectionError, ConfigurationError, SerializationError
from naq.utils.error_handling import ErrorHandler, create_error_context, wrap_naq_exception


class TestErrorHandler:
    """Test cases for the ErrorHandler class."""

    def test_init_with_default_logger(self):
        """Test ErrorHandler initialization with default logger."""
        error_handler = ErrorHandler()
        assert error_handler._logger is not None
        assert error_handler._handlers == {}

    def test_init_with_custom_logger(self):
        """Test ErrorHandler initialization with custom logger."""
        custom_logger = MagicMock()
        error_handler = ErrorHandler(custom_logger)
        assert error_handler._logger is custom_logger
        assert error_handler._handlers == {}

    def test_register_handler_with_valid_exception_and_handler(self):
        """Test registering a handler with valid exception type and handler."""
        error_handler = ErrorHandler()
        handler = MagicMock()

        error_handler.register_handler(ValueError, handler)
        assert ValueError in error_handler._handlers
        assert error_handler._handlers[ValueError] is handler

    def test_register_handler_with_invalid_exception_type(self):
        """Test registering a handler with invalid exception type."""
        error_handler = ErrorHandler()
        handler = MagicMock()

        with pytest.raises(ValueError, match="is not a valid Exception class"):
            error_handler.register_handler(str, handler)  # str is not an Exception

    def test_register_handler_with_invalid_handler(self):
        """Test registering a handler with invalid handler function."""
        error_handler = ErrorHandler()

        with pytest.raises(ValueError, match="Handler for .* must be callable"):
            error_handler.register_handler(ValueError, "not_a_function")

    def test_handle_error_with_no_registered_handler(self):
        """Test handling an error with no registered handler."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        context = {"operation": "test"}

        with patch.object(error_handler, '_logger') as mock_logger:
            with pytest.raises(ValueError, match="Test error"):
                error_handler.handle_error(error, context, reraise=True)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            assert "Error occurred: Test error" in mock_logger.error.call_args[0][0]
            assert "Context: operation=test" in mock_logger.error.call_args[0][0]

    def test_handle_error_with_sync_handler(self):
        """Test handling an error with a synchronous handler."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock(return_value=None)
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger'):
            # Should not raise exception since reraise is False by default
            result = error_handler.handle_error(error, reraise=False)
            assert result is False  # Handler didn't suppress the exception

            # Verify handler was called
            handler.assert_called_once_with(error, {})

    def test_handle_error_with_sync_handler_that_suppresses(self):
        """Test handling an error with a sync handler that suppresses the exception."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock(return_value=True)  # Return True to suppress
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=True)
            assert result is True  # Exception was suppressed

            # Verify handler was called
            handler.assert_called_once_with(error, {})

    def test_handle_error_with_async_handler(self):
        """Test handling an error with an asynchronous handler."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = AsyncMock(return_value=None)
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=False)
            assert result is False  # Handler didn't suppress the exception

            # Verify handler was called
            handler.assert_called_once_with(error, {})

    def test_handle_error_with_async_handler_that_suppresses(self):
        """Test handling an error with an async handler that suppresses the exception."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = AsyncMock(return_value=True)  # Return True to suppress
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=True)
            assert result is True  # Exception was suppressed

            # Verify handler was called
            handler.assert_called_once_with(error, {})

    def test_handle_error_with_parent_exception_handler(self):
        """Test handling an error with a handler for a parent exception type."""
        error_handler = ErrorHandler()
        error = NaqConnectionError("Connection failed")
        handler = MagicMock(return_value=True)
        # Register handler for parent class
        error_handler.register_handler(NaqException, handler)

        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=True)
            assert result is True  # Exception was suppressed

            # Verify handler was called
            handler.assert_called_once_with(error, {})

    def test_handle_error_with_multiple_handlers_in_hierarchy(self):
        """Test handling an error with multiple handlers in the exception hierarchy."""
        error_handler = ErrorHandler()
        error = NaqConnectionError("Connection failed")
        
        # Register handlers for both specific and parent exception types
        specific_handler = MagicMock(return_value=None)
        parent_handler = MagicMock(return_value=True)
        
        error_handler.register_handler(NaqConnectionError, specific_handler)
        error_handler.register_handler(NaqException, parent_handler)

        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=True)
            assert result is True  # Exception was suppressed by parent handler

            # Verify specific handler was called first
            specific_handler.assert_called_once_with(error, {})
            # Verify parent handler was called after specific handler
            parent_handler.assert_called_once_with(error, {})

    def test_handle_error_with_handler_exception(self):
        """Test handling an error when the handler itself raises an exception."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock(side_effect=RuntimeError("Handler failed"))
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger') as mock_logger:
            with pytest.raises(ValueError, match="Test error"):
                error_handler.handle_error(error, reraise=True)

            # Verify original error was logged
            assert mock_logger.error.call_count == 2
            # First call is for the original error
            assert "Error occurred: Test error" in mock_logger.error.call_args_list[0][0][0]
            # Second call is for the handler error
            assert "Error executing handler for ValueError" in mock_logger.error.call_args_list[1][0][0]

    def test_handle_error_with_context(self):
        """Test handling an error with context information."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock()
        error_handler.register_handler(ValueError, handler)

        context = {"operation": "test", "user_id": 123}
        
        with patch.object(error_handler, '_logger'):
            error_handler.handle_error(error, context, reraise=False)

            # Verify handler was called with context
            handler.assert_called_once_with(error, context)

    def test_handle_error_with_none_context(self):
        """Test handling an error with None context."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock()
        error_handler.register_handler(ValueError, handler)

        with patch.object(error_handler, '_logger'):
            error_handler.handle_error(error, None, reraise=False)

            # Verify handler was called with empty dict
            handler.assert_called_once_with(error, {})

    def test_call_handler_with_sync_handler(self):
        """Test _call_handler with a synchronous handler."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = MagicMock(return_value="result")
        context = {"key": "value"}

        result = error_handler._call_handler(handler, error, context)
        assert result == "result"
        handler.assert_called_once_with(error, context)

    def test_call_handler_with_async_handler(self):
        """Test _call_handler with an asynchronous handler."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        handler = AsyncMock(return_value="result")
        context = {"key": "value"}

        result = error_handler._call_handler(handler, error, context)
        assert result == "result"
        handler.assert_called_once_with(error, context)

    def test_log_error_with_context(self):
        """Test _log_error with context information."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        context = {"operation": "test", "user_id": 123}

        with patch.object(error_handler, '_logger') as mock_logger:
            error_handler._log_error(error, context)

            # Verify error was logged with context
            mock_logger.error.assert_called_once()
            log_message = mock_logger.error.call_args[0][0]
            assert "Error occurred: Test error" in log_message
            assert "Context: operation=test, user_id=123" in log_message
            assert "Traceback:" in log_message

    def test_log_error_without_context(self):
        """Test _log_error without context information."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        context = {}

        with patch.object(error_handler, '_logger') as mock_logger:
            error_handler._log_error(error, context)

            # Verify error was logged without context
            mock_logger.error.assert_called_once()
            log_message = mock_logger.error.call_args[0][0]
            assert "Error occurred: Test error" in log_message
            assert "Context:" not in log_message
            assert "Traceback:" in log_message

    def test_get_exception_hierarchy_with_simple_exception(self):
        """Test _get_exception_hierarchy with a simple exception."""
        error_handler = ErrorHandler()
        hierarchy = error_handler._get_exception_hierarchy(ValueError)
        
        # Should include ValueError and its parent classes
        assert ValueError in hierarchy
        assert Exception in hierarchy
        assert BaseException in hierarchy

    def test_get_exception_hierarchy_with_custom_exception(self):
        """Test _get_exception_hierarchy with a custom exception."""
        error_handler = ErrorHandler()
        hierarchy = error_handler._get_exception_hierarchy(NaqConnectionError)
        
        # Should include the specific exception and all parent classes
        assert NaqConnectionError in hierarchy
        assert NaqException in hierarchy
        assert Exception in hierarchy
        assert BaseException in hierarchy

    def test_get_exception_hierarchy_order(self):
        """Test _get_exception_hierarchy returns types in correct order."""
        error_handler = ErrorHandler()
        hierarchy = error_handler._get_exception_hierarchy(NaqConnectionError)
        
        # Most specific type should be first
        assert hierarchy[0] is NaqConnectionError
        assert hierarchy[1] is NaqException
        assert hierarchy[2] is Exception
        assert hierarchy[3] is BaseException

    @pytest.mark.asyncio
    async def test_async_handler_integration(self):
        """Test integration with async handlers in async context."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        
        # Create an async handler that does some async work
        async def async_handler(error, context):
            await asyncio.sleep(0.01)  # Simulate async work
            return True  # Suppress the exception
        
        error_handler.register_handler(ValueError, async_handler)

        # This should work even in an async context
        result = error_handler.handle_error(error, reraise=True)
        assert result is True  # Exception was suppressed

    def test_handler_with_custom_return_values(self):
        """Test handlers with different return values."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        
        # Test with None return (default)
        handler_none = MagicMock(return_value=None)
        error_handler.register_handler(ValueError, handler_none)
        
        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=False)
            assert result is False  # Exception not suppressed
        
        # Test with False return
        handler_false = MagicMock(return_value=False)
        error_handler._handlers[ValueError] = handler_false
        
        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=False)
            assert result is False  # Exception not suppressed
        
        # Test with True return
        handler_true = MagicMock(return_value=True)
        error_handler._handlers[ValueError] = handler_true
        
        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=True)
            assert result is True  # Exception suppressed

    def test_handler_with_custom_object_return(self):
        """Test handlers with custom object return values."""
        error_handler = ErrorHandler()
        error = ValueError("Test error")
        
        # Test with custom object return (should not suppress)
        custom_obj = object()
        handler_custom = MagicMock(return_value=custom_obj)
        error_handler.register_handler(ValueError, handler_custom)
        
        with patch.object(error_handler, '_logger'):
            result = error_handler.handle_error(error, reraise=False)
            assert result is False  # Exception not suppressed (only True suppresses)


class TestCreateErrorContext:
    """Test cases for the create_error_context function."""

    def test_create_error_context_returns_dict_with_all_keys(self):
        """Test that create_error_context returns a dictionary with all expected keys."""
        operation_name = "test_operation"
        error_context = create_error_context(operation_name)
        
        # Verify all expected keys are present
        assert "operation_name" in error_context
        assert "timestamp" in error_context
        assert "traceback" in error_context
        assert "thread_id" in error_context
        
        # Verify it's a dictionary
        assert isinstance(error_context, dict)

    def test_create_error_context_operation_name(self):
        """Test that create_error_context correctly sets the operation_name."""
        operation_name = "test_operation"
        error_context = create_error_context(operation_name)
        
        assert error_context["operation_name"] == operation_name

    def test_create_error_context_timestamp(self):
        """Test that create_error_context includes a valid timestamp."""
        operation_name = "test_operation"
        before_call = time.time()
        error_context = create_error_context(operation_name)
        after_call = time.time()
        
        # Verify timestamp is a number
        assert isinstance(error_context["timestamp"], (int, float))
        
        # Verify timestamp is between before and after call times
        assert before_call <= error_context["timestamp"] <= after_call

    def test_create_error_context_thread_id(self):
        """Test that create_error_context includes the correct thread ID."""
        operation_name = "test_operation"
        error_context = create_error_context(operation_name)
        
        # Verify thread_id is an integer
        assert isinstance(error_context["thread_id"], int)
        
        # Verify thread_id matches current thread ID
        assert error_context["thread_id"] == threading.get_ident()

    def test_create_error_context_traceback_without_exception(self):
        """Test that create_error_context includes traceback information."""
        operation_name = "test_operation"
        error_context = create_error_context(operation_name)
        
        # Verify traceback is a string
        assert isinstance(error_context["traceback"], str)
        
        # When called outside of exception context, should indicate no exception
        assert "NoneType: None" in error_context["traceback"] or "No exception" in error_context["traceback"]

    def test_create_error_context_traceback_with_exception(self):
        """Test that create_error_context captures exception traceback correctly."""
        operation_name = "test_operation"
        
        try:
            # Force an exception
            1 / 0
        except Exception:
            error_context = create_error_context(operation_name)
            
            # Verify traceback contains the exception information
            assert "ZeroDivisionError" in error_context["traceback"]
            assert "division by zero" in error_context["traceback"]

    def test_create_error_context_different_operations(self):
        """Test create_error_context with different operation names."""
        operations = ["op1", "operation_two", "TEST_OPERATION", "operation with spaces"]
        
        for op in operations:
            error_context = create_error_context(op)
            assert error_context["operation_name"] == op

    def test_create_error_context_multiple_calls(self):
        """Test that multiple calls to create_error_context return different timestamps."""
        operation_name = "test_operation"
        
        # Call the function twice with a small delay
        context1 = create_error_context(operation_name)
        time.sleep(0.01)  # Small delay to ensure different timestamps
        context2 = create_error_context(operation_name)
        
        # Verify timestamps are different
        assert context1["timestamp"] < context2["timestamp"]
        
        # Verify other fields are as expected
        assert context1["operation_name"] == context2["operation_name"] == operation_name
        assert context1["thread_id"] == context2["thread_id"] == threading.get_ident()

    def test_create_error_context_thread_consistency(self):
        """Test that create_error_context returns consistent thread ID."""
        operation_name = "test_operation"
        
        # Call the function multiple times
        context1 = create_error_context(operation_name)
        context2 = create_error_context(operation_name)
        
        # Verify thread ID is consistent across calls
        assert context1["thread_id"] == context2["thread_id"]
        assert context1["thread_id"] == threading.get_ident()


class TestWrapNaqException:
    """Test cases for the wrap_naq_exception function."""

    def test_wrap_connection_error(self):
        """Test wrapping a ConnectionError to NaqConnectionError."""
        original_error = ConnectionError("Connection failed")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, NaqConnectionError)
        assert str(wrapped_error) == "Connection failed"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_value_error(self):
        """Test wrapping a ValueError to ConfigurationError."""
        original_error = ValueError("Invalid value")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, ConfigurationError)
        assert str(wrapped_error) == "Invalid value"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_type_error(self):
        """Test wrapping a TypeError to ConfigurationError."""
        original_error = TypeError("Type mismatch")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, ConfigurationError)
        assert str(wrapped_error) == "Type mismatch"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_pickle_pickling_error(self):
        """Test wrapping a pickle.PicklingError to SerializationError."""
        original_error = pickle.PicklingError("Cannot pickle object")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, SerializationError)
        assert str(wrapped_error) == "Cannot pickle object"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_pickle_unpickling_error(self):
        """Test wrapping a pickle.UnpicklingError to SerializationError."""
        original_error = pickle.UnpicklingError("Cannot unpickle object")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, SerializationError)
        assert str(wrapped_error) == "Cannot unpickle object"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_json_decode_error(self):
        """Test wrapping a json.JSONDecodeError to SerializationError."""
        original_error = json.JSONDecodeError("Invalid JSON", "doc", 0)
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, SerializationError)
        assert "Invalid JSON" in str(wrapped_error)
        assert wrapped_error.__cause__ is original_error

    def test_wrap_generic_exception(self):
        """Test wrapping a generic Exception to NaqException."""
        original_error = Exception("Generic error")
        wrapped_error = wrap_naq_exception(original_error)

        assert isinstance(wrapped_error, NaqException)
        assert str(wrapped_error) == "Generic error"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_with_context(self):
        """Test wrapping an exception with context string."""
        original_error = ConnectionError("Connection failed")
        context = "During server connection"
        wrapped_error = wrap_naq_exception(original_error, context=context)

        assert isinstance(wrapped_error, NaqConnectionError)
        assert str(wrapped_error) == "During server connection: Connection failed"
        assert wrapped_error.__cause__ is original_error

    def test_wrap_without_original_traceback(self):
        """Test wrapping an exception without preserving original traceback."""
        original_error = ConnectionError("Connection failed")
        wrapped_error = wrap_naq_exception(original_error, original_traceback=False)

        assert isinstance(wrapped_error, NaqConnectionError)
        assert str(wrapped_error) == "Connection failed"
        assert not hasattr(wrapped_error, '__cause__') or wrapped_error.__cause__ is None

    def test_wrap_with_context_and_no_traceback(self):
        """Test wrapping with context but without original traceback."""
        original_error = ValueError("Invalid value")
        context = "Configuration validation"
        wrapped_error = wrap_naq_exception(
            original_error,
            context=context,
            original_traceback=False
        )

        assert isinstance(wrapped_error, ConfigurationError)
        assert str(wrapped_error) == "Configuration validation: Invalid value"
        assert not hasattr(wrapped_error, '__cause__') or wrapped_error.__cause__ is None

    def test_wrap_with_non_exception_raises_type_error(self):
        """Test that wrapping a non-Exception raises TypeError."""
        with pytest.raises(TypeError, match="Expected Exception instance, got"):
            wrap_naq_exception("not an exception")

    def test_wrap_with_none_raises_type_error(self):
        """Test that wrapping None raises TypeError."""
        with pytest.raises(TypeError, match="Expected Exception instance, got"):
            wrap_naq_exception(None)

    def test_wrap_preserves_exception_hierarchy(self):
        """Test that wrapped exceptions maintain proper inheritance hierarchy."""
        # Test that all wrapped exceptions inherit from NaqException
        test_cases = [
            (ConnectionError("test"), NaqConnectionError),
            (ValueError("test"), ConfigurationError),
            (TypeError("test"), ConfigurationError),
            (pickle.PicklingError("test"), SerializationError),
            (json.JSONDecodeError("test", "doc", 0), SerializationError),
            (Exception("test"), NaqException),
        ]

        for original_error, expected_type in test_cases:
            wrapped_error = wrap_naq_exception(original_error)
            assert isinstance(wrapped_error, expected_type)
            assert isinstance(wrapped_error, NaqException)

    def test_wrap_empty_context_string(self):
        """Test wrapping with empty context string."""
        original_error = ValueError("test")
        wrapped_error = wrap_naq_exception(original_error, context="")

        assert str(wrapped_error) == "test"

    def test_wrap_exception_with_empty_message(self):
        """Test wrapping an exception with empty message."""
        original_error = ValueError("")
        wrapped_error = wrap_naq_exception(original_error, context="context")

        assert str(wrapped_error) == "context: "

    def test_wrap_exception_with_no_message_and_no_context(self):
        """Test wrapping an exception with no message and no context."""
        original_error = ValueError("")
        wrapped_error = wrap_naq_exception(original_error)

        assert str(wrapped_error) == ""

    def test_wrap_custom_exception_message_formatting(self):
        """Test that context and original message are properly formatted."""
        original_error = ConnectionError("Network unreachable")
        context = "API call to example.com"
        wrapped_error = wrap_naq_exception(original_error, context=context)

        expected_message = "API call to example.com: Network unreachable"
        assert str(wrapped_error) == expected_message

    def test_original_exception_chaining_behavior(self):
        """Test that original exception chaining works correctly."""
        try:
            raise ConnectionError("Original error")
        except ConnectionError as e:
            wrapped_error = wrap_naq_exception(e, context="Wrapper context")
            
            # Test that the original exception is properly chained
            assert wrapped_error.__cause__ is e
            assert str(wrapped_error.__cause__) == "Original error"
            
            # Test that the traceback includes the original exception
            try:
                raise wrapped_error
            except NaqConnectionError:
                import sys
                exc_type, exc_value, exc_traceback = sys.exc_info()
                assert exc_value is wrapped_error
                assert exc_value.__cause__ is e

    def test_no_exception_chaining_when_disabled(self):
        """Test that exception chaining is disabled when original_traceback=False."""
        original_error = ConnectionError("Original error")
        wrapped_error = wrap_naq_exception(original_error, original_traceback=False)
        
        # Should not have __cause__ attribute or it should be None
        assert not hasattr(wrapped_error, '__cause__') or wrapped_error.__cause__ is None