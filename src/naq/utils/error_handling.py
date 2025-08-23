"""Error handling utilities for NAQ.

This module contains common error handling utilities used throughout the NAQ codebase.
"""

import json
import pickle
import threading
import time
import traceback
from typing import Any, Callable, Dict, Optional, Type

import anyio
from loguru import logger

from ..exceptions import (
    ConfigurationError,
    NaqConnectionError,
    NaqException,
    SerializationError,
)


class ErrorHandler:
    """A centralized error handler for managing exceptions in the NAQ system.

    The ErrorHandler class provides a mechanism to register specific handlers for
    different types of exceptions and ensures consistent error logging and handling
    throughout the application. It supports both synchronous and asynchronous
    handler functions and can handle exception hierarchies by attempting to call
    handlers for parent exception types when no specific handler is found.

    Examples:
        Basic usage with a custom handler:

        ```python
        from naq.utils.error_handling import ErrorHandler
        from naq.exceptions import NaqConnectionError

        def handle_connection_error(error, context):
            print(f"Connection error occurred: {error}")
            # Perform recovery actions

        error_handler = ErrorHandler()
        error_handler.register_handler(NaqConnectionError, handle_connection_error)

        try:
            # Code that might raise NaqConnectionError
            pass
        except NaqConnectionError as e:
            error_handler.handle_error(e, context={"operation": "connect"})
        ```

        Using with an async handler:

        ```python
        async def async_handle_error(error, context):
            # Async error handling logic
            pass

        error_handler.register_handler(NaqException, async_handle_error)

        try:
            # Code that might raise NaqException
            pass
        except NaqException as e:
            await error_handler.handle_error(e, context={"operation": "process"}, reraise=False)
        ```
    """

    def __init__(self, logger_instance: Optional[Any] = None) -> None:
        """Initialize the ErrorHandler with an optional logger.

        Args:
            logger_instance: Optional logger instance to use for error logging.
                If None, uses the default loguru logger.
        """
        self._logger = logger_instance or logger
        self._handlers: Dict[Type[Exception], Callable] = {}

    def register_handler(
        self,
        exception_type: Type[Exception],
        handler: Callable[[Exception, Dict[str, Any]], Any],
    ) -> None:
        """Register a handler function for a specific exception type.

        Args:
            exception_type: The exception type to register the handler for.
            handler: The handler function to call when the exception occurs.
                The handler should accept two parameters:
                - The exception instance
                - A context dictionary containing additional information

        Examples:
            Registering a handler for ValueError:

            ```python
            def handle_value_error(error, context):
                print(f"Value error: {error}")

            error_handler.register_handler(ValueError, handle_value_error)
            ```
        """
        if not issubclass(exception_type, Exception):
            raise ValueError(f"{exception_type} is not a valid Exception class")

        if not callable(handler):
            raise ValueError(f"Handler for {exception_type} must be callable")

        self._handlers[exception_type] = handler

    def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        reraise: bool = True,
    ) -> bool:
        """Handle an error by logging it and calling registered handlers.

        This method logs the error with context and traceback, then attempts to call
        a specific handler for the exception type. If no specific handler is found,
        it attempts to call handlers for parent exception types in the hierarchy.

        Args:
            error: The exception to handle.
            context: Optional dictionary containing additional context information
                that will be passed to the handler and included in the log.
            reraise: Whether to re-raise the exception after handling. If True,
                the exception will be re-raised unless a handler explicitly
                suppresses it by returning True.

        Returns:
            bool: True if the exception was handled and suppressed, False otherwise.

        Examples:
            Basic error handling:

            ```python
            try:
                # Code that might raise an exception
                pass
            except Exception as e:
                handled = error_handler.handle_error(e, context={"operation": "process"})
                if handled:
                    print("Error was handled and suppressed")
            ```
        """
        if context is None:
            context = {}

        # Log the error with context and traceback
        self._log_error(error, context)

        # Try to find and call an appropriate handler
        handler_suppressed = False

        # Get the exception type and all its parent types
        exception_types = self._get_exception_hierarchy(type(error))

        for exc_type in exception_types:
            handler = self._handlers.get(exc_type)
            if handler:
                try:
                    result = self._call_handler(handler, error, context)

                    # If handler returns True, it suppresses the exception
                    if result is True:
                        handler_suppressed = True
                        break

                except Exception as handler_error:
                    # Log handler execution error but continue with other handlers
                    self._logger.error(
                        f"Error executing handler for {exc_type.__name__}: {handler_error}"
                    )

        # Re-raise if requested and not suppressed
        if reraise and not handler_suppressed:
            raise error

        return handler_suppressed

    def _call_handler(
        self,
        handler: Callable[[Exception, Dict[str, Any]], Any],
        error: Exception,
        context: Dict[str, Any],
    ) -> Any:
        """Call a handler function, handling both sync and async handlers.

        Args:
            handler: The handler function to call.
            error: The exception to pass to the handler.
            context: The context dictionary to pass to the handler.

        Returns:
            The result of the handler function.
        """
        # Check if the handler is async
        if anyio.is_async_callable(handler):
            # Run the async handler
            return anyio.run(handler, error, context)
        else:
            # Run the sync handler
            return handler(error, context)

    def _log_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Log the error with context and traceback.

        Args:
            error: The exception to log.
            context: Additional context information to include in the log.
        """
        # Get the traceback as a string
        tb_str = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

        # Prepare log message with context
        context_str = ", ".join(f"{k}={v}" for k, v in context.items())
        if context_str:
            context_str = f" | Context: {context_str}"

        # Log the error with full details
        self._logger.error(
            f"Error occurred: {error}{context_str}\nTraceback:\n{tb_str}"
        )

    def _get_exception_hierarchy(
        self, exception_type: Type[Exception]
    ) -> list[Type[Exception]]:
        """Get the exception type and all its parent types in order.

        Args:
            exception_type: The exception type to get the hierarchy for.

        Returns:
            A list of exception types, starting with the most specific.
        """
        hierarchy = []
        current_type = exception_type

        # Add the type and all its parent types
        while current_type is not None and issubclass(current_type, Exception):
            hierarchy.append(current_type)
            # Move to the parent class
            current_type = current_type.__bases__[0] if current_type.__bases__ else None

        return hierarchy


def create_error_context(operation_name: str) -> Dict[str, Any]:
    """Create a dictionary with contextual information about an error.

    This function captures essential contextual information when an error occurs,
    including the operation name, timestamp, current traceback, and thread ID.
    This information is useful for debugging and error reporting purposes.

    Args:
        operation_name: The name of the operation that failed or where the error occurred.
            This should be a descriptive string that identifies the specific operation
            or function that was being executed when the error was encountered.

    Returns:
        A dictionary containing the following keys:
            - operation_name: The name of the operation that failed.
            - timestamp: The current Unix timestamp when the error context was created.
            - traceback: The current exception traceback as a string, formatted using
                traceback.format_exc(). If no exception is currently being handled,
                this will contain a message indicating no traceback is available.
            - thread_id: The identifier of the current thread where the error occurred,
                obtained using threading.get_ident().

    Examples:
        Basic usage:

        ```python
        from naq.utils.error_handling import create_error_context

        try:
            # Code that might raise an exception
            result = 1 / 0
        except Exception:
            error_context = create_error_context("division_operation")
            # error_context will contain:
            # {
            #     "operation_name": "division_operation",
            #     "timestamp": 1625097600.0,
            #     "traceback": "Traceback (most recent call last):\n...",
            #     "thread_id": 12345
            # }
        ```

        Using with error logging:

        ```python
        import logging

        try:
            # Risky operation
            process_data(data)
        except Exception:
            context = create_error_context("data_processing")
            logging.error(f"Error in {context['operation_name']}: {context['traceback']}")
            # Additional error handling logic
        ```

    Note:
        This function should be called within an exception handler (except block)
        to capture the most relevant traceback information. If called outside of
        an exception context, the traceback value will indicate that no exception
        is currently being handled.
    """
    return {
        "operation_name": operation_name,
        "timestamp": time.time(),
        "traceback": traceback.format_exc(),
        "thread_id": threading.get_ident(),
    }


def wrap_naq_exception(
    exception: Exception,
    context: Optional[str] = None,
    original_traceback: bool = True,
) -> NaqException:
    """Wrap a generic exception in an appropriate NAQ-specific exception.

    This function maps common Python exceptions to their NAQ-specific counterparts,
    providing more meaningful error context and maintaining the original exception
    chain when requested.

    Args:
        exception: The original exception to wrap.
        context: Optional string providing additional context about where or why
            the exception occurred. This will be included in the exception message.
        original_traceback: If True, the original exception is chained using the
            'from' keyword to preserve the full traceback. If False, only the
            message is preserved without chaining.

    Returns:
        An appropriate NAQ-specific exception based on the type of the original
        exception. If no specific mapping is found, returns a generic NaqException.

    Raises:
        TypeError: If the provided exception is not an instance of Exception.

    Examples:
        Basic usage with a ConnectionError:

        ```python
        try:
            # Code that might raise ConnectionError
            connect_to_server()
        except ConnectionError as e:
            naq_error = wrap_naq_exception(e, context="server connection")
            raise naq_error
        ```

        Usage with context and without original traceback:

        ```python
        try:
            # Code that might raise ValueError
            process_config(config_data)
        except ValueError as e:
            naq_error = wrap_naq_exception(
                e,
                context="configuration processing",
                original_traceback=False
            )
            raise naq_error
        ```

        Handling different exception types:

        ```python
        try:
            # Code that might raise various exceptions
            result = risky_operation()
        except Exception as e:
            naq_error = wrap_naq_exception(e, context="risky operation")
            # naq_error will be appropriately typed based on the original exception
            logger.error(f"Operation failed: {naq_error}")
            raise naq_error
        ```

    Note:
        The following exception mappings are supported:
        - ConnectionError -> NaqConnectionError
        - ValueError -> ConfigurationError
        - TypeError -> ConfigurationError
        - pickle.PicklingError -> SerializationError
        - pickle.UnpicklingError -> SerializationError
        - json.JSONDecodeError -> SerializationError
        - All other exceptions -> NaqException
    """
    if not isinstance(exception, Exception):
        raise TypeError(f"Expected Exception instance, got {type(exception)}")

    # Determine the appropriate NAQ exception type based on the original exception
    if isinstance(exception, ConnectionError):
        naq_exception_class = NaqConnectionError
    elif isinstance(exception, (ValueError, TypeError)) and not isinstance(
        exception, json.JSONDecodeError
    ):
        naq_exception_class = ConfigurationError
    elif isinstance(
        exception, (pickle.PicklingError, pickle.UnpicklingError, json.JSONDecodeError)
    ):
        naq_exception_class = SerializationError
    else:
        naq_exception_class = NaqException

    # Create the exception message with optional context
    message = str(exception)
    if context:
        message = f"{context}: {message}"

    # Create the NAQ exception
    naq_exception = naq_exception_class(message)

    # Chain the original exception if requested
    if original_traceback:
        naq_exception.__cause__ = exception

    return naq_exception
