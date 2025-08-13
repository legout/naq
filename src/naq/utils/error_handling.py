# src/naq/utils/error_handling.py
"""
Centralized error handling utilities for NAQ.

This module provides standardized error handling classes, exception wrapping,
and error context management.
"""

import traceback
import threading
import time
import asyncio
from typing import Type, Callable, Optional, Any, Dict, Union, List
from contextlib import contextmanager, asynccontextmanager

from loguru import logger

# Import NAQ exceptions
try:
    from ..exceptions import (
        NaqException,
        NaqConnectionError, 
        ConfigurationError,
        SerializationError
    )
except ImportError:
    # Fallback for basic exception hierarchy if not available
    class NaqException(Exception):
        """Base NAQ exception."""
        pass
    
    class NaqConnectionError(NaqException):
        """Connection-related error."""
        pass
    
    class ConfigurationError(NaqException):
        """Configuration error."""
        pass
    
    class SerializationError(NaqException):
        """Serialization error."""
        pass


class ErrorHandler:
    """
    Centralized error handling with configurable strategies.
    
    Provides error registration, handling, and context management.
    """
    
    def __init__(self, logger_instance=None, default_reraise: bool = True):
        """
        Initialize error handler.
        
        Args:
            logger_instance: Logger to use (defaults to loguru)
            default_reraise: Whether to reraise exceptions by default
        """
        self.logger = logger_instance or logger
        self.default_reraise = default_reraise
        self.error_callbacks: Dict[Type[Exception], Callable] = {}
        self.global_handlers: List[Callable] = []
        self.metrics = {
            'total_errors': 0,
            'errors_by_type': {},
            'errors_by_context': {}
        }
    
    def register_handler(
        self, 
        exception_type: Type[Exception], 
        handler: Callable[[Exception, str], Any]
    ):
        """
        Register error handler for specific exception type.
        
        Args:
            exception_type: Exception type to handle
            handler: Handler function that takes (exception, context)
        """
        self.error_callbacks[exception_type] = handler
        self.logger.debug(f"Registered error handler for {exception_type.__name__}")
    
    def register_global_handler(self, handler: Callable[[Exception, str], Any]):
        """
        Register a global error handler that processes all errors.
        
        Args:
            handler: Handler function that takes (exception, context)
        """
        self.global_handlers.append(handler)
        self.logger.debug("Registered global error handler")
    
    async def handle_error(
        self, 
        error: Exception, 
        context: str = "",
        reraise: Optional[bool] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """
        Handle error with registered handlers.
        
        Args:
            error: Exception to handle
            context: Error context for logging
            reraise: Whether to reraise (overrides default)
            extra_data: Additional data for handlers and logging
            
        Returns:
            Result from error handler, or None
        """
        reraise = reraise if reraise is not None else self.default_reraise
        error_type = type(error)
        extra_data = extra_data or {}
        
        # Update metrics
        self._update_metrics(error_type, context)
        
        # Create error context
        error_context = self._create_error_context(error, context, extra_data)
        
        # Log the error
        self.logger.error(
            f"Error in {context}: {error}", 
            exc_info=True,
            extra=error_context
        )
        
        # Run global handlers first
        for global_handler in self.global_handlers:
            try:
                await self._call_handler(global_handler, error, context, error_context)
            except Exception as handler_error:
                self.logger.error(f"Global error handler failed: {handler_error}")
        
        # Try specific handler first
        if error_type in self.error_callbacks:
            try:
                result = await self._call_handler(
                    self.error_callbacks[error_type], 
                    error, 
                    context,
                    error_context
                )
                if result is not None and not reraise:
                    return result
            except Exception as handler_error:
                self.logger.error(f"Specific error handler failed: {handler_error}")
        
        # Try parent class handlers
        for registered_type, handler in self.error_callbacks.items():
            if isinstance(error, registered_type) and registered_type != error_type:
                try:
                    result = await self._call_handler(handler, error, context, error_context)
                    if result is not None and not reraise:
                        return result
                except Exception as handler_error:
                    self.logger.error(f"Parent class error handler failed: {handler_error}")
        
        if reraise:
            raise error
        return None
    
    def _update_metrics(self, error_type: Type[Exception], context: str):
        """Update error metrics."""
        self.metrics['total_errors'] += 1
        
        error_name = error_type.__name__
        if error_name not in self.metrics['errors_by_type']:
            self.metrics['errors_by_type'][error_name] = 0
        self.metrics['errors_by_type'][error_name] += 1
        
        if context not in self.metrics['errors_by_context']:
            self.metrics['errors_by_context'][context] = 0
        self.metrics['errors_by_context'][context] += 1
    
    def _create_error_context(
        self, 
        error: Exception, 
        context: str, 
        extra_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create error context with operation details."""
        return {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'timestamp': time.time(),
            'traceback': traceback.format_exc(),
            'thread_id': threading.get_ident(),
            **extra_data
        }
    
    async def _call_handler(
        self, 
        handler: Callable, 
        error: Exception, 
        context: str,
        error_context: Dict[str, Any]
    ) -> Any:
        """Call error handler (async or sync)."""
        if asyncio.iscoroutinefunction(handler):
            return await handler(error, context, error_context)
        else:
            return handler(error, context, error_context)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get error handling metrics."""
        return self.metrics.copy()
    
    def reset_metrics(self):
        """Reset error metrics."""
        self.metrics = {
            'total_errors': 0,
            'errors_by_type': {},
            'errors_by_context': {}
        }


def create_error_context(
    operation_name: str,
    **extra_data
) -> Dict[str, Any]:
    """
    Create error context with operation details.
    
    Args:
        operation_name: Name of the operation
        **extra_data: Additional context data
        
    Returns:
        Error context dictionary
    """
    return {
        'operation': operation_name,
        'timestamp': time.time(),
        'traceback': traceback.format_exc(),
        'thread_id': threading.get_ident(),
        **extra_data
    }


def wrap_naq_exception(
    error: Exception, 
    context: str = "",
    original_traceback: bool = True
) -> NaqException:
    """
    Wrap exception in NAQ-specific exception with context.
    
    Args:
        error: Original exception
        context: Additional context
        original_traceback: Whether to preserve original traceback
        
    Returns:
        NAQ-specific exception
    """
    message = f"{context}: {error}" if context else str(error)
    
    if isinstance(error, NaqException):
        return error
    
    # Map common exceptions to NAQ exceptions
    if isinstance(error, (ConnectionError, OSError)):
        new_error = NaqConnectionError(message)
        if original_traceback:
            new_error.__cause__ = error
        return new_error
    elif isinstance(error, (ValueError, TypeError)):
        new_error = ConfigurationError(message)
        if original_traceback:
            new_error.__cause__ = error
        return new_error
    elif hasattr(error, '__module__') and any(
        lib in str(error.__module__) for lib in ['pickle', 'json', 'msgpack']
    ):
        new_error = SerializationError(message)
        if original_traceback:
            new_error.__cause__ = error
        return new_error
    else:
        new_error = NaqException(message)
        if original_traceback:
            new_error.__cause__ = error
        return new_error


@contextmanager
def error_handler_context(
    error_handler: ErrorHandler,
    context: str,
    reraise: bool = True,
    suppress_types: tuple = ()
):
    """
    Context manager for standardized error handling.
    
    Args:
        error_handler: ErrorHandler instance
        context: Context name for logging
        reraise: Whether to reraise exceptions
        suppress_types: Exception types to suppress
        
    Usage:
        with error_handler_context(handler, "database_operation"):
            perform_database_operation()
    """
    try:
        yield
    except suppress_types:
        # Silently suppress these exception types
        pass
    except Exception as e:
        # This will be synchronous since we're in a sync context manager
        try:
            # Convert to async and wait
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're in an async context, we can't handle this properly
                # Just log and reraise or suppress based on reraise flag
                error_handler.logger.error(f"Error in {context}: {e}")
                if reraise:
                    raise
            else:
                # No running loop, we can create one
                asyncio.run(error_handler.handle_error(e, context, reraise))
        except Exception:
            # Fallback: just log and handle reraise
            error_handler.logger.error(f"Error in {context}: {e}")
            if reraise:
                raise


@asynccontextmanager
async def async_error_handler_context(
    error_handler: ErrorHandler,
    context: str,
    reraise: bool = True,
    suppress_types: tuple = ()
):
    """
    Async context manager for standardized error handling.
    
    Args:
        error_handler: ErrorHandler instance
        context: Context name for logging
        reraise: Whether to reraise exceptions
        suppress_types: Exception types to suppress
        
    Usage:
        async with async_error_handler_context(handler, "api_call"):
            await make_api_call()
    """
    try:
        yield
    except suppress_types:
        # Silently suppress these exception types
        pass
    except Exception as e:
        await error_handler.handle_error(e, context, reraise)


class ErrorAccumulator:
    """
    Accumulate multiple errors and handle them together.
    
    Useful for batch operations where you want to collect all errors
    before deciding how to handle them.
    """
    
    def __init__(self, max_errors: Optional[int] = None):
        """
        Initialize error accumulator.
        
        Args:
            max_errors: Maximum errors to collect before auto-raising
        """
        self.errors: List[tuple] = []  # (exception, context) tuples
        self.max_errors = max_errors
    
    def add_error(self, error: Exception, context: str = ""):
        """Add an error to the accumulator."""
        self.errors.append((error, context))
        
        if self.max_errors and len(self.errors) >= self.max_errors:
            self.raise_accumulated()
    
    def has_errors(self) -> bool:
        """Check if any errors were accumulated."""
        return len(self.errors) > 0
    
    def get_errors(self) -> List[tuple]:
        """Get all accumulated errors."""
        return self.errors.copy()
    
    def clear(self):
        """Clear all accumulated errors."""
        self.errors.clear()
    
    def raise_accumulated(self):
        """Raise an exception containing all accumulated errors."""
        if not self.errors:
            return
        
        error_messages = []
        for error, context in self.errors:
            msg = f"{context}: {error}" if context else str(error)
            error_messages.append(msg)
        
        combined_message = f"Multiple errors occurred:\n" + "\n".join(error_messages)
        raise NaqException(combined_message)
    
    @contextmanager
    def collect_errors(self, context: str = ""):
        """
        Context manager to collect errors instead of raising them.
        
        Args:
            context: Context for any errors that occur
            
        Usage:
            accumulator = ErrorAccumulator()
            with accumulator.collect_errors("batch_operation"):
                # Errors in this block are collected, not raised
                risky_operation()
                
            if accumulator.has_errors():
                # Handle accumulated errors
                pass
        """
        try:
            yield
        except Exception as e:
            self.add_error(e, context)


# Global error handler instance for convenience
_global_error_handler = ErrorHandler()


def get_global_error_handler() -> ErrorHandler:
    """Get the global error handler instance."""
    return _global_error_handler


def set_global_error_handler(handler: ErrorHandler):
    """Set the global error handler instance."""
    global _global_error_handler
    _global_error_handler = handler