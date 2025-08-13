"""
Error handling utilities for NAQ.

This module provides centralized error handling patterns, including error context creation,
exception wrapping, recovery strategies, and structured error logging for the NAQ job queue system.
"""

import asyncio
import inspect
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any, Callable, Coroutine, Dict, List, Optional, Type, TypeVar, Union, Tuple, Set
)
from datetime import datetime
import uuid

from loguru import logger

from ..exceptions import (
    NaqException, ConfigurationError, SerializationError, 
    JobExecutionError, JobNotFoundError, NaqConnectionError
)
from .retry import RetryConfig, RetryError
from .logging import get_logger, LogContext, StructuredLogger

T = TypeVar("T")


class ErrorSeverity(Enum):
    """Enumeration of error severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Enumeration of error categories for classification and handling."""
    CONNECTION = "connection"
    CONFIGURATION = "configuration"
    SERIALIZATION = "serialization"
    EXECUTION = "execution"
    VALIDATION = "validation"
    TIMEOUT = "timeout"
    RESOURCE = "resource"
    UNKNOWN = "unknown"


class RecoveryStrategy(Enum):
    """Enumeration of error recovery strategies."""
    RETRY = "retry"
    FALLBACK = "fallback"
    SKIP = "skip"
    ABORT = "abort"
    CUSTOM = "custom"


@dataclass
class ErrorContext:
    """
    Structured context for error information.
    
    This class provides a standardized way to capture and propagate error context
    throughout the application, including operation details, timestamps, and tracebacks.
    """
    
    operation: str
    error_type: str
    error_message: str
    timestamp: datetime = field(default_factory=datetime.now)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    severity: ErrorSeverity = ErrorSeverity.ERROR
    category: ErrorCategory = ErrorCategory.UNKNOWN
    traceback: Optional[str] = None
    context_data: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    original_exception: Optional[Exception] = None
    
    def __post_init__(self):
        """Validate and normalize error context data."""
        if isinstance(self.severity, str):
            self.severity = ErrorSeverity(self.severity.lower())
        if isinstance(self.category, str):
            self.category = ErrorCategory(self.category.lower())
        
        # Capture traceback if not provided and original exception exists
        if self.traceback is None and self.original_exception is not None:
            self.traceback = "".join(
                traceback.format_exception(
                    type(self.original_exception),
                    self.original_exception,
                    self.original_exception.__traceback__
                )
            )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error context to dictionary for serialization."""
        return {
            "operation": self.operation,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat(),
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "severity": self.severity.value,
            "category": self.category.value,
            "traceback": self.traceback,
            "context_data": self.context_data,
            "retry_count": self.retry_count,
        }
    
    def add_context(self, **kwargs: Any) -> None:
        """Add additional context data."""
        self.context_data.update(kwargs)


@dataclass
class RecoveryConfig:
    """Configuration for error recovery strategies."""
    
    strategy: RecoveryStrategy = RecoveryStrategy.RETRY
    max_retries: int = 3
    retry_delay: float = 1.0
    fallback_value: Any = None
    custom_handler: Optional[Callable[[ErrorContext], Any]] = None
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (
        KeyboardInterrupt, SystemExit, MemoryError, SyntaxError
    )
    
    def __post_init__(self):
        """Validate recovery configuration."""
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")
        if self.strategy == RecoveryStrategy.CUSTOM and self.custom_handler is None:
            raise ValueError("custom_handler must be provided when strategy is CUSTOM")


class ErrorHandler:
    """
    Centralized error handler with configurable strategies.
    
    This class provides comprehensive error handling capabilities including
    error callback registration, sync/async error handling, and integration
    with existing retry mechanisms.
    """
    
    def __init__(self, default_recovery_config: Optional[RecoveryConfig] = None):
        """
        Initialize the error handler.
        
        Args:
            default_recovery_config: Default recovery configuration to use
        """
        self.default_recovery_config = default_recovery_config or RecoveryConfig()
        self.error_callbacks: Dict[ErrorCategory, List[Callable[[ErrorContext], None]]] = {}
        self.global_callbacks: List[Callable[[ErrorContext], None]] = []
        self._lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        
        # Initialize callback dictionaries for all error categories
        for category in ErrorCategory:
            self.error_callbacks[category] = []
    
    def register_callback(
        self, 
        callback: Callable[[ErrorContext], None],
        category: Optional[ErrorCategory] = None
    ) -> None:
        """
        Register an error callback.
        
        Args:
            callback: Function to call when an error occurs
            category: Error category to register for, or None for global callback
        """
        with self._lock:
            if category is None:
                self.global_callbacks.append(callback)
            else:
                self.error_callbacks[category].append(callback)
    
    def unregister_callback(
        self, 
        callback: Callable[[ErrorContext], None],
        category: Optional[ErrorCategory] = None
    ) -> None:
        """
        Unregister an error callback.
        
        Args:
            callback: Function to remove from callbacks
            category: Error category to unregister from, or None for global callback
        """
        with self._lock:
            if category is None:
                if callback in self.global_callbacks:
                    self.global_callbacks.remove(callback)
            else:
                if callback in self.error_callbacks[category]:
                    self.error_callbacks[category].remove(callback)
    
    async def register_callback_async(
        self, 
        callback: Callable[[ErrorContext], Coroutine[Any, Any, None]],
        category: Optional[ErrorCategory] = None
    ) -> None:
        """
        Register an async error callback.
        
        Args:
            callback: Async function to call when an error occurs
            category: Error category to register for, or None for global callback
        """
        async with self._async_lock:
            sync_callback = self._wrap_async_callback(callback)
            self.register_callback(sync_callback, category)
    
    def _wrap_async_callback(
        self, 
        async_callback: Callable[[ErrorContext], Coroutine[Any, Any, None]]
    ) -> Callable[[ErrorContext], None]:
        """
        Wrap an async callback to be called synchronously.
        
        Args:
            async_callback: Async callback function to wrap
            
        Returns:
            Synchronous wrapper function
        """
        def wrapper(error_context: ErrorContext) -> None:
            try:
                # Run the async callback in a new event loop
                asyncio.run(async_callback(error_context))
            except Exception as e:
                logger.error(f"Error in async error callback: {e}")
        
        return wrapper
    
    def handle_error(
        self,
        exception: Exception,
        operation: str,
        recovery_config: Optional[RecoveryConfig] = None,
        **context_data: Any
    ) -> ErrorContext:
        """
        Handle an error with the configured recovery strategy.
        
        Args:
            exception: The exception to handle
            operation: Name of the operation that failed
            recovery_config: Optional recovery configuration to use
            **context_data: Additional context data
            
        Returns:
            ErrorContext object with error details
        """
        config = recovery_config or self.default_recovery_config
        
        # Create error context
        error_context = create_error_context(
            operation=operation,
            exception=exception,
            **context_data
        )
        
        # Determine error category
        error_context.category = self._classify_error(exception)
        
        # Trigger callbacks
        self._trigger_callbacks(error_context)
        
        # Apply recovery strategy
        return self._apply_recovery_strategy(error_context, config)
    
    async def handle_error_async(
        self,
        exception: Exception,
        operation: str,
        recovery_config: Optional[RecoveryConfig] = None,
        **context_data: Any
    ) -> ErrorContext:
        """
        Handle an error asynchronously with the configured recovery strategy.
        
        Args:
            exception: The exception to handle
            operation: Name of the operation that failed
            recovery_config: Optional recovery configuration to use
            **context_data: Additional context data
            
        Returns:
            ErrorContext object with error details
        """
        config = recovery_config or self.default_recovery_config
        
        # Create error context
        error_context = create_error_context(
            operation=operation,
            exception=exception,
            **context_data
        )
        
        # Determine error category
        error_context.category = self._classify_error(exception)
        
        # Trigger callbacks
        await self._trigger_callbacks_async(error_context)
        
        # Apply recovery strategy
        return await self._apply_recovery_strategy_async(error_context, config)
    
    def _classify_error(self, exception: Exception) -> ErrorCategory:
        """
        Classify an exception into an error category.
        
        Args:
            exception: The exception to classify
            
        Returns:
            ErrorCategory for the exception
        """
        if isinstance(exception, NaqConnectionError):
            return ErrorCategory.CONNECTION
        elif isinstance(exception, ConfigurationError):
            return ErrorCategory.CONFIGURATION
        elif isinstance(exception, SerializationError):
            return ErrorCategory.SERIALIZATION
        elif isinstance(exception, JobExecutionError):
            return ErrorCategory.EXECUTION
        elif isinstance(exception, JobNotFoundError):
            return ErrorCategory.EXECUTION
        elif isinstance(exception, (TimeoutError, asyncio.TimeoutError)):
            return ErrorCategory.TIMEOUT
        elif isinstance(exception, (MemoryError, OSError)):
            return ErrorCategory.RESOURCE
        else:
            return ErrorCategory.UNKNOWN
    
    def _trigger_callbacks(self, error_context: ErrorContext) -> None:
        """
        Trigger registered callbacks for the error context.
        
        Args:
            error_context: Error context to pass to callbacks
        """
        # Trigger category-specific callbacks
        for callback in self.error_callbacks[error_context.category]:
            try:
                callback(error_context)
            except Exception as e:
                logger.error(f"Error in error callback: {e}")
        
        # Trigger global callbacks
        for callback in self.global_callbacks:
            try:
                callback(error_context)
            except Exception as e:
                logger.error(f"Error in global error callback: {e}")
    
    async def _trigger_callbacks_async(self, error_context: ErrorContext) -> None:
        """
        Trigger registered async callbacks for the error context.
        
        Args:
            error_context: Error context to pass to callbacks
        """
        # Trigger category-specific callbacks
        for callback in self.error_callbacks[error_context.category]:
            try:
                if inspect.iscoroutinefunction(callback):
                    await callback(error_context)
                else:
                    callback(error_context)
            except Exception as e:
                logger.error(f"Error in error callback: {e}")
        
        # Trigger global callbacks
        for callback in self.global_callbacks:
            try:
                if inspect.iscoroutinefunction(callback):
                    await callback(error_context)
                else:
                    callback(error_context)
            except Exception as e:
                logger.error(f"Error in global error callback: {e}")
    
    def _apply_recovery_strategy(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """
        Apply the configured recovery strategy.
        
        Args:
            error_context: Error context to recover from
            config: Recovery configuration to use
            
        Returns:
            Updated error context
        """
        strategy = config.strategy
        
        if strategy == RecoveryStrategy.RETRY:
            return self._handle_retry(error_context, config)
        elif strategy == RecoveryStrategy.FALLBACK:
            return self._handle_fallback(error_context, config)
        elif strategy == RecoveryStrategy.SKIP:
            return self._handle_skip(error_context)
        elif strategy == RecoveryStrategy.ABORT:
            return self._handle_abort(error_context)
        elif strategy == RecoveryStrategy.CUSTOM:
            return self._handle_custom(error_context, config)
        else:
            logger.error(f"Unknown recovery strategy: {strategy}")
            return error_context
    
    async def _apply_recovery_strategy_async(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """
        Apply the configured recovery strategy asynchronously.
        
        Args:
            error_context: Error context to recover from
            config: Recovery configuration to use
            
        Returns:
            Updated error context
        """
        strategy = config.strategy
        
        if strategy == RecoveryStrategy.RETRY:
            return await self._handle_retry_async(error_context, config)
        elif strategy == RecoveryStrategy.FALLBACK:
            return self._handle_fallback(error_context, config)
        elif strategy == RecoveryStrategy.SKIP:
            return self._handle_skip(error_context)
        elif strategy == RecoveryStrategy.ABORT:
            return self._handle_abort(error_context)
        elif strategy == RecoveryStrategy.CUSTOM:
            return await self._handle_custom_async(error_context, config)
        else:
            logger.error(f"Unknown recovery strategy: {strategy}")
            return error_context
    
    def _handle_retry(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """Handle retry recovery strategy."""
        if error_context.retry_count >= config.max_retries:
            logger.error(f"Max retries exceeded for {error_context.operation}")
            error_context.severity = ErrorSeverity.CRITICAL
            return error_context
        
        # Check if exception is retryable
        if isinstance(error_context.original_exception, config.non_retryable_exceptions):
            logger.error(f"Non-retryable exception: {error_context.original_exception}")
            return error_context
        
        if not isinstance(error_context.original_exception, config.retryable_exceptions):
            logger.error(f"Exception not in retryable list: {error_context.original_exception}")
            return error_context
        
        # Log retry attempt
        logger.warning(
            f"Retrying {error_context.operation} "
            f"(attempt {error_context.retry_count + 1}/{config.max_retries}) "
            f"after {config.retry_delay}s delay"
        )
        
        # Increment retry count
        error_context.retry_count += 1
        
        # Sleep before retry (in a real implementation, this would be handled by the caller)
        time.sleep(config.retry_delay)
        
        return error_context
    
    async def _handle_retry_async(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """Handle retry recovery strategy asynchronously."""
        if error_context.retry_count >= config.max_retries:
            logger.error(f"Max retries exceeded for {error_context.operation}")
            error_context.severity = ErrorSeverity.CRITICAL
            return error_context
        
        # Check if exception is retryable
        if isinstance(error_context.original_exception, config.non_retryable_exceptions):
            logger.error(f"Non-retryable exception: {error_context.original_exception}")
            return error_context
        
        if not isinstance(error_context.original_exception, config.retryable_exceptions):
            logger.error(f"Exception not in retryable list: {error_context.original_exception}")
            return error_context
        
        # Log retry attempt
        logger.warning(
            f"Retrying {error_context.operation} "
            f"(attempt {error_context.retry_count + 1}/{config.max_retries}) "
            f"after {config.retry_delay}s delay"
        )
        
        # Increment retry count
        error_context.retry_count += 1
        
        # Sleep before retry (in a real implementation, this would be handled by the caller)
        await asyncio.sleep(config.retry_delay)
        
        return error_context
    
    def _handle_fallback(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """Handle fallback recovery strategy."""
        logger.info(f"Using fallback value for {error_context.operation}")
        error_context.add_context(fallback_value=config.fallback_value)
        error_context.severity = ErrorSeverity.WARNING
        return error_context
    
    def _handle_skip(self, error_context: ErrorContext) -> ErrorContext:
        """Handle skip recovery strategy."""
        logger.info(f"Skipping {error_context.operation} due to error")
        error_context.severity = ErrorSeverity.WARNING
        return error_context
    
    def _handle_abort(self, error_context: ErrorContext) -> ErrorContext:
        """Handle abort recovery strategy."""
        logger.error(f"Aborting {error_context.operation} due to error")
        error_context.severity = ErrorSeverity.CRITICAL
        return error_context
    
    def _handle_custom(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """Handle custom recovery strategy."""
        if config.custom_handler:
            try:
                result = config.custom_handler(error_context)
                error_context.add_context(custom_recovery_result=result)
                logger.info(f"Custom recovery applied for {error_context.operation}")
            except Exception as e:
                logger.error(f"Custom recovery handler failed: {e}")
                error_context.add_context(custom_recovery_error=str(e))
        return error_context
    
    async def _handle_custom_async(
        self, 
        error_context: ErrorContext, 
        config: RecoveryConfig
    ) -> ErrorContext:
        """Handle custom recovery strategy asynchronously."""
        if config.custom_handler:
            try:
                if inspect.iscoroutinefunction(config.custom_handler):
                    result = await config.custom_handler(error_context)
                else:
                    result = config.custom_handler(error_context)
                error_context.add_context(custom_recovery_result=result)
                logger.info(f"Custom recovery applied for {error_context.operation}")
            except Exception as e:
                logger.error(f"Custom recovery handler failed: {e}")
                error_context.add_context(custom_recovery_error=str(e))
        return error_context


# Thread-safe error context storage
_thread_local = threading.local()
_error_context_stack: List[ErrorContext] = []


def create_error_context(
    operation: str,
    exception: Exception,
    severity: ErrorSeverity = ErrorSeverity.ERROR,
    category: Optional[ErrorCategory] = None,
    **context_data: Any
) -> ErrorContext:
    """
    Create a structured error context.
    
    This function provides a consistent way to create error context objects
    with operation details, timestamps, and tracebacks.
    
    Args:
        operation: Name of the operation that failed
        exception: The exception that occurred
        severity: Error severity level
        category: Error category (auto-detected if None)
        **context_data: Additional context data
        
    Returns:
        ErrorContext object with structured error information
        
    Example:
        ```python
        try:
            # Some operation that might fail
            result = risky_operation()
        except Exception as e:
            error_ctx = create_error_context(
                operation="risky_operation",
                exception=e,
                input_data=data,
                user_id=user.id
            )
            logger.error(f"Operation failed: {error_ctx.to_dict()}")
        ```
    """
    # Auto-detect category if not provided
    if category is None:
        if isinstance(exception, NaqConnectionError):
            category = ErrorCategory.CONNECTION
        elif isinstance(exception, ConfigurationError):
            category = ErrorCategory.CONFIGURATION
        elif isinstance(exception, SerializationError):
            category = ErrorCategory.SERIALIZATION
        elif isinstance(exception, JobExecutionError):
            category = ErrorCategory.EXECUTION
        elif isinstance(exception, JobNotFoundError):
            category = ErrorCategory.EXECUTION
        elif isinstance(exception, (TimeoutError, asyncio.TimeoutError)):
            category = ErrorCategory.TIMEOUT
        elif isinstance(exception, (MemoryError, OSError)):
            category = ErrorCategory.RESOURCE
        else:
            category = ErrorCategory.UNKNOWN
    
    return ErrorContext(
        operation=operation,
        error_type=type(exception).__name__,
        error_message=str(exception),
        severity=severity,
        category=category,
        original_exception=exception,
        context_data=context_data
    )


@contextmanager
def error_context(
    operation: str,
    **context_data: Any
):
    """
    Context manager for automatic error context creation and management.
    
    This context manager automatically creates error context when an exception
    occurs within the managed block and provides thread-safe context storage.
    
    Args:
        operation: Name of the operation being performed
        **context_data: Additional context data
        
    Example:
        ```python
        with error_context("process_data", data_id=data.id) as ctx:
            # Operation that might fail
            result = process_data(data)
            # Add result to context
            ctx.add_context(result=result)
        ```
    """
    error_ctx = None
    try:
        # Create initial context
        error_ctx = ErrorContext(
            operation=operation,
            error_type="",
            error_message="",
            severity=ErrorSeverity.INFO,
            context_data=context_data
        )
        
        # Push to thread-local storage
        if not hasattr(_thread_local, 'error_context_stack'):
            _thread_local.error_context_stack = []
        _thread_local.error_context_stack.append(error_ctx)
        
        yield error_ctx
        
    except Exception as e:
        # Update context with error information
        if error_ctx is not None:
            error_ctx.error_type = type(e).__name__
            error_ctx.error_message = str(e)
            error_ctx.original_exception = e
            error_ctx.severity = ErrorSeverity.ERROR
            error_ctx.category = _classify_error_for_context(e)
            
            # Capture traceback
            error_ctx.traceback = "".join(
                traceback.format_exception(
                    type(e),
                    e,
                    e.__traceback__
                )
            )
        
        # Re-raise the exception
        raise
        
    finally:
        # Pop from thread-local storage
        if hasattr(_thread_local, 'error_context_stack') and _thread_local.error_context_stack:
            _thread_local.error_context_stack.pop()


def get_current_error_context() -> Optional[ErrorContext]:
    """
    Get the current error context from thread-local storage.
    
    Returns:
        Current ErrorContext if available, None otherwise
    """
    if hasattr(_thread_local, 'error_context_stack') and _thread_local.error_context_stack:
        return _thread_local.error_context_stack[-1]
    return None


def _classify_error_for_context(exception: Exception) -> ErrorCategory:
    """Classify an exception for error context creation."""
    if isinstance(exception, NaqConnectionError):
        return ErrorCategory.CONNECTION
    elif isinstance(exception, ConfigurationError):
        return ErrorCategory.CONFIGURATION
    elif isinstance(exception, SerializationError):
        return ErrorCategory.SERIALIZATION
    elif isinstance(exception, JobExecutionError):
        return ErrorCategory.EXECUTION
    elif isinstance(exception, JobNotFoundError):
        return ErrorCategory.EXECUTION
    elif isinstance(exception, (TimeoutError, asyncio.TimeoutError)):
        return ErrorCategory.TIMEOUT
    elif isinstance(exception, (MemoryError, OSError)):
        return ErrorCategory.RESOURCE
    else:
        return ErrorCategory.UNKNOWN


def wrap_naq_exception(
    exception: Exception,
    operation: str = "",
    **context_data: Any
) -> NaqException:
    """
    Wrap a generic exception in a NAQ-specific exception.
    
    This function provides standardized exception conversion, mapping common
    exceptions to NAQ-specific exceptions while preserving original traceback.
    
    Args:
        exception: The original exception to wrap
        operation: Name of the operation that failed
        **context_data: Additional context data
        
    Returns:
        NAQ-specific exception with original context preserved
        
    Example:
        ```python
        try:
            # Some operation that might raise a generic exception
            result = external_api_call()
        except Exception as e:
            naq_exception = wrap_naq_exception(
                e, 
                operation="external_api_call",
                api_endpoint="https://api.example.com/data"
            )
            raise naq_exception from e
        ```
    """
    # Create error context for the original exception
    error_context = create_error_context(
        operation=operation or "unknown_operation",
        exception=exception,
        **context_data
    )
    
    # Map common exceptions to NAQ-specific exceptions
    if isinstance(exception, (ConnectionError, OSError)) and "connection" in str(exception).lower():
        naq_exception = NaqConnectionError(str(exception))
    elif isinstance(exception, (ValueError, KeyError)) and "config" in str(exception).lower():
        naq_exception = ConfigurationError(str(exception))
    elif isinstance(exception, (pickle.PickleError, json.JSONDecodeError)):
        naq_exception = SerializationError(str(exception))
    elif isinstance(exception, (TimeoutError, asyncio.TimeoutError)):
        naq_exception = JobExecutionError(f"Timeout during {operation}: {exception}")
    elif isinstance(exception, (FileNotFoundError, AttributeError)):
        naq_exception = JobNotFoundError(str(exception))
    else:
        # Default to JobExecutionError for unknown exceptions
        naq_exception = JobExecutionError(f"Error during {operation}: {exception}")
    
    # Attach error context to the new exception
    naq_exception.error_context = error_context
    
    # Preserve original traceback
    naq_exception.__cause__ = exception
    
    return naq_exception


class ErrorReporter:
    """
    Error reporting and aggregation utility.
    
    This class provides structured error logging, error aggregation,
    and reporting capabilities for monitoring and debugging.
    """
    
    def __init__(self, logger_name: str = "naq.error_reporter"):
        """
        Initialize the error reporter.
        
        Args:
            logger_name: Name for the structured logger
        """
        self.logger = StructuredLogger(logger_name)
        self.error_counts: Dict[str, int] = {}
        self.error_history: List[ErrorContext] = []
        self._lock = threading.Lock()
        self._max_history_size = 1000
    
    def report_error(
        self,
        error_context: ErrorContext,
        log_level: str = "ERROR"
    ) -> None:
        """
        Report an error with structured logging.
        
        Args:
            error_context: Error context to report
            log_level: Log level to use
        """
        with self._lock:
            # Update error counts
            error_key = f"{error_context.operation}:{error_context.error_type}"
            self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
            
            # Add to history
            self.error_history.append(error_context)
            
            # Limit history size
            if len(self.error_history) > self._max_history_size:
                self.error_history = self.error_history[-self._max_history_size:]
        
        # Log the error
        log_data = error_context.to_dict()
        getattr(self.logger, log_level.lower())(
            f"Error in {error_context.operation}",
            **log_data
        )
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get a summary of reported errors.
        
        Returns:
            Dictionary with error statistics and summary
        """
        with self._lock:
            return {
                "total_errors": sum(self.error_counts.values()),
                "unique_error_types": len(self.error_counts),
                "error_counts": self.error_counts.copy(),
                "recent_errors": [
                    ctx.to_dict() for ctx in self.error_history[-10:]
                ]
            }
    
    def get_errors_by_category(self, category: ErrorCategory) -> List[ErrorContext]:
        """
        Get errors filtered by category.
        
        Args:
            category: Error category to filter by
            
        Returns:
            List of error contexts matching the category
        """
        with self._lock:
            return [ctx for ctx in self.error_history if ctx.category == category]
    
    def get_errors_by_operation(self, operation: str) -> List[ErrorContext]:
        """
        Get errors filtered by operation.
        
        Args:
            operation: Operation name to filter by
            
        Returns:
            List of error contexts matching the operation
        """
        with self._lock:
            return [ctx for ctx in self.error_history if ctx.operation == operation]
    
    def clear_history(self) -> None:
        """Clear error history and counts."""
        with self._lock:
            self.error_counts.clear()
            self.error_history.clear()


# Global error handler instance
_default_error_handler = ErrorHandler()
_default_error_reporter = ErrorReporter()


def get_default_error_handler() -> ErrorHandler:
    """Get the default error handler instance."""
    return _default_error_handler


def get_default_error_reporter() -> ErrorReporter:
    """Get the default error reporter instance."""
    return _default_error_reporter


def handle_error_with_default_handler(
    exception: Exception,
    operation: str,
    **context_data: Any
) -> ErrorContext:
    """
    Handle an error using the default error handler.
    
    Args:
        exception: The exception to handle
        operation: Name of the operation that failed
        **context_data: Additional context data
        
    Returns:
        ErrorContext object with error details
    """
    return _default_error_handler.handle_error(exception, operation, **context_data)


async def handle_error_with_default_handler_async(
    exception: Exception,
    operation: str,
    **context_data: Any
) -> ErrorContext:
    """
    Handle an error asynchronously using the default error handler.
    
    Args:
        exception: The exception to handle
        operation: Name of the operation that failed
        **context_data: Additional context data
        
    Returns:
        ErrorContext object with error details
    """
    return await _default_error_handler.handle_error_async(exception, operation, **context_data)


def report_error_with_default_reporter(
    error_context: ErrorContext,
    log_level: str = "ERROR"
) -> None:
    """
    Report an error using the default error reporter.
    
    Args:
        error_context: Error context to report
        log_level: Log level to use
    """
    _default_error_reporter.report_error(error_context, log_level)


# Decorators for error handling

def with_error_handling(
    operation: Optional[str] = None,
    recovery_config: Optional[RecoveryConfig] = None,
    reraise: bool = True,
    error_handler: Optional[ErrorHandler] = None
):
    """
    Decorator for automatic error handling.
    
    Args:
        operation: Operation name (defaults to function name)
        recovery_config: Recovery configuration to use
        reraise: Whether to re-raise exceptions after handling
        error_handler: Error handler to use (defaults to default handler)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation or func.__name__
        handler = error_handler or _default_error_handler
        
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    error_context = await handler.handle_error_async(
                        e, op_name, recovery_config, 
                        function_args=args, function_kwargs=kwargs
                    )
                    if reraise:
                        raise
                    return None  # Or return a default value
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_context = handler.handle_error(
                        e, op_name, recovery_config,
                        function_args=args, function_kwargs=kwargs
                    )
                    if reraise:
                        raise
                    return None  # Or return a default value
            return sync_wrapper
    
    return decorator


def with_error_context(operation: Optional[str] = None):
    """
    Decorator for automatic error context creation.
    
    Args:
        operation: Operation name (defaults to function name)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation or func.__name__
        
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                with error_context(op_name, function_args=args, function_kwargs=kwargs) as ctx:
                    try:
                        result = await func(*args, **kwargs)
                        ctx.add_context(success=True, result=result)
                        return result
                    except Exception as e:
                        ctx.add_context(success=False)
                        raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                with error_context(op_name, function_args=args, function_kwargs=kwargs) as ctx:
                    try:
                        result = func(*args, **kwargs)
                        ctx.add_context(success=True, result=result)
                        return result
                    except Exception as e:
                        ctx.add_context(success=False)
                        raise
            return sync_wrapper
    
    return decorator


# Import required modules
import functools
import pickle
import json