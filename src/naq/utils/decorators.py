"""
Decorators module for NAQ.

This module provides a comprehensive collection of decorators for common patterns
used throughout the NAQ job queue system, including connection management,
retry mechanisms, logging, and performance monitoring.

All decorators support both synchronous and asynchronous functions.
"""

import asyncio
import functools
import time
from typing import Any, Callable, Coroutine, Optional, Type, Union, TypeVar, Dict, List, Tuple
from enum import Enum

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from ..exceptions import NaqException
from .retry import RetryConfig, RetryError, calculate_delay, should_retry
from .logging import get_logger

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Enumeration of supported backoff strategies for retry decorators."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


# ===== Connection Decorators =====

def with_nats_connection(config_key: Optional[str] = None):
    """
    Decorator to inject NATS connection into function.
    
    This decorator automatically manages NATS connection lifecycle, providing
    a connection to the decorated function. The connection is established
    before the function is called and closed after the function completes.
    
    Args:
        config_key: Optional configuration key to use for connection settings
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @with_nats_connection()
        async def publish_message(conn, subject: str, message: bytes):
            await conn.publish(subject, message)
        ```
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                from .connection.context_managers import get_config, nats_connection
                config = get_config()
                async with nats_connection(config) as conn:
                    return await func(conn, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in with_nats_connection decorator: {e}")
                raise
        return wrapper
    return decorator


def with_jetstream_context(config_key: Optional[str] = None):
    """
    Decorator to inject JetStream context into function.
    
    This decorator automatically manages JetStream context lifecycle, providing
    a JetStream context to the decorated function. The context is established
    before the function is called and closed after the function completes.
    
    Args:
        config_key: Optional configuration key to use for connection settings
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @with_jetstream_context()
        async def create_stream(js: JetStreamContext, stream_name: str):
            await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])
        ```
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                from .connection.context_managers import get_config, nats_jetstream
                config = get_config()
                async with nats_jetstream(config) as (conn, js):
                    return await func(js, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in with_jetstream_context decorator: {e}")
                raise
        return wrapper
    return decorator


# ===== Retry Decorators =====

def retry_with_backoff(
    func: Optional[Callable[..., T]] = None,
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    exponential_base: float = 2.0,
    jitter: bool = True,
    jitter_factor: float = 0.1,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator to retry a function with configurable backoff strategies.
    
    This decorator supports multiple backoff strategies and can handle both
    synchronous and asynchronous functions.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        backoff_strategy: Backoff strategy to use (exponential, linear, or fixed)
        exponential_base: Base for exponential backoff
        jitter: Whether to add jitter to delays
        jitter_factor: Factor for jitter calculation (0-1)
        retryable_exceptions: Exception types to retry on
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @retry_with_backoff(max_attempts=5, backoff_strategy=BackoffStrategy.LINEAR)
        def fetch_data(url):
            # Make HTTP request
            pass
            
        @retry_with_backoff(backoff_strategy=BackoffStrategy.EXPONENTIAL, jitter=True)
        async def async_fetch_data(url):
            # Make async HTTP request
            pass
        ```
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        jitter_factor=jitter_factor,
        retryable_exceptions=retryable_exceptions
    )
    
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(f):
            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                last_exception = None
                
                for attempt in range(config.max_attempts):
                    try:
                        return await f(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        
                        if not should_retry(e, attempt, config):
                            raise RetryError(
                                f"Function failed after {attempt + 1} attempts: {e}",
                                attempts=attempt + 1,
                                last_exception=e
                            ) from e
                        
                        if attempt < config.max_attempts - 1:
                            delay = _calculate_backoff_delay(attempt, config, backoff_strategy)
                            logger.warning(f"Retry attempt {attempt + 1}/{config.max_attempts} after {delay:.2f}s delay")
                            await asyncio.sleep(delay)
                
                # This should never be reached
                raise RetryError(
                    f"Function failed after {config.max_attempts} attempts",
                    attempts=config.max_attempts,
                    last_exception=last_exception
                )
            
            return async_wrapper
        else:
            @functools.wraps(f)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                last_exception = None
                
                for attempt in range(config.max_attempts):
                    try:
                        return f(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        
                        if not should_retry(e, attempt, config):
                            raise RetryError(
                                f"Function failed after {attempt + 1} attempts: {e}",
                                attempts=attempt + 1,
                                last_exception=e
                            ) from e
                        
                        if attempt < config.max_attempts - 1:
                            delay = _calculate_backoff_delay(attempt, config, backoff_strategy)
                            logger.warning(f"Retry attempt {attempt + 1}/{config.max_attempts} after {delay:.2f}s delay")
                            time.sleep(delay)
                
                # This should never be reached
                raise RetryError(
                    f"Function failed after {config.max_attempts} attempts",
                    attempts=config.max_attempts,
                    last_exception=last_exception
                )
            
            return sync_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def _calculate_backoff_delay(attempt: int, config: RetryConfig, strategy: BackoffStrategy) -> float:
    """
    Calculate delay based on the specified backoff strategy.
    
    Args:
        attempt: Current attempt number (0-based)
        config: Retry configuration
        strategy: Backoff strategy to use
        
    Returns:
        Delay in seconds
    """
    if strategy == BackoffStrategy.EXPONENTIAL:
        delay = config.base_delay * (config.exponential_base ** attempt)
    elif strategy == BackoffStrategy.LINEAR:
        delay = config.base_delay * (attempt + 1)
    elif strategy == BackoffStrategy.FIXED:
        delay = config.base_delay
    else:
        raise ValueError(f"Unknown backoff strategy: {strategy}")
    
    # Apply maximum delay cap
    delay = min(delay, config.max_delay)
    
    # Add jitter if enabled
    if config.jitter:
        jitter_range = delay * config.jitter_factor
        delay = delay + random.uniform(-jitter_range, jitter_range)
    
    # Ensure delay is non-negative
    return max(0, delay)


# ===== Timing and Performance Decorators =====

def timing(
    func: Optional[Callable[..., T]] = None,
    *,
    log_level: str = "INFO",
    slow_threshold: Optional[float] = None,
    slow_log_level: str = "WARNING",
    include_args: bool = False,
    include_result: bool = False
):
    """
    Decorator to measure and log function execution time.
    
    This decorator measures the execution time of functions and logs it.
    It can also log slow operations with a different log level.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        log_level: Log level for normal execution time
        slow_threshold: Threshold in seconds for considering an operation slow
        slow_log_level: Log level for slow operations
        include_args: Whether to include function arguments in the log
        include_result: Whether to include function result in the log
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @timing(slow_threshold=1.0)
        def process_data(data):
            # Process data
            pass
            
        @timing(include_args=True, include_result=True)
        async def async_process_data(data):
            # Process data asynchronously
            pass
        ```
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(f):
            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                start_time = time.time()
                
                # Log function call with args if requested
                if include_args:
                    getattr(logger_, log_level.lower())(
                        f"Calling {f.__name__} with args={args}, kwargs={kwargs}"
                    )
                
                try:
                    result = await f(*args, **kwargs)
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    # Determine log level based on threshold
                    actual_log_level = slow_log_level if slow_threshold and elapsed > slow_threshold else log_level
                    
                    log_message = f"{f.__name__} completed in {elapsed:.4f} seconds"
                    if include_result:
                        log_message += f", result={result}"
                    
                    getattr(logger_, actual_log_level.lower())(log_message)
                    
                    return result
                except Exception as e:
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    log_message = f"{f.__name__} failed after {elapsed:.4f} seconds: {e}"
                    getattr(logger_, log_level.lower())(log_message)
                    raise
            
            return async_wrapper
        else:
            @functools.wraps(f)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                start_time = time.time()
                
                # Log function call with args if requested
                if include_args:
                    getattr(logger_, log_level.lower())(
                        f"Calling {f.__name__} with args={args}, kwargs={kwargs}"
                    )
                
                try:
                    result = f(*args, **kwargs)
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    # Determine log level based on threshold
                    actual_log_level = slow_log_level if slow_threshold and elapsed > slow_threshold else log_level
                    
                    log_message = f"{f.__name__} completed in {elapsed:.4f} seconds"
                    if include_result:
                        log_message += f", result={result}"
                    
                    getattr(logger_, actual_log_level.lower())(log_message)
                    
                    return result
                except Exception as e:
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    log_message = f"{f.__name__} failed after {elapsed:.4f} seconds: {e}"
                    getattr(logger_, log_level.lower())(log_message)
                    raise
            
            return sync_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# ===== Error Handling Decorators =====

def error_handler(
    func: Optional[Callable[..., T]] = None,
    *,
    log_level: str = "ERROR",
    reraise: bool = True,
    default_return: Any = None,
    exception_types: Tuple[Type[Exception], ...] = (Exception,),
    on_error: Optional[Callable[[Exception], Any]] = None,
    include_traceback: bool = True
):
    """
    Decorator to handle and log function errors with configurable behavior.
    
    This decorator provides comprehensive error handling with options for logging,
    re-raising exceptions, returning default values, and custom error handling.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        log_level: Log level for error messages
        reraise: Whether to re-raise the exception after handling
        default_return: Default value to return if not re-raising
        exception_types: Exception types to handle
        on_error: Optional callback function to call when an error occurs
        include_traceback: Whether to include traceback in log messages
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @error_handler(log_level="WARNING", reraise=False, default_return=None)
        def fetch_data(url):
            # Make HTTP request
            pass
            
        @error_handler(on_error=lambda e: send_alert(e))
        async def async_fetch_data(url):
            # Make async HTTP request
            pass
        ```
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(f):
            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                try:
                    return await f(*args, **kwargs)
                except exception_types as e:
                    # Log the error
                    log_message = f"{f.__name__} raised {type(e).__name__}: {e}"
                    getattr(logger_, log_level.lower())(log_message, exc_info=include_traceback)
                    
                    # Call custom error handler if provided
                    if on_error:
                        try:
                            on_error(e)
                        except Exception as callback_error:
                            logger_.error(f"Error in error handler callback: {callback_error}")
                    
                    # Re-raise or return default value
                    if reraise:
                        raise
                    return default_return
            
            return async_wrapper
        else:
            @functools.wraps(f)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                try:
                    return f(*args, **kwargs)
                except exception_types as e:
                    # Log the error
                    log_message = f"{f.__name__} raised {type(e).__name__}: {e}"
                    getattr(logger_, log_level.lower())(log_message, exc_info=include_traceback)
                    
                    # Call custom error handler if provided
                    if on_error:
                        try:
                            on_error(e)
                        except Exception as callback_error:
                            logger_.error(f"Error in error handler callback: {callback_error}")
                    
                    # Re-raise or return default value
                    if reraise:
                        raise
                    return default_return
            
            return sync_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# ===== Logging Decorators =====

def log_function_call(
    func: Optional[Callable[..., T]] = None,
    *,
    log_level: str = "DEBUG",
    include_args: bool = True,
    include_result: bool = False,
    include_exception: bool = True
):
    """
    Decorator to log function calls with arguments and return values.
    
    This decorator provides comprehensive logging of function execution,
    including arguments, return values, and exceptions.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        log_level: Log level to use
        include_args: Whether to include function arguments in the log
        include_result: Whether to include function result in the log
        include_exception: Whether to include exception details in the log
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @log_function_call(include_args=True, include_result=True)
        def calculate_sum(a, b):
            return a + b
            
        @log_function_call(log_level="INFO")
        async def async_process_data(data):
            # Process data asynchronously
            pass
        ```
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(f):
            @functools.wraps(f)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                
                # Log function call
                call_message = f"Calling {f.__name__}"
                if include_args:
                    call_message += f" with args={args}, kwargs={kwargs}"
                getattr(logger_, log_level.lower())(call_message)
                
                try:
                    result = await f(*args, **kwargs)
                    
                    # Log successful completion
                    completion_message = f"{f.__name__} completed"
                    if include_result:
                        completion_message += f", result={result}"
                    getattr(logger_, log_level.lower())(completion_message)
                    
                    return result
                except Exception as e:
                    # Log exception
                    if include_exception:
                        getattr(logger_, log_level.lower())(
                            f"{f.__name__} raised {type(e).__name__}: {e}"
                        )
                    raise
            
            return async_wrapper
        else:
            @functools.wraps(f)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                logger_ = get_logger(f.__name__)
                
                # Log function call
                call_message = f"Calling {f.__name__}"
                if include_args:
                    call_message += f" with args={args}, kwargs={kwargs}"
                getattr(logger_, log_level.lower())(call_message)
                
                try:
                    result = f(*args, **kwargs)
                    
                    # Log successful completion
                    completion_message = f"{f.__name__} completed"
                    if include_result:
                        completion_message += f", result={result}"
                    getattr(logger_, log_level.lower())(completion_message)
                    
                    return result
                except Exception as e:
                    # Log exception
                    if include_exception:
                        getattr(logger_, log_level.lower())(
                            f"{f.__name__} raised {type(e).__name__}: {e}"
                        )
                    raise
            
            return sync_wrapper
    
    if func is None:
        return decorator
    return decorator(func)


# ===== Utility Decorators =====

def deprecated(
    func: Optional[Callable[..., T]] = None,
    *,
    reason: str = "",
    version: str = "",
    replacement: str = ""
):
    """
    Decorator to mark functions as deprecated.
    
    This decorator logs a deprecation warning when the decorated function is called.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        reason: Reason for deprecation
        version: Version when the function was deprecated
        replacement: Replacement function or method
        
    Returns:
        Decorator function
        
    Example:
        ```python
        @deprecated(reason="Replaced by new_api", version="2.0.0", replacement="new_api")
        def old_api():
            pass
        ```
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            logger_ = get_logger(f.__name__)
            
            # Build deprecation message
            message = f"Call to deprecated function {f.__name__}"
            if version:
                message += f" (deprecated in version {version})"
            if reason:
                message += f": {reason}"
            if replacement:
                message += f". Use {replacement} instead."
            
            logger_.warning(message)
            
            return f(*args, **kwargs)
        
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def singleton(cls):
    """
    Decorator to implement the singleton pattern for classes.
    
    This decorator ensures that only one instance of a class is created.
    
    Args:
        cls: Class to decorate
        
    Returns:
        Decorated class
        
    Example:
        ```python
        @singleton
        class DatabaseConnection:
            def __init__(self):
                self.connection = None
    """
    instances = {}
    
    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return get_instance


# Import random for jitter calculation
import random