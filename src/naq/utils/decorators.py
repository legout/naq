"""Decorator utilities for NAQ.

This module contains common decorators used throughout the NAQ codebase.
"""

import asyncio
import functools
import random
import sys
import time
import traceback
from typing import Any, Callable, Optional, Tuple, Type, Union

from loguru import logger
from ..exceptions import NaqException


class RetryError(NaqException):
    """Raised when a function fails after all retry attempts."""

    pass


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: str = "linear",
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """
    A flexible retry decorator that supports both synchronous and asynchronous functions
    with configurable backoff strategies.

    This decorator can be applied to both regular functions and async functions. It will
    automatically detect the function type and handle retries appropriately.

    Args:
        max_attempts: Maximum number of attempts before giving up (default: 3).
            Must be at least 1.
        delay: Base delay in seconds between retry attempts (default: 1.0).
            The actual delay depends on the backoff strategy.
        backoff: Backoff strategy for calculating delays between retries.
            Supported values:
            - "linear": Constant delay between attempts (delay * attempt)
            - "exponential": Exponentially increasing delay (delay * 2^(attempt-1))
            - "jitter": Random jitter added to linear delay
              (delay * attempt * random(0.5, 1.5))
            Default: "linear".
        exceptions: Exception type(s) that should trigger a retry.
            Can be a single exception type or a tuple of exception types.
            Default: Exception (retries on all exceptions).
        on_retry: Optional callback function that gets called before each retry attempt.
            The callback should accept two arguments: the exception that triggered
            the retry and the current attempt number (1-based).
            Default: None.

    Returns:
        A decorator function that wraps the original function with retry logic.

    Raises:
        ValueError: If max_attempts is less than 1 or if backoff strategy is invalid.

    Examples:
        Synchronous function with linear backoff:
        ```python
        @retry(max_attempts=3, delay=1.0, backoff="linear", exceptions=ConnectionError)
        def fetch_data(url):
            # This will retry up to 3 times on ConnectionError
            # with 1s, 2s, 3s delays between attempts
            return requests.get(url).json()
        ```

        Asynchronous function with exponential backoff:
        ```python
        @retry(max_attempts=5, delay=0.5, backoff="exponential")
        async def async_fetch_data(url):
            # This will retry up to 5 times on any exception
            # with 0.5s, 1s, 2s, 4s, 8s delays between attempts
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    return await response.json()
        ```

        Using jitter backoff with a retry callback:
        ```python
        def log_retry(exc, attempt):
            print(f"Attempt {attempt} failed with {exc.__class__.__name__}: {exc}")

        @retry(
            max_attempts=4,
            delay=1.0,
            backoff="jitter",
            exceptions=(TimeoutError, ConnectionError),
            on_retry=log_retry
        )
        def unstable_operation():
            # This will retry up to 4 times on TimeoutError or ConnectionError
            # with jittered delays and log each retry attempt
            return perform_unstable_operation()
        ```

        Using default parameters:
        ```python
        @retry()
        def simple_function():
            # This will retry up to 3 times on any exception
            # with 1s, 2s, 3s delays between attempts (linear backoff)
            return do_something()
        ```
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    if backoff not in ("linear", "exponential", "jitter"):
        raise ValueError(
            f"Invalid backoff strategy: {backoff}. "
            f"Must be one of: linear, exponential, jitter"
        )

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        raise RetryError(
                            f"Function {func.__name__} failed after "
                            f"{max_attempts} attempts"
                        ) from exc

                    # Calculate delay based on backoff strategy
                    if backoff == "linear":
                        sleep_time = delay * attempt
                    elif backoff == "exponential":
                        sleep_time = delay * (2 ** (attempt - 1))
                    else:  # jitter
                        sleep_time = delay * attempt * random.uniform(0.5, 1.5)

                    # Call retry callback if provided
                    if on_retry is not None:
                        on_retry(exc, attempt)

                    time.sleep(sleep_time)
                    attempt += 1

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        raise RetryError(
                            f"Function {func.__name__} failed after "
                            f"{max_attempts} attempts"
                        ) from exc

                    # Calculate delay based on backoff strategy
                    if backoff == "linear":
                        sleep_time = delay * attempt
                    elif backoff == "exponential":
                        sleep_time = delay * (2 ** (attempt - 1))
                    else:  # jitter
                        sleep_time = delay * attempt * random.uniform(0.5, 1.5)

                    # Call retry callback if provided
                    if on_retry is not None:
                        on_retry(exc, attempt)

                    await asyncio.sleep(sleep_time)
                    attempt += 1

        # Return the appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def log_errors(
    logger_instance: Optional[Any] = None,
    level: str = "ERROR",
    reraise: bool = True,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
) -> Callable:
    """
    A decorator that logs exceptions that occur in decorated functions.

    This decorator can be applied to both regular functions and async functions. It will
    automatically detect the function type and handle error logging appropriately.
    When an exception occurs, it logs the error message and traceback, and optionally
    re-raises the exception based on the reraise parameter.

    Args:
        logger_instance: Logger instance to use for logging. If None, uses the default
            loguru logger. Default: None.
        level: Logging level to use when logging errors. Supported values:
            "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". Default: "ERROR".
        reraise: Whether to re-raise the exception after logging. If True, the exception
            is re-raised after logging. If False, the exception is caught and logged,
            and None is returned for sync functions or None is awaited for async
            functions. Default: True.
        exceptions: Exception type(s) that should be caught and logged.
            Can be a single exception type or a tuple of exception types.
            Default: Exception (catches all exceptions).

    Returns:
        A decorator function that wraps the original function with error logging logic.

    Raises:
        ValueError: If level is not a valid logging level.

    Examples:
        Basic usage with default logger:
        ```python
        @log_errors()
        def risky_function():
            # This will log any exception that occurs
            return perform_risky_operation()
        ```

        With custom logging level and reraise=False:
        ```python
        @log_errors(level="WARNING", reraise=False)
        def might_fail():
            # This will log warnings instead of errors and won't re-raise
            return operation_that_might_fail()
        ```

        With custom logger and specific exceptions:
        ```python
        custom_logger = logger.bind(component="database")

        @log_errors(
            logger_instance=custom_logger,
            level="ERROR",
            exceptions=(ConnectionError, TimeoutError)
        )
        def query_database():
            # This will only log ConnectionError and TimeoutError using custom_logger
            return db.execute("SELECT * FROM users")
        ```

        With async function:
        ```python
        @log_errors(reraise=False)
        async def async_operation():
            # This will log any exception but won't re-raise
            await perform_async_operation()
        ```

        Using default parameters:
        ```python
        @log_errors()
        def simple_function():
            # This will log any exception at ERROR level and re-raise it
            return do_something_risky()
        ```
    """
    # Validate logging level
    valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    if level not in valid_levels:
        raise ValueError(
            f"Invalid logging level: {level}. Must be one of: {', '.join(valid_levels)}"
        )

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except exceptions as exc:
                # Get the traceback as a string
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_str = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                # Log the error with traceback
                log_message = f"Exception in {func.__name__}: {str(exc)}\n{tb_str}"

                if logger_instance is not None:
                    # Use the provided logger instance
                    log_method = getattr(logger_instance, level.lower())
                    log_method(log_message)
                else:
                    # Use the default loguru logger
                    log_method = getattr(logger, level.lower())
                    log_method(log_message)

                # Re-raise if requested
                if reraise:
                    raise
                return None

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except exceptions as exc:
                # Get the traceback as a string
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_str = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )

                # Log the error with traceback
                log_message = f"Exception in {func.__name__}: {str(exc)}\n{tb_str}"

                if logger_instance is not None:
                    # Use the provided logger instance
                    log_method = getattr(logger_instance, level.lower())
                    log_method(log_message)
                else:
                    # Use the default loguru logger
                    log_method = getattr(logger, level.lower())
                    log_method(log_message)

                # Re-raise if requested
                if reraise:
                    raise
                return None

        # Return the appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def timing(
    logger_instance: Optional[Any] = None,
    threshold_ms: Optional[int] = None,
    message: Optional[str] = None,
) -> Callable:
    """
    A decorator that measures function execution time and optionally logs slow
    operations.

    This decorator can be applied to both regular functions and async functions. It will
    automatically detect the function type and handle timing appropriately. If execution
    time exceeds the threshold, a warning will be logged.

    Args:
        logger_instance: Logger instance to use for logging. If None, uses the default
            loguru logger. Default: None.
        threshold_ms: Threshold in milliseconds for logging slow operations. If None,
            no threshold logging is performed. Default: None.
        message: Optional custom message to include in the log. If None, a default
            message is used. Can include placeholders: {function_name},
            {execution_time_ms}. Default: None.

    Returns:
        A decorator function that wraps the original function with timing logic.

    Examples:
        Basic usage with default logger:
        ```python
        @timing()
        def slow_function():
            time.sleep(0.1)
            return "done"
        ```

        With threshold for slow operations:
        ```python
        @timing(threshold_ms=50)
        def potentially_slow_function():
            # This will log a warning if execution takes more than 50ms
            return process_data()
        ```

        With custom logger and message:
        ```python
        custom_logger = logger.bind(component="database")

        @timing(
            logger_instance=custom_logger,
            threshold_ms=100,
            message="Database query {function_name} took {execution_time_ms}ms"
        )
        def query_database():
            return db.execute("SELECT * FROM users")
        ```

        With async function:
        ```python
        @timing(threshold_ms=200)
        async def async_operation():
            await asyncio.sleep(0.15)
            return "async result"
        ```
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                execution_time_ms = (end_time - start_time) * 1000

                # Log if threshold is exceeded
                if threshold_ms is not None and execution_time_ms >= threshold_ms:
                    log_message = message or (
                        f"Function {func.__name__} exceeded threshold: "
                        f"{execution_time_ms:.2f}ms > {threshold_ms}ms"
                    )
                    log_message = log_message.format(
                        function_name=func.__name__, execution_time_ms=execution_time_ms
                    )

                    if logger_instance is not None:
                        logger_instance.warning(log_message)
                    else:
                        logger.warning(log_message)

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                execution_time_ms = (end_time - start_time) * 1000

                # Log if threshold is exceeded
                if threshold_ms is not None and execution_time_ms >= threshold_ms:
                    log_message = message or (
                        f"Function {func.__name__} exceeded threshold: "
                        f"{execution_time_ms:.2f}ms > {threshold_ms}ms"
                    )
                    log_message = log_message.format(
                        function_name=func.__name__, execution_time_ms=execution_time_ms
                    )

                    if logger_instance is not None:
                        logger_instance.warning(log_message)
                    else:
                        logger.warning(log_message)

        # Return the appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    # Handle the case where the decorator is called without parentheses
    if callable(logger_instance):
        # logger_instance is actually the function being decorated
        func = logger_instance
        logger_instance = None
        return decorator(func)

    return decorator
