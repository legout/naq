"""
Retry utilities for NAQ.

This module provides comprehensive retry mechanisms and backoff strategies
for handling transient failures in distributed systems.
"""

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Optional, Type, Union, TypeVar

from ..exceptions import NaqException

T = TypeVar("T")


class RetryError(NaqException):
    """Raised when all retry attempts have been exhausted."""
    
    def __init__(self, message: str, attempts: int, last_exception: Optional[Exception] = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,)
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be non-negative")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.exponential_base < 1:
            raise ValueError("exponential_base must be >= 1")
        if not (0 <= self.jitter_factor <= 1):
            raise ValueError("jitter_factor must be between 0 and 1")


def calculate_delay(
    attempt: int, 
    config: RetryConfig
) -> float:
    """
    Calculate the delay for the next retry attempt.
    
    Args:
        attempt: The current attempt number (0-based)
        config: Retry configuration
        
    Returns:
        Delay in seconds
    """
    # Calculate exponential backoff
    delay = config.base_delay * (config.exponential_base ** attempt)
    
    # Apply maximum delay cap
    delay = min(delay, config.max_delay)
    
    # Add jitter if enabled
    if config.jitter:
        jitter_range = delay * config.jitter_factor
        delay = delay + random.uniform(-jitter_range, jitter_range)
    
    # Ensure delay is non-negative
    return max(0, delay)


def should_retry(
    exception: Exception,
    attempt: int,
    config: RetryConfig
) -> bool:
    """
    Determine if a retry should be attempted.
    
    Args:
        exception: The exception that was raised
        attempt: The current attempt number (0-based)
        config: Retry configuration
        
    Returns:
        True if retry should be attempted, False otherwise
    """
    # Check if we've exceeded max attempts
    if attempt >= config.max_attempts - 1:
        return False
    
    # Check if the exception is retryable
    return isinstance(exception, config.retryable_exceptions)


async def retry_with_backoff_async(
    func: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> T:
    """
    Retry an async function with exponential backoff.
    
    Args:
        func: The async function to retry
        *args: Positional arguments to pass to the function
        config: Optional retry configuration
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
        
    Raises:
        RetryError: If all retry attempts are exhausted
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            if not should_retry(e, attempt, config):
                raise RetryError(
                    f"Function failed after {attempt + 1} attempts: {e}",
                    attempts=attempt + 1,
                    last_exception=e
                ) from e
            
            if attempt < config.max_attempts - 1:  # Don't sleep on the last attempt
                delay = calculate_delay(attempt, config)
                await asyncio.sleep(delay)
    
    # This should never be reached, but just in case
    raise RetryError(
        f"Function failed after {config.max_attempts} attempts",
        attempts=config.max_attempts,
        last_exception=last_exception
    )


def retry_with_backoff(
    func: Callable[..., T],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> T:
    """
    Retry a synchronous function with exponential backoff.
    
    Args:
        func: The function to retry
        *args: Positional arguments to pass to the function
        config: Optional retry configuration
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
        
    Raises:
        RetryError: If all retry attempts are exhausted
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            if not should_retry(e, attempt, config):
                raise RetryError(
                    f"Function failed after {attempt + 1} attempts: {e}",
                    attempts=attempt + 1,
                    last_exception=e
                ) from e
            
            if attempt < config.max_attempts - 1:  # Don't sleep on the last attempt
                delay = calculate_delay(attempt, config)
                time.sleep(delay)
    
    # This should never be reached, but just in case
    raise RetryError(
        f"Function failed after {config.max_attempts} attempts",
        attempts=config.max_attempts,
        last_exception=last_exception
    )


def retry_on_exceptions(
    *exceptions: Type[Exception],
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
):
    """
    Decorator to retry a function on specific exceptions.
    
    Args:
        *exceptions: Exception types to retry on
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add jitter to delays
        
    Returns:
        Decorator function
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retryable_exceptions=exceptions or (Exception,)
    )
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_with_backoff(func, *args, config=config, **kwargs)
        
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_with_backoff_async(func, *args, config=config, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper
    
    return decorator


def retry_with_jitter(
    func: Callable[..., T],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    jitter_factor: float = 0.1,
    **kwargs: Any
) -> T:
    """
    Retry a function with random jitter to avoid thundering herd problems.
    
    Args:
        func: The function to retry
        *args: Positional arguments to pass to the function
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        jitter_factor: Factor for jitter calculation (0-1)
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        jitter=True,
        jitter_factor=jitter_factor,
        exponential_base=1.0  # Linear backoff
    )
    
    return retry_with_backoff(func, *args, config=config, **kwargs)


def retry(
    func: Callable[..., T] = None,
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator to retry a function with exponential backoff.
    
    Args:
        func: Function to decorate (optional, for decorator without parentheses)
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add jitter to delays
        retryable_exceptions: Exception types to retry on
        
    Returns:
        Decorator function
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions
    )
    
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_with_backoff(f, *args, config=config, **kwargs)
        
        if asyncio.iscoroutinefunction(f):
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                return await retry_with_backoff_async(f, *args, config=config, **kwargs)
            return async_wrapper
        else:
            return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


def retry_async(
    func: Callable[..., Coroutine[Any, Any, T]] = None,
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator to retry an async function with exponential backoff.
    
    Args:
        func: Async function to decorate (optional, for decorator without parentheses)
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add jitter to delays
        retryable_exceptions: Exception types to retry on
        
    Returns:
        Decorator function
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions
    )
    
    def decorator(f: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_with_backoff_async(f, *args, config=config, **kwargs)
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)


class RetryContextManager:
    """
    Context manager for retry operations.
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        """
        Initialize the retry context.
        
        Args:
            config: Optional retry configuration
        """
        self.config = config or RetryConfig()
        self.attempts = 0
        self.last_exception = None
    
    def __enter__(self):
        """Enter the context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        if exc_type is None:
            # No exception, operation succeeded
            return True
        
        # Handle the exception
        self.last_exception = exc_val
        
        if not should_retry(exc_val, self.attempts, self.config):
            # Don't retry, re-raise the exception
            return False
        
        # Increment attempt counter
        self.attempts += 1
        
        if self.attempts < self.config.max_attempts:
            # Sleep before retry
            delay = calculate_delay(self.attempts, self.config)
            time.sleep(delay)
            # Suppress the exception to allow retry
            return True
        else:
            # Max attempts reached, re-raise
            return False
    
    def should_continue(self) -> bool:
        """
        Check if retrying should continue.
        
        Returns:
            True if more retries should be attempted
        """
        return self.attempts < self.config.max_attempts
    
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute a function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
        """
        for attempt in range(self.config.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.last_exception = e
                
                if not should_retry(e, attempt, self.config):
                    raise RetryError(
                        f"Function failed after {attempt + 1} attempts: {e}",
                        attempts=attempt + 1,
                        last_exception=e
                    ) from e
                
                if attempt < self.config.max_attempts - 1:
                    delay = calculate_delay(attempt, self.config)
                    time.sleep(delay)
        
        # This should never be reached
        raise RetryError(
            f"Function failed after {self.config.max_attempts} attempts",
            attempts=self.config.max_attempts,
            last_exception=self.last_exception
        )


def retry_context(config: Optional[RetryConfig] = None) -> RetryContextManager:
    """
    Create a retry context manager.
    
    Args:
        config: Optional retry configuration
        
    Returns:
        RetryContextManager instance
    """
    return RetryContextManager(config)


def retry_async_context(config: Optional[RetryConfig] = None) -> RetryContextManager:
    """
    Create a retry context manager for async operations.
    
    Args:
        config: Optional retry configuration
        
    Returns:
        RetryContextManager instance
    """
    return RetryContextManager(config)


def with_retry(
    func: Callable[..., T],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> T:
    """
    Retry a function with the given configuration.
    
    Args:
        func: Function to retry
        *args: Positional arguments to pass to the function
        config: Optional retry configuration
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
    """
    return retry_with_backoff(func, *args, config=config, **kwargs)


def with_retry_async(
    func: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> Coroutine[Any, Any, T]:
    """
    Retry an async function with the given configuration.
    
    Args:
        func: Async function to retry
        *args: Positional arguments to pass to the function
        config: Optional retry configuration
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Coroutine that resolves to the function result
    """
    return retry_with_backoff_async(func, *args, config=config, **kwargs)


def calculate_backoff(attempt: int, config: RetryConfig) -> float:
    """
    Calculate backoff delay for a given attempt.
    
    Args:
        attempt: Attempt number (0-based)
        config: Retry configuration
        
    Returns:
        Delay in seconds
    """
    return calculate_delay(attempt, config)


def is_retryable_error(exception: Exception) -> bool:
    """
    Check if an exception is retryable.
    
    Args:
        exception: Exception to check
        
    Returns:
        True if the exception should be retried
    """
    # Default implementation - retry all exceptions except certain system errors
    non_retryable = (KeyboardInterrupt, SystemExit, MemoryError, SyntaxError)
    return not isinstance(exception, non_retryable)


# Alias for backward compatibility
RetryContext = RetryContextManager

