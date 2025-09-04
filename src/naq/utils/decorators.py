"""
Decorator utilities for NAQ

This module provides common decorators used throughout the NAQ library.
"""

import asyncio
import functools
import time
from typing import Any, Callable, Type, Union, Tuple, Optional

from ..exceptions import NaqException


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    jitter: bool = True,
) -> Callable:
    """
    Retry decorator for async functions.
    
    Args:
        max_attempts: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay on each retry.
        exceptions: Exception types to retry on.
        jitter: Whether to add random jitter to delay.
        
    Returns:
        Callable: Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    
                    # Calculate delay with optional jitter
                    if jitter:
                        import random
                        actual_delay = current_delay * (0.5 + random.random())
                    else:
                        actual_delay = current_delay
                    
                    # Wait before retry
                    await asyncio.sleep(actual_delay)
                    
                    # Increase delay for next attempt
                    current_delay *= backoff
                    attempt += 1
            
            # This should never be reached, but just in case
            raise NaqException("Retry decorator failed unexpectedly")
        
        return wrapper
    return decorator


def timeout(
    timeout_seconds: float,
    timeout_message: str = "Operation timed out",
) -> Callable:
    """
    Timeout decorator for async functions.
    
    Args:
        timeout_seconds: Timeout in seconds.
        timeout_message: Message to include in timeout exception.
        
    Returns:
        Callable: Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise NaqException(timeout_message)
        
        return wrapper
    return decorator


def measure_time(func: Optional[Callable] = None, *, threshold_ms: Optional[int] = None) -> Callable:
    """
    Decorator to measure execution time of async functions.
    
    Args:
        func: Function to measure.
        threshold_ms: Optional threshold in milliseconds for logging warnings.
        
    Returns:
        Callable: Decorated function.
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.perf_counter()
            try:
                result = await f(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                duration = end_time - start_time
                duration_ms = duration * 1000
                
                # Add duration to result if it's a dict
                if isinstance(result, dict):
                    result["_execution_time"] = duration
                
                # Import here to avoid circular imports
                from .logging import get_logger
                logger = get_logger("naq.timing")
                
                # Log with appropriate level based on threshold
                if threshold_ms is not None and duration_ms > threshold_ms:
                    logger.warning(
                        "Function execution time exceeded threshold",
                        function=f.__name__,
                        duration_seconds=duration,
                        duration_ms=duration_ms,
                        threshold_ms=threshold_ms,
                    )
                else:
                    logger.debug(
                        "Function execution time",
                        function=f.__name__,
                        duration_seconds=duration,
                    )
        
        return wrapper
    
    # Handle both @measure_time and @measure_time(threshold_ms=100) usage
    if func is None:
        return decorator
    else:
        return decorator(func)


# Backward compatibility alias
timing = measure_time


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: Type[Exception] = Exception,
) -> Callable:
    """
    Circuit breaker decorator for async functions.
    
    Args:
        failure_threshold: Number of failures before opening circuit.
        recovery_timeout: Time to wait before attempting recovery.
        expected_exception: Exception type that counts as failure.
        
    Returns:
        Callable: Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        state = {
            "failure_count": 0,
            "last_failure_time": 0,
            "circuit_open": False,
        }
        
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Check if circuit is open
            if state["circuit_open"]:
                # Check if recovery timeout has passed
                if time.time() - state["last_failure_time"] > recovery_timeout:
                    # Attempt to reset circuit
                    state["circuit_open"] = False
                    state["failure_count"] = 0
                else:
                    raise NaqException("Circuit breaker is open")
            
            try:
                result = await func(*args, **kwargs)
                # Reset failure count on success
                state["failure_count"] = 0
                return result
            except expected_exception as e:
                # Increment failure count
                state["failure_count"] += 1
                state["last_failure_time"] = time.time()
                
                # Open circuit if threshold reached
                if state["failure_count"] >= failure_threshold:
                    state["circuit_open"] = True
                
                raise
        
        return wrapper
    return decorator


def rate_limit(
    calls_per_second: float,
    burst_limit: Optional[int] = None,
) -> Callable:
    """
    Rate limit decorator for async functions.
    
    Args:
        calls_per_second: Maximum calls per second.
        burst_limit: Optional burst limit for initial calls.
        
    Returns:
        Callable: Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        # Use a semaphore for rate limiting
        if burst_limit is None:
            burst_limit = max(1, int(calls_per_second))
        
        semaphore = asyncio.Semaphore(burst_limit)
        last_call_time = 0.0
        min_interval = 1.0 / calls_per_second if calls_per_second > 0 else 0.0
        
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal last_call_time
            
            # Acquire semaphore
            async with semaphore:
                # Calculate required delay
                current_time = time.time()
                elapsed = current_time - last_call_time
                required_delay = max(0, min_interval - elapsed)
                
                # Wait if needed
                if required_delay > 0:
                    await asyncio.sleep(required_delay)
                
                # Update last call time and execute
                last_call_time = time.time()
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator