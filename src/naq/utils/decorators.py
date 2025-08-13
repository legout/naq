# src/naq/utils/decorators.py
"""
Common decorators for NAQ.

This module provides reusable decorators to reduce code duplication and improve 
maintainability across the NAQ codebase.
"""

import asyncio
import time
import random
import logging
from functools import wraps
from typing import Callable, Optional, Union, Tuple, Type, Any

from loguru import logger


def retry(
    max_attempts: int = 3,
    delay: Union[float, Tuple[float, float]] = 1.0,
    backoff: str = "exponential",  # "linear", "exponential", "jitter"
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with configurable backoff strategies.
    
    Args:
        max_attempts: Maximum retry attempts
        delay: Base delay (float) or range (tuple)
        backoff: Backoff strategy ("linear", "exponential", "jitter")
        exceptions: Exception types to retry on
        on_retry: Callback function called on retry
        
    Usage:
        @retry(max_attempts=3, backoff="exponential")
        async def unreliable_operation():
            # This will be retried up to 3 times with exponential backoff
            pass
            
        @retry(max_attempts=5, delay=(1.0, 3.0), backoff="jitter")
        async def flaky_network_call():
            # This will be retried with random jitter between 1-3 seconds
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay
                    if isinstance(delay, tuple):
                        base_delay = random.uniform(delay[0], delay[1])
                    else:
                        base_delay = delay
                    
                    if backoff == "exponential":
                        actual_delay = base_delay * (2 ** attempt)
                    elif backoff == "jitter":
                        actual_delay = base_delay + random.uniform(0, base_delay)
                    else:  # linear
                        actual_delay = base_delay
                    
                    if on_retry:
                        if asyncio.iscoroutinefunction(on_retry):
                            await on_retry(attempt + 1, e, actual_delay)
                        else:
                            on_retry(attempt + 1, e, actual_delay)
                    
                    logger.debug(f"Retry attempt {attempt + 1}/{max_attempts} for {func.__name__} in {actual_delay}s")
                    await asyncio.sleep(actual_delay)
            
            raise last_exception
        
        @wraps(func)  
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay
                    if isinstance(delay, tuple):
                        base_delay = random.uniform(delay[0], delay[1])
                    else:
                        base_delay = delay
                    
                    if backoff == "exponential":
                        actual_delay = base_delay * (2 ** attempt)
                    elif backoff == "jitter":
                        actual_delay = base_delay + random.uniform(0, base_delay)
                    else:  # linear
                        actual_delay = base_delay
                    
                    if on_retry:
                        on_retry(attempt + 1, e, actual_delay)
                    
                    logger.debug(f"Retry attempt {attempt + 1}/{max_attempts} for {func.__name__} in {actual_delay}s")
                    time.sleep(actual_delay)
            
            raise last_exception
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def timing(
    logger_instance: Optional[logging.Logger] = None,
    threshold_ms: Optional[float] = None,
    message: Optional[str] = None,
    log_level: int = logging.INFO
):
    """
    Timing decorator with optional slow operation logging.
    
    Args:
        logger_instance: Logger instance to use (defaults to loguru)
        threshold_ms: Only log if execution time exceeds this threshold
        message: Custom log message format string
        log_level: Log level to use
        
    Usage:
        @timing(threshold_ms=1000)
        async def slow_operation():
            # Only logs if takes more than 1 second
            pass
            
        @timing(message="Database query completed")
        async def db_query():
            # Logs with custom message
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                if threshold_ms is None or duration_ms > threshold_ms:
                    msg = message or f"{func.__name__} execution time"
                    if logger_instance:
                        logger_instance.log(log_level, f"{msg}: {duration_ms:.2f}ms")
                    else:
                        logger.info(f"{msg}: {duration_ms:.2f}ms")
                        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                if threshold_ms is None or duration_ms > threshold_ms:
                    msg = message or f"{func.__name__} execution time"
                    if logger_instance:
                        logger_instance.log(log_level, f"{msg}: {duration_ms:.2f}ms")
                    else:
                        logger.info(f"{msg}: {duration_ms:.2f}ms")
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def log_errors(
    logger_instance: Optional[logging.Logger] = None,
    level: int = logging.ERROR,
    reraise: bool = True,
    message: Optional[str] = None,
    include_args: bool = False
):
    """
    Error logging decorator.
    
    Args:
        logger_instance: Logger instance to use (defaults to loguru)
        level: Log level for errors
        reraise: Whether to reraise the exception after logging
        message: Custom error message format
        include_args: Whether to include function arguments in log
        
    Usage:
        @log_errors(reraise=False)
        async def safe_operation():
            # Exceptions are logged but not reraised
            pass
            
        @log_errors(message="API call failed", include_args=True)
        async def api_call(endpoint, data):
            # Logs with custom message and function arguments
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                error_msg = message or f"Error in {func.__name__}"
                
                if include_args:
                    error_msg += f" (args={args}, kwargs={kwargs})"
                
                if logger_instance:
                    logger_instance.log(level, f"{error_msg}: {e}", exc_info=True)
                else:
                    logger.error(f"{error_msg}: {e}")
                
                if reraise:
                    raise
                return None
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = message or f"Error in {func.__name__}"
                
                if include_args:
                    error_msg += f" (args={args}, kwargs={kwargs})"
                
                if logger_instance:
                    logger_instance.log(level, f"{error_msg}: {e}", exc_info=True)
                else:
                    logger.error(f"{error_msg}: {e}")
                
                if reraise:
                    raise
                return None
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def rate_limit(
    calls_per_second: float,
    burst_size: Optional[int] = None
):
    """
    Rate limiting decorator using token bucket algorithm.
    
    Args:
        calls_per_second: Maximum calls per second allowed
        burst_size: Maximum burst size (defaults to calls_per_second)
        
    Usage:
        @rate_limit(calls_per_second=10, burst_size=20)
        async def api_call():
            # Limited to 10 calls/second with burst of 20
            pass
    """
    burst_size = burst_size or int(calls_per_second)
    bucket_capacity = burst_size
    tokens = bucket_capacity
    last_refill = time.time()
    lock = asyncio.Lock() if asyncio.iscoroutinefunction else None
    
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            nonlocal tokens, last_refill
            
            async with lock:
                now = time.time()
                elapsed = now - last_refill
                
                # Refill tokens
                tokens = min(bucket_capacity, tokens + elapsed * calls_per_second)
                last_refill = now
                
                if tokens < 1:
                    wait_time = (1 - tokens) / calls_per_second
                    await asyncio.sleep(wait_time)
                    tokens = 0
                else:
                    tokens -= 1
            
            return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            nonlocal tokens, last_refill
            
            now = time.time()
            elapsed = now - last_refill
            
            # Refill tokens
            tokens = min(bucket_capacity, tokens + elapsed * calls_per_second)
            last_refill = now
            
            if tokens < 1:
                wait_time = (1 - tokens) / calls_per_second
                time.sleep(wait_time)
                tokens = 0
            else:
                tokens -= 1
            
            return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def cache(
    max_size: int = 128,
    ttl_seconds: Optional[float] = None
):
    """
    Simple LRU cache decorator with optional TTL.
    
    Args:
        max_size: Maximum cache size
        ttl_seconds: Time-to-live for cached items
        
    Usage:
        @cache(max_size=256, ttl_seconds=300)
        async def expensive_computation(arg1, arg2):
            # Results cached for 5 minutes
            pass
    """
    from collections import OrderedDict
    
    cache_dict = OrderedDict()
    
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))
            now = time.time()
            
            # Check cache
            if key in cache_dict:
                result, timestamp = cache_dict[key]
                if ttl_seconds is None or (now - timestamp) < ttl_seconds:
                    # Move to end (most recently used)
                    cache_dict.move_to_end(key)
                    return result
                else:
                    del cache_dict[key]
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Store in cache
            cache_dict[key] = (result, now)
            
            # Evict oldest if necessary
            if len(cache_dict) > max_size:
                cache_dict.popitem(last=False)
            
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))
            now = time.time()
            
            # Check cache
            if key in cache_dict:
                result, timestamp = cache_dict[key]
                if ttl_seconds is None or (now - timestamp) < ttl_seconds:
                    # Move to end (most recently used)
                    cache_dict.move_to_end(key)
                    return result
                else:
                    del cache_dict[key]
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            cache_dict[key] = (result, now)
            
            # Evict oldest if necessary
            if len(cache_dict) > max_size:
                cache_dict.popitem(last=False)
            
            return result
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


# Backward compatibility alias for tests
cache_result = cache