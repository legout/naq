# src/naq/utils/context_managers.py
"""
Context managers for NAQ.

This module provides reusable context managers for resource management,
error handling, and operation timeouts.
"""

import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, Optional, Any, Callable, Awaitable, Type, Tuple

from loguru import logger


@asynccontextmanager
async def managed_resource(
    acquire_func: Callable[[], Awaitable[Any]],
    release_func: Callable[[Any], Awaitable[None]],
    on_error: Optional[Callable[[Exception], Awaitable[None]]] = None
) -> AsyncIterator[Any]:
    """
    Generic resource management context manager.
    
    Safely acquires and releases resources with error handling.
    
    Args:
        acquire_func: Async function to acquire the resource
        release_func: Async function to release the resource
        on_error: Optional error handler called before cleanup
        
    Usage:
        async def acquire_db():
            return await database.connect()
            
        async def release_db(conn):
            await conn.close()
            
        async with managed_resource(acquire_db, release_db) as db:
            # Use database connection
            pass
    """
    resource = None
    try:
        resource = await acquire_func()
        yield resource
    except Exception as e:
        if on_error:
            try:
                await on_error(e)
            except Exception as cleanup_error:
                logger.warning(f"Error in error handler: {cleanup_error}")
        raise
    finally:
        if resource is not None:
            try:
                await release_func(resource)
            except Exception as cleanup_error:
                logger.warning(f"Resource cleanup error: {cleanup_error}")


@asynccontextmanager
async def timeout_context(
    seconds: float,
    error_message: Optional[str] = None
) -> AsyncIterator[None]:
    """
    Context manager for timeout operations.
    
    Args:
        seconds: Timeout in seconds
        error_message: Custom error message for timeout
        
    Usage:
        async with timeout_context(30.0) as ctx:
            # Operation must complete within 30 seconds
            await long_running_operation()
    """
    try:
        async with asyncio.timeout(seconds):
            yield
    except asyncio.TimeoutError:
        msg = error_message or f"Operation timed out after {seconds} seconds"
        logger.warning(msg)
        raise asyncio.TimeoutError(msg) from None


@asynccontextmanager
async def error_context(
    operation_name: str,
    logger_instance: Optional[Any] = None,
    suppress_exceptions: Tuple[Type[Exception], ...] = (),
    reraise: bool = True
) -> AsyncIterator[None]:
    """
    Context manager for operation error handling.
    
    Args:
        operation_name: Name of the operation for logging
        logger_instance: Logger instance (defaults to loguru)
        suppress_exceptions: Exception types to suppress
        reraise: Whether to reraise non-suppressed exceptions
        
    Usage:
        async with error_context("database_query", suppress_exceptions=(ConnectionError,)):
            # ConnectionError will be suppressed and logged
            # Other exceptions will be reraised
            await db.query("SELECT * FROM users")
    """
    log = logger_instance or logger
    
    try:
        yield
    except suppress_exceptions as e:
        log.debug(f"{operation_name} completed with suppressed exception: {e}")
    except Exception as e:
        log.error(f"Error in {operation_name}: {e}")
        if reraise:
            raise


@asynccontextmanager
async def retry_context(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    operation_name: Optional[str] = None
) -> AsyncIterator[None]:
    """
    Context manager that provides retry logic for operations.
    
    Args:
        max_attempts: Maximum retry attempts
        delay: Initial delay between retries
        backoff_multiplier: Multiplier for exponential backoff
        exceptions: Exception types to retry on
        operation_name: Name for logging
        
    Usage:
        for attempt in retry_context(max_attempts=3):
            async with attempt:
                # This block will be retried up to 3 times
                await unreliable_operation()
    """
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            yield
            return  # Success, exit retry loop
        except exceptions as e:
            last_exception = e
            
            if attempt == max_attempts - 1:
                # Last attempt failed
                break
            
            wait_time = delay * (backoff_multiplier ** attempt)
            op_name = operation_name or "operation"
            
            logger.debug(
                f"{op_name} failed (attempt {attempt + 1}/{max_attempts}): {e}. "
                f"Retrying in {wait_time:.1f}s..."
            )
            
            await asyncio.sleep(wait_time)
    
    # All attempts failed, raise the last exception
    raise last_exception


@asynccontextmanager
async def performance_context(
    operation_name: str,
    logger_instance: Optional[Any] = None,
    threshold_ms: Optional[float] = None
) -> AsyncIterator[dict]:
    """
    Context manager for performance monitoring.
    
    Args:
        operation_name: Name of the operation being measured
        logger_instance: Logger instance (defaults to loguru)
        threshold_ms: Only log if execution exceeds this threshold
        
    Yields:
        Dictionary with timing information that gets updated
        
    Usage:
        async with performance_context("database_query") as perf:
            # Perform operation
            await db.query("SELECT * FROM users")
            # perf['duration_ms'] will be available after context exits
    """
    log = logger_instance or logger
    start_time = time.perf_counter()
    perf_info = {"start_time": start_time}
    
    try:
        yield perf_info
    finally:
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        perf_info.update({
            "end_time": end_time,
            "duration_ms": duration_ms
        })
        
        if threshold_ms is None or duration_ms > threshold_ms:
            log.info(f"{operation_name} completed in {duration_ms:.2f}ms")


@asynccontextmanager
async def circuit_breaker_context(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> AsyncIterator[None]:
    """
    Circuit breaker context manager.
    
    Implements the circuit breaker pattern to prevent cascading failures.
    
    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before attempting recovery
        exceptions: Exception types that count as failures
        
    Usage:
        async with circuit_breaker_context(failure_threshold=3) as cb:
            # This operation will be circuit-broken after 3 failures
            await external_service_call()
    """
    # This is a simplified implementation - in practice you'd want
    # to persist state across instances
    if not hasattr(circuit_breaker_context, '_state'):
        circuit_breaker_context._state = {
            'failure_count': 0,
            'last_failure_time': None,
            'state': 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        }
    
    state = circuit_breaker_context._state
    now = time.time()
    
    # Check if we should attempt recovery
    if (state['state'] == 'OPEN' and 
        state['last_failure_time'] and 
        now - state['last_failure_time'] > recovery_timeout):
        state['state'] = 'HALF_OPEN'
        logger.info("Circuit breaker entering HALF_OPEN state")
    
    # If circuit is open, fail fast
    if state['state'] == 'OPEN':
        raise Exception("Circuit breaker is OPEN - failing fast")
    
    try:
        yield
        
        # Success - reset failure count
        if state['failure_count'] > 0:
            logger.info("Circuit breaker: Operation succeeded, resetting failure count")
            state['failure_count'] = 0
            state['state'] = 'CLOSED'
            
    except exceptions as e:
        state['failure_count'] += 1
        state['last_failure_time'] = now
        
        if state['failure_count'] >= failure_threshold:
            state['state'] = 'OPEN'
            logger.warning(
                f"Circuit breaker OPENED after {state['failure_count']} failures. "
                f"Will retry after {recovery_timeout}s"
            )
        
        raise


@contextmanager  
def sync_timeout_context(seconds: float) -> Iterator[None]:
    """
    Synchronous timeout context manager using signal (Unix only).
    
    Args:
        seconds: Timeout in seconds
        
    Usage:
        with sync_timeout_context(30.0):
            # Synchronous operation with timeout
            result = blocking_operation()
    """
    import signal
    
    class TimeoutError(Exception):
        pass
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(int(seconds))
    
    try:
        yield
    finally:
        # Restore the old handler and cancel the alarm
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


@contextmanager
def temporary_attribute(obj: Any, attr_name: str, temp_value: Any) -> Iterator[None]:
    """
    Temporarily set an attribute on an object.
    
    Args:
        obj: Object to modify
        attr_name: Attribute name
        temp_value: Temporary value to set
        
    Usage:
        with temporary_attribute(config, 'debug', True):
            # config.debug is temporarily True
            debug_operation()
        # config.debug is restored to original value
    """
    original_value = getattr(obj, attr_name, None)
    had_attr = hasattr(obj, attr_name)
    
    try:
        setattr(obj, attr_name, temp_value)
        yield
    finally:
        if had_attr:
            setattr(obj, attr_name, original_value)
        else:
            delattr(obj, attr_name)