"""
Context managers for NAQ.

This module provides comprehensive context managers for resource management,
error handling, timeouts, and structured logging. It consolidates existing
context managers from various modules and implements new ones as specified
in Task 08.

The module supports both synchronous and asynchronous context managers
where appropriate, following the NAQ pattern of async-first implementation
with sync wrappers.
"""

import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from typing import (
    Any, AsyncIterator, Callable, Dict, Iterator, Optional, Tuple, Type, Union,
    TypeVar, Awaitable
)

from ..exceptions import NaqException, NaqConnectionError
from .logging import get_logger, StructuredLogger
from .retry import RetryConfig, RetryContextManager

# Type variables for generic context managers
T = TypeVar('T')
R = TypeVar('R')


# =============================================================================
# Connection Context Managers (Consolidated from src/naq/connection/context_managers.py)
# =============================================================================

@asynccontextmanager
async def nats_connection(config=None):
    """
    Context manager for NATS connections.
    
    This context manager provides a safe way to manage NATS connections,
    ensuring proper cleanup and error handling.
    
    Args:
        config: Optional configuration object. If None, uses default configuration.
        
    Yields:
        NATSClient: The NATS connection object
        
    Raises:
        NaqConnectionError: If connection fails or encounters errors
    """
    # Import here to avoid circular imports
    from ..connection.context_managers import get_config, Config
    
    config = config or get_config()
    conn = None
    
    try:
        import nats
        from nats.aio.client import Client as NATSClient
        
        conn = await nats.connect(
            servers=config.nats.servers,
            name=config.nats.client_name,
            max_reconnect_attempts=config.nats.max_reconnect_attempts,
            reconnect_time_wait=config.nats.reconnect_time_wait,
        )
        
        yield conn
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"NATS connection error: {e}")
        raise NaqConnectionError(f"Failed to establish NATS connection: {e}") from e
        
    finally:
        if conn is not None:
            try:
                await conn.close()
            except Exception as e:
                logger = get_logger(__name__)
                logger.warning(f"Error closing NATS connection: {e}")


@asynccontextmanager
async def jetstream_context(conn):
    """
    Context manager for JetStream contexts.
    
    This context manager provides a safe way to manage JetStream contexts,
    ensuring proper error handling.
    
    Args:
        conn: NATSClient instance
        
    Yields:
        JetStreamContext: The JetStream context object
        
    Raises:
        NaqException: If JetStream context creation fails
    """
    logger = get_logger(__name__)
    
    try:
        js = conn.jetstream()
        yield js
        
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise NaqException(f"Failed to create JetStream context: {e}") from e


@asynccontextmanager
async def nats_jetstream(config=None):
    """
    Combined context manager for NATS connection and JetStream.
    
    This context manager provides both NATS connection and JetStream context
    in a single, convenient interface.
    
    Args:
        config: Optional configuration object
        
    Yields:
        Tuple[NATSClient, JetStreamContext]: The connection and JetStream context
        
    Raises:
        NaqConnectionError: If connection fails
        NaqException: If JetStream context creation fails
    """
    async with nats_connection(config) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js


@asynccontextmanager
async def nats_kv_store(bucket_name, config=None):
    """
    Context manager for NATS KeyValue operations.
    
    This context manager provides a safe way to manage NATS KeyValue store
    operations with proper error handling.
    
    Args:
        bucket_name: Name of the KeyValue bucket
        config: Optional configuration object
        
    Yields:
        KeyValue: The KeyValue store object
        
    Raises:
        NaqException: If KV store operations fail
    """
    logger = get_logger(__name__)
    
    async with nats_jetstream(config) as (conn, js):
        try:
            kv = await js.key_value(bucket_name)
            yield kv
            
        except Exception as e:
            logger.error(f"KV store error for bucket {bucket_name}: {e}")
            raise NaqException(f"Failed to access KV store '{bucket_name}': {e}") from e


# =============================================================================
# Retry Context Managers (Consolidated from src/naq/utils/retry.py)
# =============================================================================

def retry_context(config: Optional[RetryConfig] = None) -> RetryContextManager:
    """
    Create a retry context manager for synchronous operations.
    
    This context manager provides automatic retry logic for operations that
    might fail transiently.
    
    Args:
        config: Optional retry configuration
        
    Returns:
        RetryContextManager: Context manager for retry operations
        
    Example:
        with retry_context(RetryConfig(max_attempts=3)) as retry:
            while retry.should_continue():
                try:
                    return risky_operation()
                except Exception as e:
                    retry.last_exception = e
                    if not retry.should_continue():
                        raise
    """
    return RetryContextManager(config)


def retry_async_context(config: Optional[RetryConfig] = None) -> RetryContextManager:
    """
    Create a retry context manager for asynchronous operations.
    
    This context manager provides automatic retry logic for async operations
    that might fail transiently.
    
    Args:
        config: Optional retry configuration
        
    Returns:
        RetryContextManager: Context manager for retry operations
        
    Example:
        async with retry_async_context(RetryConfig(max_attempts=3)) as retry:
            while retry.should_continue():
                try:
                    return await risky_async_operation()
                except Exception as e:
                    retry.last_exception = e
                    if not retry.should_continue():
                        raise
    """
    return RetryContextManager(config)


# =============================================================================
# Logging Context Managers (Consolidated from src/naq/utils/logging_utils.py)
# =============================================================================

@contextmanager
def LogContext(**context: Dict[str, Any]):
    """
    Context manager for adding structured context to log messages.
    
    This context manager temporarily adds structured context to all log
    messages created within its scope.
    
    Args:
        **context: Context key-value pairs to add to log messages
        
    Example:
        with LogContext(job_id="123", operation="process"):
            logger.info("Starting operation")  # Will include job_id and operation
            # ... do work ...
            logger.info("Operation completed")  # Will include job_id and operation
    """
    logger_ = get_logger()
    with logger_.contextualize(**context):
        try:
            yield
        finally:
            pass


class PerformanceTimer:
    """
    A timer for measuring and logging performance metrics.
    
    This class provides both a standalone timer and context manager
    functionality for measuring execution time.
    """
    
    def __init__(self, name: str = None, auto_log: bool = True, level: str = "INFO"):
        """
        Initialize the performance timer.
        
        Args:
            name: Name for the timer (used in log messages)
            auto_log: Whether to automatically log when stopped
            level: Log level for auto-logging
        """
        self.name = name or "PerformanceTimer"
        self.auto_log = auto_log
        self.level = level
        self.start_time = None
        self.end_time = None
        self.elapsed = None
    
    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.time()
        self.end_time = None
        self.elapsed = None
    
    def stop(self) -> float:
        """
        Stop the timer and return elapsed time.
        
        Returns:
            Elapsed time in seconds
        """
        if self.start_time is None:
            raise RuntimeError("Timer not started")
        
        self.end_time = time.time()
        self.elapsed = self.end_time - self.start_time
        
        if self.auto_log:
            logger_ = get_logger(self.name)
            getattr(logger_, self.level.lower())(
                f"Timer '{self.name}' completed in {self.elapsed:.4f} seconds"
            )
        
        return self.elapsed
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
    
    def __str__(self) -> str:
        """String representation."""
        if self.elapsed is None:
            return f"{self.name} (not started)"
        return f"{self.name}: {self.elapsed:.4f}s"


# =============================================================================
# New Context Managers (As specified in Task 08)
# =============================================================================

@asynccontextmanager
async def managed_resource(
    acquire_func: Callable[[], Awaitable[T]],
    release_func: Callable[[T], Awaitable[None]],
    on_error: Optional[Callable[[Exception], Awaitable[None]]] = None
) -> AsyncIterator[T]:
    """
    Generic resource management context manager for async operations.
    
    This context manager provides a generic way to manage resources that need
    to be acquired and released asynchronously, with proper error handling.
    
    Args:
        acquire_func: Async function that acquires the resource
        release_func: Async function that releases the resource
        on_error: Optional async function called when an error occurs
        
    Yields:
        T: The acquired resource
        
    Example:
        async def acquire_connection():
            return await create_database_connection()
            
        async def release_connection(conn):
            await conn.close()
            
        async with managed_resource(acquire_connection, release_connection) as conn:
            result = await conn.execute_query("SELECT * FROM table")
    """
    resource = None
    logger = get_logger(__name__)
    
    try:
        resource = await acquire_func()
        yield resource
        
    except Exception as e:
        logger.error(f"Error in managed resource: {e}")
        
        if on_error:
            try:
                await on_error(e)
            except Exception as callback_error:
                logger.error(f"Error in on_error callback: {callback_error}")
        
        raise
        
    finally:
        if resource is not None:
            try:
                await release_func(resource)
            except Exception as cleanup_error:
                logger.warning(f"Resource cleanup error: {cleanup_error}")


@contextmanager
def managed_resource_sync(
    acquire_func: Callable[[], T],
    release_func: Callable[[T], None],
    on_error: Optional[Callable[[Exception], None]] = None
) -> Iterator[T]:
    """
    Generic resource management context manager for synchronous operations.
    
    This context manager provides a generic way to manage resources that need
    to be acquired and released synchronously, with proper error handling.
    
    Args:
        acquire_func: Function that acquires the resource
        release_func: Function that releases the resource
        on_error: Optional function called when an error occurs
        
    Yields:
        T: The acquired resource
        
    Example:
        def acquire_file():
            return open("file.txt", "r")
            
        def release_file(file_handle):
            file_handle.close()
            
        with managed_resource_sync(acquire_file, release_file) as f:
            content = f.read()
    """
    resource = None
    logger = get_logger(__name__)
    
    try:
        resource = acquire_func()
        yield resource
        
    except Exception as e:
        logger.error(f"Error in managed resource: {e}")
        
        if on_error:
            try:
                on_error(e)
            except Exception as callback_error:
                logger.error(f"Error in on_error callback: {callback_error}")
        
        raise
        
    finally:
        if resource is not None:
            try:
                release_func(resource)
            except Exception as cleanup_error:
                logger.warning(f"Resource cleanup error: {cleanup_error}")


@asynccontextmanager
async def timeout_context(seconds: float) -> AsyncIterator[None]:
    """
    Context manager for timeout operations.
    
    This context manager ensures that the enclosed operation completes
    within the specified timeout, raising a TimeoutError if it doesn't.
    
    Args:
        seconds: Timeout in seconds
        
    Raises:
        asyncio.TimeoutError: If the operation times out
        
    Example:
        try:
            async with timeout_context(5.0):
                result = await long_running_operation()
        except asyncio.TimeoutError:
            logger.error("Operation timed out after 5 seconds")
    """
    logger = get_logger(__name__)
    
    try:
        async with asyncio.timeout(seconds):
            yield
            
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out after {seconds} seconds")
        raise


@contextmanager
def timeout_context_sync(seconds: float) -> Iterator[None]:
    """
    Synchronous context manager for timeout operations.
    
    This context manager ensures that the enclosed operation completes
    within the specified timeout, raising a TimeoutError if it doesn't.
    
    Args:
        seconds: Timeout in seconds
        
    Raises:
        TimeoutError: If the operation times out
        
    Example:
        try:
            with timeout_context_sync(5.0):
                result = long_running_sync_operation()
        except TimeoutError:
            logger.error("Operation timed out after 5 seconds")
    """
    logger = get_logger(__name__)
    start_time = time.time()
    
    try:
        yield
        
    finally:
        elapsed = time.time() - start_time
        if elapsed > seconds:
            logger.warning(f"Operation exceeded timeout of {seconds} seconds (took {elapsed:.2f}s)")
            raise TimeoutError(f"Operation timed out after {seconds} seconds")


@asynccontextmanager
async def error_context(
    operation_name: str,
    logger: Optional[StructuredLogger] = None,
    suppress_exceptions: Tuple[Type[Exception], ...] = ()
) -> AsyncIterator[None]:
    """
    Context manager for operation error handling with exception suppression.
    
    This context manager provides structured error handling for operations,
    with the ability to suppress specific exceptions and log errors appropriately.
    
    Args:
        operation_name: Name of the operation for logging purposes
        logger: Optional logger instance (uses default if None)
        suppress_exceptions: Tuple of exception types to suppress
        
    Example:
        async with error_context("database_operation", suppress_exceptions=(ValueError,)):
            result = await risky_database_operation()
            # ValueError will be suppressed and logged as debug
            # Other exceptions will be logged as error and re-raised
    """
    if logger is None:
        logger = get_logger(__name__)
    
    try:
        yield
        
    except suppress_exceptions:
        logger.debug(f"{operation_name} completed with suppressed exception")
        
    except Exception as e:
        logger.error(f"Error in {operation_name}: {e}", exc_info=True)
        raise


@contextmanager
def error_context_sync(
    operation_name: str,
    logger: Optional[StructuredLogger] = None,
    suppress_exceptions: Tuple[Type[Exception], ...] = ()
) -> Iterator[None]:
    """
    Synchronous context manager for operation error handling with exception suppression.
    
    This context manager provides structured error handling for synchronous operations,
    with the ability to suppress specific exceptions and log errors appropriately.
    
    Args:
        operation_name: Name of the operation for logging purposes
        logger: Optional logger instance (uses default if None)
        suppress_exceptions: Tuple of exception types to suppress
        
    Example:
        with error_context_sync("file_operation", suppress_exceptions=(FileNotFoundError,)):
            result = risky_file_operation()
            # FileNotFoundError will be suppressed and logged as debug
            # Other exceptions will be logged as error and re-raised
    """
    if logger is None:
        logger = get_logger(__name__)
    
    try:
        yield
        
    except suppress_exceptions:
        logger.debug(f"{operation_name} completed with suppressed exception")
        
    except Exception as e:
        logger.error(f"Error in {operation_name}: {e}", exc_info=True)
        raise


@asynccontextmanager
async def operation_context(
    operation_name: str,
    logger: Optional[StructuredLogger] = None,
    **context: Dict[str, Any]
) -> AsyncIterator[str]:
    """
    Async context manager for structured operation logging.
    
    This context manager provides comprehensive logging for operations,
    including start/end timing, success/failure status, and structured context.
    
    Args:
        operation_name: Name of the operation for logging
        logger: Optional logger instance (uses default if None)
        **context: Additional context fields to include in log messages
        
    Yields:
        str: Operation ID for tracking
        
    Example:
        async with operation_context("process_job", job_id="123", worker_id="worker1") as op_id:
            result = await process_job(job_id)
            # Logs: Starting process_job, Completed process_job (with timing)
    """
    if logger is None:
        logger = get_logger(__name__)
    
    import uuid
    operation_id = str(uuid.uuid4())
    start_time = time.perf_counter()
    
    # Prepare context
    log_context = {
        'operation': operation_name,
        'operation_id': operation_id,
        'start_time': start_time,
        **context
    }
    
    logger.info(f"Starting {operation_name}", **log_context)
    
    try:
        yield operation_id
        
        # Log success
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.info(
            f"Completed {operation_name}",
            duration_ms=duration_ms,
            status="success",
            **log_context
        )
        
    except Exception as e:
        # Log failure
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            f"Failed {operation_name}: {e}",
            error_type=type(e).__name__,
            duration_ms=duration_ms,
            status="failed",
            **log_context,
            exc_info=True
        )
        raise


@contextmanager
def operation_context_sync(
    operation_name: str,
    logger: Optional[StructuredLogger] = None,
    **context: Dict[str, Any]
) -> Iterator[str]:
    """
    Synchronous context manager for structured operation logging.
    
    This context manager provides comprehensive logging for synchronous operations,
    including start/end timing, success/failure status, and structured context.
    
    Args:
        operation_name: Name of the operation for logging
        logger: Optional logger instance (uses default if None)
        **context: Additional context fields to include in log messages
        
    Yields:
        str: Operation ID for tracking
        
    Example:
        with operation_context_sync("process_file", filename="data.txt") as op_id:
            result = process_file(filename)
            # Logs: Starting process_file, Completed process_file (with timing)
    """
    if logger is None:
        logger = get_logger(__name__)
    
    import uuid
    operation_id = str(uuid.uuid4())
    start_time = time.perf_counter()
    
    # Prepare context
    log_context = {
        'operation': operation_name,
        'operation_id': operation_id,
        'start_time': start_time,
        **context
    }
    
    logger.info(f"Starting {operation_name}", **log_context)
    
    try:
        yield operation_id
        
        # Log success
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.info(
            f"Completed {operation_name}",
            duration_ms=duration_ms,
            status="success",
            **log_context
        )
        
    except Exception as e:
        # Log failure
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            f"Failed {operation_name}: {e}",
            error_type=type(e).__name__,
            duration_ms=duration_ms,
            status="failed",
            **log_context,
            exc_info=True
        )
        raise


# =============================================================================
# Utility Context Managers
# =============================================================================

@asynccontextmanager
async def combined_context(*context_managers):
    """
    Combine multiple async context managers into a single context.
    
    This utility allows combining multiple context managers to run
    concurrently, with proper cleanup of all resources.
    
    Args:
        *context_managers: Async context managers to combine
        
    Yields:
        Tuple: Results from all context managers
        
    Example:
        async with combined_context(
            nats_connection(config),
            timeout_context(5.0)
        ) as (conn, _):
            result = await conn.request("subject", data)
    """
    async with contextlib.AsyncExitStack() as stack:
        results = []
        for cm in context_managers:
            result = await stack.enter_async_context(cm)
            results.append(result)
        
        yield tuple(results)


@contextmanager
def combined_context_sync(*context_managers):
    """
    Combine multiple synchronous context managers into a single context.
    
    This utility allows combining multiple context managers to run
    concurrently, with proper cleanup of all resources.
    
    Args:
        *context_managers: Context managers to combine
        
    Yields:
        Tuple: Results from all context managers
        
    Example:
        with combined_context_sync(
            open_file_context("file1.txt"),
            open_file_context("file2.txt")
        ) as (f1, f2):
            data1 = f1.read()
            data2 = f2.read()
    """
    import contextlib
    
    with contextlib.ExitStack() as stack:
        results = []
        for cm in context_managers:
            result = stack.enter_context(cm)
            results.append(result)
        
        yield tuple(results)


# Import contextlib at module level for combined_context
import contextlib