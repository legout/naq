"""Context manager utilities for NAQ.

This module contains common context managers used throughout the NAQ codebase.
"""

import asyncio
import sys
import traceback
from collections.abc import Sequence
from typing import Awaitable, Callable, Optional, Type, TypeVar

from loguru import logger

from ..exceptions import NaqException

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


class ResourceManagementError(NaqException):
    """Raised when resource management fails."""

    pass


async def managed_resource(
    acquire_func: Callable[[], Awaitable[T]],
    release_func: Callable[[T], Awaitable[None]],
    on_error: Optional[Callable[[T, Exception], Awaitable[None]]] = None,
) -> T:
    """Generic async context manager for acquiring and releasing resources.

    This context manager ensures proper resource acquisition and release, even if errors
    occur within the async with block. It provides a clean way to manage resources that
    need to be properly cleaned up after use.

    Args:
        acquire_func: An async function that acquires the resource and returns it.
        release_func: An async function that releases the resource. Takes the resource
            as its only argument.
        on_error: Optional async callback function that is called if an exception
            occurs within the context manager block. Receives the resource and the
            exception as arguments.

    Yields:
        The acquired resource.

    Raises:
        ResourceManagementError: If resource acquisition fails.
        Any exception raised within the context block will be propagated after
        resource cleanup.

    Example:
        Basic usage with a database connection:

        ```python
        async def acquire_db_connection():
            return await create_database_connection()

        async def release_db_connection(conn):
            await conn.close()

        async with managed_resource(
            acquire_db_connection, release_db_connection
        ) as conn:
            result = await conn.execute_query("SELECT * FROM users")
            process_result(result)
        ```

        Example with error handling:

        ```python
        async def handle_connection_error(conn, error):
            await conn.rollback()
            logger.error(f"Database error occurred: {error}")

        async with managed_resource(
            acquire_db_connection,
            release_db_connection,
            on_error=handle_connection_error
        ) as conn:
            result = await conn.execute_query("SELECT * FROM users")
            process_result(result)
        ```
    """
    resource: Optional[T] = None
    try:
        # Acquire the resource
        logger.debug("Acquiring resource")
        resource = await acquire_func()
        logger.debug("Resource acquired successfully")

        # Yield the resource to the context block
        yield resource

    except Exception as e:
        logger.error(f"Exception occurred in resource context: {e}")

        # If we have a resource and an error handler, call it
        if resource is not None and on_error is not None:
            try:
                logger.debug("Calling error handler")
                await on_error(resource, e)
            except Exception as handler_error:
                logger.error(f"Error in error handler: {handler_error}")
                # Don't suppress the original exception

        # Re-raise the original exception
        raise

    finally:
        # Always release the resource if it was acquired
        if resource is not None:
            try:
                logger.debug("Releasing resource")
                await release_func(resource)
                logger.debug("Resource released successfully")
            except Exception as release_error:
                logger.error(f"Failed to release resource: {release_error}")
                # Convert to ResourceManagementError for consistent error handling
                raise ResourceManagementError(
                    f"Resource release failed: {release_error}"
                ) from release_error


async def timeout_context(seconds: float) -> None:
    """Async context manager that enforces a timeout on the enclosed code block.

    This context manager uses asyncio.timeout to ensure that the enclosed code block
    completes within the specified time limit. If the timeout is exceeded, an
    asyncio.TimeoutError is raised and a warning is logged.

    Args:
        seconds: The timeout duration in seconds. Must be a positive number.

    Yields:
        None: This context manager doesn't yield any specific value.

    Raises:
        asyncio.TimeoutError: If the enclosed code block doesn't complete within
            the specified timeout.
        ValueError: If seconds is not a positive number.

    Example:
        Basic usage with a time-consuming operation:

        ```python
        try:
            async with timeout_context(5.0):
                # This operation must complete within 5 seconds
                result = await long_running_operation()
                process_result(result)
        except asyncio.TimeoutError:
            print("Operation timed out")
        ```

        Example with nested context managers:

        ```python
        async with timeout_context(10.0):
            async with managed_resource(acquire_db, release_db) as conn:
                # Database operations must complete within 10 seconds
                data = await conn.fetch_data()
                await process_data(data)
        ```
    """
    if seconds <= 0:
        raise ValueError(f"Timeout seconds must be positive, got {seconds}")

    logger.debug(f"Starting timeout context with {seconds} seconds timeout")

    try:
        async with asyncio.timeout(seconds):
            yield
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out after {seconds} seconds")
        raise


async def error_context(
    operation_name: str,
    logger_instance: Optional[logger] = None,
    suppress_exceptions: Optional[Sequence[Type[Exception]]] = None,
) -> None:
    """Async context manager for standardized error handling and logging.

    This context manager provides consistent error handling and logging for
    async operations.
    It logs errors with full traceback information and allows for selective exception
    suppression based on exception types.

    Args:
        operation_name: A descriptive name for the operation being performed. This is
            included in log messages to help identify the context of the error.
        logger_instance: The logger instance to use for error logging. If None,
            the default loguru logger is used.
        suppress_exceptions: A sequence of exception types that should be suppressed
            (not re-raised). If an exception occurs that is in this sequence, it will
            be logged but not re-raised. If None, all exceptions are re-raised.

    Yields:
        None: This context manager doesn't yield any specific value.

    Raises:
        Any exception that occurs within the context block, unless it is in the
        suppress_exceptions list.

    Example:
        Basic usage with default logger and no exception suppression:

        ```python
        try:
            async with error_context("database_query"):
                result = await db.execute("SELECT * FROM users")
                return result
        except Exception as e:
            print(f"Query failed: {e}")
        ```

        Example with custom logger and exception suppression:

        ```python
        from loguru import logger

        try:
            async with error_context(
                "file_processing",
                logger_instance=logger,
                suppress_exceptions=[FileNotFoundError, PermissionError]
            ):
                data = await process_file("data.txt")
                return data
        except Exception as e:
            # Only reaches here if exception is not FileNotFoundError or PermissionError
            print(f"Unexpected error: {e}")
        ```

        Example with nested context managers:

        ```python
        async with error_context("data_processing"):
            async with timeout_context(30.0):
                async with managed_resource(acquire_db, release_db) as conn:
                    result = await conn.fetch_data()
                    return await process_result(result)
        ```
    """
    # Use provided logger or default to module logger
    effective_logger = logger_instance or logger

    # Default to empty tuple if suppress_exceptions is None
    effective_suppress = suppress_exceptions or ()

    try:
        effective_logger.debug(f"Starting operation: {operation_name}")
        yield
        effective_logger.debug(f"Completed operation: {operation_name}")

    except Exception as e:
        # Get the full traceback information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

        # Log the error with full traceback
        effective_logger.error(
            f"Error in operation '{operation_name}': {e}\nTraceback:\n{tb_str}"
        )

        # Check if this exception type should be suppressed
        should_suppress = any(
            isinstance(e, suppressed_type) for suppressed_type in effective_suppress
        )

        if not should_suppress:
            # Re-raise the exception if it's not in the suppress list
            raise
        else:
            # Log that we're suppressing this exception
            effective_logger.info(
                f"Suppressed exception of type {type(e).__name__} "
                f"in operation '{operation_name}'"
            )
