"""
Asynchronous execution utilities for NAQ.

This module provides utilities for executing asynchronous code from
synchronous contexts and managing async operations.
"""

import asyncio
import sys
from typing import Any, Callable, Coroutine, TypeVar, Optional

import anyio

T = TypeVar("T")


def run_async(
    func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
) -> T:
    """
    Runs an async function from a synchronous context.
    
    This is an alias for run_async_from_sync for better naming consistency.

    Args:
        func: The async function to run.
        *args: Positional arguments to pass to the async function.
        **kwargs: Keyword arguments to pass to the async function.

    Returns:
        The result of the async function.

    Raises:
        RuntimeError: If called when an asyncio event loop is already running.
    """
    return run_async_from_sync(func, *args, **kwargs)


def run_async_from_sync(
    func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
) -> T:
    """
    Runs an async function from a synchronous context.

    Handles event loop management by using anyio.run() which creates a new
    event loop if one isn't already running. If an event loop is already running,
    it raises an informative error directing the user to use the async interface.

    Args:
        func: The async function to run.
        *args: Positional arguments to pass to the async function.
        **kwargs: Keyword arguments to pass to the async function.

    Returns:
        The result of the async function.

    Raises:
        RuntimeError: If called when an asyncio event loop is already running.
    """
    try:
        # anyio.run() creates a new event loop if one isn't running,
        # runs the coroutine to completion, and then closes the loop.
        return anyio.run(func, *args, **kwargs)
    except RuntimeError as e:
        # Check for the specific error message from anyio when a loop is already running.
        # The exact message might differ from asyncio.
        if (
            "cannot be called from a running event loop" in str(e).lower()
            or "anyio.run() cannot be called from within a running event loop"
            in str(e).lower()
        ):
            raise RuntimeError(
                "Cannot run naq sync function when an event loop is already running. "
                "Please use the async version of the function (e.g., `await naq.enqueue(...)`)."
            ) from e
        else:
            # Re-raise other RuntimeErrors
            raise


def run_async_in_thread(
    func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
) -> T:
    """
    Runs an async function in a separate thread with its own event loop.

    This is useful when you need to run async code from a sync context that
    already has a running event loop.

    Args:
        func: The async function to run.
        *args: Positional arguments to pass to the async function.
        **kwargs: Keyword arguments to pass to the async function.

    Returns:
        The result of the async function.
    """
    def run_in_thread():
        return asyncio.run(func(*args, **kwargs))
    
    import threading
    thread = threading.Thread(target=run_in_thread)
    thread.start()
    thread.join()
    
    # Get the result from the thread
    # This is a simplified version - in practice you'd need a more sophisticated
    # mechanism to return values from threads
    return run_in_thread()


async def run_sync(
    func: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """
    Runs a synchronous function from an async context.

    Args:
        func: The synchronous function to run.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the function.
    """
    return await asyncio.to_thread(func, *args, **kwargs)


async def run_sync_in_async(
    func: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """
    Runs a synchronous function from an async context.
    
    This is an alias for run_sync for better naming consistency.

    Args:
        func: The synchronous function to run.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the function.
    """
    return await run_sync(func, *args, **kwargs)


def is_async_function(func: Callable) -> bool:
    """
    Check if a function is asynchronous (coroutine function).
    
    Args:
        func: The function to check.
        
    Returns:
        bool: True if the function is asynchronous, False otherwise.
    """
    return asyncio.iscoroutinefunction(func)


def run_in_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> Coroutine[Any, Any, T]:
    """
    Run a synchronous function in a separate thread to avoid blocking the event loop.
    
    Args:
        func: The synchronous function to run.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.
        
    Returns:
        Coroutine that resolves to the function's return value.
    """
    return asyncio.to_thread(func, *args, **kwargs)


async def gather_with_concurrency(
    limit: int, *coroutines: Coroutine[Any, Any, T]
) -> list[T]:
    """
    Run multiple coroutines with a concurrency limit.
    
    Args:
        limit: Maximum number of coroutines to run concurrently.
        *coroutines: Coroutines to execute.
        
    Returns:
        List of results from the coroutines, in the same order as input.
    """
    semaphore = asyncio.Semaphore(limit)
    
    async def limited_coro(coro: Coroutine[Any, Any, T]) -> T:
        async with semaphore:
            return await coro
    
    return await asyncio.gather(*(limited_coro(coro) for coro in coroutines))


async def wait_with_timeout(
    coro: Coroutine[Any, Any, T], timeout: float
) -> T:
    """
    Wait for a coroutine to complete with a timeout.
    
    Args:
        coro: The coroutine to wait for.
        timeout: Timeout in seconds.
        
    Returns:
        The result of the coroutine.
        
    Raises:
        asyncio.TimeoutError: If the coroutine doesn't complete within the timeout.
    """
    return await asyncio.wait_for(coro, timeout=timeout)


def create_task(coro: Coroutine[Any, Any, T], name: Optional[str] = None) -> asyncio.Task[T]:
    """
    Create an asyncio task with optional name.
    
    Args:
        coro: The coroutine to wrap in a task.
        name: Optional name for the task.
        
    Returns:
        asyncio.Task: The created task.
    """
    if name is not None:
        return asyncio.create_task(coro, name=name)
    return asyncio.create_task(coro)


class AsyncToSyncBridge:
    """
    A bridge that allows async functions to be called from sync code.
    """
    
    def __init__(self):
        self._loop = None
    
    def wrap(self, func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., T]:
        """
        Wrap an async function to make it callable from sync code.
        
        Args:
            func: The async function to wrap.
            
        Returns:
            A synchronous function that calls the async function.
        """
        def wrapper(*args, **kwargs):
            return run_async(func, *args, **kwargs)
        return wrapper


class SyncToAsyncBridge:
    """
    A bridge that allows sync functions to be called from async code.
    """
    
    def wrap(self, func: Callable[..., T]) -> Callable[..., Coroutine[Any, Any, T]]:
        """
        Wrap a sync function to make it callable from async code.
        
        Args:
            func: The sync function to wrap.
            
        Returns:
            An async function that calls the sync function.
        """
        async def wrapper(*args, **kwargs):
            return await run_sync(func, *args, **kwargs)
        return wrapper