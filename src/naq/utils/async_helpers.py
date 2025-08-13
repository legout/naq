# src/naq/utils/async_helpers.py
"""
Async utilities and helpers for NAQ.

This module provides utilities for working with asynchronous operations,
including concurrency control, thread pool execution, and async/sync conversion.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import (
    Callable, Any, Optional, TypeVar, List, Awaitable, Union, 
    Iterable, AsyncIterator, Iterator, Coroutine
)
from functools import wraps, partial
import threading
import time

from loguru import logger

T = TypeVar('T')


async def run_in_thread(
    func: Callable[..., T], 
    *args, 
    executor: Optional[ThreadPoolExecutor] = None,
    **kwargs
) -> T:
    """
    Run synchronous function in thread pool.
    
    Args:
        func: Synchronous function to execute
        *args: Positional arguments for the function
        executor: Optional thread pool executor
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result from the function
        
    Usage:
        result = await run_in_thread(blocking_function, arg1, arg2, key=value)
    """
    loop = asyncio.get_event_loop()
    partial_func = partial(func, *args, **kwargs)
    return await loop.run_in_executor(executor, partial_func)


async def run_in_process(
    func: Callable[..., T],
    *args,
    executor: Optional[ProcessPoolExecutor] = None,
    **kwargs
) -> T:
    """
    Run function in process pool.
    
    Args:
        func: Function to execute in separate process
        *args: Positional arguments for the function
        executor: Optional process pool executor
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result from the function
        
    Usage:
        result = await run_in_process(cpu_intensive_function, data)
    """
    loop = asyncio.get_event_loop()
    partial_func = partial(func, *args, **kwargs)
    return await loop.run_in_executor(executor, partial_func)


async def gather_with_concurrency(
    tasks: List[Awaitable[T]], 
    concurrency: int = 10,
    return_exceptions: bool = False
) -> List[Union[T, Exception]]:
    """
    Execute tasks with limited concurrency.
    
    Args:
        tasks: List of awaitable tasks
        concurrency: Maximum number of concurrent tasks
        return_exceptions: Whether to return exceptions instead of raising
        
    Returns:
        List of results in the same order as input tasks
        
    Usage:
        results = await gather_with_concurrency([
            fetch_data(url1),
            fetch_data(url2),
            fetch_data(url3)
        ], concurrency=2)
    """
    semaphore = asyncio.Semaphore(concurrency)
    
    async def bounded_task(task):
        async with semaphore:
            return await task
    
    bounded_tasks = [bounded_task(task) for task in tasks]
    return await asyncio.gather(*bounded_tasks, return_exceptions=return_exceptions)


async def retry_async(
    func: Callable[..., Awaitable[T]],
    *args,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None,
    **kwargs
) -> T:
    """
    Retry async function with exponential backoff.
    
    Args:
        func: Async function to retry
        *args: Positional arguments for the function
        max_attempts: Maximum retry attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier
        exceptions: Exception types to retry on
        on_retry: Optional callback called on retry
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result from the function
        
    Usage:
        result = await retry_async(
            unreliable_async_function,
            arg1, arg2,
            max_attempts=5,
            delay=2.0,
            backoff=1.5
        )
    """
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            
            if attempt == max_attempts - 1:
                break
            
            wait_time = delay * (backoff ** attempt)
            
            if on_retry:
                if asyncio.iscoroutinefunction(on_retry):
                    await on_retry(attempt + 1, e, wait_time)
                else:
                    on_retry(attempt + 1, e, wait_time)
            
            logger.debug(f"Retry attempt {attempt + 1}/{max_attempts} for {func.__name__} in {wait_time}s")
            await asyncio.sleep(wait_time)
    
    raise last_exception


def sync_to_async(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """
    Convert synchronous function to async using thread pool.
    
    Args:
        func: Synchronous function to convert
        
    Returns:
        Async version of the function
        
    Usage:
        async_version = sync_to_async(blocking_function)
        result = await async_version(arg1, arg2)
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await run_in_thread(func, *args, **kwargs)
    return wrapper


def async_to_sync(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """
    Convert async function to synchronous (creates new event loop if needed).
    
    Args:
        func: Async function to convert
        
    Returns:
        Synchronous version of the function
        
    Usage:
        sync_version = async_to_sync(async_function)
        result = sync_version(arg1, arg2)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Try to get existing loop
            loop = asyncio.get_running_loop()
            # If we're already in a loop, we need to run in a thread
            return asyncio.run_coroutine_threadsafe(
                func(*args, **kwargs), loop
            ).result()
        except RuntimeError:
            # No running loop, create new one
            return asyncio.run(func(*args, **kwargs))
    return wrapper


async def async_map(
    func: Callable[[T], Awaitable[Any]],
    iterable: Iterable[T],
    concurrency: int = 10
) -> List[Any]:
    """
    Async version of map with concurrency control.
    
    Args:
        func: Async function to apply to each item
        iterable: Items to process
        concurrency: Maximum concurrent executions
        
    Returns:
        List of results
        
    Usage:
        results = await async_map(
            async_process_item,
            items,
            concurrency=5
        )
    """
    tasks = [func(item) for item in iterable]
    return await gather_with_concurrency(tasks, concurrency)


async def async_filter(
    predicate: Callable[[T], Awaitable[bool]],
    iterable: Iterable[T],
    concurrency: int = 10
) -> List[T]:
    """
    Async version of filter with concurrency control.
    
    Args:
        predicate: Async predicate function
        iterable: Items to filter
        concurrency: Maximum concurrent executions
        
    Returns:
        List of items that passed the predicate
        
    Usage:
        filtered = await async_filter(
            async_is_valid,
            items,
            concurrency=5
        )
    """
    items = list(iterable)
    results = await async_map(predicate, items, concurrency)
    return [item for item, keep in zip(items, results) if keep]


class AsyncBatch:
    """
    Process items in batches asynchronously.
    
    Usage:
        async for batch in AsyncBatch(items, batch_size=100):
            results = await process_batch(batch)
    """
    
    def __init__(self, items: Iterable[T], batch_size: int):
        self.items = list(items)
        self.batch_size = batch_size
        self.index = 0
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        
        batch = self.items[self.index:self.index + self.batch_size]
        self.index += self.batch_size
        return batch


async def debounce_async(
    func: Callable[..., Awaitable[T]],
    delay: float
) -> Callable[..., Awaitable[T]]:
    """
    Debounce async function calls.
    
    Args:
        func: Async function to debounce
        delay: Delay in seconds
        
    Returns:
        Debounced async function
        
    Usage:
        debounced_save = await debounce_async(save_data, 1.0)
        await debounced_save(data)  # Will wait 1 second before executing
    """
    last_call_time = 0
    lock = asyncio.Lock()
    
    @wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal last_call_time
        
        async with lock:
            now = time.time()
            last_call_time = now
            
            await asyncio.sleep(delay)
            
            # Check if another call was made during the delay
            if time.time() - last_call_time >= delay - 0.001:  # Small tolerance
                return await func(*args, **kwargs)
    
    return wrapper


async def throttle_async(
    func: Callable[..., Awaitable[T]],
    interval: float
) -> Callable[..., Awaitable[T]]:
    """
    Throttle async function calls.
    
    Args:
        func: Async function to throttle
        interval: Minimum interval between calls in seconds
        
    Returns:
        Throttled async function
        
    Usage:
        throttled_api_call = await throttle_async(api_call, 2.0)
        await throttled_api_call(data)  # Will ensure 2 seconds between calls
    """
    last_call_time = 0
    lock = asyncio.Lock()
    
    @wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal last_call_time
        
        async with lock:
            now = time.time()
            time_since_last = now - last_call_time
            
            if time_since_last < interval:
                await asyncio.sleep(interval - time_since_last)
            
            last_call_time = time.time()
            return await func(*args, **kwargs)
    
    return wrapper


class AsyncPool:
    """
    Simple async worker pool.
    
    Usage:
        async with AsyncPool(worker_count=5) as pool:
            tasks = [pool.submit(worker_func, data) for data in items]
            results = await asyncio.gather(*tasks)
    """
    
    def __init__(self, worker_count: int):
        self.semaphore = asyncio.Semaphore(worker_count)
        self.worker_count = worker_count
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def submit(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """Submit a task to the pool."""
        async with self.semaphore:
            return await func(*args, **kwargs)


async def race(*awaitables: Awaitable[T]) -> T:
    """
    Return the result of the first awaitable to complete.
    
    Args:
        *awaitables: Awaitables to race
        
    Returns:
        Result from the first completed awaitable
        
    Usage:
        result = await race(
            fetch_from_cache(key),
            fetch_from_database(key),
            fetch_from_api(key)
        )
    """
    done, pending = await asyncio.wait(
        awaitables,
        return_when=asyncio.FIRST_COMPLETED
    )
    
    # Cancel pending tasks
    for task in pending:
        task.cancel()
    
    # Return result from completed task
    completed_task = done.pop()
    return await completed_task


class AsyncLazy:
    """
    Lazy async computation that only executes once.
    
    Usage:
        lazy_result = AsyncLazy(expensive_async_computation, arg1, arg2)
        result = await lazy_result  # Computed on first access
        result = await lazy_result  # Returns cached result
    """
    
    def __init__(self, func: Callable[..., Awaitable[T]], *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.computed = False
        self.lock = asyncio.Lock()
    
    async def __call__(self) -> T:
        if self.computed:
            return self.result
        
        async with self.lock:
            if not self.computed:
                self.result = await self.func(*self.args, **self.kwargs)
                self.computed = True
            
            return self.result