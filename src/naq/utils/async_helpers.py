"""Async helper utilities for NAQ.

This module contains common async utilities used throughout the NAQ codebase.
"""

import asyncio
import functools
import random
from typing import Any, Awaitable, Callable, List, Tuple, Type, TypeVar

T = TypeVar("T")


async def run_in_thread(
    func: Callable[..., T], *args: Any, **kwargs: Any
) -> Awaitable[T]:
    """Run a synchronous function in a thread pool executor.

    This helper function allows running blocking synchronous code in a separate
    thread, preventing it from blocking the asyncio event loop. It's particularly
    useful for integrating synchronous I/O operations or CPU-bound tasks into
    asynchronous code.

    Args:
        func: The synchronous callable to execute in a thread pool.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        An awaitable that resolves to the return value of the function.

    Examples:
        Basic usage with a synchronous function:

        ```python
        def sync_function(x: int, y: int) -> int:
            # Simulate blocking operation
            import time
            time.sleep(1)
            return x + y

        async def main():
            result = await run_in_thread(sync_function, 2, 3)
            print(result)  # Output: 5
        ```

        Using with keyword arguments:

        ```python
        def process_data(data: dict, timeout: int = 5) -> str:
            # Simulate data processing
            import time
            time.sleep(timeout)
            return f"Processed: {data}"

        async def main():
            data = {"key": "value"}
            result = await run_in_thread(process_data, data, timeout=2)
            print(result)  # Output: Processed: {'key': 'value'}
        ```

        Using with functools.partial:

        ```python
        from functools import partial

        def fetch_url(url: str, method: str = "GET") -> bytes:
            # Simulate HTTP request
            import time
            time.sleep(0.5)
            return f"{method} {url}".encode()

        async def main():
            # Create a partial function with fixed method
            post_request = partial(fetch_url, method="POST")
            result = await run_in_thread(post_request, "https://example.com")
            print(result.decode())  # Output: POST https://example.com
        ```
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))


async def gather_with_concurrency(
    tasks: List[Awaitable[T]], concurrency: int
) -> List[T]:
    """Execute a list of awaitable tasks with limited concurrency.

    This helper function allows running multiple awaitable tasks concurrently while
    limiting the maximum number of tasks that can run simultaneously. It uses
    asyncio.Semaphore to enforce the concurrency limit, making it useful for
    scenarios where you need to control resource usage or avoid overwhelming
    external services.

    Args:
        tasks: A list of awaitable tasks to execute.
        concurrency: The maximum number of tasks to run concurrently.

    Returns:
        A list of results from the completed tasks, in the same order as the
        input tasks.

    Examples:
        Basic usage with a limited number of concurrent tasks:

        ```python
        async def fetch_data(url: str) -> str:
            # Simulate network request
            await asyncio.sleep(0.5)
            return f"Data from {url}"

        async def main():
            urls = [f"https://example.com/page/{i}" for i in range(10)]
            tasks = [fetch_data(url) for url in urls]

            # Run at most 3 tasks concurrently
            results = await gather_with_concurrency(tasks, concurrency=3)
            for result in results:
                print(result)
        ```

        Using with error handling:

        ```python
        async def process_item(item_id: int) -> dict:
            if item_id == 5:
                raise ValueError(f"Invalid item: {item_id}")
            await asyncio.sleep(0.1)
            return {"id": item_id, "status": "processed"}

        async def main():
            items = list(range(10))
            tasks = [process_item(item) for item in items]

            try:
                results = await gather_with_concurrency(tasks, concurrency=2)
                print(f"Processed {len(results)} items successfully")
            except Exception as e:
                print(f"Error occurred: {e}")
        ```

        Combining with run_in_thread for mixed sync/async operations:

        ```python
        def sync_operation(x: int) -> int:
            # Simulate CPU-bound work
            import time
            time.sleep(0.2)
            return x * 2

        async def async_operation(x: int) -> int:
            # Simulate async I/O
            await asyncio.sleep(0.1)
            return x + 10

        async def main():
            numbers = list(range(10))
            tasks = []

            # Mix of sync and async operations
            for num in numbers:
                if num % 2 == 0:
                    tasks.append(run_in_thread(sync_operation, num))
                else:
                    tasks.append(async_operation(num))

            # Run with concurrency limit
            results = await gather_with_concurrency(tasks, concurrency=3)
            print(f"Results: {results}")
        ```
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def run_with_semaphore(task: Awaitable[T]) -> T:
        async with semaphore:
            return await task

    return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])


async def retry_async(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs: Any,
) -> T:
    """Retry an asynchronous function with exponential backoff.

    This helper function executes an async function and retries it if it raises
    one of the specified exceptions. The delay between retries increases exponentially
    with each attempt, with optional jitter to prevent thundering herd problems.

    Args:
        func: The async callable to execute and retry if needed.
        *args: Positional arguments to pass to the function.
        max_attempts: Maximum number of attempts before giving up. Defaults to 3.
        delay: Initial delay in seconds between retries. Defaults to 1.0.
        backoff: Factor by which the delay increases exponentially. Defaults to 2.0.
        exceptions: Tuple of exception types to retry on. Defaults to (Exception,).
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The return value of the function if successful.

    Raises:
        The last exception raised if all attempts fail.

    Examples:
        Basic usage with default parameters:

        ```python
        async def unreliable_api_call() -> str:
            # Simulate API call that might fail
            if random.random() < 0.7:
                raise ConnectionError("API unavailable")
            return "Success"

        async def main():
            try:
                result = await retry_async(unreliable_api_call)
                print(result)  # Output: Success
            except Exception as e:
                print(f"Failed after all retries: {e}")
        ```

        Custom retry parameters:

        ```python
        async def fetch_data(url: str) -> dict:
            # Simulate HTTP request that might fail
            if "timeout" in url:
                raise TimeoutError("Request timeout")
            return {"data": f"Content from {url}"}

        async def main():
            try:
                # Retry up to 5 times with 0.5s initial delay and 1.5x backoff
                result = await retry_async(
                    fetch_data,
                    "https://example.com/api/data",
                    max_attempts=5,
                    delay=0.5,
                    backoff=1.5,
                    exceptions=(TimeoutError, ConnectionError)
                )
                print(result)
            except Exception as e:
                print(f"Failed to fetch data: {e}")
        ```

        Using with keyword arguments:

        ```python
        async def process_file(path: str, mode: str = "read") -> bytes:
            # Simulate file processing that might fail
            if "corrupt" in path:
                raise ValueError("File corrupted")
            return f"Processed {path} in {mode} mode".encode()

        async def main():
            try:
                result = await retry_async(
                    process_file,
                    "document.txt",
                    mode="write",
                    max_attempts=3,
                    delay=0.2,
                    exceptions=(ValueError, IOError)
                )
                print(result.decode())
            except Exception as e:
                print(f"Failed to process file: {e}")
        ```

        Combining with other async helpers:

        ```python
        async def external_service_call(service_id: int) -> str:
            # Simulate external service that might be temporarily unavailable
            await asyncio.sleep(0.1)
            if service_id < 5:
                raise ConnectionError("Service temporarily unavailable")
            return f"Response from service {service_id}"

        async def main():
            services = [3, 7, 2, 8, 1]
            tasks = [
                retry_async(
                    external_service_call,
                    service_id,
                    max_attempts=4,
                    delay=0.5,
                    exceptions=(ConnectionError,)
                )
                for service_id in services
            ]

            # Use gather_with_concurrency to limit concurrent retries
            results = await gather_with_concurrency(tasks, concurrency=2)
            for result in results:
                print(result)
        ```
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e

            # Don't sleep on the last attempt
            if attempt < max_attempts:
                # Calculate exponential backoff with jitter
                sleep_time = delay * (backoff ** (attempt - 1))
                # Add jitter to prevent thundering herd
                jitter = random.uniform(0.8, 1.2)
                sleep_time *= jitter

                await asyncio.sleep(sleep_time)

    # If we get here, all attempts failed
    raise last_exception


def sync_to_async(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """Convert a synchronous function to an asynchronous function.

    This helper function wraps a synchronous callable, allowing it to be
    awaited in asynchronous code. It uses `run_in_thread` to execute the
    original synchronous function in a thread pool, preventing it from
    blocking the asyncio event loop.

    Args:
        func: The synchronous callable to convert to an asynchronous function.

    Returns:
        An asynchronous wrapper function that can be awaited. The wrapper
        accepts the same arguments as the original function and returns
        an awaitable that resolves to the return value of the function.

    Examples:
        Basic usage with a synchronous function:

        ```python
        def sync_function(x: int, y: int) -> int:
            # Simulate blocking operation
            import time
            time.sleep(1)
            return x + y

        async def main():
            # Convert sync function to async
            async_func = sync_to_async(sync_function)
            result = await async_func(2, 3)
            print(result)  # Output: 5
        ```

        Using with keyword arguments:

        ```python
        def process_data(data: dict, timeout: int = 5) -> str:
            # Simulate data processing
            import time
            time.sleep(timeout)
            return f"Processed: {data}"

        async def main():
            # Convert sync function to async
            async_processor = sync_to_async(process_data)
            data = {"key": "value"}
            result = await async_processor(data, timeout=2)
            print(result)  # Output: Processed: {'key': 'value'}
        ```

        Using as a decorator:

        ```python
        @sync_to_async
        def fetch_url(url: str, method: str = "GET") -> bytes:
            # Simulate HTTP request
            import time
            time.sleep(0.5)
            return f"{method} {url}".encode()

        async def main():
            # The function is now async and can be awaited
            result = await fetch_url("https://example.com", method="POST")
            print(result.decode())  # Output: POST https://example.com
        ```

        Combining with other async helpers:

        ```python
        def cpu_intensive_task(n: int) -> int:
            # Simulate CPU-bound work
            return sum(i * i for i in range(n))

        async def main():
            # Convert to async
            async_task = sync_to_async(cpu_intensive_task)

            # Use with gather_with_concurrency
            tasks = [async_task(1000) for _ in range(5)]
            results = await gather_with_concurrency(tasks, concurrency=2)
            print(f"Results: {results}")
        ```
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        return await run_in_thread(func, *args, **kwargs)

    return wrapper


def run_async_from_sync(
    func: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any
) -> T:
    """Run an asynchronous function from synchronous code.

    This helper function allows calling async functions from synchronous code
    by creating and running a new asyncio event loop. It's particularly
    useful for integrating async functionality into existing synchronous
    codebases or for testing async functions in synchronous test runners.

    Args:
        func: The asynchronous callable to execute.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The return value of the asynchronous function.

    Examples:
        Basic usage with an async function:

        ```python
        async def async_function(x: int, y: int) -> int:
            await asyncio.sleep(0.1)  # Simulate async operation
            return x + y

        def sync_code():
            # Call async function from sync code
            result = run_async_from_sync(async_function, 2, 3)
            print(result)  # Output: 5

        sync_code()
        ```

        Using with keyword arguments:

        ```python
        async def fetch_data(url: str, timeout: int = 5) -> dict:
            await asyncio.sleep(0.1)  # Simulate network request
            return {"url": url, "status": "success"}

        def sync_handler():
            # Call async function with keyword arguments
            data = run_async_from_sync(
                fetch_data,
                "https://example.com",
                timeout=10
            )
            print(data)  # Output: {'url': 'https://example.com', 'status': 'success'}

        sync_handler()
        ```

        Error handling:

        ```python
        async def async_function_with_error():
            await asyncio.sleep(0.1)
            raise ValueError("Async error occurred")

        def sync_code_with_error_handling():
            try:
                result = run_async_from_sync(async_function_with_error)
            except ValueError as e:
                print(f"Caught error: {e}")

        sync_code_with_error_handling()  # Output: Caught error: Async error occurred
        ```
    """
    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the async function
        result = loop.run_until_complete(func(*args, **kwargs))
        return result
    finally:
        # Clean up the event loop
        loop.close()


def async_to_sync(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """Convert an asynchronous function to a synchronous function.

    This helper function wraps an async callable, allowing it to be called
    from synchronous code. It uses `asyncio.run()` to execute the original
    async function in a new event loop, making it easy to integrate async
    functionality into existing synchronous codebases.

    Args:
        func: The asynchronous callable to convert to a synchronous function.

    Returns:
        A synchronous wrapper function that accepts the same arguments as the
        original function and returns the result directly (not as an awaitable).

    Examples:
        Basic usage with an async function:

        ```python
        async def async_add(x: int, y: int) -> int:
            await asyncio.sleep(0.1)  # Simulate async operation
            return x + y

        def sync_code():
            # Convert async function to sync
            sync_add = async_to_sync(async_add)
            result = sync_add(2, 3)
            print(result)  # Output: 5

        sync_code()
        ```

        Using with keyword arguments:

        ```python
        async def async_fetch_data(url: str, timeout: int = 5) -> dict:
            await asyncio.sleep(0.1)  # Simulate network request
            return {"url": url, "status": "success"}

        def sync_handler():
            # Convert async function to sync
            sync_fetch = async_to_sync(async_fetch_data)
            data = sync_fetch("https://example.com", timeout=10)
            print(data)  # Output: {'url': 'https://example.com', 'status': 'success'}

        sync_handler()
        ```

        Using as a decorator:

        ```python
        @async_to_sync
        async def async_process(text: str) -> str:
            await asyncio.sleep(0.1)
            return f"Processed: {text}"

        def sync_code():
            # The function is now sync and can be called directly
            result = async_process("hello world")
            print(result)  # Output: Processed: hello world

        sync_code()
        ```

        Error handling:

        ```python
        async def async_function_with_error():
            await asyncio.sleep(0.1)
            raise ValueError("Async error occurred")

        def sync_code_with_error_handling():
            try:
                sync_func = async_to_sync(async_function_with_error)
                result = sync_func()
            except ValueError as e:
                print(f"Caught error: {e}")

        sync_code_with_error_handling()  # Output: Caught error: Async error occurred
        ```

        Combining with other helpers:

        ```python
        async def async_task(task_id: int) -> str:
            await asyncio.sleep(0.1)
            return f"Task {task_id} completed"

        def sync_workflow():
            # Convert to sync
            sync_task = async_to_sync(async_task)

            # Use in synchronous code
            results = []
            for i in range(3):
                result = sync_task(i)
                results.append(result)

            print(results)  # Output: ['Task 0 completed', 'Task 1 completed', 'Task 2 completed']

        sync_workflow()
        ```
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return asyncio.run(func(*args, **kwargs))

    return wrapper
