"""Unit tests for async_helpers module."""

import asyncio
import pytest
from typing import Any

from naq.utils.async_helpers import async_to_sync, sync_to_async


class TestSyncToAsync:
    """Test cases for the sync_to_async function."""

    def test_sync_to_async_basic_functionality(self) -> None:
        """Test that sync_to_async correctly converts a sync function to async."""
        def sync_add(x: int, y: int) -> int:
            return x + y

        async_add = sync_to_async(sync_add)

        # Verify the returned function is awaitable
        assert asyncio.iscoroutinefunction(async_add)

        # Test the async wrapper works correctly
        async def test_async():
            result = await async_add(2, 3)
            return result

        result = asyncio.run(test_async())
        assert result == 5

    def test_sync_to_async_with_keyword_arguments(self) -> None:
        """Test sync_to_async works with keyword arguments."""
        def sync_greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}!"

        async_greet = sync_to_async(sync_greet)

        async def test_async():
            # Test with positional args
            result1 = await async_greet("Alice", "Hi")
            # Test with keyword args
            result2 = await async_greet("Bob", greeting="Welcome")
            # Test with default value
            result3 = await async_greet("Charlie")
            return result1, result2, result3

        result1, result2, result3 = asyncio.run(test_async())
        assert result1 == "Hi, Alice!"
        assert result2 == "Welcome, Bob!"
        assert result3 == "Hello, Charlie!"

    def test_sync_to_async_with_no_arguments(self) -> None:
        """Test sync_to_async works with functions that take no arguments."""
        def sync_get_constant() -> str:
            return "constant_value"

        async_get_constant = sync_to_async(sync_get_constant)

        async def test_async():
            return await async_get_constant()

        result = asyncio.run(test_async())
        assert result == "constant_value"

    def test_sync_to_async_with_exception(self) -> None:
        """Test sync_to_async properly handles exceptions from the sync function."""
        def sync_raise_error() -> None:
            raise ValueError("Test error")

        async_raise_error = sync_to_async(sync_raise_error)

        async def test_async():
            try:
                await async_raise_error()
                return False  # Should not reach here
            except ValueError as e:
                return str(e)

        result = asyncio.run(test_async())
        assert result == "Test error"

    def test_sync_to_async_preserves_function_name_and_docstring(self) -> None:
        """Test sync_to_async preserves the original function's metadata."""
        def sync_example() -> str:
            """This is a test function."""
            return "example"

        async_example = sync_to_async(sync_example)

        assert async_example.__name__ == "sync_example"
        assert "This is a test function." in async_example.__doc__

    def test_sync_to_async_with_complex_return_types(self) -> None:
        """Test sync_to_async works with complex return types."""
        def sync_return_dict() -> dict[str, Any]:
            return {"key": "value", "number": 42, "list": [1, 2, 3]}

        async_return_dict = sync_to_async(sync_return_dict)

        async def test_async():
            return await async_return_dict()

        result = asyncio.run(test_async())
        assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}

    def test_sync_to_async_with_blocking_operation(self) -> None:
        """Test sync_to_async works with blocking operations."""
        def sync_blocking_operation(seconds: float) -> str:
            import time
            time.sleep(seconds)
            return f"Slept for {seconds} seconds"

        async_blocking_operation = sync_to_async(sync_blocking_operation)

        async def test_async():
            return await async_blocking_operation(0.1)  # Short sleep for test speed

        result = asyncio.run(test_async())
        assert result == "Slept for 0.1 seconds"

    def test_sync_to_async_as_decorator(self) -> None:
        """Test sync_to_async works as a decorator."""
        @sync_to_async
        def sync_decorated_function(x: int, y: int) -> int:
            return x * y

        async def test_async():
            return await sync_decorated_function(4, 5)

        result = asyncio.run(test_async())
        assert result == 20

    def test_sync_to_async_concurrent_execution(self) -> None:
        """Test that multiple sync_to_async wrapped functions can run concurrently."""
        def sync_task(task_id: int, duration: float) -> str:
            import time
            time.sleep(duration)
            return f"Task {task_id} completed"

        async_task = sync_to_async(sync_task)

        async def test_async():
            # Create multiple tasks that should run concurrently
            tasks = [
                async_task(1, 0.1),
                async_task(2, 0.1),
                async_task(3, 0.1),
            ]
            return await asyncio.gather(*tasks)

        results = asyncio.run(test_async())
        expected = [
            "Task 1 completed",
            "Task 2 completed",
            "Task 3 completed",
        ]
        assert results == expected


class TestAsyncToSync:
    """Test cases for the async_to_sync function."""

    def test_async_to_sync_basic_functionality(self) -> None:
        """Test that async_to_sync correctly converts an async function to sync."""
        async def async_add(x: int, y: int) -> int:
            await asyncio.sleep(0.01)  # Short sleep for test speed
            return x + y

        sync_add = async_to_sync(async_add)

        # Test the sync wrapper works correctly
        result = sync_add(2, 3)
        assert result == 5

    def test_async_to_sync_with_keyword_arguments(self) -> None:
        """Test async_to_sync works with keyword arguments."""
        async def async_greet(name: str, greeting: str = "Hello") -> str:
            await asyncio.sleep(0.01)
            return f"{greeting}, {name}!"

        sync_greet = async_to_sync(async_greet)

        # Test with positional args
        result1 = sync_greet("Alice", "Hi")
        # Test with keyword args
        result2 = sync_greet("Bob", greeting="Welcome")
        # Test with default value
        result3 = sync_greet("Charlie")
        
        assert result1 == "Hi, Alice!"
        assert result2 == "Welcome, Bob!"
        assert result3 == "Hello, Charlie!"

    def test_async_to_sync_with_no_arguments(self) -> None:
        """Test async_to_sync works with functions that take no arguments."""
        async def async_get_constant() -> str:
            await asyncio.sleep(0.01)
            return "constant_value"

        sync_get_constant = async_to_sync(async_get_constant)

        result = sync_get_constant()
        assert result == "constant_value"

    def test_async_to_sync_with_exception(self) -> None:
        """Test async_to_sync properly handles exceptions from the async function."""
        async def async_raise_error() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("Test error")

        sync_raise_error = async_to_sync(async_raise_error)

        with pytest.raises(ValueError, match="Test error"):
            sync_raise_error()

    def test_async_to_sync_preserves_function_name_and_docstring(self) -> None:
        """Test async_to_sync preserves the original function's metadata."""
        async def async_example() -> str:
            """This is a test async function."""
            await asyncio.sleep(0.01)
            return "example"

        sync_example = async_to_sync(async_example)

        assert sync_example.__name__ == "async_example"
        assert "This is a test async function." in sync_example.__doc__

    def test_async_to_sync_with_complex_return_types(self) -> None:
        """Test async_to_sync works with complex return types."""
        async def async_return_dict() -> dict[str, Any]:
            await asyncio.sleep(0.01)
            return {"key": "value", "number": 42, "list": [1, 2, 3]}

        sync_return_dict = async_to_sync(async_return_dict)

        result = sync_return_dict()
        assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}

    def test_async_to_sync_with_async_operation(self) -> None:
        """Test async_to_sync works with actual async operations."""
        async def async_fetch_data(url: str) -> str:
            # Simulate network request
            await asyncio.sleep(0.01)
            return f"Data from {url}"

        sync_fetch_data = async_to_sync(async_fetch_data)

        result = sync_fetch_data("https://example.com")
        assert result == "Data from https://example.com"

    def test_async_to_sync_as_decorator(self) -> None:
        """Test async_to_sync works as a decorator."""
        @async_to_sync
        async def async_decorated_function(x: int, y: int) -> int:
            await asyncio.sleep(0.01)
            return x * y

        result = async_decorated_function(4, 5)
        assert result == 20

    def test_async_to_sync_with_multiple_await_calls(self) -> None:
        """Test async_to_sync works with functions that have multiple await calls."""
        async def async_multiple_awaits(x: int, y: int) -> int:
            await asyncio.sleep(0.01)
            temp = x + y
            await asyncio.sleep(0.01)
            return temp * 2

        sync_multiple_awaits = async_to_sync(async_multiple_awaits)

        result = sync_multiple_awaits(3, 4)
        assert result == 14

    def test_async_to_sync_with_nested_async_calls(self) -> None:
        """Test async_to_sync works with functions that call other async functions."""
        async def async_inner(value: str) -> str:
            await asyncio.sleep(0.01)
            return f"Inner: {value}"

        async def async_outer(prefix: str, suffix: str) -> str:
            await asyncio.sleep(0.01)
            inner_result = await async_inner(prefix)
            await asyncio.sleep(0.01)
            return f"{inner_result} {suffix}"

        sync_outer = async_to_sync(async_outer)

        result = sync_outer("Hello", "World")
        assert result == "Inner: Hello World"

    def test_async_to_sync_with_async_generator(self) -> None:
        """Test async_to_sync works with functions that consume async generators."""
        async def async_process_items(items: list[int]) -> list[int]:
            results = []
            for item in items:
                await asyncio.sleep(0.01)
                results.append(item * 2)
            return results

        sync_process_items = async_to_sync(async_process_items)

        result = sync_process_items([1, 2, 3, 4, 5])
        assert result == [2, 4, 6, 8, 10]

    def test_async_to_sync_with_concurrent_operations(self) -> None:
        """Test async_to_sync works with functions that use concurrent operations."""
        async def async_concurrent_tasks() -> list[str]:
            tasks = [
                asyncio.create_task(self._mock_async_task("Task 1")),
                asyncio.create_task(self._mock_async_task("Task 2")),
                asyncio.create_task(self._mock_async_task("Task 3")),
            ]
            return await asyncio.gather(*tasks)

        sync_concurrent_tasks = async_to_sync(async_concurrent_tasks)

        result = sync_concurrent_tasks()
        expected = ["Task 1 completed", "Task 2 completed", "Task 3 completed"]
        assert result == expected

    async def _mock_async_task(self, task_name: str) -> str:
        """Helper async function for testing concurrent operations."""
        await asyncio.sleep(0.01)
        return f"{task_name} completed"

    def test_async_to_sync_with_different_exception_types(self) -> None:
        """Test async_to_sync properly handles different types of exceptions."""
        async def async_raise_type_error() -> None:
            await asyncio.sleep(0.01)
            raise TypeError("Type error occurred")

        async def async_raise_runtime_error() -> None:
            await asyncio.sleep(0.01)
            raise RuntimeError("Runtime error occurred")

        sync_raise_type_error = async_to_sync(async_raise_type_error)
        sync_raise_runtime_error = async_to_sync(async_raise_runtime_error)

        with pytest.raises(TypeError, match="Type error occurred"):
            sync_raise_type_error()

        with pytest.raises(RuntimeError, match="Runtime error occurred"):
            sync_raise_runtime_error()

    def test_async_to_sync_with_async_context_manager(self) -> None:
        """Test async_to_sync works with functions that use async context managers."""
        class MockAsyncContext:
            async def __aenter__(self):
                return self
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        async def async_with_context() -> str:
            async with MockAsyncContext():
                await asyncio.sleep(0.01)
                return "Context manager used"

        sync_with_context = async_to_sync(async_with_context)

        result = sync_with_context()
        assert result == "Context manager used"

    def test_async_to_sync_combination_with_sync_to_async(self) -> None:
        """Test that async_to_sync and sync_to_async can be combined."""
        def sync_original(x: int, y: int) -> int:
            return x + y

        # Convert sync to async
        async_func = sync_to_async(sync_original)
        
        # Convert back to sync
        sync_func = async_to_sync(async_func)

        result = sync_func(5, 7)
        assert result == 12