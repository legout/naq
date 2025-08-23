"""Tests for memory leak detection in connection management."""

import asyncio
import gc
import tracemalloc
from typing import Dict, Any

import pytest
import pytest_asyncio

from naq.services.connection import ConnectionService
from naq.queue.core import Queue


@pytest.fixture
def memory_tracker():
    """Fixture to track memory allocations."""
    tracemalloc.start()
    yield
    tracemalloc.stop()


@pytest_asyncio.fixture
async def connection_service():
    """Fixture for connection service."""
    service = ConnectionService()
    await service.initialize()
    yield service
    await service.cleanup()


@pytest_asyncio.fixture
async def queue_api():
    """Fixture for queue API."""
    api = Queue(name="test_queue")
    await api.initialize()
    yield api
    await api.cleanup()


class TestMemoryLeaks:
    """Test class for memory leak detection."""

    @pytest.mark.asyncio
    async def test_connection_service_memory_leak(
        self, memory_tracker, connection_service
    ):
        """Test that ConnectionService doesn't leak memory."""
        # Take initial snapshot
        snapshot1 = tracemalloc.take_snapshot()
        
        # Perform operations
        for _ in range(10):  # Reduced iterations for faster test
            # Create and cleanup connections
            await connection_service.get_connection()
            await connection_service.get_jetstream()
        
        # Force garbage collection
        gc.collect()
        
        # Take final snapshot
        snapshot2 = tracemalloc.take_snapshot()
        
        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Allow for some growth due to caching, but it should be reasonable
        assert total_growth < 1024 * 1024, f"Memory growth too high: {total_growth} bytes"

    @pytest.mark.asyncio
    async def test_queue_api_memory_leak(self, memory_tracker, queue_api):
        """Test that Queue doesn't leak memory."""
        # Take initial snapshot
        snapshot1 = tracemalloc.take_snapshot()
        
        # Perform operations
        for _ in range(10):  # Reduced iterations for faster test
            # Create and cleanup queue connections
            await queue_api.get_connection()
            await queue_api.get_jetstream()
        
        # Force garbage collection
        gc.collect()
        
        # Take final snapshot
        snapshot2 = tracemalloc.take_snapshot()
        
        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Allow for some growth due to caching, but it should be reasonable
        assert total_growth < 1024 * 1024, f"Memory growth too high: {total_growth} bytes"

    @pytest.mark.asyncio
    async def test_repeated_connection_creation(self, memory_tracker):
        """Test memory usage with repeated connection creation and cleanup."""
        # Take initial snapshot
        snapshot1 = tracemalloc.take_snapshot()
        
        # Create and cleanup many connection services
        for _ in range(5):  # Reduced iterations for faster test
            service = ConnectionService()
            await service.initialize()
            await service.cleanup()
        
        # Force garbage collection
        gc.collect()
        
        # Take final snapshot
        snapshot2 = tracemalloc.take_snapshot()
        
        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Should be minimal growth after cleanup
        assert total_growth < 512 * 1024, f"Memory growth too high: {total_growth} bytes"

    @pytest.mark.asyncio
    async def test_concurrent_connections_memory(self, memory_tracker):
        """Test memory usage with concurrent connections."""
        # Take initial snapshot
        snapshot1 = tracemalloc.take_snapshot()
        
        # Create many concurrent connections
        async def create_and_cleanup():
            service = ConnectionService()
            await service.initialize()
            await service.get_connection()
            await service.cleanup()
        
        # Run concurrently
        await asyncio.gather(*[create_and_cleanup() for _ in range(5)])  # Reduced for faster test
        
        # Force garbage collection
        gc.collect()
        
        # Take final snapshot
        snapshot2 = tracemalloc.take_snapshot()
        
        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Should be reasonable growth for concurrent operations
        assert total_growth < 2 * 1024 * 1024, f"Memory growth too high: {total_growth} bytes"

    @pytest.mark.asyncio
    async def test_memory_cleanup_after_exception(self, memory_tracker):
        """Test that memory is properly cleaned up after exceptions."""
        # Take initial snapshot
        snapshot1 = tracemalloc.take_snapshot()
        
        # Create connections that might fail
        for _ in range(5):  # Reduced iterations for faster test
            try:
                service = ConnectionService()
                await service.initialize()
                # This might fail if NATS is not running
                await service.get_connection()
            except Exception:
                pass  # Ignore connection errors
            finally:
                try:
                    await service.cleanup()
                except Exception:
                    pass  # Ignore cleanup errors
        
        # Force garbage collection
        gc.collect()
        
        # Take final snapshot
        snapshot2 = tracemalloc.take_snapshot()
        
        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        
        # Check for significant memory growth
        total_growth = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
        
        # Should be minimal growth even with exceptions
        assert total_growth < 1024 * 1024, f"Memory growth too high: {total_growth} bytes"