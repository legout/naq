import pytest
import pytest_asyncio
import asyncio
import time
from typing import Any, Dict

import nats
from nats.js.errors import KeyNotFoundError, BucketNotFoundError

from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.services.connection import ConnectionService
from naq.services.base import ServiceConfig
from naq.exceptions import NaqException


# Test helper functions
def test_data_function() -> Dict[str, Any]:
    """Return test data for KV operations."""
    return {
        "timestamp": time.time(),
        "message": "test data",
        "values": [1, 2, 3, 4, 5],
    }


async def wait_for_kv_condition(condition_func, timeout=5.0, interval=0.1):
    """Wait for a KV condition to become true."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if await condition_func():
            return True
        await asyncio.sleep(interval)
    return False


@pytest_asyncio.fixture
async def nats_client(nats_server):
    """Create a NATS client for direct KV operations."""
    nc = await nats.connect(nats_server)
    try:
        yield nc
    finally:
        await nc.close()


@pytest_asyncio.fixture
async def kv_service(nats_server, nats_client):
    """Create a KVStoreService instance for testing."""
    config = ServiceConfig(nats_url=nats_server)
    service = KVStoreService(config=config, nats_client=nats_client)
    try:
        await service.initialize()
        yield service
    finally:
        await service.cleanup()




@pytest.mark.asyncio
class TestKVStoreServiceIntegration:
    """Integration tests for KVStoreService with real NATS."""

    async def test_basic_put_get_operations(self, kv_service):
        """Test basic put and get operations."""
        bucket_name = "test_basic_ops"
        key = "test_key"
        value = {"message": "Hello, World!", "number": 42}

        # Put value with timeout
        await asyncio.wait_for(
            kv_service.put(bucket_name, key, value),
            timeout=5.0
        )

        # Get value back with timeout
        retrieved_value = await asyncio.wait_for(
            kv_service.get(bucket_name, key),
            timeout=5.0
        )

        assert retrieved_value == value

    async def test_put_get_with_ttl(self, kv_service):
        """Test put and get operations with TTL."""
        bucket_name = "test_ttl_ops"
        key = "test_key_ttl"
        value = {"message": "This should expire"}

        # Put value
        await kv_service.put(bucket_name, key, value)

        # Get value immediately - should exist
        retrieved_value = await kv_service.get(bucket_name, key)
        assert retrieved_value == value

        # Note: TTL is handled at the bucket level in NATS KV, not individual keys
        # So we can't test TTL expiration for individual keys

    async def test_delete_operation(self, kv_service):
        """Test delete operation."""
        bucket_name = "test_delete_ops"
        key = "test_key_delete"
        value = {"message": "This will be deleted"}

        # Put value
        await kv_service.put(bucket_name, key, value)

        # Verify it exists
        retrieved_value = await kv_service.get(bucket_name, key)
        assert retrieved_value == value

        # Delete it
        delete_result = await kv_service.delete(bucket_name, key)
        assert delete_result is True

        # Verify it's gone
        with pytest.raises(NaqException, match="not found"):
            await kv_service.get(bucket_name, key)

    async def test_delete_nonexistent_key(self, kv_service):
        """Test deleting a non-existent key."""
        bucket_name = "test_delete_nonexistent"
        key = "nonexistent_key"

        # Try to delete non-existent key
        delete_result = await kv_service.delete(bucket_name, key)
        assert delete_result is False

    async def test_kv_transaction_commit(self, kv_service):
        """Test KV transaction commit."""
        bucket_name = "test_transaction"
        
        # Start transaction
        async with kv_service.kv_transaction(bucket_name) as transaction:
            # Put multiple values
            await transaction.put("key1", "value1")
            await transaction.put("key2", {"data": "value2"})
            await transaction.put("key3", [1, 2, 3])

        # Verify all values were committed
        assert await kv_service.get(bucket_name, "key1") == "value1"
        assert await kv_service.get(bucket_name, "key2") == {"data": "value2"}
        assert await kv_service.get(bucket_name, "key3") == [1, 2, 3]

    async def test_kv_transaction_rollback_on_exception(self, kv_service):
        """Test KV transaction rollback when exception occurs."""
        bucket_name = "test_transaction_rollback"
        
        # Put a value before transaction
        await kv_service.put(bucket_name, "existing_key", "existing_value")

        # Start transaction and fail
        with pytest.raises(NaqException, match="Transaction failed: Simulated error"):
            async with kv_service.kv_transaction(bucket_name) as transaction:
                await transaction.put("key1", "value1")
                await transaction.put("key2", "value2")
                # Simulate an error
                raise ValueError("Simulated error")

        # Verify existing key still exists
        assert await kv_service.get(bucket_name, "existing_key") == "existing_value"
        
        # Verify transaction keys were not committed
        with pytest.raises(NaqException, match="not found"):
            await kv_service.get(bucket_name, "key1")
        with pytest.raises(NaqException, match="not found"):
            await kv_service.get(bucket_name, "key2")

    async def test_kv_transaction_get_within_transaction(self, kv_service):
        """Test getting values within a transaction."""
        bucket_name = "test_transaction_get"
        
        # Put initial values
        await kv_service.put(bucket_name, "key1", "value1")
        await kv_service.put(bucket_name, "key2", "value2")

        # Start transaction and read values
        async with kv_service.kv_transaction(bucket_name) as transaction:
            value1 = await transaction.get("key1")
            value2 = await transaction.get("key2")
            
            assert value1 == "value1"
            assert value2 == "value2"
            
            # Put a new value
            await transaction.put("key3", "value3")

        # Verify all values exist after transaction
        assert await kv_service.get(bucket_name, "key1") == "value1"
        assert await kv_service.get(bucket_name, "key2") == "value2"
        assert await kv_service.get(bucket_name, "key3") == "value3"

    async def test_kv_transaction_delete_within_transaction(self, kv_service):
        """Test deleting values within a transaction."""
        bucket_name = "test_transaction_delete"
        
        # Put initial values
        await kv_service.put(bucket_name, "key1", "value1")
        await kv_service.put(bucket_name, "key2", "value2")

        # Start transaction and delete a key
        async with kv_service.kv_transaction(bucket_name) as transaction:
            await transaction.delete("key1")
            await transaction.put("key3", "value3")

        # Verify key1 is deleted and key3 exists
        with pytest.raises(NaqException, match="not found"):
            await kv_service.get(bucket_name, "key1")
        assert await kv_service.get(bucket_name, "key2") == "value2"
        assert await kv_service.get(bucket_name, "key3") == "value3"

    async def test_multiple_buckets_isolation(self, kv_service):
        """Test that operations on different buckets are isolated."""
        bucket1 = "test_bucket1"
        bucket2 = "test_bucket2"
        key = "same_key"
        
        # Put same key in different buckets with different values
        await kv_service.put(bucket1, key, "value_from_bucket1")
        await kv_service.put(bucket2, key, "value_from_bucket2")

        # Verify values are different
        value1 = await kv_service.get(bucket1, key)
        value2 = await kv_service.get(bucket2, key)
        
        assert value1 == "value_from_bucket1"
        assert value2 == "value_from_bucket2"
        assert value1 != value2

    async def test_kv_store_pooling(self, kv_service):
        """Test KV store pooling functionality."""
        bucket1 = "test_pool1"
        bucket2 = "test_pool2"
        bucket3 = "test_pool3"
        
        # Set small pool size for testing
        kv_service._kv_config.max_pool_size = 2
        
        # Access first two buckets
        await kv_service.put(bucket1, "key1", "value1")
        await kv_service.put(bucket2, "key2", "value2")
        
        # Verify both are in pool
        assert bucket1 in kv_service.active_buckets
        assert bucket2 in kv_service.active_buckets
        
        # Access third bucket - should evict oldest
        await kv_service.put(bucket3, "key3", "value3")
        
        # Verify pool contains only last two buckets
        active_buckets = kv_service.active_buckets
        assert bucket1 not in active_buckets  # Should be evicted
        assert bucket2 in active_buckets
        assert bucket3 in active_buckets

    async def test_complex_data_types(self, kv_service):
        """Test storing and retrieving complex data types."""
        bucket_name = "test_complex_data"
        
        test_values = {
            "dict": {"nested": {"deeply": {"nested": "value"}}, "list": [1, 2, 3]},
            "list": [{"item": 1}, {"item": 2}, {"item": 3}],
            "set": {1, 2, 3, 4, 5},  # Sets are converted to lists during serialization
            "tuple": (1, 2, 3, "four"),
            "bytes": b"raw_bytes_data",
            "string": "unicode_string_ñáéíóú",
            "number": 42.195,
            "boolean": True,
            "none": None,
        }
        
        # Store each value type
        for key, value in test_values.items():
            await kv_service.put(bucket_name, key, value)
        
        # Retrieve and verify each value type
        for key, original_value in test_values.items():
            retrieved_value = await kv_service.get(bucket_name, key)
            
            # Note: sets are converted to lists during cloudpickle serialization
            if isinstance(original_value, set):
                assert set(retrieved_value) == original_value
            else:
                assert retrieved_value == original_value

    async def test_large_data_handling(self, kv_service):
        """Test handling of large data values."""
        bucket_name = "test_large_data"
        key = "large_data"
        
        # Create a large dictionary
        large_data = {
            "items": [{"id": i, "data": "x" * 100} for i in range(1000)],
            "metadata": {"created": time.time(), "size": "large"},
        }
        
        # Store large data
        await kv_service.put(bucket_name, key, large_data)
        
        # Retrieve and verify
        retrieved_data = await kv_service.get(bucket_name, key)
        assert retrieved_data == large_data
        assert len(retrieved_data["items"]) == 1000

    async def test_concurrent_operations(self, kv_service):
        """Test concurrent operations on the same bucket."""
        bucket_name = "test_concurrent"
        num_operations = 10
        
        async def put_operation(index):
            await kv_service.put(bucket_name, f"key_{index}", f"value_{index}")
            return index
        
        async def get_operation(index):
            # Wait a bit to ensure put has happened
            await asyncio.sleep(0.1)
            value = await kv_service.get(bucket_name, f"key_{index}")
            return value == f"value_{index}"
        
        # Run concurrent put operations
        put_tasks = [put_operation(i) for i in range(num_operations)]
        put_results = await asyncio.gather(*put_tasks, return_exceptions=True)
        
        # Verify all puts succeeded
        for i, result in enumerate(put_results):
            assert result == i, f"Put operation {i} failed"
        
        # Run concurrent get operations
        get_tasks = [get_operation(i) for i in range(num_operations)]
        get_results = await asyncio.gather(*get_tasks, return_exceptions=True)
        
        # Verify all gets succeeded
        for i, result in enumerate(get_results):
            assert result is True, f"Get operation {i} failed"

    async def test_bucket_auto_creation(self, kv_service):
        """Test automatic bucket creation when enabled."""
        bucket_name = "test_auto_create"
        key = "test_key"
        value = "test_value"
        
        # Ensure auto-creation is enabled (default)
        assert kv_service._kv_config.auto_create_buckets is True
        
        # Put to non-existent bucket - should create automatically
        await kv_service.put(bucket_name, key, value)
        
        # Verify value was stored
        retrieved_value = await kv_service.get(bucket_name, key)
        assert retrieved_value == value

    async def test_bucket_no_auto_creation(self, kv_service):
        """Test behavior when auto-creation is disabled."""
        bucket_name = "test_no_auto_create"
        key = "test_key"
        value = "test_value"
        
        # Disable auto-creation
        kv_service._kv_config.auto_create_buckets = False
        
        # Try to put to non-existent bucket - should fail
        with pytest.raises(NaqException, match="auto_create is disabled"):
            await kv_service.put(bucket_name, key, value)

    async def test_kv_store_service_lifecycle(self, nats_client):
        """Test KVStoreService initialization and cleanup lifecycle."""
        config = ServiceConfig()

        # Create and initialize service
        service = KVStoreService(config=config, nats_client=nats_client)
        assert not service.is_initialized

        await service.initialize()
        assert service.is_initialized

        # Use the service
        await service.put("lifecycle_test", "test_key", "test_value")
        value = await service.get("lifecycle_test", "test_key")
        assert value == "test_value"

        # Cleanup
        await service.cleanup()
        assert not service.is_initialized

    async def test_error_handling_invalid_bucket_name(self, kv_service):
        """Test error handling for invalid bucket names."""
        # NATS has specific bucket name requirements
        invalid_bucket_names = [
            "",  # Empty
            "bucket/with/slashes",  # Contains slashes
            "bucket with spaces",  # Contains spaces
            "bucket.with.dots",  # Contains dots
        ]
        
        for invalid_name in invalid_bucket_names:
            with pytest.raises(NaqException):
                await kv_service.put(invalid_name, "key", "value")

    async def test_error_handling_invalid_key(self, kv_service):
        """Test error handling for invalid keys."""
        bucket_name = "test_invalid_keys"
        
        # Empty key should fail
        with pytest.raises(NaqException):
            await kv_service.put(bucket_name, "", "empty_key_value")
        
        # Key with spaces should fail
        with pytest.raises(NaqException):
            await kv_service.put(bucket_name, "key with spaces", "spaces_value")
        
        # Key with special characters should work
        await kv_service.put(bucket_name, "key.with.dots", "dots_value")
        assert await kv_service.get(bucket_name, "key.with.dots") == "dots_value"
        
        # Long key (but not too long to cause timeouts)
        long_key = "x" * 100
        await kv_service.put(bucket_name, long_key, "long_key_value")
        assert await kv_service.get(bucket_name, long_key) == "long_key_value"

    async def test_transactions_disabled(self, kv_service):
        """Test behavior when transactions are disabled."""
        bucket_name = "test_no_transactions"
        
        # Disable transactions
        kv_service._kv_config.enable_transactions = False
        
        # Try to start transaction - should fail
        with pytest.raises(NaqException, match="Transactions are disabled"):
            async with kv_service.kv_transaction(bucket_name):
                pass

    async def test_keys_listing(self, kv_service, nats_client):
        """Test listing keys in a bucket."""
        bucket_name = "test_keys_listing"
        
        # Put multiple keys
        test_keys = ["key1", "key2", "key3", "key4", "key5"]
        for key in test_keys:
            await kv_service.put(bucket_name, key, f"value_{key}")
        
        # Get the KV store directly to list keys
        kv = await kv_service.get_kv_store(bucket_name)
        keys = await kv.keys()
        
        # Convert bytes to strings and verify
        key_strings = [k.decode() if isinstance(k, bytes) else k for k in keys]
        for test_key in test_keys:
            assert test_key in key_strings