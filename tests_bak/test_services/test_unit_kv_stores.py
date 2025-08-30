import pytest
import pytest_asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import cloudpickle
from datetime import datetime, timezone

from naq.services.kv_stores import (
    KVStoreService,
    KVStoreServiceConfig,
    KVTransaction,
)
from naq.services.connection import ConnectionService
from naq.services.base import ServiceConfig, ServiceInitializationError
from naq.exceptions import NaqException


@pytest.mark.asyncio
class TestKVStoreService:
    """Test cases for the KVStoreService class."""

    @pytest_asyncio.fixture
    async def mock_connection_service(self):
        """Create a mock ConnectionService."""
        mock_service = MagicMock(spec=ConnectionService)
        mock_service.is_initialized = True
        mock_service.get_jetstream = AsyncMock()
        return mock_service

    @pytest_asyncio.fixture
    async def mock_kv_store(self):
        """Create a mock KeyValue store."""
        mock_kv = MagicMock()
        mock_kv.put = AsyncMock()
        mock_kv.get = AsyncMock()
        mock_kv.delete = AsyncMock()
        mock_kv.keys = AsyncMock(return_value=[])
        return mock_kv

    @pytest_asyncio.fixture
    async def mock_js(self, mock_kv_store):
        """Create a mock JetStream context."""
        mock_js = MagicMock()
        mock_js.key_value = AsyncMock(return_value=mock_kv_store)
        mock_js.create_key_value = AsyncMock(return_value=mock_kv_store)
        return mock_js

    @pytest_asyncio.fixture
    async def kv_service(self, mock_connection_service):
        """Create a KVStoreService instance with mocked dependencies."""
        config = ServiceConfig()
        service = KVStoreService(config=config, connection_service=mock_connection_service)
        await service.initialize()
        return service

    async def test_initialization_success(self, mock_connection_service, mock_js):
        """Test successful initialization of KVStoreService."""
        mock_connection_service.get_jetstream.return_value = mock_js
        
        config = ServiceConfig()
        service = KVStoreService(config=config, connection_service=mock_connection_service)
        
        await service.initialize()
        
        assert service.is_initialized
        assert service._connection_service == mock_connection_service

    async def test_initialization_without_connection_service(self):
        """Test initialization fails without ConnectionService."""
        config = ServiceConfig()
        service = KVStoreService(config=config, connection_service=None)
        
        with pytest.raises(ServiceInitializationError, match="ConnectionService is required"):
            await service.initialize()

    async def test_initialization_with_uninitialized_connection_service(self):
        """Test initialization fails with uninitialized ConnectionService."""
        mock_connection_service = MagicMock(spec=ConnectionService)
        mock_connection_service.is_initialized = False
        mock_connection_service.initialize = AsyncMock()
        
        config = ServiceConfig()
        service = KVStoreService(config=config, connection_service=mock_connection_service)
        
        await service.initialize()
        
        mock_connection_service.initialize.assert_awaited_once()

    async def test_extract_kv_config_with_custom_settings(self):
        """Test extraction of KV configuration from custom settings."""
        custom_settings = {
            "default_bucket_ttl": 3600,
            "default_key_ttl": 1800,
            "max_pool_size": 20,
            "enable_transactions": False,
            "auto_create_buckets": False,
        }
        
        config = ServiceConfig(custom_settings=custom_settings)
        service = KVStoreService(config=config)
        
        kv_config = service._extract_kv_config()
        
        assert kv_config.default_bucket_ttl == 3600
        assert kv_config.default_key_ttl == 1800
        assert kv_config.max_pool_size == 20
        assert kv_config.enable_transactions is False
        assert kv_config.auto_create_buckets is False

    async def test_extract_kv_config_with_defaults(self):
        """Test extraction of KV configuration with default values."""
        config = ServiceConfig()
        service = KVStoreService(config=config)
        
        kv_config = service._extract_kv_config()
        
        assert kv_config.default_bucket_ttl is None
        assert kv_config.default_key_ttl is None
        assert kv_config.max_pool_size == 10
        assert kv_config.enable_transactions is True
        assert kv_config.auto_create_buckets is True

    async def test_get_kv_store_existing_bucket(self, kv_service, mock_js, mock_kv_store):
        """Test getting an existing KV store."""
        kv_service._connection_service.get_jetstream.return_value = mock_js
        
        result = await kv_service.get_kv_store("test_bucket")
        
        assert result == mock_kv_store
        mock_js.key_value.assert_awaited_once_with(bucket="test_bucket")
        mock_js.create_key_value.assert_not_awaited()
        assert "test_bucket" in kv_service._kv_stores

    async def test_get_kv_store_auto_create(self, kv_service, mock_js, mock_kv_store):
        """Test auto-creating a KV store when it doesn't exist."""
        # Make key_value raise an exception to trigger auto-creation
        mock_js.key_value.side_effect = [Exception("Bucket not found"), mock_kv_store]
        kv_service._connection_service.get_jetstream.return_value = mock_js
        
        result = await kv_service.get_kv_store("new_bucket")
        
        assert result == mock_kv_store
        mock_js.key_value.assert_awaited_with(bucket="new_bucket")
        mock_js.create_key_value.assert_awaited_once()
        assert "new_bucket" in kv_service._kv_stores

    async def test_get_kv_store_auto_create_disabled(self, kv_service, mock_js):
        """Test that auto-creation can be disabled."""
        kv_service._kv_config.auto_create_buckets = False
        mock_js.key_value.side_effect = Exception("Bucket not found")
        kv_service._connection_service.get_jetstream.return_value = mock_js
        
        with pytest.raises(NaqException, match="auto_create is disabled"):
            await kv_service.get_kv_store("test_bucket")

    async def test_get_kv_store_pool_limit(self, kv_service, mock_js, mock_kv_store):
        """Test that KV store pool size is limited."""
        kv_service._kv_config.max_pool_size = 2
        
        # Fill up the pool
        kv_service._kv_stores = {"bucket1": mock_kv_store, "bucket2": mock_kv_store}
        
        kv_service._connection_service.get_jetstream.return_value = mock_js
        
        # Add another bucket - should remove the oldest
        result = await kv_service.get_kv_store("bucket3")
        
        assert result == mock_kv_store
        assert "bucket3" in kv_service._kv_stores
        assert "bucket1" not in kv_service._kv_stores  # Should be removed

    async def test_put_with_serialization(self, kv_service, mock_kv_store):
        """Test putting a value with serialization."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = {"data": "test_value"}
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        await kv_service.put(bucket_name, key, value, serialize=True)
        
        mock_kv_store.put.assert_awaited_once()
        call_args = mock_kv_store.put.await_args
        assert call_args.args[0] == key
        # Verify the value was serialized
        deserialized_value = cloudpickle.loads(call_args.args[1])
        assert deserialized_value == value

    async def test_put_without_serialization(self, kv_service, mock_kv_store):
        """Test putting a value without serialization."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = b"raw_bytes"
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        await kv_service.put(bucket_name, key, value, serialize=False)
        
        mock_kv_store.put.assert_awaited_once()
        call_args = mock_kv_store.put.await_args
        assert call_args.args[0] == key
        assert call_args.args[1] == value  # Should be passed as-is

    async def test_put_with_ttl(self, kv_service, mock_kv_store):
        """Test putting a value with TTL."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = "test_value"
        ttl = 3600
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        await kv_service.put(bucket_name, key, value, ttl=ttl)
        
        mock_kv_store.put.assert_awaited_once_with(key, cloudpickle.dumps(value), ttl=ttl)

    async def test_put_with_default_ttl(self, kv_service, mock_kv_store):
        """Test putting a value with default TTL from config."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = "test_value"
        
        # Set default TTL
        kv_service._kv_config.default_key_ttl = 1800
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        await kv_service.put(bucket_name, key, value)
        
        mock_kv_store.put.assert_awaited_once_with(key, cloudpickle.dumps(value), ttl=1800)

    async def test_get_with_deserialization(self, kv_service, mock_kv_store):
        """Test getting a value with deserialization."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = {"data": "test_value"}
        
        # Mock the KV store
        mock_kv_store.get.return_value = MagicMock(value=cloudpickle.dumps(value))
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        result = await kv_service.get(bucket_name, key, deserialize=True)
        
        assert result == value
        mock_kv_store.get.assert_awaited_once_with(key)

    async def test_get_without_deserialization(self, kv_service, mock_kv_store):
        """Test getting a value without deserialization."""
        bucket_name = "test_bucket"
        key = "test_key"
        value = b"raw_bytes"
        
        # Mock the KV store
        mock_kv_store.get.return_value = MagicMock(value=value)
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        result = await kv_service.get(bucket_name, key, deserialize=False)
        
        assert result == value
        mock_kv_store.get.assert_awaited_once_with(key)

    async def test_get_key_not_found(self, kv_service, mock_kv_store):
        """Test getting a non-existent key."""
        from nats.js.errors import KeyNotFoundError
        
        bucket_name = "test_bucket"
        key = "nonexistent_key"
        
        # Mock the KV store to raise KeyNotFoundError
        mock_kv_store.get.side_effect = KeyNotFoundError("Key not found")
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        with pytest.raises(NaqException, match="Key 'nonexistent_key' not found"):
            await kv_service.get(bucket_name, key)

    async def test_delete_success(self, kv_service, mock_kv_store):
        """Test successful key deletion."""
        bucket_name = "test_bucket"
        key = "test_key"
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        result = await kv_service.delete(bucket_name, key)
        
        assert result is True
        mock_kv_store.delete.assert_awaited_once_with(key, purge=False)

    async def test_delete_with_purge(self, kv_service, mock_kv_store):
        """Test key deletion with purge."""
        bucket_name = "test_bucket"
        key = "test_key"
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        result = await kv_service.delete(bucket_name, key, purge=True)
        
        assert result is True
        mock_kv_store.delete.assert_awaited_once_with(key, purge=True)

    async def test_delete_key_not_found(self, kv_service, mock_kv_store):
        """Test deleting a non-existent key."""
        from nats.js.errors import KeyNotFoundError
        
        bucket_name = "test_bucket"
        key = "nonexistent_key"
        
        # Mock the KV store to raise KeyNotFoundError
        mock_kv_store.delete.side_effect = KeyNotFoundError("Key not found")
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        result = await kv_service.delete(bucket_name, key)
        
        assert result is False

    async def test_kv_transaction_enabled(self, kv_service, mock_kv_store):
        """Test KV transaction when enabled."""
        bucket_name = "test_bucket"
        
        # Mock the KV store
        kv_service._kv_stores[bucket_name] = mock_kv_store
        
        async with kv_service.kv_transaction(bucket_name) as transaction:
            assert isinstance(transaction, KVTransaction)
            assert transaction._kv == mock_kv_store
            assert transaction._bucket_name == bucket_name

    async def test_kv_transaction_disabled(self, kv_service):
        """Test KV transaction when disabled."""
        bucket_name = "test_bucket"
        
        # Disable transactions
        kv_service._kv_config.enable_transactions = False
        
        with pytest.raises(NaqException, match="Transactions are disabled"):
            async with kv_service.kv_transaction(bucket_name):
                pass

    async def test_active_buckets_property(self, kv_service):
        """Test the active_buckets property."""
        # Add some buckets to the pool
        kv_service._kv_stores = {
            "bucket1": MagicMock(),
            "bucket2": MagicMock(),
            "bucket3": MagicMock(),
        }
        
        active_buckets = kv_service.active_buckets
        
        assert set(active_buckets) == {"bucket1", "bucket2", "bucket3"}

    async def test_kv_config_property(self, kv_service):
        """Test the kv_config property."""
        config = kv_service.kv_config
        
        assert isinstance(config, KVStoreServiceConfig)
        assert config.enable_transactions is True  # Default value


@pytest.mark.asyncio
class TestKVTransaction:
    """Test cases for the KVTransaction class."""

    @pytest_asyncio.fixture
    async def mock_kv_store(self):
        """Create a mock KeyValue store."""
        mock_kv = MagicMock()
        mock_kv.put = AsyncMock()
        mock_kv.get = AsyncMock()
        mock_kv.delete = AsyncMock()
        return mock_kv

    @pytest_asyncio.fixture
    async def mock_logger(self):
        """Create a mock logger."""
        return MagicMock()

    @pytest_asyncio.fixture
    async def transaction(self, mock_kv_store, mock_logger):
        """Create a KVTransaction instance."""
        return KVTransaction(mock_kv_store, "test_bucket", mock_logger)

    async def test_put_operation(self, transaction, mock_kv_store):
        """Test queuing a put operation."""
        key = "test_key"
        value = {"data": "test_value"}
        ttl = 3600
        
        await transaction.put(key, value, ttl=ttl, serialize=True)
        
        assert len(transaction._operations) == 1
        operation = transaction._operations[0]
        assert operation["type"] == "put"
        assert operation["key"] == key
        assert cloudpickle.loads(operation["data"]) == value
        assert operation["ttl"] == ttl

    async def test_put_operation_without_serialization(self, transaction, mock_kv_store):
        """Test queuing a put operation without serialization."""
        key = "test_key"
        value = b"raw_bytes"
        
        await transaction.put(key, value, serialize=False)
        
        operation = transaction._operations[0]
        assert operation["type"] == "put"
        assert operation["key"] == key
        assert operation["data"] == value

    async def test_get_operation(self, transaction, mock_kv_store):
        """Test getting a value within a transaction."""
        key = "test_key"
        value = {"data": "test_value"}
        
        # Mock the KV store to return the value
        mock_kv_store.get.return_value = MagicMock(value=cloudpickle.dumps(value))
        
        result = await transaction.get(key, deserialize=True)
        
        assert result == value
        mock_kv_store.get.assert_awaited_once_with(key)

    async def test_delete_operation(self, transaction, mock_kv_store):
        """Test queuing a delete operation."""
        key = "test_key"
        
        await transaction.delete(key, purge=True)
        
        assert len(transaction._operations) == 1
        operation = transaction._operations[0]
        assert operation["type"] == "delete"
        assert operation["key"] == key
        assert operation["purge"] is True

    async def test_commit_success(self, transaction, mock_kv_store):
        """Test successful transaction commit."""
        # Queue some operations
        await transaction.put("key1", "value1")
        await transaction.delete("key2")
        
        await transaction.commit()
        
        assert transaction._committed is True
        assert mock_kv_store.put.await_count == 1
        assert mock_kv_store.delete.await_count == 1

    async def test_commit_with_ttl(self, transaction, mock_kv_store):
        """Test transaction commit with TTL."""
        await transaction.put("key1", "value1", ttl=3600)
        
        await transaction.commit()
        
        mock_kv_store.put.assert_awaited_once_with("key1", cloudpickle.dumps("value1"), ttl=3600)

    async def test_commit_already_committed(self, transaction, mock_kv_store):
        """Test committing an already committed transaction."""
        await transaction.put("key1", "value1")
        await transaction.commit()
        
        # Reset mocks
        mock_kv_store.put.reset_mock()
        
        # Commit again
        await transaction.commit()
        
        # Should not call put again
        mock_kv_store.put.assert_not_awaited()

    async def test_rollback(self, transaction, mock_kv_store):
        """Test transaction rollback."""
        # Queue some operations
        await transaction.put("key1", "value1")
        await transaction.delete("key2")
        
        await transaction.rollback()
        
        assert len(transaction._operations) == 0
        assert transaction._committed is False

    async def test_operation_after_commit(self, transaction):
        """Test that operations cannot be added after commit."""
        await transaction.put("key1", "value1")
        await transaction.commit()
        
        with pytest.raises(NaqException, match="Transaction already committed"):
            await transaction.put("key2", "value2")

    async def test_commit_failure(self, transaction, mock_kv_store):
        """Test transaction commit failure."""
        # Make put fail
        mock_kv_store.put.side_effect = Exception("Put failed")
        
        # Queue an operation
        await transaction.put("key1", "value1")
        
        # Commit should raise an exception
        with pytest.raises(NaqException, match="Transaction failed"):
            await transaction.commit()