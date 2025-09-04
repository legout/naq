"""
Tests for the unified NATS client.

This module tests the NatsClient class which provides a unified interface
for both synchronous and asynchronous NATS operations.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from nats.js.api import ConsumerConfig, StreamConfig

from naq.nats_client import NatsClient, NatsClientConfig
from naq.exceptions import NaqConnectionError, NaqException
from naq.config import get_config


class TestNatsClientConfig:
    """Test cases for NatsClientConfig class."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        config = NatsClientConfig()
        
        assert config.nats_url == "nats://localhost:4222"
        assert config.max_reconnect_attempts == 5
        assert config.reconnect_time_wait == 2.0
        assert config.connection_timeout == 30.0
        assert config.ping_interval == 30.0
        assert config.max_outstanding_pings == 3
        assert config.client_name == "naq_client"
        assert config.connection_options == {}

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        config = NatsClientConfig(
            nats_url="nats://custom:4223",
            max_reconnect_attempts=10,
            reconnect_time_wait=5.0,
            connection_timeout=60.0,
            ping_interval=60.0,
            max_outstanding_pings=5,
            client_name="custom_client",
            custom_option="value"
        )
        
        assert config.nats_url == "nats://custom:4223"
        assert config.max_reconnect_attempts == 10
        assert config.reconnect_time_wait == 5.0
        assert config.connection_timeout == 60.0
        assert config.ping_interval == 60.0
        assert config.max_outstanding_pings == 5
        assert config.client_name == "custom_client"
        assert config.connection_options == {"custom_option": "value"}

    @patch('naq.nats_client.get_config')
    def test_init_with_config_servers(self, mock_get_config):
        """Test initialization with config servers."""
        mock_config = MagicMock()
        mock_config.nats.servers = ["nats://config:4222"]
        mock_get_config.return_value = mock_config
        
        config = NatsClientConfig()
        
        assert config.nats_url == "nats://config:4222"
        mock_get_config.assert_called_once()


class TestNatsClient:
    """Test cases for NatsClient class."""

    @pytest.fixture
    def mock_nc(self):
        """Mock NATS client."""
        nc = AsyncMock(spec=NATSClient)
        nc.is_connected = True
        return nc

    @pytest.fixture
    def mock_js(self):
        """Mock JetStream context."""
        js = AsyncMock(spec=JetStreamContext)
        return js

    @pytest.fixture
    def mock_kv(self):
        """Mock KeyValue store."""
        kv = AsyncMock(spec=KeyValue)
        return kv

    @pytest.fixture
    def client_config(self):
        """Test client configuration."""
        return NatsClientConfig(nats_url="nats://test:4222")

    @pytest.fixture
    def client(self, client_config):
        """Test NATS client."""
        return NatsClient(config=client_config)

    def test_init(self, client, client_config):
        """Test client initialization."""
        assert client._config == client_config
        assert client._nc is None
        assert client._js is None
        assert client._is_connected is False

    def test_is_connected_property(self, client):
        """Test is_connected property."""
        # Not connected
        assert client.is_connected is False
        
        # Mock connected state
        client._is_connected = True
        client._nc = MagicMock()
        type(client._nc).is_connected = PropertyMock(return_value=True)
        assert client.is_connected is True
        
        # Mock disconnected state
        type(client._nc).is_connected = PropertyMock(return_value=False)
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_connect_success(self, client, mock_nc, mock_js):
        """Test successful connection."""
        with patch('nats.connect', return_value=mock_nc) as mock_connect:
            mock_nc.jetstream.return_value = mock_js
            
            await client.connect()
            
            mock_connect.assert_called_once_with(
                servers=["nats://test:4222"],
                name="naq_client",
                max_reconnect_attempts=5,
                reconnect_time_wait=2.0,
                connect_timeout=30.0,
                ping_interval=30.0,
                max_outstanding_pings=3
            )
            
            assert client._nc == mock_nc
            assert client._js == mock_js
            assert client._is_connected is True

    @pytest.mark.asyncio
    async def test_connect_already_connected(self, client, mock_nc, mock_js):
        """Test connection when already connected."""
        client._is_connected = True
        client._nc = mock_nc
        client._js = mock_js
        
        with patch('nats.connect') as mock_connect:
            await client.connect()
            
            mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_failure(self, client):
        """Test connection failure."""
        with patch('nats.connect', side_effect=Exception("Connection failed")):
            with pytest.raises(NaqConnectionError, match="Failed to connect to NATS"):
                await client.connect()
            
            assert client._is_connected is False
            assert client._nc is None
            assert client._js is None

    @pytest.mark.asyncio
    async def test_disconnect_success(self, client, mock_nc):
        """Test successful disconnection."""
        client._is_connected = True
        client._nc = mock_nc
        
        await client.disconnect()
        
        mock_nc.close.assert_called_once()
        assert client._is_connected is False
        assert client._nc is None
        assert client._js is None

    @pytest.mark.asyncio
    async def test_disconnect_not_connected(self, client):
        """Test disconnection when not connected."""
        with patch.object(client, '_nc') as mock_nc:
            await client.disconnect()
            
            mock_nc.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_disconnect_failure(self, client, mock_nc):
        """Test disconnection failure."""
        client._is_connected = True
        client._nc = mock_nc
        mock_nc.close.side_effect = Exception("Disconnect failed")
        
        with pytest.raises(NaqException, match="Error disconnecting from NATS"):
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_connection_context_manager(self, client, mock_nc):
        """Test connection context manager."""
        client._is_connected = True
        client._nc = mock_nc
        
        async with client.connection() as nc:
            assert nc == mock_nc

    @pytest.mark.asyncio
    async def test_connection_context_manager_connects(self, client, mock_nc, mock_js):
        """Test connection context manager connects when not connected."""
        with patch('nats.connect', return_value=mock_nc) as mock_connect:
            mock_nc.jetstream.return_value = mock_js
            
            async with client.connection() as nc:
                assert nc == mock_nc
            
            mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_jetstream_context_manager(self, client, mock_js):
        """Test JetStream context manager."""
        client._is_connected = True
        client._js = mock_js
        
        async with client.jetstream() as js:
            assert js == mock_js

    @pytest.mark.asyncio
    async def test_ensure_stream_exists(self, client, mock_js):
        """Test ensure_stream when stream exists."""
        client._is_connected = True
        client._js = mock_js
        
        await client.ensure_stream("test_stream", ["test.subject"])
        
        mock_js.stream_info.assert_called_once_with("test_stream")
        mock_js.add_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_stream_create(self, client, mock_js):
        """Test ensure_stream when stream doesn't exist."""
        client._is_connected = True
        client._js = mock_js
        mock_js.stream_info.side_effect = Exception("Stream not found")
        
        await client.ensure_stream("test_stream", ["test.subject"])
        
        mock_js.stream_info.assert_called_once_with("test_stream")
        mock_js.add_stream.assert_called_once()
        
        # Verify stream config
        call_args = mock_js.add_stream.call_args[0][0]
        assert isinstance(call_args, StreamConfig)
        assert call_args.name == "test_stream"
        assert call_args.subjects == ["test.subject"]

    @pytest.mark.asyncio
    async def test_publish_success(self, client, mock_nc):
        """Test successful message publish."""
        client._is_connected = True
        client._nc = mock_nc
        mock_nc.publish.return_value = "test_message_id"
        
        result = await client.publish("test.subject", b"test_payload")
        
        assert result == "test_message_id"
        mock_nc.publish.assert_called_once_with("test.subject", b"test_payload")

    @pytest.mark.asyncio
    async def test_publish_failure(self, client, mock_nc):
        """Test message publish failure."""
        client._is_connected = True
        client._nc = mock_nc
        mock_nc.publish.side_effect = Exception("Publish failed")
        
        with pytest.raises(NaqException, match="Failed to publish message to test.subject"):
            await client.publish("test.subject", b"test_payload")

    @pytest.mark.asyncio
    async def test_jetstream_publish_success(self, client, mock_js):
        """Test successful JetStream message publish."""
        client._is_connected = True
        client._js = mock_js
        
        mock_ack = MagicMock()
        mock_ack.seq = 123
        mock_js.publish.return_value = mock_ack
        
        result = await client.jetstream_publish("test.subject", b"test_payload")
        
        assert result == "123"
        mock_js.publish.assert_called_once_with("test.subject", b"test_payload")

    @pytest.mark.asyncio
    async def test_jetstream_publish_failure(self, client, mock_js):
        """Test JetStream message publish failure."""
        client._is_connected = True
        client._js = mock_js
        mock_js.publish.side_effect = Exception("Publish failed")
        
        with pytest.raises(NaqException, match="Failed to publish JetStream message to test.subject"):
            await client.jetstream_publish("test.subject", b"test_payload")

    @pytest.mark.asyncio
    async def test_subscribe_success(self, client, mock_nc):
        """Test successful subscription."""
        client._is_connected = True
        client._nc = mock_nc
        mock_subscription = AsyncMock()
        mock_nc.subscribe.return_value = mock_subscription
        
        result = await client.subscribe("test.subject")
        
        assert result == mock_subscription
        mock_nc.subscribe.assert_called_once_with("test.subject")

    @pytest.mark.asyncio
    async def test_subscribe_with_queue_group(self, client, mock_nc):
        """Test subscription with queue group."""
        client._is_connected = True
        client._nc = mock_nc
        mock_subscription = AsyncMock()
        mock_nc.subscribe.return_value = mock_subscription
        
        result = await client.subscribe("test.subject", queue_group="test_queue")
        
        assert result == mock_subscription
        mock_nc.subscribe.assert_called_once_with("test.subject", queue="test_queue")

    @pytest.mark.asyncio
    async def test_pull_subscribe_success(self, client, mock_js):
        """Test successful pull subscription."""
        client._is_connected = True
        client._js = mock_js
        mock_subscription = AsyncMock()
        mock_js.pull_subscribe.return_value = mock_subscription
        
        result = await client.pull_subscribe("test.subject", "test_durable")
        
        assert result == mock_subscription
        mock_js.pull_subscribe.assert_called_once_with("test.subject", durable="test_durable")

    @pytest.mark.asyncio
    async def test_fetch_messages_success(self, client):
        """Test successful message fetching."""
        mock_subscription = AsyncMock()
        mock_messages = [AsyncMock(), AsyncMock()]
        mock_subscription.fetch.return_value = mock_messages
        
        result = await client.fetch_messages(mock_subscription, batch_size=2, timeout=2.0)
        
        assert result == mock_messages
        mock_subscription.fetch.assert_called_once_with(batch=2, timeout=2.0)

    @pytest.mark.asyncio
    async def test_fetch_messages_timeout(self, client):
        """Test message fetching with timeout."""
        mock_subscription = AsyncMock()
        mock_subscription.fetch.side_effect = asyncio.TimeoutError()
        
        result = await client.fetch_messages(mock_subscription, batch_size=1, timeout=1.0)
        
        assert result == []

    @pytest.mark.asyncio
    async def test_purge_stream_success(self, client, mock_js):
        """Test successful stream purge."""
        client._is_connected = True
        client._js = mock_js
        
        await client.purge_stream("test_stream")
        
        mock_js.purge_stream.assert_called_once_with("test_stream")

    @pytest.mark.asyncio
    async def test_purge_stream_with_subject(self, client, mock_js):
        """Test stream purge with subject filter."""
        client._is_connected = True
        client._js = mock_js
        
        await client.purge_stream("test_stream", subject="test.subject")
        
        mock_js.purge_stream.assert_called_once_with("test_stream", subject="test.subject")

    @pytest.mark.asyncio
    async def test_get_kv_success(self, client, mock_js, mock_kv):
        """Test successful KV store retrieval."""
        client._is_connected = True
        client._js = mock_js
        mock_js.key_value.return_value = mock_kv
        
        result = await client.get_kv("test_bucket")
        
        assert result == mock_kv
        mock_js.key_value.assert_called_once_with("test_bucket")

    @pytest.mark.asyncio
    async def test_create_kv_success(self, client, mock_js, mock_kv):
        """Test successful KV store creation."""
        client._is_connected = True
        client._js = mock_js
        mock_js.create_key_value.return_value = mock_kv
        
        result = await client.create_kv("test_bucket")
        
        assert result == mock_kv
        mock_js.create_key_value.assert_called_once_with(bucket="test_bucket")

    @pytest.mark.asyncio
    async def test_delete_kv_success(self, client, mock_js):
        """Test successful KV store deletion."""
        client._is_connected = True
        client._js = mock_js
        
        await client.delete_kv("test_bucket")
        
        mock_js.delete_key_value.assert_called_once_with("test_bucket")

    @pytest.mark.asyncio
    async def test_trigger_due_jobs_no_jobs(self, client, mock_js, mock_kv):
        """Test trigger_due_jobs with no scheduled jobs."""
        client._is_connected = True
        client._js = mock_js
        mock_js.key_value.return_value = mock_kv
        mock_kv.keys.return_value = []
        
        with patch('naq.nats_client.time.time', return_value=1000):
            processed, errors = await client.trigger_due_jobs()
            
            assert processed == 0
            assert errors == 0
            mock_kv.keys.assert_called_once()
            mock_kv.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_trigger_due_jobs_with_jobs(self, client, mock_js, mock_kv):
        """Test trigger_due_jobs with scheduled jobs."""
        client._is_connected = True
        client._js = mock_js
        mock_js.key_value.return_value = mock_kv
        mock_kv.keys.return_value = ["job1", "job2"]
        
        # Mock job entries
        mock_entry1 = MagicMock()
        mock_entry1.value = b'{"scheduled_time": 999, "subject": "test.subject", "payload": "dGVzdA=="}'
        mock_entry2 = MagicMock()
        mock_entry2.value = b'{"scheduled_time": 1001, "subject": "future.subject", "payload": "dGVzdA=="}'
        
        mock_kv.get.side_effect = [mock_entry1, mock_entry2]
        
        with patch('naq.nats_client.time.time', return_value=1000):
            with patch.object(client, 'jetstream_publish', return_value="msg_id") as mock_publish:
                processed, errors = await client.trigger_due_jobs()
                
                assert processed == 1
                assert errors == 0
                mock_publish.assert_called_once_with("test.subject", b"test")
                mock_kv.delete.assert_called_once_with("job1")

    @pytest.mark.asyncio
    async def test_context_manager(self, client, mock_nc, mock_js):
        """Test async context manager."""
        with patch('nats.connect', return_value=mock_nc) as mock_connect:
            mock_nc.jetstream.return_value = mock_js
            
            async with client as c:
                assert c == client
                assert client._is_connected is True
            
            mock_connect.assert_called_once()
            mock_nc.close.assert_called_once()

    def test_repr(self, client):
        """Test string representation."""
        client._config.nats_url = "nats://test:4222"
        client._is_connected = True
        
        result = repr(client)
        
        assert "NatsClient" in result
        assert "nats://test:4222" in result
        assert "connected=True" in result

    @pytest.mark.asyncio
    async def test_validation_errors(self, client):
        """Test parameter validation errors."""
        # Test invalid stream name
        with pytest.raises(ValueError, match="stream_name must be a string"):
            await client.ensure_stream(123, ["test.subject"])
        
        # Test invalid subjects
        with pytest.raises(ValueError, match="subjects must be a list"):
            await client.ensure_stream("test_stream", "not_a_list")
        
        # Test invalid subject
        with pytest.raises(ValueError, match="subject must be a string"):
            await client.publish(123, b"payload")
        
        # Test invalid payload
        with pytest.raises(ValueError, match="payload must be bytes"):
            await client.publish("test.subject", "not_bytes")
        
        # Test invalid durable name
        with pytest.raises(ValueError, match="durable_name must be a string"):
            await client.pull_subscribe("test.subject", 123)
        
        # Test invalid batch size
        with pytest.raises(ValueError, match="batch_size must be an integer"):
            await client.fetch_messages(AsyncMock(), batch_size="not_int")
        
        # Test invalid timeout
        with pytest.raises(ValueError, match="timeout must be a number"):
            await client.fetch_messages(AsyncMock(), timeout="not_float")