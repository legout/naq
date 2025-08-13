# tests/test_connection_utils/test_connection_utilities.py
"""Tests for connection utility modules."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import asynccontextmanager

from naq.connection_utils.context_managers import (
    nats_connection_context,
    jetstream_context,
    transaction_context
)
from naq.connection_utils.decorators import (
    with_nats_connection,
    with_jetstream,
    ensure_connected
)
from naq.connection_utils.utils import (
    get_connection_info,
    test_connection,
    close_all_connections,
    get_connection_stats
)


class TestConnectionContextManagers:
    """Test connection context managers."""

    @pytest.mark.asyncio
    async def test_nats_connection_context_success(self):
        """Test NATS connection context manager with successful connection."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            async with nats_connection_context() as nc:
                assert nc == mock_nc
                # Connection should be active during context
                assert nc is not None
            
            # Connection cleanup should be handled
            mock_nc.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_nats_connection_context_with_url(self):
        """Test NATS connection context with custom URL."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            async with nats_connection_context(url="nats://custom:4222") as nc:
                assert nc == mock_nc
            
            mock_get_conn.assert_called_with(url="nats://custom:4222")

    @pytest.mark.asyncio
    async def test_jetstream_context_success(self):
        """Test JetStream context manager."""
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_nc.jetstream.return_value = mock_js
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            async with jetstream_context() as js:
                assert js == mock_js
                mock_nc.jetstream.assert_called_once()

    @pytest.mark.asyncio
    async def test_transaction_context_success(self):
        """Test transaction context manager."""
        mock_operations = []
        
        async def mock_operation(name):
            mock_operations.append(f"start_{name}")
            yield name
            mock_operations.append(f"commit_{name}")
        
        async with transaction_context():
            result = []
            async for value in mock_operation("test"):
                result.append(value)
        
        assert "test" in result

    @pytest.mark.asyncio
    async def test_transaction_context_with_error(self):
        """Test transaction context with error handling."""
        with pytest.raises(ValueError):
            async with transaction_context():
                raise ValueError("Test transaction error")


class TestConnectionDecorators:
    """Test connection decorators."""

    @pytest.mark.asyncio
    async def test_with_nats_connection_decorator(self):
        """Test with_nats_connection decorator."""
        mock_nc = AsyncMock()
        
        @with_nats_connection
        async def test_function(nc):
            return f"Connected: {nc is not None}"
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            result = await test_function()
            assert result == "Connected: True"
            mock_get_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_with_nats_connection_custom_url(self):
        """Test with_nats_connection decorator with custom URL."""
        mock_nc = AsyncMock()
        
        @with_nats_connection(url="nats://custom:4222")
        async def test_function(nc):
            return nc
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            result = await test_function()
            assert result == mock_nc
            mock_get_conn.assert_called_with(url="nats://custom:4222")

    @pytest.mark.asyncio
    async def test_with_jetstream_decorator(self):
        """Test with_jetstream decorator."""
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_nc.jetstream.return_value = mock_js
        
        @with_jetstream
        async def test_function(js):
            return f"JetStream: {js is not None}"
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            result = await test_function()
            assert result == "JetStream: True"

    @pytest.mark.asyncio
    async def test_ensure_connected_decorator(self):
        """Test ensure_connected decorator."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = True
        
        @ensure_connected
        async def test_function(connection):
            return connection.is_connected
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            result = await test_function(mock_nc)
            assert result is True

    @pytest.mark.asyncio
    async def test_ensure_connected_reconnect(self):
        """Test ensure_connected decorator with reconnection."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = False
        
        @ensure_connected
        async def test_function(connection):
            return "function executed"
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            # Should attempt reconnection
            result = await test_function(mock_nc)
            assert result == "function executed"


class TestConnectionUtils:
    """Test connection utility functions."""

    @pytest.mark.asyncio
    async def test_get_connection_info(self):
        """Test getting connection information."""
        mock_nc = AsyncMock()
        mock_nc.connected_url = "nats://localhost:4222"
        mock_nc.client_id = 123
        mock_nc.is_connected = True
        
        info = await get_connection_info(mock_nc)
        
        assert info["url"] == "nats://localhost:4222"
        assert info["client_id"] == 123
        assert info["connected"] is True

    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test connection testing with successful connection."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = True
        
        with patch('naq.connection_utils.utils.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            result = await test_connection("nats://localhost:4222")
            assert result["success"] is True
            assert result["connected"] is True

    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test connection testing with failed connection."""
        with patch('naq.connection_utils.utils.get_nats_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")
            
            result = await test_connection("nats://localhost:4222")
            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_close_all_connections(self):
        """Test closing all connections."""
        mock_nc1 = AsyncMock()
        mock_nc2 = AsyncMock()
        
        # Mock the connection registry/cache
        with patch('naq.connection_utils.utils._connection_cache', {"url1": mock_nc1, "url2": mock_nc2}):
            closed_count = await close_all_connections()
            
            assert closed_count == 2
            mock_nc1.close.assert_called_once()
            mock_nc2.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_stats(self):
        """Test getting connection statistics."""
        mock_nc = AsyncMock()
        mock_nc.stats = {
            "in_msgs": 100,
            "out_msgs": 50,
            "in_bytes": 10000,
            "out_bytes": 5000
        }
        
        stats = await get_connection_stats(mock_nc)
        
        assert stats["in_msgs"] == 100
        assert stats["out_msgs"] == 50
        assert stats["in_bytes"] == 10000
        assert stats["out_bytes"] == 5000


class TestConnectionLifecycle:
    """Test connection lifecycle management."""

    @pytest.mark.asyncio
    async def test_connection_pooling(self):
        """Test connection pooling behavior."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.utils.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            # Multiple calls should reuse connection
            async with nats_connection_context() as nc1:
                async with nats_connection_context() as nc2:
                    # Should be the same connection instance
                    assert nc1 == nc2

    @pytest.mark.asyncio
    async def test_connection_error_recovery(self):
        """Test connection error recovery."""
        mock_nc = AsyncMock()
        
        @with_nats_connection
        async def test_operation(nc):
            if not hasattr(test_operation, 'call_count'):
                test_operation.call_count = 0
            test_operation.call_count += 1
            
            if test_operation.call_count == 1:
                raise ConnectionError("First attempt fails")
            return "success"
        
        with patch('naq.connection_utils.decorators.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            # Should handle connection errors gracefully
            with pytest.raises(ConnectionError):
                await test_operation()

    @pytest.mark.asyncio
    async def test_concurrent_connections(self):
        """Test concurrent connection handling."""
        mock_nc = AsyncMock()
        
        async def concurrent_operation(operation_id):
            async with nats_connection_context() as nc:
                await asyncio.sleep(0.01)  # Simulate work
                return f"operation_{operation_id}_completed"
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            # Run multiple operations concurrently
            tasks = [concurrent_operation(i) for i in range(5)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 5
            assert all("completed" in result for result in results)


class TestConnectionConfiguration:
    """Test connection configuration handling."""

    @pytest.mark.asyncio
    async def test_connection_with_auth(self):
        """Test connection with authentication."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            async with nats_connection_context(
                url="nats://user:pass@localhost:4222"
            ) as nc:
                assert nc == mock_nc
            
            mock_get_conn.assert_called_with(url="nats://user:pass@localhost:4222")

    @pytest.mark.asyncio
    async def test_connection_with_tls(self):
        """Test connection with TLS configuration."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.context_managers.get_nats_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_nc
            
            tls_config = {
                "cert_file": "/path/to/cert.pem",
                "key_file": "/path/to/key.pem"
            }
            
            async with nats_connection_context(
                url="nats://localhost:4222",
                **tls_config
            ) as nc:
                assert nc == mock_nc

    @pytest.mark.asyncio
    async def test_connection_retry_logic(self):
        """Test connection retry logic."""
        mock_nc = AsyncMock()
        
        with patch('naq.connection_utils.utils.get_nats_connection') as mock_get_conn:
            # First two attempts fail, third succeeds
            mock_get_conn.side_effect = [
                ConnectionError("Attempt 1 failed"),
                ConnectionError("Attempt 2 failed"),
                mock_nc
            ]
            
            # Should eventually succeed after retries
            result = await test_connection("nats://localhost:4222", max_retries=3)
            assert result["success"] is True


class TestConnectionMonitoring:
    """Test connection monitoring utilities."""

    @pytest.mark.asyncio
    async def test_connection_health_check(self):
        """Test connection health checking."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = True
        mock_nc.stats = {"in_msgs": 10, "out_msgs": 5}
        
        health = await get_connection_info(mock_nc)
        
        assert health["connected"] is True
        assert "stats" in health or health.get("in_msgs") == 10

    @pytest.mark.asyncio
    async def test_connection_metrics_collection(self):
        """Test connection metrics collection."""
        mock_nc = AsyncMock()
        mock_nc.stats = {
            "in_msgs": 100,
            "out_msgs": 50,
            "reconnects": 2,
            "errors": 1
        }
        
        metrics = await get_connection_stats(mock_nc)
        
        assert metrics["in_msgs"] == 100
        assert metrics["out_msgs"] == 50
        assert metrics["reconnects"] == 2
        assert metrics["errors"] == 1

    @pytest.mark.asyncio
    async def test_connection_diagnostics(self):
        """Test connection diagnostics."""
        mock_nc = AsyncMock()
        mock_nc.is_connected = False
        mock_nc.last_error = "Connection timeout"
        
        diagnostics = await get_connection_info(mock_nc)
        
        assert diagnostics["connected"] is False
        # Should include diagnostic information