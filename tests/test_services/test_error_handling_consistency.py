"""Tests for error handling consistency in connection management."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.exceptions import NaqConnectionError, NaqException
from naq.services.connection import ConnectionService, ConnectionServiceConfig
from naq.connection.manager import ConnectionManager
from naq.services.base import ServiceInitializationError, ServiceRuntimeError


class TestErrorHandlingConsistency:
    """Test class for error handling consistency."""

    @pytest.mark.asyncio
    async def test_connection_service_initialization_error(self):
        """Test that ConnectionService handles initialization errors consistently."""
        # Create a service with an invalid URL
        service = ConnectionService()
        
        # Mock the connection manager to raise an exception
        with patch.object(ConnectionManager, 'get_connection', side_effect=Exception("Connection failed")):
            with pytest.raises(ServiceInitializationError) as exc_info:
                await service.initialize()
            
            # Verify the error is properly wrapped
            assert "Failed to initialize service" in str(exc_info.value)
            # The cause should be a ServiceInitializationError from ConnectionService
            assert isinstance(exc_info.value.__cause__, ServiceInitializationError)
            assert "Failed to initialize ConnectionService" in str(exc_info.value.__cause__)
            # The original cause should be a NaqConnectionError
            assert isinstance(exc_info.value.__cause__.__cause__, NaqConnectionError)
            assert "Failed to get NATS connection" in str(exc_info.value.__cause__.__cause__)

    @pytest.mark.asyncio
    async def test_connection_service_get_connection_error(self):
        """Test that ConnectionService handles connection errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection manager to raise an exception
        with patch.object(ConnectionManager, 'get_connection', side_effect=Exception("Connection failed")):
            # Clear the cached connection to force a new connection attempt
            service._connections.clear()
            
            with pytest.raises(NaqConnectionError) as exc_info:
                await service.get_connection()
            
            # Verify the error is properly wrapped
            assert "Failed to get NATS connection" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, Exception)
            assert "Connection failed" in str(exc_info.value.__cause__)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_get_jetstream_error(self):
        """Test that ConnectionService handles JetStream errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection manager to raise an exception
        with patch.object(ConnectionManager, 'get_jetstream', side_effect=Exception("JetStream failed")):
            with pytest.raises(NaqConnectionError) as exc_info:
                await service.get_jetstream()
            
            # Verify the error is properly wrapped
            assert "Failed to get JetStream context" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, Exception)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_reconnection_error(self):
        """Test that ConnectionService handles reconnection errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection manager to raise an exception during reconnection
        with patch('nats.connect', side_effect=Exception("Reconnection failed")):
            with pytest.raises(NaqConnectionError) as exc_info:
                await service._reconnect("nats://localhost:4222", MagicMock())
            
            # Verify the error is properly wrapped
            assert "Failed to reconnect" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, Exception)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_close_connection_error(self):
        """Test that ConnectionService handles close connection errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection manager to raise an exception
        with patch.object(ConnectionManager, 'close_connection', side_effect=Exception("Close failed")):
            with pytest.raises(NaqConnectionError) as exc_info:
                await service.close_connection()
            
            # Verify the error is properly wrapped
            assert "Failed to close connection" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, Exception)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_cleanup_error(self):
        """Test that ConnectionService handles cleanup errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection manager to raise an exception during cleanup
        with patch.object(ConnectionManager, 'close_all', side_effect=Exception("Cleanup failed")):
            with pytest.raises(ServiceRuntimeError) as exc_info:
                await service.cleanup()
            
            # Verify the error is properly wrapped
            assert "Failed to cleanup service" in str(exc_info.value)
            # The cause should be a ServiceRuntimeError from ConnectionService
            assert isinstance(exc_info.value.__cause__, ServiceRuntimeError)
            assert "Failed to cleanup ConnectionService" in str(exc_info.value.__cause__)
            # The original cause should be the Exception we raised
            assert isinstance(exc_info.value.__cause__.__cause__, Exception)
            assert "Cleanup failed" in str(exc_info.value.__cause__.__cause__)

    @pytest.mark.asyncio
    async def test_connection_service_connection_scope_error(self):
        """Test that ConnectionService handles connection scope errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the get_connection method to raise an exception
        with patch.object(service, 'get_connection', side_effect=Exception("Scope failed")):
            with pytest.raises(NaqConnectionError) as exc_info:
                async with service.connection_scope():
                    pass
            
            # Verify the error is properly wrapped
            assert "Error in connection scope" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, Exception)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_monitor_error(self):
        """Test that ConnectionService handles monitor errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the connection to raise an exception during monitoring
        mock_connection = MagicMock()
        mock_connection.is_connected = False
        mock_connection.close = AsyncMock(side_effect=Exception("Monitor failed"))
        
        with patch('nats.connect', return_value=mock_connection):
            # This should not raise an exception, but log the error
            await service._reconnect("nats://localhost:4222", mock_connection)
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_config_validation(self):
        """Test that ConnectionService validates configuration consistently."""
        # Test with invalid configuration
        config = ConnectionServiceConfig(
            max_reconnect_attempts=-1,  # Invalid value
            reconnect_time_wait=-1.0   # Invalid value
        )
        
        service = ConnectionService(config)
        
        # The service should still initialize but use default values
        await service.initialize()
        assert service.connection_config.max_reconnect_attempts > 0
        assert service.connection_config.reconnect_time_wait > 0
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_error_logging(self):
        """Test that ConnectionService logs errors consistently."""
        service = ConnectionService()
        await service.initialize()
        
        # Mock the logger to capture log messages
        with patch.object(service, '_logger') as mock_logger:
            # Mock the connection manager to raise an exception
            with patch.object(ConnectionManager, 'get_connection', side_effect=Exception("Test error")):
                # Clear the cached connection to force a new connection attempt
                service._connections.clear()
                
                with pytest.raises(NaqConnectionError):
                    await service.get_connection()
                
                # Verify error was logged
                mock_logger.error.assert_called_once()
                assert "Failed to get NATS connection" in mock_logger.error.call_args[0][0]
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_error_chaining(self):
        """Test that ConnectionService preserves error chains."""
        service = ConnectionService()
        await service.initialize()
        
        # Create a nested exception chain
        original_error = ValueError("Original error")
        intermediate_error = RuntimeError("Intermediate error")
        intermediate_error.__cause__ = original_error
        
        # Mock the connection manager to raise the nested exception
        with patch.object(ConnectionManager, 'get_connection', side_effect=intermediate_error):
            # Clear the cached connection to force a new connection attempt
            service._connections.clear()
            
            with pytest.raises(NaqConnectionError) as exc_info:
                await service.get_connection()
            
            # Verify the entire chain is preserved
            assert exc_info.value.__cause__ is intermediate_error
            assert exc_info.value.__cause__.__cause__ is original_error
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_service_custom_error_messages(self):
        """Test that ConnectionService provides meaningful error messages."""
        service = ConnectionService()
        await service.initialize()
        
        # Test different error scenarios
        test_cases = [
            (ConnectionManager.get_connection, "get_connection", "Failed to get NATS connection"),
            (ConnectionManager.get_jetstream, "get_jetstream", "Failed to get JetStream context"),
            (ConnectionManager.close_connection, "close_connection", "Failed to close connection"),
        ]
        
        for method, method_name, expected_message in test_cases:
            with patch.object(ConnectionManager, method_name, side_effect=Exception("Test error")):
                with pytest.raises(NaqConnectionError) as exc_info:
                    if method_name == "get_connection":
                        # Clear the cached connection to force a new connection attempt
                        service._connections.clear()
                        await service.get_connection()
                    elif method_name == "get_jetstream":
                        # Clear the cached jetstream context to force a new context attempt
                        service._jetstream_contexts.clear()
                        await service.get_jetstream()
                    elif method_name == "close_connection":
                        # Get a connection first
                        nc = await service.get_connection()
                        await service.close_connection(nc)
                
                assert expected_message in str(exc_info.value)
        
        await service.cleanup()