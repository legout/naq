"""Tests for connection monitoring health."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from naq.services.connection import ConnectionService, ConnectionServiceConfig
from naq.connection.manager import ConnectionManager


class TestConnectionMonitoringHealth:
    """Test class for connection monitoring health."""

    @pytest.mark.asyncio
    async def test_connection_monitoring_task_creation(self):
        """Test that connection monitoring tasks are created correctly."""
        service = ConnectionService()
        await service.initialize()
        
        # Verify that a monitoring task is created for the connection
        assert len(service._reconnect_tasks) == 1
        assert "nats://localhost:4222" in service._reconnect_tasks
        
        # Verify the task is running
        task = service._reconnect_tasks["nats://localhost:4222"]
        assert not task.done()
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_task_cancellation(self):
        """Test that connection monitoring tasks are cancelled on cleanup."""
        service = ConnectionService()
        await service.initialize()
        
        # Get the monitoring task
        task = service._reconnect_tasks["nats://localhost:4222"]
        
        # Cleanup the service
        await service.cleanup()
        
        # Verify the task was cancelled
        assert task.cancelled()
        assert len(service._reconnect_tasks) == 0

    @pytest.mark.asyncio
    async def test_connection_monitoring_handles_disconnections(self):
        """Test that connection monitoring handles disconnections correctly."""
        service = ConnectionService()
        await service.initialize()
        
        # Get the connection and monitoring task
        nc = await service.get_connection()
        task = service._reconnect_tasks["nats://localhost:4222"]
        
        # Mock the connection to simulate a disconnection
        with patch.object(type(nc), 'is_connected', new_callable=PropertyMock) as mock_is_connected:
            # Initially connected
            mock_is_connected.return_value = True
            
            # Wait a bit to ensure monitoring is stable
            await asyncio.sleep(0.1)
            
            # Simulate disconnection
            mock_is_connected.return_value = False
            
            # Wait for monitoring to detect disconnection
            await asyncio.sleep(0.1)

            # Verify monitoring task is still running
            assert not task.done()

            # Verify connection is marked as disconnected
            assert not nc.is_connected

            # Restore connection
            mock_is_connected.return_value = True
            
            # Wait for monitoring to detect reconnection
            await asyncio.sleep(0.1)

            # Verify connection is marked as connected
            assert nc.is_connected
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_reconnects(self):
        """Test that connection monitoring attempts reconnections."""
        # Create a service with custom configuration for faster testing
        config = ConnectionServiceConfig(
            ping_interval=0.1,  # Very short interval for testing
            max_reconnect_attempts=1
        )
        service = ConnectionService(config)
        await service.initialize()
        
        # Get the connection
        nc = await service.get_connection()
        
        # Mock the _reconnect method to track reconnection attempts
        with patch.object(service, '_reconnect') as mock_reconnect:
            mock_reconnect.return_value = None
            
            # Mock the connection to simulate a disconnection
            with patch.object(type(nc), 'is_connected', new_callable=PropertyMock) as mock_is_connected:
                # Initially connected
                mock_is_connected.return_value = True
                
                # Wait a bit to ensure monitoring is stable
                await asyncio.sleep(0.2)
                
                # Simulate disconnection
                mock_is_connected.return_value = False
                
                # Wait for the monitor to attempt reconnection
                await asyncio.sleep(0.5)  # Wait for monitoring to detect and attempt reconnect
                
                # Verify reconnection was attempted
                assert mock_reconnect.called, "Reconnection was not attempted when connection was lost"
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_logging(self):
        """Test that connection monitoring logs appropriately."""
        # Create a service with custom configuration for faster testing
        config = ConnectionServiceConfig(
            ping_interval=0.1,  # Very short interval for testing
            max_reconnect_attempts=1
        )
        service = ConnectionService(config)
        await service.initialize()
        
        # Mock the logger to capture log messages
        with patch.object(service, '_logger') as mock_logger:
            # Get the connection
            nc = await service.get_connection()
            
            # Mock the connection to simulate a disconnection
            with patch.object(type(nc), 'is_connected', new_callable=PropertyMock) as mock_is_connected:
                # Initially connected
                mock_is_connected.return_value = True
                
                # Wait a bit to ensure monitoring is stable
                await asyncio.sleep(0.2)
                
                # Simulate disconnection
                mock_is_connected.return_value = False
                
                # Wait for the monitor to detect and log
                await asyncio.sleep(0.5)
                
                # Verify appropriate logging occurred
                assert mock_logger.debug.called or mock_logger.info.called or mock_logger.warning.called, "No logging occurred when connection was lost"
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_with_multiple_connections(self):
        """Test that connection monitoring works with multiple connections."""
        service = ConnectionService()
        await service.initialize()
        
        # Get connections to different URLs
        nc1 = await service.get_connection("nats://localhost:4222")
        
        # Mock the second connection to avoid trying to connect to a non-existent server
        with patch.object(ConnectionManager, 'get_connection') as mock_get_connection:
            # Create a mock connection for the second URL
            mock_nc2 = AsyncMock()
            mock_nc2.is_connected = True
            
            # Set up the mock to return our mock connection
            mock_get_connection.return_value = mock_nc2
            
            # Get the second connection
            nc2 = await service.get_connection("nats://localhost:5222")
        
        # Verify monitoring tasks exist for both connections
        assert len(service._reconnect_tasks) == 2
        assert "nats://localhost:4222" in service._reconnect_tasks
        assert "nats://localhost:5222" in service._reconnect_tasks
        
        # Verify both tasks are running
        assert not service._reconnect_tasks["nats://localhost:4222"].done()
        assert not service._reconnect_tasks["nats://localhost:5222"].done()
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_error_handling(self):
        """Test that connection monitoring handles errors gracefully."""
        service = ConnectionService()
        await service.initialize()
        
        # Get the connection
        nc = await service.get_connection()
        
        # Create a mock property that raises an exception
        with patch.object(type(nc), 'is_connected', new_callable=PropertyMock) as mock_is_connected:
            # Initially connected
            mock_is_connected.return_value = True
            
            # Wait a bit to ensure monitoring is stable
            await asyncio.sleep(0.1)
            
            # Simulate an error when checking connection status
            mock_is_connected.side_effect = Exception("Connection error")
            
            # Wait for the monitor to encounter the error
            await asyncio.sleep(0.1)
            
            # Verify the monitoring task is still running
            task = service._reconnect_tasks["nats://localhost:4222"]
            assert not task.done()
        
        await service.cleanup()

    @pytest.mark.asyncio
    async def test_connection_monitoring_configuration_respect(self):
        """Test that connection monitoring respects configuration parameters."""
        # Create a service with custom reconnection configuration
        config = ConnectionServiceConfig(
            max_reconnect_attempts=3,
            reconnect_time_wait=1.0
        )
        service = ConnectionService(config)
        await service.initialize()
        
        # Verify the configuration is used
        assert service._connection_config.max_reconnect_attempts == 3
        assert service._connection_config.reconnect_time_wait == 1.0
        
        await service.cleanup()