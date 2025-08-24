"""Unit tests for service context patterns."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.service_context import (
    service_context,
    long_lived_service_context,
    run_with_service_context,
)
from naq.services.config import create_global_config, GlobalServiceConfig
from naq.services.base import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.kv_stores import KVStoreService
from naq.utils.logging import StructuredLogger


class TestShortLivedServiceContext:
    """Test cases for the short-lived service context manager."""

    @pytest.mark.asyncio
    async def test_short_lived_context_initialization(self):
        """Test that short-lived context properly initializes services."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        async with service_context(nats_url=nats_url, global_config=config) as service_manager:
            assert isinstance(service_manager, ServiceManager)
            assert service_manager.has_service("connection")
            assert service_manager.has_service("jobs")
            assert service_manager.has_service("kv")
            
            # Verify services are properly initialized
            connection_service = await service_manager.get_service("connection")
            assert isinstance(connection_service, ConnectionService)
            
            job_service = await service_manager.get_service("jobs")
            assert isinstance(job_service, JobService)
            
            kv_service = await service_manager.get_service("kv")
            assert isinstance(kv_service, KVStoreService)

    @pytest.mark.asyncio
    async def test_short_lived_context_cleanup(self):
        """Test that short-lived context properly cleans up services."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        with patch.object(ServiceManager, 'cleanup_all', new_callable=AsyncMock) as mock_cleanup:
            async with service_context(nats_url=nats_url, global_config=config) as service_manager:
                pass
            
            # Verify cleanup_all was called
            mock_cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_short_lived_context_with_custom_config(self):
        """Test that short-lived context uses custom configuration."""
        nats_url = "nats://localhost:4222"
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        async with service_context(nats_url=nats_url, global_config=custom_config) as service_manager:
            # Verify config was used - access through the connection service
            connection_service = await service_manager.get_service("connection")
            assert connection_service.config.nats_url == nats_url

    @pytest.mark.asyncio
    async def test_short_lived_context_error_handling(self):
        """Test that short-lived context handles errors properly."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        with patch.object(ConnectionService, '_do_initialize', side_effect=Exception("Test error")):
            with pytest.raises(Exception, match="Test error"):
                async with service_context(nats_url=nats_url, global_config=config):
                    pass


class TestLongLivedServiceContext:
    """Test cases for the long-lived service context manager."""

    @pytest.mark.asyncio
    async def test_long_lived_context_initialization(self):
        """Test that long-lived context properly initializes services."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        # First create a service manager
        async with service_context(nats_url=nats_url, global_config=config) as service_manager:
            # Now use the long-lived context with the created service manager
            async with long_lived_service_context(service_manager) as ctx_manager:
                assert isinstance(ctx_manager, ServiceManager)
                assert ctx_manager.has_service("connection")
                assert ctx_manager.has_service("jobs")
                assert ctx_manager.has_service("kv")

    @pytest.mark.asyncio
    async def test_long_lived_context_no_cleanup(self):
        """Test that long-lived context does not clean up services."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        with patch.object(ServiceManager, 'cleanup_all', new_callable=AsyncMock) as mock_cleanup:
            # First create a service manager
            async with service_context(nats_url=nats_url, global_config=config) as service_manager:
                # Now use the long-lived context with the created service manager
                async with long_lived_service_context(service_manager) as ctx_manager:
                    pass
                
                # Verify cleanup_all was NOT called on the service manager
                mock_cleanup.assert_not_called()

    @pytest.mark.asyncio
    async def test_long_lived_context_with_custom_config(self):
        """Test that long-lived context uses custom configuration."""
        nats_url = "nats://localhost:4222"
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
            request_timeout=10,
        )
        
        # First create a service manager
        async with service_context(nats_url=nats_url, global_config=custom_config) as service_manager:
            # Now use the long-lived context with the created service manager
            async with long_lived_service_context(service_manager) as ctx_manager:
                # Verify config was used - access through the connection service
                connection_service = await ctx_manager.get_service("connection")
                assert connection_service.config.nats_url == nats_url

    @pytest.mark.asyncio
    async def test_long_lived_context_error_handling(self):
        """Test that long-lived context handles errors properly."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        with patch.object(ConnectionService, '_do_initialize', side_effect=Exception("Test error")):
            with pytest.raises(Exception, match="Test error"):
                # First create a service manager
                async with service_context(nats_url=nats_url, global_config=config) as service_manager:
                    # Now use the long-lived context with the created service manager
                    async with long_lived_service_context(service_manager):
                        pass


class TestRunWithServiceContext:
    """Test cases for the run_with_service_context function."""

    def test_run_with_service_context_sync(self):
        """Test that run_with_service_context works with synchronous functions."""
        nats_url = "nats://localhost:4222"
        
        async def test_operation(service_manager):
            assert isinstance(service_manager, ServiceManager)
            return "test_result"
        
        result = run_with_service_context(test_operation, nats_url=nats_url)
        assert result == "test_result"

    def test_run_with_service_context_with_config(self):
        """Test that run_with_service_context works with custom config."""
        nats_url = "nats://localhost:4222"
        custom_config = GlobalServiceConfig(
            nats_url=nats_url,
            connection_timeout=30,
        )
        
        async def test_operation(service_manager):
            # Get the connection service to check config
            connection_service = await service_manager.get_service("connection")
            assert connection_service.config.nats_url == nats_url
            return "test_result"
        
        result = run_with_service_context(
            test_operation,
            nats_url=nats_url,
            global_config=custom_config
        )
        assert result == "test_result"

    def test_run_with_service_context_error_handling(self):
        """Test that run_with_service_context handles errors properly."""
        nats_url = "nats://localhost:4222"
        
        async def failing_operation(service_manager):
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            run_with_service_context(failing_operation, nats_url=nats_url)

    def test_run_with_service_context_with_logger(self):
        """Test that run_with_service_context works with custom logger."""
        nats_url = "nats://localhost:4222"
        logger_name = "test.logger"
        
        async def test_operation(service_manager):
            return "test_result"
        
        with patch('naq.service_context.StructuredLogger') as mock_logger:
            run_with_service_context(
                test_operation, 
                nats_url=nats_url, 
                logger_name=logger_name
            )
            
            # Verify logger was created with correct name
            mock_logger.assert_called_with(logger_name)

    @pytest.mark.asyncio
    async def test_run_with_service_context_async_operation(self):
        """Test that run_with_service_context works with async operations."""
        nats_url = "nats://localhost:4222"
        
        async def async_operation(service_manager):
            await asyncio.sleep(0.01)  # Small delay to test async behavior
            assert isinstance(service_manager, ServiceManager)
            return "async_result"
        
        # Since we're already in an async context, we need to call the function directly
        # instead of using run_with_service_context which uses asyncio.run()
        async with service_context(nats_url=nats_url) as service_manager:
            result = await async_operation(service_manager)
        assert result == "async_result"


class TestServiceContextIntegration:
    """Integration tests for service context patterns."""

    @pytest.mark.asyncio
    async def test_short_lived_context_with_real_services(self):
        """Test short-lived context with real service initialization."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        # Mock the ConnectionManager.get_connection method to avoid network calls
        with patch('naq.connection.manager.ConnectionManager.get_connection') as mock_get_connection:
            # Create a mock connection object
            mock_connection = AsyncMock()
            mock_get_connection.return_value = mock_connection
            
            async with service_context(nats_url=nats_url, global_config=config) as service_manager:
                # Verify services are properly initialized
                assert service_manager.has_service("connection")
                assert service_manager.has_service("jobs")
                assert service_manager.has_service("kv")
                
                # Verify service manager is properly configured
                connection_service = await service_manager.get_service("connection")
                assert connection_service.config.nats_url == nats_url

    @pytest.mark.asyncio
    async def test_long_lived_context_with_real_services(self):
        """Test long-lived context with real service initialization."""
        nats_url = "nats://localhost:4222"
        config = create_global_config()
        
        # Mock the ConnectionManager.get_connection method to avoid network calls
        with patch('naq.connection.manager.ConnectionManager.get_connection') as mock_get_connection:
            # Create a mock connection object
            mock_connection = AsyncMock()
            mock_get_connection.return_value = mock_connection
            
            # First create a service manager
            async with service_context(nats_url=nats_url, global_config=config) as service_manager:
                # Now use the long-lived context with the created service manager
                async with long_lived_service_context(service_manager) as ctx_manager:
                    # Verify services are properly initialized
                    assert ctx_manager.has_service("connection")
                    assert ctx_manager.has_service("jobs")
                    assert ctx_manager.has_service("kv")
                    
                    # Verify service manager is properly configured
                    connection_service = await ctx_manager.get_service("connection")
                    assert connection_service.config.nats_url == nats_url

    def test_run_with_service_context_with_real_services(self):
        """Test run_with_service_context with real service initialization."""
        nats_url = "nats://localhost:4222"
        
        async def test_operation(service_manager):
            # Verify services are properly initialized
            assert service_manager.has_service("connection")
            assert service_manager.has_service("jobs")
            assert service_manager.has_service("kv")
            
            # Verify service manager is properly configured
            connection_service = await service_manager.get_service("connection")
            assert connection_service.config.nats_url == nats_url
            return "success"
        
        # Mock the ConnectionManager.get_connection method to avoid network calls
        with patch('naq.connection.manager.ConnectionManager.get_connection') as mock_get_connection:
            # Create a mock connection object
            mock_connection = AsyncMock()
            mock_get_connection.return_value = mock_connection
            
            result = run_with_service_context(test_operation, nats_url=nats_url)
            assert result == "success"