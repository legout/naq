"""
Connection Service Tests

This module contains tests for the ConnectionService, focusing on
service lifecycle management and basic functionality.
"""
from pathlib import Path
import sys
sys.path.append(Path(__file__).parent)
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional

from naq.services.base import ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from naq.services.connection import ConnectionService, ConnectionServiceConfig
from naq.exceptions import NaqConnectionError

from service_test_utils import (
    ServiceTestContext,
    create_mock_service,
    assert_service_initialized,
    assert_service_cleaned_up,
    assert_service_dependency
)
from service_lifecycle_utils import (
    ServiceLifecycleTracker,
    ServiceLifecycleTestHarness,
    MonitoredService,
    test_service_initialization_failure,
    test_service_cleanup_failure
)


@pytest_asyncio.fixture
async def connection_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for ConnectionService tests.
    
    Returns:
        A ServiceConfig instance with connection-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "max_reconnect_attempts": 3,
            "reconnect_time_wait": 1.0,
            "connection_timeout": 10.0,
            "ping_interval": 30.0,
            "max_outstanding_pings": 3,
            "prefer_thread_local": False
        }
    )


@pytest_asyncio.fixture
async def connection_service_instance(connection_service_config: ServiceConfig) -> ConnectionService:
    """
    Fixture providing a ConnectionService instance with proper lifecycle management.
    
    Args:
        connection_service_config: Configuration for the service.
        
    Returns:
        A ConnectionService instance.
    """
    service = ConnectionService(config=connection_service_config)
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_connection_service() -> AsyncMock:
    """
    Fixture providing a mock ConnectionService.
    
    Returns:
        An AsyncMock instance configured as a ConnectionService.
    """
    return create_mock_service(ConnectionService)


class TestConnectionServiceLifecycle:
    """Test cases for ConnectionService lifecycle management."""
    
    async def test_service_initialization(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService initialization."""
        async with ServiceTestContext(ConnectionService, connection_service_config) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
            assert service.config == connection_service_config
            assert service.connection_config is not None
            assert service.connection_config.nats_url == "nats://localhost:4222"
    
    async def test_service_cleanup(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService cleanup."""
        service = ConnectionService(config=connection_service_config)
        await service.initialize()
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService as a context manager."""
        async with ConnectionService(config=connection_service_config) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test ConnectionService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="",  # Empty URL should cause initialization to fail
            log_level="DEBUG"
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(ConnectionService, invalid_config):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that ConnectionService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            ConnectionService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        connection_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that ConnectionService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            ConnectionService,
            connection_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        connection_service_config: ServiceConfig,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test ConnectionService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            ConnectionService,
            connection_service_config
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(ConnectionService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestConnectionServiceConfiguration:
    """Test cases for ConnectionService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test ConnectionService with default configuration."""
        service = ConnectionService()
        
        # Check that default configuration is applied
        assert service.connection_config is not None
        assert service.connection_config.max_reconnect_attempts == 5
        assert service.connection_config.reconnect_time_wait == 2.0
        assert service.connection_config.connection_timeout == 30.0
        assert service.connection_config.ping_interval == 30.0
        assert service.connection_config.max_outstanding_pings == 3
        assert service.connection_config.prefer_thread_local is False
    
    async def test_custom_configuration(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService with custom configuration."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that custom configuration is applied
        assert service.connection_config is not None
        assert service.connection_config.max_reconnect_attempts == 3
        assert service.connection_config.reconnect_time_wait == 1.0
        assert service.connection_config.connection_timeout == 10.0
        assert service.connection_config.ping_interval == 30.0
        assert service.connection_config.max_outstanding_pings == 3
        assert service.connection_config.prefer_thread_local is False
    
    async def test_configuration_property(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService configuration property."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == connection_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestConnectionServiceDependencies:
    """Test cases for ConnectionService dependencies."""
    
    async def test_connection_manager_dependency(self, connection_service_config: ServiceConfig) -> None:
        """Test that ConnectionService has a connection manager dependency."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that the service has a connection manager
        assert hasattr(service, '_connection_manager')
        assert service._connection_manager is not None
    
    async def test_connection_dictionaries(self, connection_service_config: ServiceConfig) -> None:
        """Test that ConnectionService has connection and jetstream context dictionaries."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that the service has the required dictionaries
        assert hasattr(service, '_connections')
        assert hasattr(service, '_jetstream_contexts')
        assert hasattr(service, '_reconnect_tasks')
        assert hasattr(service, '_connection_stats')
        assert hasattr(service, '_connection_locks')
        
        # Check that the dictionaries are empty initially
        assert len(service._connections) == 0
        assert len(service._jetstream_contexts) == 0
        assert len(service._reconnect_tasks) == 0
        assert len(service._connection_stats) == 0
        assert len(service._connection_locks) == 0


class TestConnectionServiceProperties:
    """Test cases for ConnectionService properties."""
    
    async def test_connection_config_property(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService connection_config property."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that the connection_config property returns the correct configuration
        assert service.connection_config is not None
        assert isinstance(service.connection_config, ConnectionServiceConfig)
        assert service.connection_config.nats_url == "nats://localhost:4222"
    
    async def test_active_connections_property(self, connection_service_config: ServiceConfig) -> None:
        """Test ConnectionService active_connections property."""
        service = ConnectionService(config=connection_service_config)
        
        # Check that active_connections is initially empty
        assert service.active_connections == {}
        
        # After initialization, it should still be empty (no connections established yet)
        await service.initialize()
        assert service.active_connections == {}
        
        # After cleanup, it should still be empty
        await service.cleanup()
        assert service.active_connections == {}


class TestConnectionServiceMocking:
    """Test cases for mocking ConnectionService."""
    
    async def test_mock_service_creation(self, mock_connection_service: AsyncMock) -> None:
        """Test that mock ConnectionService is created correctly."""
        assert mock_connection_service is not None
        assert mock_connection_service._is_initialized is True
        assert mock_connection_service.initialize is not None
        assert mock_connection_service.cleanup is not None
        assert mock_connection_service.get_connection is not None
        assert mock_connection_service.get_jetstream is not None
    
    async def test_mock_service_methods(self, mock_connection_service: AsyncMock) -> None:
        """Test that mock ConnectionService methods work correctly."""
        # Test that mock methods can be called
        await mock_connection_service.initialize()
        await mock_connection_service.cleanup()
        await mock_connection_service.get_connection()
        await mock_connection_service.get_jetstream()
        
        # Verify that the methods were called
        mock_connection_service.initialize.assert_called_once()
        mock_connection_service.cleanup.assert_called_once()
        mock_connection_service.get_connection.assert_called_once()
        mock_connection_service.get_jetstream.assert_called_once()
    
    async def test_mock_service_with_dependencies(self, mock_connection_service: AsyncMock) -> None:
        """Test that mock ConnectionService can be used with dependencies."""
        # Create a service that depends on the mock connection service
        from naq.services.jobs import JobService
        
        job_service_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "enable_job_execution": True,
                "enable_result_storage": True,
                "enable_event_logging": True
            }
        )
        
        job_service = JobService(
            config=job_service_config,
            connection_service=mock_connection_service
        )
        
        # Verify that the job service has the mock connection service as a dependency
        assert_service_dependency(job_service, '_connection_service', AsyncMock)
        assert job_service._connection_service == mock_connection_service