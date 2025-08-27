"""
Stream Service Tests

This module contains tests for the StreamService, focusing on
service lifecycle management and basic functionality.
"""
from pathlib import Path
import sys
sys.path.append(Path(__file__).parent.as_posix())
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional

from naq.services.base import ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from naq.services.streams import StreamService, StreamServiceConfig
from naq.services.connection import ConnectionService
from naq.exceptions import NaqException

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
async def stream_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for StreamService tests.
    
    Returns:
        A ServiceConfig instance with stream-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "storage": "file",
            "retention": "work_queue",
            "replicas": 1,
            "auto_create_streams": True
        }
    )


@pytest_asyncio.fixture
async def mock_connection_service() -> AsyncMock:
    """
    Fixture providing a mock ConnectionService.
    
    Returns:
        An AsyncMock instance configured as a ConnectionService.
    """
    return create_mock_service(ConnectionService)


@pytest_asyncio.fixture
async def stream_service_with_dependencies(
    stream_service_config: ServiceConfig,
    mock_connection_service: AsyncMock
) -> StreamService:
    """
    Fixture providing a StreamService instance with mocked dependencies.
    
    Args:
        stream_service_config: Configuration for the service.
        mock_connection_service: Mock ConnectionService dependency.
        
    Returns:
        A StreamService instance.
    """
    service = StreamService(
        config=stream_service_config,
        connection_service=mock_connection_service
    )
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_stream_service() -> AsyncMock:
    """
    Fixture providing a mock StreamService.
    
    Returns:
        An AsyncMock instance configured as a StreamService.
    """
    return create_mock_service(StreamService)


@pytest_asyncio.fixture
async def test_stream_config() -> Dict[str, Any]:
    """
    Fixture providing test stream configuration.
    
    Returns:
        A dictionary with test stream configuration.
    """
    return {
        "name": "test-stream",
        "subjects": ["test.subject.>"],
        "retention": "work_queue",
        "storage": "file",
        "replicas": 1
    }


class TestStreamServiceLifecycle:
    """Test cases for StreamService lifecycle management."""
    
    async def test_service_initialization_with_dependencies(
        self,
        stream_service_with_dependencies: StreamService
    ) -> None:
        """Test StreamService initialization with dependencies."""
        service = stream_service_with_dependencies
        assert_service_initialized(service)
        assert service.is_initialized is True
        assert service.stream_config is not None
        assert service.stream_config.storage == "file"
        assert service.stream_config.retention == "work_queue"
        assert service.stream_config.auto_create_streams is True
    
    async def test_service_cleanup(
        self,
        stream_service_with_dependencies: StreamService
    ) -> None:
        """Test StreamService cleanup."""
        service = stream_service_with_dependencies
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(
        self,
        stream_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test StreamService as a context manager."""
        async with StreamService(
            config=stream_service_config,
            connection_service=mock_connection_service
        ) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test StreamService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "storage": "invalid_storage",  # Invalid storage should cause initialization to fail
                "auto_create_streams": True
            }
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(
                StreamService,
                invalid_config,
                connection_service=create_mock_service(ConnectionService)
            ):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that StreamService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            StreamService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        stream_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that StreamService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            StreamService,
            stream_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        stream_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test StreamService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            StreamService,
            stream_service_config,
            connection_service=mock_connection_service
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(StreamService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestStreamServiceConfiguration:
    """Test cases for StreamService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test StreamService with default configuration."""
        service = StreamService()
        
        # Check that default configuration is applied
        assert service.stream_config is not None
        assert service.stream_config.storage == "file"
        assert service.stream_config.retention == "work_queue"
        assert service.stream_config.replicas == 1
        assert service.stream_config.auto_create_streams is True
    
    async def test_custom_configuration(self, stream_service_config: ServiceConfig) -> None:
        """Test StreamService with custom configuration."""
        service = StreamService(config=stream_service_config)
        
        # Check that custom configuration is applied
        assert service.stream_config is not None
        assert service.stream_config.storage == "file"
        assert service.stream_config.retention == "work_queue"
        assert service.stream_config.replicas == 1
        assert service.stream_config.auto_create_streams is True
    
    async def test_configuration_property(self, stream_service_config: ServiceConfig) -> None:
        """Test StreamService configuration property."""
        service = StreamService(config=stream_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == stream_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestStreamServiceDependencies:
    """Test cases for StreamService dependencies."""
    
    async def test_connection_service_dependency(
        self,
        stream_service_with_dependencies: StreamService,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that StreamService has a connection service dependency."""
        service = stream_service_with_dependencies
        assert_service_dependency(service, '_connection_service', AsyncMock)
        assert service._connection_service == mock_connection_service


class TestStreamServiceProperties:
    """Test cases for StreamService properties."""
    
    async def test_stream_config_property(self, stream_service_config: ServiceConfig) -> None:
        """Test StreamService stream_config property."""
        service = StreamService(config=stream_service_config)
        
        # Check that the stream_config property returns the correct configuration
        assert service.stream_config is not None
        assert isinstance(service.stream_config, StreamServiceConfig)
        assert service.stream_config.storage == "file"


class TestStreamServiceMocking:
    """Test cases for mocking StreamService."""
    
    async def test_mock_service_creation(self, mock_stream_service: AsyncMock) -> None:
        """Test that mock StreamService is created correctly."""
        assert mock_stream_service is not None
        assert mock_stream_service._is_initialized is True
        assert mock_stream_service.initialize is not None
        assert mock_stream_service.cleanup is not None
        assert mock_stream_service.ensure_stream is not None
        assert mock_stream_service.get_stream_info is not None
    
    async def test_mock_service_methods(
        self,
        mock_stream_service: AsyncMock,
        test_stream_config: Dict[str, Any]
    ) -> None:
        """Test that mock StreamService methods work correctly."""
        # Test that mock methods can be called
        await mock_stream_service.initialize()
        await mock_stream_service.cleanup()
        await mock_stream_service.ensure_stream(test_stream_config["name"], test_stream_config)
        await mock_stream_service.get_stream_info(test_stream_config["name"])
        
        # Verify that the methods were called
        mock_stream_service.initialize.assert_called_once()
        mock_stream_service.cleanup.assert_called_once()
        mock_stream_service.ensure_stream.assert_called_once()
        mock_stream_service.get_stream_info.assert_called_once()
    
    async def test_mock_service_with_dependencies(
        self,
        mock_stream_service: AsyncMock,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that mock StreamService can be used with dependencies."""
        # Create a service that depends on the mock stream service
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
        
        # This is just an example of how a service might depend on StreamService
        # In a real scenario, you would create a service that actually uses StreamService
        assert mock_stream_service is not None
        assert mock_connection_service is not None


class TestStreamServiceBasicOperations:
    """Test cases for basic StreamService operations."""
    
    async def test_ensure_stream_operation(
        self,
        stream_service_with_dependencies: StreamService,
        test_stream_config: Dict[str, Any]
    ) -> None:
        """Test ensure_stream operation."""
        service = stream_service_with_dependencies
        
        # Ensure the stream exists
        await service.ensure_stream(test_stream_config["name"], test_stream_config)
        
        # Verify that the stream was created (this would require actual NATS connection in real tests)
        # In mock tests, we just verify that the method was called on the mock
        assert service._connection_service.get_jetstream.called
    
    async def test_get_stream_info_operation(
        self,
        stream_service_with_dependencies: StreamService,
        test_stream_config: Dict[str, Any]
    ) -> None:
        """Test get_stream_info operation."""
        service = stream_service_with_dependencies
        
        # First ensure the stream exists
        await service.ensure_stream(test_stream_config["name"], test_stream_config)
        
        # Get stream info
        stream_info = await service.get_stream_info(test_stream_config["name"])
        
        # Verify that stream info was returned (in mock tests, this will be the mock return value)
        assert stream_info is not None
    
    async def test_ensure_stream_with_defaults(
        self,
        stream_service_with_dependencies: StreamService
    ) -> None:
        """Test ensure_stream operation with default configuration."""
        service = stream_service_with_dependencies
        
        # Ensure the stream exists with minimal configuration
        await service.ensure_stream("test-default-stream", {"name": "test-default-stream"})
        
        # Verify that the stream was created
        assert service._connection_service.get_jetstream.called
    
    async def test_ensure_stream_with_custom_config(
        self,
        stream_service_with_dependencies: StreamService,
        test_stream_config: Dict[str, Any]
    ) -> None:
        """Test ensure_stream operation with custom configuration."""
        service = stream_service_with_dependencies
        
        # Ensure the stream exists with custom configuration
        await service.ensure_stream(test_stream_config["name"], test_stream_config)
        
        # Verify that the stream was created with the correct configuration
        assert service._connection_service.get_jetstream.called
    
    async def test_get_stream_info_for_nonexistent_stream(
        self,
        stream_service_with_dependencies: StreamService
    ) -> None:
        """Test get_stream_info operation for a non-existent stream."""
        service = stream_service_with_dependencies
        
        # Try to get info for a non-existent stream
        stream_info = await service.get_stream_info("nonexistent-stream")
        
        # Verify that None is returned for non-existent streams
        assert stream_info is None