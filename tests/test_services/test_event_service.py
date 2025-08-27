"""
Event Service Tests

This module contains tests for the EventService, focusing on
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
from naq.services.events import EventService, EventServiceConfig
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService
from naq.exceptions import NaqException
from naq.models.events import JobEvent, WorkerEvent

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
async def event_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for EventService tests.
    
    Returns:
        A ServiceConfig instance with event-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "enable_event_logging": True,
            "auto_create_bucket": True,
            "events_bucket_name": "test_events",
            "batch_size": 100,
            "flush_interval": 1.0,
            "max_buffer_size": 1000
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
async def mock_kv_store_service() -> AsyncMock:
    """
    Fixture providing a mock KVStoreService.
    
    Returns:
        An AsyncMock instance configured as a KVStoreService.
    """
    return create_mock_service(KVStoreService)


@pytest_asyncio.fixture
async def event_service_with_dependencies(
    event_service_config: ServiceConfig,
    mock_connection_service: AsyncMock,
    mock_kv_store_service: AsyncMock
) -> EventService:
    """
    Fixture providing an EventService instance with mocked dependencies.
    
    Args:
        event_service_config: Configuration for the service.
        mock_connection_service: Mock ConnectionService dependency.
        mock_kv_store_service: Mock KVStoreService dependency.
        
    Returns:
        An EventService instance.
    """
    service = EventService(
        config=event_service_config,
        connection_service=mock_connection_service,
        kv_store_service=mock_kv_store_service
    )
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_event_service() -> AsyncMock:
    """
    Fixture providing a mock EventService.
    
    Returns:
        An AsyncMock instance configured as an EventService.
    """
    return create_mock_service(EventService)


@pytest_asyncio.fixture
async def test_job_event() -> JobEvent:
    """
    Fixture providing a test job event.
    
    Returns:
        A JobEvent instance for testing.
    """
    return JobEvent.enqueued(
        job_id="test-job-123",
        queue_name="default",
        nats_subject="jobs.default",
        nats_sequence=1
    )


@pytest_asyncio.fixture
async def test_worker_event() -> WorkerEvent:
    """
    Fixture providing a test worker event.
    
    Returns:
        A WorkerEvent instance for testing.
    """
    return WorkerEvent.started(
        worker_id="test-worker-456",
        queue_name="default"
    )


class TestEventServiceLifecycle:
    """Test cases for EventService lifecycle management."""
    
    async def test_service_initialization_with_dependencies(
        self,
        event_service_with_dependencies: EventService
    ) -> None:
        """Test EventService initialization with dependencies."""
        service = event_service_with_dependencies
        assert_service_initialized(service)
        assert service.is_initialized is True
        assert service.event_config is not None
        assert service.event_config.enable_event_logging is True
        assert service.event_config.auto_create_bucket is True
    
    async def test_service_initialization_without_dependencies(
        self,
        event_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test EventService initialization without all dependencies."""
        # Only provide connection service, KV store should be created automatically
        service = EventService(
            config=event_service_config,
            connection_service=mock_connection_service
        )
        
        try:
            await service.initialize()
            assert_service_initialized(service)
            assert service.is_initialized is True
            assert service._kv_store_service is not None
        finally:
            if service.is_initialized:
                await service.cleanup()
    
    async def test_service_cleanup(
        self,
        event_service_with_dependencies: EventService
    ) -> None:
        """Test EventService cleanup."""
        service = event_service_with_dependencies
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(
        self,
        event_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        mock_kv_store_service: AsyncMock
    ) -> None:
        """Test EventService as a context manager."""
        async with EventService(
            config=event_service_config,
            connection_service=mock_connection_service,
            kv_store_service=mock_kv_store_service
        ) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test EventService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "batch_size": 0,  # Invalid batch size should cause initialization to fail
                "enable_event_logging": True
            }
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(
                EventService,
                invalid_config,
                connection_service=create_mock_service(ConnectionService)
            ):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that EventService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            EventService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        event_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that EventService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            EventService,
            event_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        event_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test EventService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            EventService,
            event_service_config,
            connection_service=mock_connection_service
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(EventService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestEventServiceConfiguration:
    """Test cases for EventService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test EventService with default configuration."""
        service = EventService()
        
        # Check that default configuration is applied
        assert service.event_config is not None
        assert service.event_config.enable_event_logging is True
        assert service.event_config.auto_create_bucket is True
        assert service.event_config.events_bucket_name == "naq_events"
        assert service.event_config.batch_size == 100
        assert service.event_config.flush_interval == 1.0
        assert service.event_config.max_buffer_size == 1000
    
    async def test_custom_configuration(self, event_service_config: ServiceConfig) -> None:
        """Test EventService with custom configuration."""
        service = EventService(config=event_service_config)
        
        # Check that custom configuration is applied
        assert service.event_config is not None
        assert service.event_config.enable_event_logging is True
        assert service.event_config.auto_create_bucket is True
        assert service.event_config.events_bucket_name == "test_events"
        assert service.event_config.batch_size == 100
        assert service.event_config.flush_interval == 1.0
        assert service.event_config.max_buffer_size == 1000
    
    async def test_configuration_property(self, event_service_config: ServiceConfig) -> None:
        """Test EventService configuration property."""
        service = EventService(config=event_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == event_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestEventServiceDependencies:
    """Test cases for EventService dependencies."""
    
    async def test_connection_service_dependency(
        self,
        event_service_with_dependencies: EventService,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that EventService has a connection service dependency."""
        service = event_service_with_dependencies
        assert_service_dependency(service, '_connection_service', AsyncMock)
        assert service._connection_service == mock_connection_service
    
    async def test_kv_store_service_dependency(
        self,
        event_service_with_dependencies: EventService,
        mock_kv_store_service: AsyncMock
    ) -> None:
        """Test that EventService has a KV store service dependency."""
        service = event_service_with_dependencies
        assert_service_dependency(service, '_kv_store_service', AsyncMock)
        assert service._kv_store_service == mock_kv_store_service
    
    async def test_dependency_creation(
        self,
        event_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that EventService creates missing dependencies."""
        service = EventService(
            config=event_service_config,
            connection_service=mock_connection_service
        )
        
        try:
            await service.initialize()
            
            # Check that KV store service was created
            assert service._kv_store_service is not None
            assert service._kv_store_service.is_initialized is True
        finally:
            if service.is_initialized:
                await service.cleanup()


class TestEventServiceProperties:
    """Test cases for EventService properties."""
    
    async def test_event_config_property(self, event_service_config: ServiceConfig) -> None:
        """Test EventService event_config property."""
        service = EventService(config=event_service_config)
        
        # Check that the event_config property returns the correct configuration
        assert service.event_config is not None
        assert isinstance(service.event_config, EventServiceConfig)
        assert service.event_config.events_bucket_name == "test_events"
    
    async def test_is_event_logging_enabled_property(self, event_service_config: ServiceConfig) -> None:
        """Test EventService is_event_logging_enabled property."""
        service = EventService(config=event_service_config)
        
        # Check that the property returns the correct value
        assert service.is_event_logging_enabled is True
        
        # Update the configuration
        service._event_config.enable_event_logging = False
        assert service.is_event_logging_enabled is False


class TestEventServiceMocking:
    """Test cases for mocking EventService."""
    
    async def test_mock_service_creation(self, mock_event_service: AsyncMock) -> None:
        """Test that mock EventService is created correctly."""
        assert mock_event_service is not None
        assert mock_event_service._is_initialized is True
        assert mock_event_service.initialize is not None
        assert mock_event_service.cleanup is not None
        assert mock_event_service.log_job_event is not None
        assert mock_event_service.log_worker_event is not None
        assert mock_event_service.get_job_events is not None
        assert mock_event_service.get_worker_events is not None
    
    async def test_mock_service_methods(
        self,
        mock_event_service: AsyncMock,
        test_job_event: JobEvent,
        test_worker_event: WorkerEvent
    ) -> None:
        """Test that mock EventService methods work correctly."""
        # Test that mock methods can be called
        await mock_event_service.initialize()
        await mock_event_service.cleanup()
        await mock_event_service.log_job_event(test_job_event)
        await mock_event_service.log_worker_event(test_worker_event)
        await mock_event_service.get_job_events("test-job-123")
        await mock_event_service.get_worker_events("test-worker-456")
        
        # Verify that the methods were called
        mock_event_service.initialize.assert_called_once()
        mock_event_service.cleanup.assert_called_once()
        mock_event_service.log_job_event.assert_called_once()
        mock_event_service.log_worker_event.assert_called_once()
        mock_event_service.get_job_events.assert_called_once()
        mock_event_service.get_worker_events.assert_called_once()
    
    async def test_mock_service_with_dependencies(
        self,
        mock_event_service: AsyncMock,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that mock EventService can be used with dependencies."""
        # Create a service that depends on the mock event service
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
            connection_service=mock_connection_service,
            event_service=mock_event_service
        )
        
        # Verify that the job service has the mock event service as a dependency
        assert_service_dependency(job_service, '_event_service', AsyncMock)
        assert job_service._event_service == mock_event_service