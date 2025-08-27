"""
ServiceManager Integration Tests

This module contains comprehensive integration tests for the ServiceManager,
focusing on cross-service communication, dependency injection, and lifecycle management.
"""

from pathlib import Path
import sys
sys.path.append(Path(__file__).parent.as_posix())
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional, Type
import asyncio

from naq.services.base import ServiceConfig, ServiceManager, ServiceInitializationError, ServiceRuntimeError
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.services.scheduler import SchedulerService
from naq.services.worker import WorkerService
from naq.exceptions import NaqException
from naq.models.jobs import Job, JobResult
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JOB_STATUS

from service_test_utils import (
    ServiceTestContext,
    create_mock_service,
    assert_service_initialized,
    assert_service_cleaned_up,
    assert_service_dependency,
    create_mock_service_manager
)
from service_lifecycle_utils import (
    ServiceLifecycleTracker,
    ServiceLifecycleTestHarness,
    MonitoredService,
    test_service_initialization_failure,
    test_service_cleanup_failure
)


@pytest_asyncio.fixture
async def service_manager_config() -> ServiceConfig:
    """
    Fixture providing a configuration for ServiceManager tests.
    
    Returns:
        A ServiceConfig instance with settings for all services.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "test_mode": True,
            "auto_create_buckets": True,
            "enable_event_logging": True,
            "enable_job_execution": True,
            "enable_result_storage": True,
            "results_bucket_name": "test_job_results",
            "events_bucket_name": "test_events",
            "default_result_ttl": 3600,
            "max_job_execution_time": 300
        }
    )


@pytest_asyncio.fixture
async def service_manager_with_real_services(service_manager_config: ServiceConfig) -> ServiceManager:
    """
    Fixture providing a ServiceManager instance with real services.
    
    Args:
        service_manager_config: Configuration for the services.
        
    Returns:
        A ServiceManager instance with all services registered.
    """
    manager = ServiceManager(config=service_manager_config)
    
    # Register all services
    manager.register_service("connection", ConnectionService)
    manager.register_service("kv_store", KVStoreService)
    manager.register_service("events", EventService)
    manager.register_service("stream", StreamService)
    manager.register_service("scheduler", SchedulerService)
    manager.register_service("jobs", JobService)
    manager.register_service("worker", WorkerService)
    
    try:
        await manager.initialize_all()
        yield manager
    finally:
        await manager.cleanup_all()


@pytest_asyncio.fixture
async def service_manager_with_mixed_services(service_manager_config: ServiceConfig) -> ServiceManager:
    """
    Fixture providing a ServiceManager instance with a mix of real and mock services.
    
    Args:
        service_manager_config: Configuration for the services.
        
    Returns:
        A ServiceManager instance with mixed services.
    """
    manager = ServiceManager(config=service_manager_config)
    
    # Register real connection service
    manager.register_service("connection", ConnectionService)
    
    # Register mock services for others
    manager.register_service("kv_store", create_mock_service(KVStoreService))
    manager.register_service("events", create_mock_service(EventService))
    manager.register_service("stream", create_mock_service(StreamService))
    manager.register_service("scheduler", create_mock_service(SchedulerService))
    manager.register_service("jobs", create_mock_service(JobService))
    manager.register_service("worker", create_mock_service(WorkerService))
    
    try:
        await manager.initialize_all()
        yield manager
    finally:
        await manager.cleanup_all()


@pytest_asyncio.fixture
async def mock_service_manager() -> AsyncMock:
    """
    Fixture providing a mock ServiceManager.
    
    Returns:
        An AsyncMock instance configured as a ServiceManager.
    """
    return create_mock_service_manager()


@pytest_asyncio.fixture
async def service_lifecycle_tracker() -> ServiceLifecycleTracker:
    """
    Fixture providing a service lifecycle tracker.
    
    Returns:
        A ServiceLifecycleTracker instance.
    """
    return ServiceLifecycleTracker()


@pytest_asyncio.fixture
async def test_job() -> Job:
    """
    Fixture providing a test job.
    
    Returns:
        A Job instance for testing.
    """
    return Job(
        job_id="test-job-123",
        queue_name="default",
        func=lambda: "test result",
        args=(),
        kwargs={},
        status=JOB_STATUS.PENDING.value,
        retry_count=0,
        max_retries=3,
        timeout=30,
        scheduled_at=None,
        created_at=None,
        started_at=None,
        completed_at=None,
        error=None,
        traceback=None,
        result=None
    )


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


class TestServiceManagerInitialization:
    """Test cases for ServiceManager initialization and service registration."""
    
    async def test_service_manager_creation(self, service_manager_config: ServiceConfig) -> None:
        """Test ServiceManager creation with configuration."""
        manager = ServiceManager(config=service_manager_config)
        
        assert manager is not None
        assert manager.config == service_manager_config
        assert len(manager._services) == 0
        assert len(manager._service_configs) == 0
    
    async def test_service_registration(self, service_manager_config: ServiceConfig) -> None:
        """Test service registration with ServiceManager."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("kv_store", KVStoreService)
        manager.register_service("events", EventService)
        
        # Check that services are registered
        assert len(manager._service_configs) == 3
        assert "connection" in manager._service_configs
        assert "kv_store" in manager._service_configs
        assert "events" in manager._service_configs
        
        # Check service configurations
        assert manager._service_configs["connection"]["service_class"] == ConnectionService
        assert manager._service_configs["kv_store"]["service_class"] == KVStoreService
        assert manager._service_configs["events"]["service_class"] == EventService
    
    async def test_duplicate_service_registration(self, service_manager_config: ServiceConfig) -> None:
        """Test that duplicate service registration raises an error."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register a service
        manager.register_service("connection", ConnectionService)
        
        # Try to register the same service again
        with pytest.raises(ValueError, match="Service 'connection' is already registered"):
            manager.register_service("connection", ConnectionService)
    
    async def test_service_initialization_order(self, service_manager_config: ServiceConfig) -> None:
        """Test that services are initialized in the correct dependency order."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services in reverse dependency order
        manager.register_service("jobs", JobService)
        manager.register_service("events", EventService)
        manager.register_service("kv_store", KVStoreService)
        manager.register_service("connection", ConnectionService)
        
        # Track initialization order
        init_order = []
        
        # Patch the initialize method of each service to track order
        original_init = {}
        for service_name in ["connection", "kv_store", "events", "jobs"]:
            service_config = manager._service_configs[service_name]
            service_class = service_config["service_class"]
            
            async def tracked_init(self, service_name=service_name, init_order=init_order):
                init_order.append(service_name)
                return await original_init[service_name](self)
            
            original_init[service_name] = service_class._do_initialize
            service_class._do_initialize = tracked_init
        
        # Initialize all services
        await manager.initialize_all()
        
        # Check that services were initialized in the correct order
        # Connection should be first, then kv_store, then events, then jobs
        assert init_order[0] == "connection"
        assert init_order[1] == "kv_store"
        assert init_order[2] == "events"
        assert init_order[3] == "jobs"
        
        # Cleanup
        await manager.cleanup_all()
    
    async def test_service_manager_with_pre_registered_services(self, service_manager_config: ServiceConfig) -> None:
        """Test ServiceManager with pre-registered service instances."""
        manager = ServiceManager(config=service_manager_config)
        
        # Create and register a pre-initialized service
        connection_service = ConnectionService(config=service_manager_config)
        await connection_service.initialize()
        
        manager.register_service("connection", connection_service)
        
        # Check that the service is registered and initialized
        assert manager.has_service("connection")
        assert manager.get_service("connection") == connection_service
        assert connection_service.is_initialized
        
        # Cleanup
        await manager.cleanup_all()
        assert not connection_service.is_initialized


class TestServiceManagerDependencyInjection:
    """Test cases for dependency injection across services."""
    
    async def test_automatic_dependency_injection(self, service_manager_with_real_services: ServiceManager) -> None:
        """Test that dependencies are automatically injected."""
        manager = service_manager_with_real_services
        
        # Get services
        connection_service = manager.get_service("connection", ConnectionService)
        kv_store_service = manager.get_service("kv_store", KVStoreService)
        event_service = manager.get_service("events", EventService)
        job_service = manager.get_service("jobs", JobService)
        
        # Check that JobService has all its dependencies
        assert_service_dependency(job_service, '_connection_service', ConnectionService)
        assert_service_dependency(job_service, '_kv_store_service', KVStoreService)
        assert_service_dependency(job_service, '_event_service', EventService)
        
        # Check that the dependencies are the same instances
        assert job_service._connection_service == connection_service
        assert job_service._kv_store_service == kv_store_service
        assert job_service._event_service == event_service
    
    async def test_circular_dependency_detection(self, service_manager_config: ServiceConfig) -> None:
        """Test that circular dependencies are detected and reported."""
        manager = ServiceManager(config=service_manager_config)
        
        # Create mock services with circular dependencies
        service_a = create_mock_service(ConnectionService)
        service_b = create_mock_service(KVStoreService)
        
        # Patch the service classes to have circular dependencies
        original_init_a = ConnectionService._do_initialize
        original_init_b = KVStoreService._do_initialize
        
        async def init_with_dependency_a(self):
            self._dependency = manager.get_service("service_b", KVStoreService)
            await original_init_a(self)
        
        async def init_with_dependency_b(self):
            self._dependency = manager.get_service("service_a", ConnectionService)
            await original_init_b(self)
        
        ConnectionService._do_initialize = init_with_dependency_a
        KVStoreService._do_initialize = init_with_dependency_b
        
        # Register services
        manager.register_service("service_a", ConnectionService)
        manager.register_service("service_b", KVStoreService)
        
        # Try to initialize - should detect circular dependency
        with pytest.raises(ServiceInitializationError, match="Circular dependency detected"):
            await manager.initialize_all()
        
        # Restore original methods
        ConnectionService._do_initialize = original_init_a
        KVStoreService._do_initialize = original_init_b
    
    async def test_optional_dependency_handling(self, service_manager_config: ServiceConfig) -> None:
        """Test that optional dependencies are handled correctly."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register only connection service
        manager.register_service("connection", ConnectionService)
        
        # Register job service which has optional dependencies
        manager.register_service("jobs", JobService)
        
        # Initialize
        await manager.initialize_all()
        
        # Get services
        connection_service = manager.get_service("connection", ConnectionService)
        job_service = manager.get_service("jobs", JobService)
        
        # Check that JobService has connection service
        assert_service_dependency(job_service, '_connection_service', ConnectionService)
        assert job_service._connection_service == connection_service
        
        # Check that JobService created its own optional dependencies
        assert job_service._kv_store_service is not None
        assert job_service._event_service is not None
        assert job_service._kv_store_service.is_initialized
        assert job_service._event_service.is_initialized
        
        # Cleanup
        await manager.cleanup_all()
    
    async def test_dependency_injection_with_mixed_services(self, service_manager_with_mixed_services: ServiceManager) -> None:
        """Test dependency injection with a mix of real and mock services."""
        manager = service_manager_with_mixed_services
        
        # Get services
        connection_service = manager.get_service("connection", ConnectionService)
        mock_kv_store_service = manager.get_service("kv_store", KVStoreService)
        mock_event_service = manager.get_service("events", EventService)
        job_service = manager.get_service("jobs", JobService)
        
        # Check that JobService has both real and mock dependencies
        assert_service_dependency(job_service, '_connection_service', ConnectionService)
        assert_service_dependency(job_service, '_kv_store_service', KVStoreService)
        assert_service_dependency(job_service, '_event_service', EventService)
        
        # Check that the dependencies are the correct instances
        assert job_service._connection_service == connection_service
        assert job_service._kv_store_service == mock_kv_store_service
        assert job_service._event_service == mock_event_service


class TestServiceManagerCrossServiceCommunication:
    """Test cases for cross-service communication and data flow."""
    
    async def test_job_to_event_service_communication(self, service_manager_with_real_services: ServiceManager, test_job_event: JobEvent) -> None:
        """Test communication from JobService to EventService."""
        manager = service_manager_with_real_services
        
        # Get services
        job_service = manager.get_service("jobs", JobService)
        event_service = manager.get_service("events", EventService)
        
        # Log a job event
        await job_service._event_service.log_job_event(test_job_event)
        
        # Verify that the event was logged
        events = await event_service.get_job_events(test_job_event.job_id)
        assert len(events) == 1
        assert events[0].job_id == test_job_event.job_id
        assert events[0].event_type == test_job_event.event_type
    
    async def test_kv_store_to_job_service_communication(self, service_manager_with_real_services: ServiceManager, test_job: Job) -> None:
        """Test communication from KVStoreService to JobService."""
        manager = service_manager_with_real_services
        
        # Get services
        job_service = manager.get_service("jobs", JobService)
        kv_store_service = manager.get_service("kv_store", KVStoreService)
        
        # Store a job result
        result = JobResult.from_job(test_job)
        await job_service.store_result(test_job.job_id, result)
        
        # Verify that the result was stored in the KV store
        stored_result = await kv_store_service.get(f"job_result:{test_job.job_id}")
        assert stored_result is not None
        # Note: The actual stored data might be serialized, so we check for presence rather than exact equality
    
    async def test_connection_to_all_services_communication(self, service_manager_with_real_services: ServiceManager) -> None:
        """Test that ConnectionService provides connections to all other services."""
        manager = service_manager_with_real_services
        
        # Get services
        connection_service = manager.get_service("connection", ConnectionService)
        kv_store_service = manager.get_service("kv_store", KVStoreService)
        event_service = manager.get_service("events", EventService)
        stream_service = manager.get_service("stream", StreamService)
        scheduler_service = manager.get_service("scheduler", SchedulerService)
        job_service = manager.get_service("jobs", JobService)
        worker_service = manager.get_service("worker", WorkerService)
        
        # Check that all services have a connection to the same NATS server
        assert kv_store_service._connection_service == connection_service
        assert event_service._connection_service == connection_service
        assert stream_service._connection_service == connection_service
        assert scheduler_service._connection_service == connection_service
        assert job_service._connection_service == connection_service
        assert worker_service._connection_service == connection_service
    
    async def test_event_to_worker_service_communication(self, service_manager_with_real_services: ServiceManager, test_worker_event: WorkerEvent) -> None:
        """Test communication from EventService to WorkerService."""
        manager = service_manager_with_real_services
        
        # Get services
        worker_service = manager.get_service("worker", WorkerService)
        event_service = manager.get_service("events", EventService)
        
        # Log a worker event
        await event_service.log_worker_event(test_worker_event)
        
        # Verify that the event was logged
        events = await event_service.get_worker_events(test_worker_event.worker_id)
        assert len(events) == 1
        assert events[0].worker_id == test_worker_event.worker_id
        assert events[0].event_type == test_worker_event.event_type
    
    async def test_stream_to_job_service_communication(self, service_manager_with_real_services: ServiceConfig) -> None:
        """Test communication from StreamService to JobService."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("stream", StreamService)
        manager.register_service("jobs", JobService)
        
        # Initialize
        await manager.initialize_all()
        
        # Get services
        stream_service = manager.get_service("stream", StreamService)
        job_service = manager.get_service("jobs", JobService)
        
        # Create a stream for jobs
        stream_name = "test_jobs"
        await stream_service.ensure_stream(stream_name, subjects=["jobs.>"])
        
        # Verify that the stream was created
        stream_info = await stream_service.get_stream_info(stream_name)
        assert stream_info is not None
        assert stream_info.config.name == stream_name
        
        # Cleanup
        await manager.cleanup_all()


class TestServiceManagerLifecycleManagement:
    """Test cases for lifecycle management of all services."""
    
    async def test_full_lifecycle_management(self, service_manager_config: ServiceConfig, service_lifecycle_tracker: ServiceLifecycleTracker) -> None:
        """Test full lifecycle management for all services."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register all services with lifecycle tracking
        services = [
            ("connection", ConnectionService),
            ("kv_store", KVStoreService),
            ("events", EventService),
            ("stream", StreamService),
            ("scheduler", SchedulerService),
            ("jobs", JobService),
            ("worker", WorkerService)
        ]
        
        for name, service_class in services:
            monitored_service = MonitoredService(service_class, service_lifecycle_tracker, service_manager_config)
            manager.register_service(name, monitored_service)
        
        # Initialize all services
        await manager.initialize_all()
        
        # Check that all services are initialized
        for name, _ in services:
            service = manager.get_service(name)
            assert_service_initialized(service)
        
        # Check that initialization events were recorded
        for name, _ in services:
            service_lifecycle_tracker.assert_initialization_sequence(name)
        
        # Cleanup all services
        await manager.cleanup_all()
        
        # Check that all services are cleaned up
        for name, _ in services:
            service = manager.get_service(name)
            assert_service_cleaned_up(service)
        
        # Check that cleanup events were recorded
        for name, _ in services:
            service_lifecycle_tracker.assert_cleanup_sequence(name)
    
    async def test_partial_initialization_cleanup(self, service_manager_config: ServiceConfig) -> None:
        """Test partial initialization and cleanup of services."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register only some services
        manager.register_service("connection", ConnectionService)
        manager.register_service("kv_store", KVStoreService)
        
        # Initialize only these services
        await manager.initialize_all()
        
        # Check that they are initialized
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("kv_store"))
        
        # Add more services
        manager.register_service("events", EventService)
        manager.register_service("jobs", JobService)
        
        # Initialize only the new services
        await manager.initialize_service("events")
        await manager.initialize_service("jobs")
        
        # Check that all services are initialized
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("kv_store"))
        assert_service_initialized(manager.get_service("events"))
        assert_service_initialized(manager.get_service("jobs"))
        
        # Cleanup only specific services
        await manager.cleanup_service("jobs")
        await manager.cleanup_service("events")
        
        # Check that only the specified services are cleaned up
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("kv_store"))
        assert_service_cleaned_up(manager.get_service("events"))
        assert_service_cleaned_up(manager.get_service("jobs"))
        
        # Cleanup remaining services
        await manager.cleanup_all()
        
        # Check that all services are cleaned up
        assert_service_cleaned_up(manager.get_service("connection"))
        assert_service_cleaned_up(manager.get_service("kv_store"))
    
    async def test_lifecycle_error_handling(self, service_manager_config: ServiceConfig) -> None:
        """Test error handling during lifecycle operations."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register a service that will fail initialization
        class FailingService(ConnectionService):
            async def _do_initialize(self) -> None:
                raise ServiceInitializationError("Simulated initialization failure")
        
        manager.register_service("connection", ConnectionService)
        manager.register_service("failing", FailingService)
        
        # Try to initialize all services - should fail
        with pytest.raises(ServiceInitializationError, match="Simulated initialization failure"):
            await manager.initialize_all()
        
        # Check that the connection service was cleaned up
        connection_service = manager.get_service("connection")
        assert_service_cleaned_up(connection_service)
    
    async def test_service_reinitialization(self, service_manager_config: ServiceConfig) -> None:
        """Test service reinitialization after cleanup."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("kv_store", KVStoreService)
        
        # Initialize
        await manager.initialize_all()
        
        # Check that services are initialized
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("kv_store"))
        
        # Cleanup
        await manager.cleanup_all()
        
        # Check that services are cleaned up
        assert_service_cleaned_up(manager.get_service("connection"))
        assert_service_cleaned_up(manager.get_service("kv_store"))
        
        # Reinitialize
        await manager.initialize_all()
        
        # Check that services are initialized again
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("kv_store"))
        
        # Final cleanup
        await manager.cleanup_all()


class TestServiceManagerErrorHandling:
    """Test cases for error handling in ServiceManager."""
    
    async def test_get_nonexistent_service(self, service_manager_config: ServiceConfig) -> None:
        """Test getting a non-existent service."""
        manager = ServiceManager(config=service_manager_config)
        
        with pytest.raises(ValueError, match="Service 'nonexistent' is not registered"):
            manager.get_service("nonexistent")
    
    async def test_initialize_nonexistent_service(self, service_manager_config: ServiceConfig) -> None:
        """Test initializing a non-existent service."""
        manager = ServiceManager(config=service_manager_config)
        
        with pytest.raises(ValueError, match="Service 'nonexistent' is not registered"):
            await manager.initialize_service("nonexistent")
    
    async def test_cleanup_nonexistent_service(self, service_manager_config: ServiceConfig) -> None:
        """Test cleaning up a non-existent service."""
        manager = ServiceManager(config=service_manager_config)
        
        with pytest.raises(ValueError, match="Service 'nonexistent' is not registered"):
            await manager.cleanup_service("nonexistent")
    
    async def test_service_initialization_failure_propagation(self, service_manager_config: ServiceConfig) -> None:
        """Test that service initialization failures are properly propagated."""
        manager = ServiceManager(config=service_manager_config)
        
        # Create a service that fails initialization
        class FailingService(ConnectionService):
            async def _do_initialize(self) -> None:
                raise ServiceInitializationError("Service initialization failed")
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("failing", FailingService)
        manager.register_service("kv_store", KVStoreService)
        
        # Try to initialize all services
        with pytest.raises(ServiceInitializationError, match="Service initialization failed"):
            await manager.initialize_all()
        
        # Check that no services are initialized
        assert not manager.get_service("connection").is_initialized
        assert not manager.get_service("failing").is_initialized
        assert not manager.get_service("kv_store").is_initialized
    
    async def test_service_cleanup_failure_propagation(self, service_manager_config: ServiceConfig) -> None:
        """Test that service cleanup failures are properly propagated."""
        manager = ServiceManager(config=service_manager_config)
        
        # Create a service that fails cleanup
        class FailingCleanupService(ConnectionService):
            async def _do_cleanup(self) -> None:
                raise ServiceRuntimeError("Service cleanup failed")
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("failing_cleanup", FailingCleanupService)
        
        # Initialize all services
        await manager.initialize_all()
        
        # Check that services are initialized
        assert_service_initialized(manager.get_service("connection"))
        assert_service_initialized(manager.get_service("failing_cleanup"))
        
        # Try to cleanup all services
        with pytest.raises(ServiceRuntimeError, match="Service cleanup failed"):
            await manager.cleanup_all()
        
        # Check that the connection service was still cleaned up
        assert_service_cleaned_up(manager.get_service("connection"))


class TestServiceManagerUtilities:
    """Test cases for ServiceManager utility methods."""
    
    async def test_has_service(self, service_manager_config: ServiceConfig) -> None:
        """Test the has_service method."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register a service
        manager.register_service("connection", ConnectionService)
        
        # Check that the service is registered
        assert manager.has_service("connection")
        assert not manager.has_service("nonexistent")
    
    async def test_get_service_names(self, service_manager_config: ServiceConfig) -> None:
        """Test the get_service_names method."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("kv_store", KVStoreService)
        manager.register_service("events", EventService)
        
        # Get service names
        service_names = manager.get_service_names()
        
        # Check that all registered services are included
        assert "connection" in service_names
        assert "kv_store" in service_names
        assert "events" in service_names
        assert len(service_names) == 3
    
    async def test_get_service_configs(self, service_manager_config: ServiceConfig) -> None:
        """Test the get_service_configs method."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services with custom configs
        custom_config = ServiceConfig(nats_url="nats://localhost:4223")
        manager.register_service("connection", ConnectionService, config=custom_config)
        manager.register_service("kv_store", KVStoreService)
        
        # Get service configs
        configs = manager.get_service_configs()
        
        # Check that configs are correct
        assert "connection" in configs
        assert "kv_store" in configs
        assert configs["connection"]["config"] == custom_config
        assert configs["kv_store"]["config"] == service_manager_config
    
    async def test_service_manager_context_manager(self, service_manager_config: ServiceConfig) -> None:
        """Test ServiceManager as a context manager."""
        # Register services
        async with ServiceManager(config=service_manager_config) as manager:
            manager.register_service("connection", ConnectionService)
            manager.register_service("kv_store", KVStoreService)
            
            # Check that services are initialized
            assert_service_initialized(manager.get_service("connection"))
            assert_service_initialized(manager.get_service("kv_store"))
        
        # Check that services are cleaned up after exiting context
        assert_service_cleaned_up(manager.get_service("connection"))
        assert_service_cleaned_up(manager.get_service("kv_store"))
    
    async def test_service_manager_string_representation(self, service_manager_config: ServiceConfig) -> None:
        """Test the string representation of ServiceManager."""
        manager = ServiceManager(config=service_manager_config)
        
        # Register services
        manager.register_service("connection", ConnectionService)
        manager.register_service("kv_store", KVStoreService)
        
        # Check string representation
        str_repr = str(manager)
        assert "ServiceManager" in str_repr
        assert "connection" in str_repr
        assert "kv_store" in str_repr
        
        # Check repr
        repr_repr = repr(manager)
        assert "ServiceManager" in repr_repr
        assert "connection" in repr_repr
        assert "kv_store" in repr_repr