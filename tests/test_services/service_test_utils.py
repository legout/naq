"""
Service Testing Utilities

This module provides utility functions and fixtures for testing NAQ services
with consistent mocking and lifecycle management.
"""

import pytest
import pytest_asyncio
from typing import Any, Dict, Optional, Type, AsyncContextManager
from unittest.mock import AsyncMock, MagicMock

from naq.services.base import BaseService, ServiceConfig, ServiceManager
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.services.scheduler import SchedulerService


class ServiceTestContext:
    """
    Context manager for service testing with proper lifecycle management.
    
    This class provides a standardized way to create, initialize, and cleanup
    services for testing, ensuring consistent behavior across all service tests.
    """
    
    def __init__(
        self,
        service_class: Type[BaseService],
        config: Optional[ServiceConfig] = None,
        dependencies: Optional[Dict[str, Any]] = None,
        auto_initialize: bool = True
    ):
        """
        Initialize the service test context.
        
        Args:
            service_class: The service class to test.
            config: Optional configuration for the service.
            dependencies: Optional dictionary of service dependencies.
            auto_initialize: Whether to automatically initialize the service.
        """
        self.service_class = service_class
        self.config = config or ServiceConfig()
        self.dependencies = dependencies or {}
        self.auto_initialize = auto_initialize
        self.service: Optional[BaseService] = None
        
    async def __aenter__(self) -> BaseService:
        """Enter the async context manager and return the initialized service."""
        # Create service instance with dependencies
        self.service = self.service_class(config=self.config, **self.dependencies)
        
        # Initialize if requested
        if self.auto_initialize:
            await self.service.initialize()
            
        return self.service
        
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the async context manager and cleanup the service."""
        if self.service and self.service.is_initialized:
            await self.service.cleanup()


def create_mock_service(service_class: Type[BaseService]) -> AsyncMock:
    """
    Create a mock service with proper async methods.
    
    Args:
        service_class: The service class to mock.
        
    Returns:
        An AsyncMock instance configured for the service.
    """
    mock = AsyncMock(spec=service_class)
    mock._is_initialized = True
    mock.initialize = AsyncMock()
    mock.cleanup = AsyncMock()
    
    # Add common service methods
    if hasattr(service_class, 'get_connection'):
        mock.get_connection = AsyncMock()
    if hasattr(service_class, 'get_jetstream'):
        mock.get_jetstream = AsyncMock()
    if hasattr(service_class, 'enqueue_job'):
        mock.enqueue_job = AsyncMock()
    if hasattr(service_class, 'execute_job'):
        mock.execute_job = AsyncMock()
    if hasattr(service_class, 'store_result'):
        mock.store_result = AsyncMock()
    if hasattr(service_class, 'get_result'):
        mock.get_result = AsyncMock()
    if hasattr(service_class, 'log_job_event'):
        mock.log_job_event = AsyncMock()
    if hasattr(service_class, 'log_worker_event'):
        mock.log_worker_event = AsyncMock()
    if hasattr(service_class, 'ensure_stream'):
        mock.ensure_stream = AsyncMock()
    if hasattr(service_class, 'put'):
        mock.put = AsyncMock()
    if hasattr(service_class, 'get'):
        mock.get = AsyncMock()
    if hasattr(service_class, 'delete'):
        mock.delete = AsyncMock()
    if hasattr(service_class, 'get_kv_store'):
        mock.get_kv_store = AsyncMock()
        
    return mock


def create_mock_service_manager() -> AsyncMock:
    """
    Create a mock ServiceManager with all services mocked.
    
    Returns:
        An AsyncMock instance configured as a ServiceManager.
    """
    manager = AsyncMock(spec=ServiceManager)
    manager._services = {}
    manager._service_configs = {}
    manager.has_service = MagicMock(return_value=True)
    manager.get_service = AsyncMock()
    
    # Create mock services
    mock_connection_service = create_mock_service(ConnectionService)
    mock_job_service = create_mock_service(JobService)
    mock_event_service = create_mock_service(EventService)
    mock_stream_service = create_mock_service(StreamService)
    mock_kv_store_service = create_mock_service(KVStoreService)
    mock_scheduler_service = create_mock_service(SchedulerService)
    
    # Configure service manager to return mock services
    async def mock_get_service(name: str, service_class: Optional[Type[BaseService]] = None):
        """Mock get_service method."""
        if name == "connection" and (service_class is None or issubclass(service_class, ConnectionService)):
            return mock_connection_service
        elif name == "jobs" and (service_class is None or issubclass(service_class, JobService)):
            return mock_job_service
        elif name == "events" and (service_class is None or issubclass(service_class, EventService)):
            return mock_event_service
        elif name == "stream" and (service_class is None or issubclass(service_class, StreamService)):
            return mock_stream_service
        elif name == "kv_store" and (service_class is None or issubclass(service_class, KVStoreService)):
            return mock_kv_store_service
        elif name == "scheduler" and (service_class is None or issubclass(service_class, SchedulerService)):
            return mock_scheduler_service
        else:
            raise ValueError(f"Unknown service: {name}")
    
    manager.get_service.side_effect = mock_get_service
    
    # Store mock services for direct access
    manager._mock_connection_service = mock_connection_service
    manager._mock_job_service = mock_job_service
    manager._mock_event_service = mock_event_service
    manager._mock_stream_service = mock_stream_service
    manager._mock_kv_store_service = mock_kv_store_service
    manager._mock_scheduler_service = mock_scheduler_service
    
    return manager


@pytest_asyncio.fixture
async def service_test_config() -> ServiceConfig:
    """
    Fixture providing a standard service configuration for testing.
    
    Returns:
        A ServiceConfig instance with test settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "test_mode": True,
            "auto_create_buckets": True,
            "enable_event_logging": True,
            "enable_job_execution": True,
            "enable_result_storage": True
        }
    )


@pytest_asyncio.fixture
async def mock_service_manager_fixture() -> AsyncMock:
    """
    Fixture providing a mock ServiceManager with all services mocked.
    
    Returns:
        An AsyncMock instance configured as a ServiceManager.
    """
    return create_mock_service_manager()


@pytest_asyncio.fixture
async def service_lifecycle_manager() -> AsyncContextManager[ServiceTestContext]:
    """
    Fixture providing a service lifecycle manager for testing.
    
    Returns:
        A ServiceTestContext class for creating service test contexts.
    """
    return ServiceTestContext


def assert_service_initialized(service: BaseService) -> None:
    """
    Assert that a service is properly initialized.
    
    Args:
        service: The service to check.
        
    Raises:
        AssertionError: If the service is not initialized.
    """
    assert service.is_initialized is True, f"Service {service.__class__.__name__} is not initialized"


def assert_service_cleaned_up(service: BaseService) -> None:
    """
    Assert that a service is properly cleaned up.
    
    Args:
        service: The service to check.
        
    Raises:
        AssertionError: If the service is still initialized.
    """
    assert service.is_initialized is False, f"Service {service.__class__.__name__} is still initialized"


def assert_service_dependency(service: BaseService, dependency_name: str, dependency_type: Type) -> None:
    """
    Assert that a service has a properly configured dependency.
    
    Args:
        service: The service to check.
        dependency_name: The name of the dependency attribute.
        dependency_type: The expected type of the dependency.
        
    Raises:
        AssertionError: If the dependency is not properly configured.
    """
    assert hasattr(service, dependency_name), f"Service {service.__class__.__name__} missing dependency: {dependency_name}"
    dependency = getattr(service, dependency_name)
    assert isinstance(dependency, dependency_type), f"Dependency {dependency_name} is not of type {dependency_type.__name__}"