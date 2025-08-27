"""
Service Lifecycle Testing Utilities

This module provides utilities for testing service lifecycle management,
including initialization, cleanup, and error handling scenarios.
"""

import pytest
import pytest_asyncio
from typing import Any, Dict, Optional, Type, List, Callable, Awaitable
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

from naq.services.base import BaseService, ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.services.scheduler import SchedulerService


class ServiceLifecycleTracker:
    """
    Tracker for service lifecycle events during testing.
    
    This class monitors and records lifecycle events for services,
    allowing tests to verify that initialization and cleanup
    occur in the expected order and with the expected behavior.
    """
    
    def __init__(self) -> None:
        """Initialize the lifecycle tracker."""
        self.events: List[Dict[str, Any]] = []
        self.errors: List[Exception] = []
        
    def record_event(self, event_type: str, service_name: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a lifecycle event.
        
        Args:
            event_type: Type of event (e.g., 'initialize_start', 'initialize_end').
            service_name: Name of the service.
            details: Optional additional details about the event.
        """
        self.events.append({
            "type": event_type,
            "service": service_name,
            "details": details or {},
            "timestamp": asyncio.get_event_loop().time()
        })
        
    def record_error(self, error: Exception, service_name: str) -> None:
        """
        Record a lifecycle error.
        
        Args:
            error: The exception that occurred.
            service_name: Name of the service that encountered the error.
        """
        self.errors.append({
            "error": error,
            "service": service_name,
            "timestamp": asyncio.get_event_loop().time()
        })
        
    def get_events_for_service(self, service_name: str) -> List[Dict[str, Any]]:
        """
        Get all events for a specific service.
        
        Args:
            service_name: Name of the service.
            
        Returns:
            List of events for the service.
        """
        return [event for event in self.events if event["service"] == service_name]
        
    def get_errors_for_service(self, service_name: str) -> List[Dict[str, Any]]:
        """
        Get all errors for a specific service.
        
        Args:
            service_name: Name of the service.
            
        Returns:
            List of errors for the service.
        """
        return [error for error in self.errors if error["service"] == service_name]
        
    def clear(self) -> None:
        """Clear all recorded events and errors."""
        self.events.clear()
        self.errors.clear()
        
    def assert_initialization_sequence(self, service_name: str) -> None:
        """
        Assert that a service was properly initialized.
        
        Args:
            service_name: Name of the service.
            
        Raises:
            AssertionError: If the initialization sequence is invalid.
        """
        events = self.get_events_for_service(service_name)
        
        # Check that initialization started
        init_start_events = [e for e in events if e["type"] == "initialize_start"]
        assert len(init_start_events) == 1, f"Expected exactly one initialize_start event for {service_name}"
        
        # Check that initialization ended
        init_end_events = [e for e in events if e["type"] == "initialize_end"]
        assert len(init_end_events) == 1, f"Expected exactly one initialize_end event for {service_name}"
        
        # Check that initialization ended after it started
        assert init_start_events[0]["timestamp"] < init_end_events[0]["timestamp"], \
            f"Initialization for {service_name} ended before it started"
            
    def assert_cleanup_sequence(self, service_name: str) -> None:
        """
        Assert that a service was properly cleaned up.
        
        Args:
            service_name: Name of the service.
            
        Raises:
            AssertionError: If the cleanup sequence is invalid.
        """
        events = self.get_events_for_service(service_name)
        
        # Check that cleanup started
        cleanup_start_events = [e for e in events if e["type"] == "cleanup_start"]
        assert len(cleanup_start_events) == 1, f"Expected exactly one cleanup_start event for {service_name}"
        
        # Check that cleanup ended
        cleanup_end_events = [e for e in events if e["type"] == "cleanup_end"]
        assert len(cleanup_end_events) == 1, f"Expected exactly one cleanup_end event for {service_name}"
        
        # Check that cleanup ended after it started
        assert cleanup_start_events[0]["timestamp"] < cleanup_end_events[0]["timestamp"], \
            f"Cleanup for {service_name} ended before it started"


class MonitoredService(BaseService):
    """
    A service wrapper that monitors lifecycle events for testing.
    
    This class wraps another service and tracks all lifecycle events
    using a ServiceLifecycleTracker.
    """
    
    def __init__(
        self,
        service_class: Type[BaseService],
        tracker: ServiceLifecycleTracker,
        config: Optional[ServiceConfig] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize the monitored service.
        
        Args:
            service_class: The service class to wrap.
            tracker: The lifecycle tracker to use.
            config: Optional configuration for the service.
            **kwargs: Additional arguments for the service constructor.
        """
        super().__init__(config)
        self._service_class = service_class
        self._tracker = tracker
        self._service_kwargs = kwargs
        self._wrapped_service: Optional[BaseService] = None
        
    async def _do_initialize(self) -> None:
        """Initialize the wrapped service and track the event."""
        service_name = self._service_class.__name__
        self._tracker.record_event("initialize_start", service_name)
        
        try:
            # Create and initialize the wrapped service
            self._wrapped_service = self._service_class(
                config=self._config,
                **self._service_kwargs
            )
            await self._wrapped_service.initialize()
            
            self._tracker.record_event("initialize_end", service_name)
            
        except Exception as e:
            self._tracker.record_error(e, service_name)
            raise
            
    async def _do_cleanup(self) -> None:
        """Clean up the wrapped service and track the event."""
        if self._wrapped_service is None:
            return
            
        service_name = self._service_class.__name__
        self._tracker.record_event("cleanup_start", service_name)
        
        try:
            await self._wrapped_service.cleanup()
            self._tracker.record_event("cleanup_end", service_name)
            
        except Exception as e:
            self._tracker.record_error(e, service_name)
            raise


class ServiceLifecycleTestHarness:
    """
    Test harness for service lifecycle testing.
    
    This class provides a comprehensive testing environment for
    service lifecycle management, including error scenarios and
    dependency management.
    """
    
    def __init__(self) -> None:
        """Initialize the test harness."""
        self.tracker = ServiceLifecycleTracker()
        self.services: List[BaseService] = []
        
    async def create_service(
        self,
        service_class: Type[BaseService],
        config: Optional[ServiceConfig] = None,
        **kwargs: Any
    ) -> MonitoredService:
        """
        Create a monitored service instance.
        
        Args:
            service_class: The service class to create.
            config: Optional configuration for the service.
            **kwargs: Additional arguments for the service constructor.
            
        Returns:
            A monitored service instance.
        """
        service = MonitoredService(service_class, self.tracker, config, **kwargs)
        self.services.append(service)
        return service
        
    async def initialize_all_services(self) -> None:
        """Initialize all created services."""
        for service in self.services:
            await service.initialize()
            
    async def cleanup_all_services(self) -> None:
        """Clean up all created services."""
        for service in reversed(self.services):
            if service.is_initialized:
                await service.cleanup()
                
    def assert_all_services_initialized(self) -> None:
        """Assert that all services are initialized."""
        for service in self.services:
            assert service.is_initialized, f"Service {service.__class__.__name__} is not initialized"
            
    def assert_all_services_cleaned_up(self) -> None:
        """Assert that all services are cleaned up."""
        for service in self.services:
            assert not service.is_initialized, f"Service {service.__class__.__name__} is still initialized"
            
    def assert_service_lifecycle(self, service_class: Type[BaseService]) -> None:
        """
        Assert that a service had a proper lifecycle.
        
        Args:
            service_class: The service class to check.
        """
        service_name = service_class.__name__
        self.tracker.assert_initialization_sequence(service_name)
        self.tracker.assert_cleanup_sequence(service_name)
        
    def clear(self) -> None:
        """Clear the test harness."""
        self.tracker.clear()
        self.services.clear()


@pytest_asyncio.fixture
async def service_lifecycle_tracker() -> ServiceLifecycleTracker:
    """
    Fixture providing a service lifecycle tracker.
    
    Returns:
        A ServiceLifecycleTracker instance.
    """
    return ServiceLifecycleTracker()


@pytest_asyncio.fixture
async def service_lifecycle_harness() -> ServiceLifecycleTestHarness:
    """
    Fixture providing a service lifecycle test harness.
    
    Returns:
        A ServiceLifecycleTestHarness instance.
    """
    return ServiceLifecycleTestHarness()


@pytest_asyncio.fixture
async def failing_service_config() -> ServiceConfig:
    """
    Fixture providing a service configuration that will cause initialization to fail.
    
    Returns:
        A ServiceConfig instance that will cause initialization failure.
    """
    return ServiceConfig(
        nats_url="nats://invalid:4222",  # Invalid URL to cause connection failure
        log_level="DEBUG",
        custom_settings={
            "test_mode": True,
            "force_initialization_failure": True
        }
    )


async def test_service_initialization_failure(
    service_class: Type[BaseService],
    failing_service_config: ServiceConfig,
    service_lifecycle_tracker: ServiceLifecycleTracker
) -> None:
    """
    Test that service initialization failures are handled correctly.
    
    Args:
        service_class: The service class to test.
        failing_service_config: Configuration that will cause initialization to fail.
        service_lifecycle_tracker: Tracker for lifecycle events.
    """
    service = MonitoredService(service_class, service_lifecycle_tracker, failing_service_config)
    
    # Attempt to initialize (should fail)
    with pytest.raises(ServiceInitializationError):
        await service.initialize()
    
    # Verify that the error was recorded
    errors = service_lifecycle_tracker.get_errors_for_service(service_class.__name__)
    assert len(errors) == 1, "Expected exactly one error to be recorded"
    assert isinstance(errors[0]["error"], ServiceInitializationError), \
        "Expected ServiceInitializationError to be recorded"


async def test_service_cleanup_failure(
    service_class: Type[BaseService],
    service_test_config: ServiceConfig,
    service_lifecycle_tracker: ServiceLifecycleTracker
) -> None:
    """
    Test that service cleanup failures are handled correctly.
    
    Args:
        service_class: The service class to test.
        service_test_config: Configuration for the service.
        service_lifecycle_tracker: Tracker for lifecycle events.
    """
    service = MonitoredService(service_class, service_lifecycle_tracker, service_test_config)
    
    # Initialize successfully
    await service.initialize()
    assert service.is_initialized
    
    # Mock the cleanup method to raise an exception
    if service._wrapped_service:
        original_cleanup = service._wrapped_service.cleanup
        async def failing_cleanup():
            raise ServiceRuntimeError("Simulated cleanup failure")
        service._wrapped_service.cleanup = failing_cleanup
    
    # Attempt to cleanup (should fail)
    with pytest.raises(ServiceRuntimeError):
        await service.cleanup()
    
    # Verify that the error was recorded
    errors = service_lifecycle_tracker.get_errors_for_service(service_class.__name__)
    assert len(errors) == 1, "Expected exactly one error to be recorded"
    assert isinstance(errors[0]["error"], ServiceRuntimeError), \
        "Expected ServiceRuntimeError to be recorded"