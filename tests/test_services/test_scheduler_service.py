"""
Scheduler Service Tests

This module contains tests for the SchedulerService, focusing on
service lifecycle management and basic functionality.
"""
from pathlib import Path
import sys
sys.path.append(Path(__file__).parent)
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional
from datetime import datetime, timedelta

from naq.services.base import ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from naq.services.scheduler import SchedulerService, SchedulerServiceConfig
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
async def scheduler_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for SchedulerService tests.
    
    Returns:
        A ServiceConfig instance with scheduler-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "max_concurrent_jobs": 10,
            "job_timeout": 300,
            "enable_retry": True,
            "max_retries": 3,
            "retry_delay": 60
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
async def scheduler_service_with_dependencies(
    scheduler_service_config: ServiceConfig,
    mock_connection_service: AsyncMock
) -> SchedulerService:
    """
    Fixture providing a SchedulerService instance with mocked dependencies.
    
    Args:
        scheduler_service_config: Configuration for the service.
        mock_connection_service: Mock ConnectionService dependency.
        
    Returns:
        A SchedulerService instance.
    """
    service = SchedulerService(
        config=scheduler_service_config,
        connection_service=mock_connection_service
    )
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_scheduler_service() -> AsyncMock:
    """
    Fixture providing a mock SchedulerService.
    
    Returns:
        An AsyncMock instance configured as a SchedulerService.
    """
    return create_mock_service(SchedulerService)


@pytest_asyncio.fixture
async def test_job_config() -> Dict[str, Any]:
    """
    Fixture providing test job configuration.
    
    Returns:
        A dictionary with test job configuration.
    """
    return {
        "name": "test-job",
        "schedule": "0 0 * * *",  # Daily at midnight
        "payload": {"task": "test", "data": "example"},
        "enabled": True,
        "retry_policy": {
            "max_retries": 3,
            "retry_delay": 60
        }
    }


class TestSchedulerServiceLifecycle:
    """Test cases for SchedulerService lifecycle management."""
    
    async def test_service_initialization_with_dependencies(
        self,
        scheduler_service_with_dependencies: SchedulerService
    ) -> None:
        """Test SchedulerService initialization with dependencies."""
        service = scheduler_service_with_dependencies
        assert_service_initialized(service)
        assert service.is_initialized is True
        assert service.scheduler_config is not None
        assert service.scheduler_config.max_concurrent_jobs == 10
        assert service.scheduler_config.job_timeout == 300
        assert service.scheduler_config.enable_retry is True
        assert service.scheduler_config.max_retries == 3
        assert service.scheduler_config.retry_delay == 60
    
    async def test_service_cleanup(
        self,
        scheduler_service_with_dependencies: SchedulerService
    ) -> None:
        """Test SchedulerService cleanup."""
        service = scheduler_service_with_dependencies
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(
        self,
        scheduler_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test SchedulerService as a context manager."""
        async with SchedulerService(
            config=scheduler_service_config,
            connection_service=mock_connection_service
        ) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test SchedulerService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "max_concurrent_jobs": -1,  # Invalid value should cause initialization to fail
                "enable_retry": True
            }
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(
                SchedulerService,
                invalid_config,
                connection_service=create_mock_service(ConnectionService)
            ):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that SchedulerService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            SchedulerService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        scheduler_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that SchedulerService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            SchedulerService,
            scheduler_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        scheduler_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test SchedulerService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            SchedulerService,
            scheduler_service_config,
            connection_service=mock_connection_service
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(SchedulerService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestSchedulerServiceConfiguration:
    """Test cases for SchedulerService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test SchedulerService with default configuration."""
        service = SchedulerService()
        
        # Check that default configuration is applied
        assert service.scheduler_config is not None
        assert service.scheduler_config.max_concurrent_jobs == 10
        assert service.scheduler_config.job_timeout == 300
        assert service.scheduler_config.enable_retry is True
        assert service.scheduler_config.max_retries == 3
        assert service.scheduler_config.retry_delay == 60
    
    async def test_custom_configuration(self, scheduler_service_config: ServiceConfig) -> None:
        """Test SchedulerService with custom configuration."""
        service = SchedulerService(config=scheduler_service_config)
        
        # Check that custom configuration is applied
        assert service.scheduler_config is not None
        assert service.scheduler_config.max_concurrent_jobs == 10
        assert service.scheduler_config.job_timeout == 300
        assert service.scheduler_config.enable_retry is True
        assert service.scheduler_config.max_retries == 3
        assert service.scheduler_config.retry_delay == 60
    
    async def test_configuration_property(self, scheduler_service_config: ServiceConfig) -> None:
        """Test SchedulerService configuration property."""
        service = SchedulerService(config=scheduler_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == scheduler_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestSchedulerServiceDependencies:
    """Test cases for SchedulerService dependencies."""
    
    async def test_connection_service_dependency(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that SchedulerService has a connection service dependency."""
        service = scheduler_service_with_dependencies
        assert_service_dependency(service, '_connection_service', AsyncMock)
        assert service._connection_service == mock_connection_service


class TestSchedulerServiceProperties:
    """Test cases for SchedulerService properties."""
    
    async def test_scheduler_config_property(self, scheduler_service_config: ServiceConfig) -> None:
        """Test SchedulerService scheduler_config property."""
        service = SchedulerService(config=scheduler_service_config)
        
        # Check that the scheduler_config property returns the correct configuration
        assert service.scheduler_config is not None
        assert isinstance(service.scheduler_config, SchedulerServiceConfig)
        assert service.scheduler_config.max_concurrent_jobs == 10


class TestSchedulerServiceMocking:
    """Test cases for mocking SchedulerService."""
    
    async def test_mock_service_creation(self, mock_scheduler_service: AsyncMock) -> None:
        """Test that mock SchedulerService is created correctly."""
        assert mock_scheduler_service is not None
        assert mock_scheduler_service._is_initialized is True
        assert mock_scheduler_service.initialize is not None
        assert mock_scheduler_service.cleanup is not None
        assert mock_scheduler_service.schedule_job is not None
        assert mock_scheduler_service.cancel_job is not None
        assert mock_scheduler_service.get_job_status is not None
    
    async def test_mock_service_methods(
        self,
        mock_scheduler_service: AsyncMock,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test that mock SchedulerService methods work correctly."""
        # Test that mock methods can be called
        await mock_scheduler_service.initialize()
        await mock_scheduler_service.cleanup()
        await mock_scheduler_service.schedule_job(test_job_config["name"], test_job_config)
        await mock_scheduler_service.cancel_job(test_job_config["name"])
        await mock_scheduler_service.get_job_status(test_job_config["name"])
        
        # Verify that the methods were called
        mock_scheduler_service.initialize.assert_called_once()
        mock_scheduler_service.cleanup.assert_called_once()
        mock_scheduler_service.schedule_job.assert_called_once()
        mock_scheduler_service.cancel_job.assert_called_once()
        mock_scheduler_service.get_job_status.assert_called_once()
    
    async def test_mock_service_with_dependencies(
        self,
        mock_scheduler_service: AsyncMock,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that mock SchedulerService can be used with dependencies."""
        # Create a service that depends on the mock scheduler service
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
        
        # This is just an example of how a service might depend on SchedulerService
        # In a real scenario, you would create a service that actually uses SchedulerService
        assert mock_scheduler_service is not None
        assert mock_connection_service is not None


class TestSchedulerServiceBasicOperations:
    """Test cases for basic SchedulerService operations."""
    
    async def test_schedule_job_operation(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test schedule_job operation."""
        service = scheduler_service_with_dependencies
        
        # Schedule a job
        job_id = await service.schedule_job(test_job_config["name"], test_job_config)
        
        # Verify that the job was scheduled (this would require actual NATS connection in real tests)
        # In mock tests, we just verify that the method was called on the mock
        assert job_id is not None
        assert service._connection_service.get_jetstream.called
    
    async def test_cancel_job_operation(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test cancel_job operation."""
        service = scheduler_service_with_dependencies
        
        # First schedule a job
        job_id = await service.schedule_job(test_job_config["name"], test_job_config)
        
        # Cancel the job
        result = await service.cancel_job(job_id)
        
        # Verify that the job was cancelled
        assert result is True
    
    async def test_get_job_status_operation(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test get_job_status operation."""
        service = scheduler_service_with_dependencies
        
        # First schedule a job
        job_id = await service.schedule_job(test_job_config["name"], test_job_config)
        
        # Get job status
        job_status = await service.get_job_status(job_id)
        
        # Verify that job status was returned (in mock tests, this will be the mock return value)
        assert job_status is not None
        assert job_status.get("job_id") == job_id
    
    async def test_schedule_job_with_defaults(
        self,
        scheduler_service_with_dependencies: SchedulerService
    ) -> None:
        """Test schedule_job operation with default configuration."""
        service = scheduler_service_with_dependencies
        
        # Schedule a job with minimal configuration
        job_id = await service.schedule_job("test-default-job", {"name": "test-default-job"})
        
        # Verify that the job was scheduled
        assert job_id is not None
        assert service._connection_service.get_jetstream.called
    
    async def test_schedule_job_with_custom_config(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test schedule_job operation with custom configuration."""
        service = scheduler_service_with_dependencies
        
        # Schedule a job with custom configuration
        job_id = await service.schedule_job(test_job_config["name"], test_job_config)
        
        # Verify that the job was scheduled with the correct configuration
        assert job_id is not None
        assert service._connection_service.get_jetstream.called
    
    async def test_cancel_nonexistent_job(
        self,
        scheduler_service_with_dependencies: SchedulerService
    ) -> None:
        """Test cancel_job operation for a non-existent job."""
        service = scheduler_service_with_dependencies
        
        # Try to cancel a non-existent job
        result = await service.cancel_job("nonexistent-job-id")
        
        # Verify that False is returned for non-existent jobs
        assert result is False
    
    async def test_get_status_for_nonexistent_job(
        self,
        scheduler_service_with_dependencies: SchedulerService
    ) -> None:
        """Test get_job_status operation for a non-existent job."""
        service = scheduler_service_with_dependencies
        
        # Try to get status for a non-existent job
        job_status = await service.get_job_status("nonexistent-job-id")
        
        # Verify that None is returned for non-existent jobs
        assert job_status is None
    
    async def test_list_scheduled_jobs(
        self,
        scheduler_service_with_dependencies: SchedulerService,
        test_job_config: Dict[str, Any]
    ) -> None:
        """Test list_scheduled_jobs operation."""
        service = scheduler_service_with_dependencies
        
        # Schedule a few jobs
        job_id1 = await service.schedule_job("job1", test_job_config)
        job_id2 = await service.schedule_job("job2", test_job_config)
        
        # List scheduled jobs
        jobs = await service.list_scheduled_jobs()
        
        # Verify that the scheduled jobs are returned
        assert jobs is not None
        assert len(jobs) >= 2  # At least the two jobs we scheduled
        job_ids = [job.get("job_id") for job in jobs]
        assert job_id1 in job_ids
        assert job_id2 in job_ids