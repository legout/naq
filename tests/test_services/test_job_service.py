"""
Job Service Tests

This module contains tests for the JobService, focusing on
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
from naq.services.jobs import JobService, JobServiceConfig
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService
from naq.services.events import EventService
from naq.exceptions import JobExecutionError, NaqException
from naq.models.jobs import Job, JobResult
from naq.models.enums import JOB_STATUS

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
async def job_service_config() -> ServiceConfig:
    """
    Fixture providing a configuration for JobService tests.
    
    Returns:
        A ServiceConfig instance with job-specific settings.
    """
    return ServiceConfig(
        nats_url="nats://localhost:4222",
        log_level="DEBUG",
        custom_settings={
            "results_bucket_name": "test_job_results",
            "default_result_ttl": 3600,
            "enable_job_execution": True,
            "enable_result_storage": True,
            "enable_event_logging": True,
            "auto_create_buckets": True,
            "max_job_execution_time": 300
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
async def mock_event_service() -> AsyncMock:
    """
    Fixture providing a mock EventService.
    
    Returns:
        An AsyncMock instance configured as an EventService.
    """
    return create_mock_service(EventService)


@pytest_asyncio.fixture
async def job_service_with_dependencies(
    job_service_config: ServiceConfig,
    mock_connection_service: AsyncMock,
    mock_kv_store_service: AsyncMock,
    mock_event_service: AsyncMock
) -> JobService:
    """
    Fixture providing a JobService instance with mocked dependencies.
    
    Args:
        job_service_config: Configuration for the service.
        mock_connection_service: Mock ConnectionService dependency.
        mock_kv_store_service: Mock KVStoreService dependency.
        mock_event_service: Mock EventService dependency.
        
    Returns:
        A JobService instance.
    """
    service = JobService(
        config=job_service_config,
        connection_service=mock_connection_service,
        kv_store_service=mock_kv_store_service,
        event_service=mock_event_service
    )
    try:
        await service.initialize()
        yield service
    finally:
        if service.is_initialized:
            await service.cleanup()


@pytest_asyncio.fixture
async def mock_job_service() -> AsyncMock:
    """
    Fixture providing a mock JobService.
    
    Returns:
        An AsyncMock instance configured as a JobService.
    """
    return create_mock_service(JobService)


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


class TestJobServiceLifecycle:
    """Test cases for JobService lifecycle management."""
    
    async def test_service_initialization_with_dependencies(
        self,
        job_service_with_dependencies: JobService
    ) -> None:
        """Test JobService initialization with dependencies."""
        service = job_service_with_dependencies
        assert_service_initialized(service)
        assert service.is_initialized is True
        assert service.job_config is not None
        assert service.job_config.enable_job_execution is True
        assert service.job_config.enable_result_storage is True
        assert service.job_config.enable_event_logging is True
    
    async def test_service_initialization_without_dependencies(
        self,
        job_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test JobService initialization without all dependencies."""
        # Only provide connection service, others should be created automatically
        service = JobService(
            config=job_service_config,
            connection_service=mock_connection_service
        )
        
        try:
            await service.initialize()
            assert_service_initialized(service)
            assert service.is_initialized is True
            assert service._kv_store_service is not None
            assert service._event_service is not None
        finally:
            if service.is_initialized:
                await service.cleanup()
    
    async def test_service_cleanup(
        self,
        job_service_with_dependencies: JobService
    ) -> None:
        """Test JobService cleanup."""
        service = job_service_with_dependencies
        assert service.is_initialized is True
        
        await service.cleanup()
        assert_service_cleaned_up(service)
        assert service.is_initialized is False
    
    async def test_service_context_manager(
        self,
        job_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        mock_kv_store_service: AsyncMock,
        mock_event_service: AsyncMock
    ) -> None:
        """Test JobService as a context manager."""
        async with JobService(
            config=job_service_config,
            connection_service=mock_connection_service,
            kv_store_service=mock_kv_store_service,
            event_service=mock_event_service
        ) as service:
            assert_service_initialized(service)
            assert service.is_initialized is True
        
        assert service.is_initialized is False
    
    async def test_service_initialization_with_invalid_config(self) -> None:
        """Test JobService initialization with invalid configuration."""
        invalid_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG",
            custom_settings={
                "default_result_ttl": -1,  # Invalid TTL should cause initialization to fail
                "enable_job_execution": True
            }
        )
        
        with pytest.raises(ServiceInitializationError):
            async with ServiceTestContext(
                JobService,
                invalid_config,
                connection_service=create_mock_service(ConnectionService)
            ):
                pass
    
    async def test_service_initialization_failure_tracking(
        self,
        failing_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that JobService initialization failures are tracked correctly."""
        await test_service_initialization_failure(
            JobService,
            failing_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_cleanup_failure_tracking(
        self,
        job_service_config: ServiceConfig,
        service_lifecycle_tracker: ServiceLifecycleTracker
    ) -> None:
        """Test that JobService cleanup failures are tracked correctly."""
        await test_service_cleanup_failure(
            JobService,
            job_service_config,
            service_lifecycle_tracker
        )
    
    async def test_service_lifecycle_with_harness(
        self,
        job_service_config: ServiceConfig,
        mock_connection_service: AsyncMock,
        service_lifecycle_harness: ServiceLifecycleTestHarness
    ) -> None:
        """Test JobService lifecycle using the test harness."""
        # Create and initialize service
        service = await service_lifecycle_harness.create_service(
            JobService,
            job_service_config,
            connection_service=mock_connection_service
        )
        await service.initialize()
        
        # Verify initialization
        service_lifecycle_harness.assert_all_services_initialized()
        service_lifecycle_harness.assert_service_lifecycle(JobService)
        
        # Cleanup
        await service_lifecycle_harness.cleanup_all_services()
        service_lifecycle_harness.assert_all_services_cleaned_up()


class TestJobServiceConfiguration:
    """Test cases for JobService configuration handling."""
    
    async def test_default_configuration(self) -> None:
        """Test JobService with default configuration."""
        service = JobService()
        
        # Check that default configuration is applied
        assert service.job_config is not None
        assert service.job_config.results_bucket_name == "naq_job_results"
        assert service.job_config.default_result_ttl == 86400
        assert service.job_config.enable_job_execution is True
        assert service.job_config.enable_result_storage is True
        assert service.job_config.enable_event_logging is True
        assert service.job_config.auto_create_buckets is True
        assert service.job_config.max_job_execution_time is None
    
    async def test_custom_configuration(self, job_service_config: ServiceConfig) -> None:
        """Test JobService with custom configuration."""
        service = JobService(config=job_service_config)
        
        # Check that custom configuration is applied
        assert service.job_config is not None
        assert service.job_config.results_bucket_name == "test_job_results"
        assert service.job_config.default_result_ttl == 3600
        assert service.job_config.enable_job_execution is True
        assert service.job_config.enable_result_storage is True
        assert service.job_config.enable_event_logging is True
        assert service.job_config.auto_create_buckets is True
        assert service.job_config.max_job_execution_time == 300
    
    async def test_configuration_property(self, job_service_config: ServiceConfig) -> None:
        """Test JobService configuration property."""
        service = JobService(config=job_service_config)
        
        # Check that the config property returns the correct configuration
        assert service.config == job_service_config
        
        # Update the configuration
        new_config = ServiceConfig(nats_url="nats://localhost:4223")
        service.config = new_config
        assert service.config == new_config


class TestJobServiceDependencies:
    """Test cases for JobService dependencies."""
    
    async def test_connection_service_dependency(
        self,
        job_service_with_dependencies: JobService,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that JobService has a connection service dependency."""
        service = job_service_with_dependencies
        assert_service_dependency(service, '_connection_service', AsyncMock)
        assert service._connection_service == mock_connection_service
    
    async def test_kv_store_service_dependency(
        self,
        job_service_with_dependencies: JobService,
        mock_kv_store_service: AsyncMock
    ) -> None:
        """Test that JobService has a KV store service dependency."""
        service = job_service_with_dependencies
        assert_service_dependency(service, '_kv_store_service', AsyncMock)
        assert service._kv_store_service == mock_kv_store_service
    
    async def test_event_service_dependency(
        self,
        job_service_with_dependencies: JobService,
        mock_event_service: AsyncMock
    ) -> None:
        """Test that JobService has an event service dependency."""
        service = job_service_with_dependencies
        assert_service_dependency(service, '_event_service', AsyncMock)
        assert service._event_service == mock_event_service
    
    async def test_dependency_creation(
        self,
        job_service_config: ServiceConfig,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that JobService creates missing dependencies."""
        service = JobService(
            config=job_service_config,
            connection_service=mock_connection_service
        )
        
        try:
            await service.initialize()
            
            # Check that KV store service was created
            assert service._kv_store_service is not None
            assert service._kv_store_service.is_initialized is True
            
            # Check that event service was created
            assert service._event_service is not None
            assert service._event_service.is_initialized is True
        finally:
            if service.is_initialized:
                await service.cleanup()


class TestJobServiceProperties:
    """Test cases for JobService properties."""
    
    async def test_job_config_property(self, job_service_config: ServiceConfig) -> None:
        """Test JobService job_config property."""
        service = JobService(config=job_service_config)
        
        # Check that the job_config property returns the correct configuration
        assert service.job_config is not None
        assert isinstance(service.job_config, JobServiceConfig)
        assert service.job_config.results_bucket_name == "test_job_results"
    
    async def test_is_job_execution_enabled_property(self, job_service_config: ServiceConfig) -> None:
        """Test JobService is_job_execution_enabled property."""
        service = JobService(config=job_service_config)
        
        # Check that the property returns the correct value
        assert service.is_job_execution_enabled is True
        
        # Update the configuration
        service._job_config.enable_job_execution = False
        assert service.is_job_execution_enabled is False
    
    async def test_is_result_storage_enabled_property(self, job_service_config: ServiceConfig) -> None:
        """Test JobService is_result_storage_enabled property."""
        service = JobService(config=job_service_config)
        
        # Check that the property returns the correct value
        assert service.is_result_storage_enabled is True
        
        # Update the configuration
        service._job_config.enable_result_storage = False
        assert service.is_result_storage_enabled is False
    
    async def test_is_event_logging_enabled_property(self, job_service_config: ServiceConfig) -> None:
        """Test JobService is_event_logging_enabled property."""
        service = JobService(config=job_service_config)
        
        # Check that the property returns the correct value
        assert service.is_event_logging_enabled is True
        
        # Update the configuration
        service._job_config.enable_event_logging = False
        assert service.is_event_logging_enabled is False


class TestJobServiceMocking:
    """Test cases for mocking JobService."""
    
    async def test_mock_service_creation(self, mock_job_service: AsyncMock) -> None:
        """Test that mock JobService is created correctly."""
        assert mock_job_service is not None
        assert mock_job_service._is_initialized is True
        assert mock_job_service.initialize is not None
        assert mock_job_service.cleanup is not None
        assert mock_job_service.enqueue_job is not None
        assert mock_job_service.execute_job is not None
        assert mock_job_service.store_result is not None
        assert mock_job_service.get_result is not None
    
    async def test_mock_service_methods(self, mock_job_service: AsyncMock, test_job: Job) -> None:
        """Test that mock JobService methods work correctly."""
        # Test that mock methods can be called
        await mock_job_service.initialize()
        await mock_job_service.cleanup()
        await mock_job_service.enqueue_job(test_job, "test.subject")
        await mock_job_service.execute_job(test_job)
        await mock_job_service.store_result("test-job-123", JobResult.from_job(test_job))
        await mock_job_service.get_result("test-job-123")
        
        # Verify that the methods were called
        mock_job_service.initialize.assert_called_once()
        mock_job_service.cleanup.assert_called_once()
        mock_job_service.enqueue_job.assert_called_once()
        mock_job_service.execute_job.assert_called_once()
        mock_job_service.store_result.assert_called_once()
        mock_job_service.get_result.assert_called_once()
    
    async def test_mock_service_with_dependencies(
        self,
        mock_job_service: AsyncMock,
        mock_connection_service: AsyncMock
    ) -> None:
        """Test that mock JobService can be used with dependencies."""
        # Create a service that depends on the mock job service
        from naq.services.worker import WorkerService
        
        worker_service_config = ServiceConfig(
            nats_url="nats://localhost:4222",
            log_level="DEBUG"
        )
        
        # This is just an example of how a service might depend on JobService
        # In a real scenario, you would create a service that actually uses JobService
        assert mock_job_service is not None
        assert mock_connection_service is not None