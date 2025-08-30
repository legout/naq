"""
Unit tests for the JobService.

This module contains comprehensive unit tests for the JobService class,
testing job execution, result storage, event logging, and failure handling.
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch

from naq.exceptions import JobExecutionError, NaqException
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType, JOB_STATUS
from naq.models.jobs import Job, JobResult
from naq.services.base import ServiceConfig
from naq.services.connection import ConnectionService
from naq.services.events import EventService, EventServiceConfig
from naq.services.jobs import JobService, JobServiceConfig
from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig


@pytest.fixture
def mock_connection_service():
    """Create a mock ConnectionService."""
    service = MagicMock(spec=ConnectionService)
    service.is_initialized = True
    return service


@pytest.fixture
def mock_kv_store_service():
    """Create a mock KVStoreService."""
    service = MagicMock(spec=KVStoreService)
    service.is_initialized = True
    service.put = AsyncMock()
    service.get = AsyncMock()
    service.delete = AsyncMock()
    return service


@pytest.fixture
def mock_event_service():
    """Create a mock EventService."""
    service = MagicMock(spec=EventService)
    service.is_initialized = True
    service.log_job_event = AsyncMock()
    service.log_worker_event = AsyncMock()
    return service


@pytest.fixture
def job_service_config():
    """Create a JobServiceConfig for testing."""
    return JobServiceConfig(
        enable_job_execution=True,
        enable_result_storage=True,
        enable_event_logging=True,
        auto_create_buckets=True,
        max_job_execution_time=30
    )


@pytest.fixture
def job_service(mock_connection_service, mock_kv_store_service, mock_event_service, job_service_config):
    """Create a JobService instance for testing."""
    service_config = ServiceConfig(custom_settings=job_service_config.as_dict())
    return JobService(
        config=service_config,
        connection_service=mock_connection_service,
        kv_store_service=mock_kv_store_service,
        event_service=mock_event_service
    )


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def test_function(x, y):
        return x + y
    
    return Job(
        function=test_function,
        args=(2, 3),
        queue_name="test_queue",
        max_retries=2,
        retry_delay=1.0
    )


@pytest.fixture
def failing_job():
    """Create a job that fails for testing."""
    def failing_function():
        raise ValueError("Test error")
    
    return Job(
        function=failing_function,
        queue_name="test_queue",
        max_retries=1,
        retry_delay=1.0
    )


class TestJobServiceInitialization:
    """Test JobService initialization and configuration."""

    @pytest.mark.asyncio
    async def test_initialize_with_all_services(self, job_service):
        """Test initialization with all services provided."""
        await job_service.initialize()
        assert job_service.is_initialized
        assert job_service.is_job_execution_enabled
        assert job_service.is_result_storage_enabled
        assert job_service.is_event_logging_enabled

    @pytest.mark.asyncio
    async def test_initialize_with_connection_service_only(self, mock_connection_service):
        """Test initialization with only ConnectionService provided."""
        config = JobServiceConfig()
        service_config = ServiceConfig(custom_settings=config.as_dict())
        job_service = JobService(
            config=service_config,
            connection_service=mock_connection_service
        )
        
        with patch('naq.services.jobs.KVStoreService') as mock_kv_class, \
             patch('naq.services.jobs.EventService') as mock_event_class:
            
            mock_kv_instance = AsyncMock()
            mock_kv_class.return_value = mock_kv_instance
            mock_event_instance = AsyncMock()
            mock_event_class.return_value = mock_event_instance
            
            await job_service.initialize()
            
            mock_kv_class.assert_called_once()
            mock_event_class.assert_called_once()
            mock_kv_instance.initialize.assert_called_once()
            mock_event_instance.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_with_invalid_config(self, mock_connection_service):
        """Test initialization with invalid configuration."""
        config = JobServiceConfig(default_result_ttl=-1)
        service_config = ServiceConfig(custom_settings=config.as_dict())
        job_service = JobService(
            config=service_config,
            connection_service=mock_connection_service
        )
        
        with pytest.raises(Exception):  # Should raise ServiceInitializationError
            await job_service.initialize()

    @pytest.mark.asyncio
    async def test_initialize_without_required_services(self):
        """Test initialization without required services."""
        job_service = JobService()
        
        with pytest.raises(Exception):  # Should raise ServiceInitializationError
            await job_service.initialize()


class TestJobExecution:
    """Test job execution functionality."""

    @pytest.mark.asyncio
    async def test_execute_job_success(self, job_service, sample_job, mock_event_service):
        """Test successful job execution."""
        await job_service.initialize()
        
        result = await job_service.execute_job(sample_job, "test-worker")
        
        assert result.job_id == sample_job.job_id
        assert result.result == 5  # 2 + 3
        assert result.status == JOB_STATUS.COMPLETED.value
        
        # Verify events were logged
        mock_event_service.log_job_event.assert_called()
        assert mock_event_service.log_job_event.call_count == 2  # started + completed

    @pytest.mark.asyncio
    async def test_execute_job_with_timeout(self, job_service, mock_event_service):
        """Test job execution with timeout."""
        async def slow_function():
            await asyncio.sleep(0.1)
            return "done"
        
        slow_job = Job(
            function=slow_function,
            queue_name="test_queue",
            max_retries=0
        )
        
        # Set a very short timeout
        job_service._job_config.max_job_execution_time = 0.01
        
        await job_service.initialize()
        
        with pytest.raises(JobExecutionError, match="timed out"):
            await job_service.execute_job(slow_job, "test-worker")

    @pytest.mark.asyncio
    async def test_execute_job_disabled(self, job_service, sample_job):
        """Test job execution when disabled."""
        job_service._job_config.enable_job_execution = False
        await job_service.initialize()
        
        with pytest.raises(JobExecutionError, match="Job execution is disabled"):
            await job_service.execute_job(sample_job, "test-worker")

    @pytest.mark.asyncio
    async def test_execute_job_without_worker_id(self, job_service, sample_job, mock_event_service):
        """Test job execution without providing worker_id."""
        await job_service.initialize()
        
        result = await job_service.execute_job(sample_job)
        
        assert result.job_id == sample_job.job_id
        assert result.result == 5
        
        # Verify events were logged with default worker_id
        mock_event_service.log_job_event.assert_called()
        call_args = mock_event_service.log_job_event.call_args[0][0]
        assert call_args.worker_id == "unknown-worker"


class TestResultStorage:
    """Test result storage functionality."""

    @pytest.mark.asyncio
    async def test_store_result(self, job_service, mock_kv_store_service):
        """Test storing job results."""
        await job_service.initialize()
        
        job_result = JobResult(
            job_id="test-job-id",
            status=JOB_STATUS.COMPLETED.value,
            result="test result"
        )
        
        await job_service.store_result("test-job-id", job_result)
        
        mock_kv_store_service.put.assert_called_once_with(
            job_service._job_config.results_bucket_name,
            "job:test-job-id:result",
            job_result,
            ttl=job_service._job_config.default_result_ttl,
            serialize=True
        )

    @pytest.mark.asyncio
    async def test_store_result_disabled(self, job_service, mock_kv_store_service):
        """Test storing results when disabled."""
        job_service._job_config.enable_result_storage = False
        await job_service.initialize()
        
        job_result = JobResult(
            job_id="test-job-id",
            status=JOB_STATUS.COMPLETED.value,
            result="test result"
        )
        
        await job_service.store_result("test-job-id", job_result)
        
        mock_kv_store_service.put.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_result_exists(self, job_service, mock_kv_store_service):
        """Test getting existing job results."""
        await job_service.initialize()
        
        expected_result = JobResult(
            job_id="test-job-id",
            status=JOB_STATUS.COMPLETED.value,
            result="test result"
        )
        mock_kv_store_service.get.return_value = expected_result
        
        result = await job_service.get_result("test-job-id")
        
        assert result == expected_result
        mock_kv_store_service.get.assert_called_once_with(
            job_service._job_config.results_bucket_name,
            "job:test-job-id:result",
            deserialize=True
        )

    @pytest.mark.asyncio
    async def test_get_result_not_exists(self, job_service, mock_kv_store_service):
        """Test getting non-existing job results."""
        await job_service.initialize()
        
        mock_kv_store_service.get.side_effect = NaqException("Key not found")
        
        result = await job_service.get_result("test-job-id")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_result_invalid_type(self, job_service, mock_kv_store_service):
        """Test getting job results with invalid stored type."""
        await job_service.initialize()
        
        mock_kv_store_service.get.return_value = "invalid_result"
        
        result = await job_service.get_result("test-job-id")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_result(self, job_service, mock_kv_store_service):
        """Test deleting job results."""
        await job_service.initialize()
        
        mock_kv_store_service.delete.return_value = True
        
        result = await job_service.delete_result("test-job-id")
        
        assert result is True
        mock_kv_store_service.delete.assert_called_once_with(
            job_service._job_config.results_bucket_name,
            "job:test-job-id:result"
        )

    @pytest.mark.asyncio
    async def test_delete_result_not_exists(self, job_service, mock_kv_store_service):
        """Test deleting non-existing job results."""
        await job_service.initialize()
        
        mock_kv_store_service.delete.return_value = False
        
        result = await job_service.delete_result("test-job-id")
        
        assert result is False


class TestJobFailureHandling:
    """Test job failure handling functionality."""

    @pytest.mark.asyncio
    async def test_handle_job_failure_with_retry(self, job_service, failing_job, mock_event_service):
        """Test handling job failure with retry."""
        await job_service.initialize()
        
        error = ValueError("Test error")
        start_time = time.time()
        
        await job_service.handle_job_failure(failing_job, error, "test-worker", start_time)
        
        # Verify failure event was logged
        mock_event_service.log_job_event.assert_called()
        failure_event = mock_event_service.log_job_event.call_args[0][0]
        assert failure_event.event_type == JobEventType.FAILED
        assert failure_event.error_type == "ValueError"
        assert failure_event.error_message == "Test error"
        
        # Verify retry event was logged
        retry_event = mock_event_service.log_job_event.call_args_list[1][0][0]
        assert retry_event.event_type == JobEventType.RETRY_SCHEDULED

    @pytest.mark.asyncio
    async def test_handle_job_failure_no_retry(self, job_service, mock_event_service):
        """Test handling job failure without retry."""
        def failing_function():
            raise ValueError("Test error")
        
        job = Job(
            function=failing_function,
            queue_name="test_queue",
            max_retries=0  # No retries
        )
        
        await job_service.initialize()
        
        error = ValueError("Test error")
        start_time = time.time()
        
        await job_service.handle_job_failure(job, error, "test-worker", start_time)
        
        # Verify failure event was logged
        mock_event_service.log_job_event.assert_called()
        failure_event = mock_event_service.log_job_event.call_args[0][0]
        assert failure_event.event_type == JobEventType.FAILED
        
        # Verify no retry event was logged
        assert mock_event_service.log_job_event.call_count == 1

    @pytest.mark.asyncio
    async def test_handle_job_failure_stores_result(self, job_service, failing_job, mock_kv_store_service):
        """Test that job failure stores failure result."""
        await job_service.initialize()
        
        error = ValueError("Test error")
        start_time = time.time()
        
        await job_service.handle_job_failure(failing_job, error, "test-worker", start_time)
        
        # Verify failure result was stored
        mock_kv_store_service.put.assert_called()
        call_args = mock_kv_store_service.put.call_args[0]
        assert call_args[1] == f"job:{failing_job.job_id}:result"
        
        stored_result = call_args[2]
        assert stored_result.status == JOB_STATUS.FAILED.value
        assert stored_result.error == "Test error"
        assert "ValueError" in stored_result.traceback

    @pytest.mark.asyncio
    async def test_handle_job_failure_with_exception(self, job_service, failing_job):
        """Test handling job failure when the handler itself fails."""
        await job_service.initialize()
        
        # Make the event service fail
        job_service._event_service.log_job_event.side_effect = Exception("Event logging failed")
        
        error = ValueError("Test error")
        start_time = time.time()
        
        with pytest.raises(JobExecutionError, match="Failed to handle job failure"):
            await job_service.handle_job_failure(failing_job, error, "test-worker", start_time)


class TestJobServiceProperties:
    """Test JobService properties and configuration access."""

    @pytest.mark.asyncio
    async def test_job_config_property(self, job_service, job_service_config):
        """Test job_config property access."""
        await job_service.initialize()
        
        config = job_service.job_config
        assert config.enable_job_execution == job_service_config.enable_job_execution
        assert config.enable_result_storage == job_service_config.enable_result_storage
        assert config.enable_event_logging == job_service_config.enable_event_logging

    @pytest.mark.asyncio
    async def test_is_job_execution_enabled(self, job_service):
        """Test is_job_execution_enabled property."""
        await job_service.initialize()
        
        assert job_service.is_job_execution_enabled is True
        
        job_service._job_config.enable_job_execution = False
        assert job_service.is_job_execution_enabled is False

    @pytest.mark.asyncio
    async def test_is_result_storage_enabled(self, job_service):
        """Test is_result_storage_enabled property."""
        await job_service.initialize()
        
        assert job_service.is_result_storage_enabled is True
        
        job_service._job_config.enable_result_storage = False
        assert job_service.is_result_storage_enabled is False

    @pytest.mark.asyncio
    async def test_is_event_logging_enabled(self, job_service):
        """Test is_event_logging_enabled property."""
        await job_service.initialize()
        
        assert job_service.is_event_logging_enabled is True
        
        job_service._job_config.enable_event_logging = False
        assert job_service.is_event_logging_enabled is False


class TestJobServiceCleanup:
    """Test JobService cleanup functionality."""

    @pytest.mark.asyncio
    async def test_cleanup_with_created_services(self):
        """Test cleanup when services were created by JobService."""
        mock_connection_service = MagicMock(spec=ConnectionService)
        mock_connection_service.is_initialized = True
        
        config = JobServiceConfig()
        service_config = ServiceConfig(custom_settings=config.as_dict())
        job_service = JobService(
            config=service_config,
            connection_service=mock_connection_service
        )
        
        with patch('naq.services.jobs.KVStoreService') as mock_kv_class, \
             patch('naq.services.jobs.EventService') as mock_event_class:
            
            mock_kv_instance = AsyncMock()
            mock_kv_class.return_value = mock_kv_instance
            mock_event_instance = AsyncMock()
            mock_event_class.return_value = mock_event_instance
            
            await job_service.initialize()
            await job_service.cleanup()
            
            mock_kv_instance.cleanup.assert_called_once()
            mock_event_instance.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_with_provided_services(self, job_service, mock_kv_store_service, mock_event_service):
        """Test cleanup when services were provided externally."""
        await job_service.initialize()
        await job_service.cleanup()
        
        # Should not cleanup externally provided services
        mock_kv_store_service.cleanup.assert_not_called()
        mock_event_service.cleanup.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_not_initialized(self, job_service):
        """Test cleanup when service is not initialized."""
        await job_service.cleanup()
        
        # Should not raise an exception
        assert job_service.is_initialized is False