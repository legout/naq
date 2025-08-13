"""
Integration tests for the JobService.

This module contains integration tests for the JobService class,
testing end-to-end job processing, event logging, and result retrieval
with actual service implementations.
"""

import asyncio
import pytest
import time
from unittest.mock import patch

from naq.exceptions import JobExecutionError, NaqException
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType, JOB_STATUS
from naq.models.jobs import Job, JobResult
from naq.services.base import ServiceConfig
from naq.services.connection import ConnectionService, ConnectionServiceConfig
from naq.services.events import EventService, EventServiceConfig
from naq.services.jobs import JobService, JobServiceConfig
from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig


@pytest.fixture
def connection_service_config():
    """Create a ConnectionServiceConfig for testing."""
    return ConnectionServiceConfig(
        enable_connection_pooling=True,
        max_connections=10,
        connection_timeout=30
    )


@pytest.fixture
def kv_store_service_config():
    """Create a KVStoreServiceConfig for testing."""
    return KVStoreServiceConfig(
        default_bucket_name="test_jobs",
        auto_create_buckets=True,
        default_ttl=3600
    )


@pytest.fixture
def event_service_config():
    """Create an EventServiceConfig for testing."""
    return EventServiceConfig(
        enable_event_logging=True,
        auto_create_buckets=True,
        default_event_ttl=86400,
        max_events_per_job=100,
        max_events_per_worker=100
    )


@pytest.fixture
def job_service_config():
    """Create a JobServiceConfig for testing."""
    return JobServiceConfig(
        enable_job_execution=True,
        enable_result_storage=True,
        enable_event_logging=True,
        auto_create_buckets=True,
        max_job_execution_time=30,
        default_result_ttl=3600
    )


@pytest.fixture
async def connection_service(connection_service_config):
    """Create and initialize a ConnectionService for testing."""
    config = ServiceConfig(custom_settings=connection_service_config.as_dict())
    service = ConnectionService(config=config)
    await service.initialize()
    yield service
    await service.cleanup()


@pytest.fixture
async def kv_store_service(kv_store_service_config):
    """Create and initialize a KVStoreService for testing."""
    config = ServiceConfig(custom_settings=kv_store_service_config.as_dict())
    service = KVStoreService(config=config)
    await service.initialize()
    yield service
    await service.cleanup()


@pytest.fixture
async def event_service(event_service_config, kv_store_service):
    """Create and initialize an EventService for testing."""
    config = ServiceConfig(custom_settings=event_service_config.as_dict())
    service = EventService(config=config, kv_store_service=kv_store_service)
    await service.initialize()
    yield service
    await service.cleanup()


@pytest.fixture
async def job_service(job_service_config, connection_service, kv_store_service, event_service):
    """Create and initialize a JobService for testing."""
    config = ServiceConfig(custom_settings=job_service_config.as_dict())
    service = JobService(
        config=config,
        connection_service=connection_service,
        kv_store_service=kv_store_service,
        event_service=event_service
    )
    await service.initialize()
    yield service
    await service.cleanup()


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
def async_sample_job():
    """Create a sample async job for testing."""
    async def async_test_function(x, y):
        await asyncio.sleep(0.01)  # Simulate async work
        return x * y
    
    return Job(
        function=async_test_function,
        args=(3, 4),
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


@pytest.fixture
def retryable_job():
    """Create a job that fails once then succeeds."""
    call_count = 0
    
    def retryable_function():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("First attempt fails")
        return "success"
    
    return Job(
        function=retryable_function,
        queue_name="test_queue",
        max_retries=2,
        retry_delay=0.1
    )


class TestJobServiceIntegration:
    """Integration tests for JobService."""

    @pytest.mark.asyncio
    async def test_full_job_lifecycle(self, job_service, sample_job, event_service):
        """Test complete job lifecycle from execution to result retrieval."""
        # Execute job
        result = await job_service.execute_job(sample_job, "test-worker")
        
        # Verify result
        assert result.job_id == sample_job.job_id
        assert result.result == 5  # 2 + 3
        assert result.status == JOB_STATUS.COMPLETED.value
        assert result.worker_id == "test-worker"
        assert result.execution_time > 0
        
        # Retrieve stored result
        stored_result = await job_service.get_result(sample_job.job_id)
        assert stored_result is not None
        assert stored_result.job_id == sample_job.job_id
        assert stored_result.result == 5
        assert stored_result.status == JOB_STATUS.COMPLETED.value
        
        # Verify events were logged
        job_events = await event_service.get_job_events(sample_job.job_id)
        assert len(job_events) >= 2  # At least started and completed
        
        started_event = next(e for e in job_events if e.event_type == JobEventType.STARTED)
        assert started_event.worker_id == "test-worker"
        
        completed_event = next(e for e in job_events if e.event_type == JobEventType.COMPLETED)
        assert completed_event.worker_id == "test-worker"
        assert completed_event.result == 5

    @pytest.mark.asyncio
    async def test_async_job_execution(self, job_service, async_sample_job, event_service):
        """Test execution of async jobs."""
        # Execute async job
        result = await job_service.execute_job(async_sample_job, "test-worker")
        
        # Verify result
        assert result.job_id == async_sample_job.job_id
        assert result.result == 12  # 3 * 4
        assert result.status == JOB_STATUS.COMPLETED.value
        
        # Retrieve stored result
        stored_result = await job_service.get_result(async_sample_job.job_id)
        assert stored_result is not None
        assert stored_result.result == 12
        
        # Verify events were logged
        job_events = await event_service.get_job_events(async_sample_job.job_id)
        assert len(job_events) >= 2

    @pytest.mark.asyncio
    async def test_job_failure_handling(self, job_service, failing_job, event_service):
        """Test handling of job failures."""
        # Execute failing job
        with pytest.raises(JobExecutionError, match="Test error"):
            await job_service.execute_job(failing_job, "test-worker")
        
        # Verify failure result was stored
        stored_result = await job_service.get_result(failing_job.job_id)
        assert stored_result is not None
        assert stored_result.status == JOB_STATUS.FAILED.value
        assert stored_result.error == "Test error"
        assert "ValueError" in stored_result.traceback
        
        # Verify failure event was logged
        job_events = await event_service.get_job_events(failing_job.job_id)
        assert len(job_events) >= 2  # At least started and failed
        
        started_event = next(e for e in job_events if e.event_type == JobEventType.STARTED)
        assert started_event.worker_id == "test-worker"
        
        failed_event = next(e for e in job_events if e.event_type == JobEventType.FAILED)
        assert failed_event.worker_id == "test-worker"
        assert failed_event.error_type == "ValueError"
        assert failed_event.error_message == "Test error"

    @pytest.mark.asyncio
    async def test_job_retry_logic(self, job_service, retryable_job, event_service):
        """Test job retry logic."""
        # Execute retryable job
        result = await job_service.execute_job(retryable_job, "test-worker")
        
        # Verify result after retry
        assert result.job_id == retryable_job.job_id
        assert result.result == "success"
        assert result.status == JOB_STATUS.COMPLETED.value
        
        # Verify events were logged
        job_events = await event_service.get_job_events(retryable_job.job_id)
        assert len(job_events) >= 4  # started, failed, retry_scheduled, started, completed
        
        # Check for failure and retry events
        failed_event = next(e for e in job_events if e.event_type == JobEventType.FAILED)
        assert failed_event.error_message == "First attempt fails"
        
        retry_event = next(e for e in job_events if e.event_type == JobEventType.RETRY_SCHEDULED)
        assert retry_event.retry_count == 1

    @pytest.mark.asyncio
    async def test_job_timeout(self, job_service, event_service):
        """Test job execution timeout."""
        async def slow_function():
            await asyncio.sleep(0.2)  # Longer than timeout
            return "done"
        
        slow_job = Job(
            function=slow_function,
            queue_name="test_queue",
            max_retries=0
        )
        
        # Set a short timeout
        job_service._job_config.max_job_execution_time = 0.1
        
        # Execute job with timeout
        with pytest.raises(JobExecutionError, match="timed out"):
            await job_service.execute_job(slow_job, "test-worker")
        
        # Verify timeout was recorded
        stored_result = await job_service.get_result(slow_job.job_id)
        assert stored_result is not None
        assert stored_result.status == JOB_STATUS.FAILED.value
        assert "timeout" in stored_result.error.lower()
        
        # Verify timeout event was logged
        job_events = await event_service.get_job_events(slow_job.job_id)
        assert len(job_events) >= 2
        
        failed_event = next(e for e in job_events if e.event_type == JobEventType.FAILED)
        assert "timeout" in failed_event.error_message.lower()

    @pytest.mark.asyncio
    async def test_result_deletion(self, job_service, sample_job):
        """Test deletion of job results."""
        # Execute job
        await job_service.execute_job(sample_job, "test-worker")
        
        # Verify result exists
        stored_result = await job_service.get_result(sample_job.job_id)
        assert stored_result is not None
        
        # Delete result
        delete_success = await job_service.delete_result(sample_job.job_id)
        assert delete_success is True
        
        # Verify result is deleted
        deleted_result = await job_service.get_result(sample_job.job_id)
        assert deleted_result is None

    @pytest.mark.asyncio
    async def test_worker_events_logging(self, job_service, sample_job, event_service):
        """Test that worker events are properly logged."""
        worker_id = "test-worker"
        
        # Execute job
        await job_service.execute_job(sample_job, worker_id)
        
        # Verify worker events were logged
        worker_events = await event_service.get_worker_events(worker_id)
        assert len(worker_events) >= 2  # At least job_started and job_completed
        
        job_started_event = next(e for e in worker_events if e.event_type == WorkerEventType.JOB_STARTED)
        assert job_started_event.job_id == sample_job.job_id
        
        job_completed_event = next(e for e in worker_events if e.event_type == WorkerEventType.JOB_COMPLETED)
        assert job_completed_event.job_id == sample_job.job_id
        assert job_completed_event.result == 5

    @pytest.mark.asyncio
    async def test_service_configuration(self, job_service, job_service_config):
        """Test that service configuration is properly applied."""
        # Verify configuration
        assert job_service.is_job_execution_enabled == job_service_config.enable_job_execution
        assert job_service.is_result_storage_enabled == job_service_config.enable_result_storage
        assert job_service.is_event_logging_enabled == job_service_config.enable_event_logging
        
        # Test configuration access
        config = job_service.job_config
        assert config.max_job_execution_time == job_service_config.max_job_execution_time
        assert config.default_result_ttl == job_service_config.default_result_ttl

    @pytest.mark.asyncio
    async def test_service_initialization_with_dependencies(self, connection_service, kv_store_service):
        """Test service initialization with dependency injection."""
        # Create service with minimal dependencies
        config = JobServiceConfig()
        service_config = ServiceConfig(custom_settings=config.as_dict())
        
        job_service = JobService(
            config=service_config,
            connection_service=connection_service,
            kv_store_service=kv_store_service
        )
        
        # Should create its own EventService
        await job_service.initialize()
        
        # Verify EventService was created and initialized
        assert job_service._event_service is not None
        assert job_service._event_service.is_initialized
        
        # Cleanup
        await job_service.cleanup()

    @pytest.mark.asyncio
    async def test_service_cleanup(self, job_service, sample_job):
        """Test service cleanup."""
        # Execute a job
        await job_service.execute_job(sample_job, "test-worker")
        
        # Cleanup service
        await job_service.cleanup()
        
        # Verify service is cleaned up
        assert job_service.is_initialized is False
        
        # Verify dependencies are still initialized (they weren't created by JobService)
        assert job_service._connection_service.is_initialized
        assert job_service._kv_store_service.is_initialized
        assert job_service._event_service.is_initialized

    @pytest.mark.asyncio
    async def test_concurrent_job_execution(self, job_service, event_service):
        """Test concurrent execution of multiple jobs."""
        async def concurrent_job_function(job_id):
            await asyncio.sleep(0.05)  # Simulate work
            return f"result-{job_id}"
        
        # Create multiple jobs
        jobs = [
            Job(
                function=concurrent_job_function,
                args=(f"job-{i}",),
                queue_name="test_queue",
                max_retries=0
            )
            for i in range(5)
        ]
        
        # Execute jobs concurrently
        tasks = [job_service.execute_job(job, f"worker-{i}") for i, job in enumerate(jobs)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all jobs completed successfully
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pytest.fail(f"Job {i} failed: {result}")
            assert result.result == f"result-job-{i}"
            assert result.status == JOB_STATUS.COMPLETED.value
        
        # Verify all results are stored
        for job in jobs:
            stored_result = await job_service.get_result(job.job_id)
            assert stored_result is not None
            assert stored_result.status == JOB_STATUS.COMPLETED.value
        
        # Verify events were logged for all jobs
        for job in jobs:
            job_events = await event_service.get_job_events(job.job_id)
            assert len(job_events) >= 2

    @pytest.mark.asyncio
    async def test_job_with_kwargs(self, job_service, event_service):
        """Test job execution with keyword arguments."""
        def kwargs_function(x, y, z=10):
            return x + y + z
        
        job = Job(
            function=kwargs_function,
            args=(2, 3),
            kwargs={"z": 5},
            queue_name="test_queue",
            max_retries=0
        )
        
        # Execute job
        result = await job_service.execute_job(job, "test-worker")
        
        # Verify result
        assert result.result == 10  # 2 + 3 + 5
        assert result.status == JOB_STATUS.COMPLETED.value
        
        # Verify result is stored
        stored_result = await job_service.get_result(job.job_id)
        assert stored_result is not None
        assert stored_result.result == 10
        
        # Verify events were logged
        job_events = await event_service.get_job_events(job.job_id)
        assert len(job_events) >= 2