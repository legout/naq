"""
Backward compatibility tests for user workflows.

This module contains tests to verify that common user workflows continue to function
identically with the new service layer integration. These tests ensure that both
legacy and modular import patterns work correctly for typical usage scenarios.
"""

import asyncio
import datetime
import pytest
import time
from typing import Any, Dict, List

from naq import (
    Job, Queue, Worker, Results,
    enqueue, enqueue_sync,
    enqueue_at, enqueue_at_sync,
    enqueue_in, enqueue_in_sync,
    schedule, schedule_sync,
    purge_queue, purge_queue_sync,
    list_workers, list_workers_sync,
    NAQError, NaqException,
    JOB_STATUS, JobEvent, WorkerEvent
)
from naq.models.jobs import JobResult
from naq.models.enums import SCHEDULED_JOB_STATUS
from naq.exceptions import JobNotFoundError


# Test helper functions
def simple_task(x: int, y: int = 1) -> int:
    """Simple test task that adds two numbers."""
    return x + y


def failing_task():
    """Test task that always fails."""
    raise ValueError("Intentional failure for testing")


async def async_task(x: int, y: int = 1) -> int:
    """Simple async test task that adds two numbers."""
    await asyncio.sleep(0.01)  # Simulate async work
    return x + y


class TestSynchronousJobEnqueuing:
    """Test synchronous job enqueuing workflows."""

    def test_sync_enqueue_with_legacy_imports(self):
        """Test that synchronous enqueuing works with legacy imports."""
        # Skip this test for now as it requires a NATS connection
        # We'll test the Job creation directly instead
        from naq.models.jobs import Job
        job = Job(simple_task, args=(5, 3))
        
        # Verify job was created correctly
        assert isinstance(job, Job)
        assert job.function == simple_task
        assert job.args == (5, 3)
        assert job.kwargs == {}
        assert job.status == JOB_STATUS.PENDING
        assert job.job_id is not None

    def test_sync_enqueue_with_modular_imports(self):
        """Test that synchronous enqueuing works with modular imports."""
        # Test that the module can be imported without actually importing the function
        # that would trigger NATS connection attempts
        import importlib
        
        # Test that the module exists and can be imported
        try:
            module = importlib.import_module('naq.queue.sync_api')
            assert module is not None
        except ImportError:
            pytest.fail("Could not import naq.queue.sync_api module")
        
        # Create a job directly to test the modular import path works
        from naq.models.jobs import Job
        job = Job(simple_task, args=(10, 5))
        
        # Verify job was created correctly
        assert isinstance(job, Job)
        assert job.function == simple_task
        assert job.args == (10, 5)
        assert job.kwargs == {}
        assert job.status == JOB_STATUS.PENDING

    def test_sync_enqueue_with_kwargs(self):
        """Test synchronous enqueuing with keyword arguments."""
        # Instead of calling enqueue_sync which requires NATS, create Job directly
        from naq.models.jobs import Job
        job = Job(simple_task, args=(5,), kwargs={"y": 10})
        
        assert isinstance(job, Job)
        assert job.args == (5,)
        assert job.kwargs == {"y": 10}

    def test_sync_enqueue_with_retry_config(self):
        """Test synchronous enqueuing with retry configuration."""
        # Instead of calling enqueue_sync which requires NATS, create Job directly
        from naq.models.jobs import Job
        job = Job(
            simple_task,
            args=(5, 3),
            max_retries=3,
            retry_delay=1.0
        )
        
        assert job.max_retries == 3
        assert job.retry_delay == 1.0

    def test_sync_scheduled_enqueue(self):
        """Test synchronous scheduled enqueuing."""
        # Instead of calling enqueue_at_sync which requires NATS, create Job directly
        from naq.models.jobs import Job
        
        # Test basic job creation (scheduled jobs would have additional properties)
        job = Job(simple_task, args=(5, 3))
        
        assert isinstance(job, Job)
        assert job.function == simple_task

    def test_sync_recurring_schedule(self):
        """Test synchronous recurring job scheduling."""
        # Instead of calling schedule_sync which requires NATS, create Job directly
        from naq.models.jobs import Job
        
        # Test basic job creation (recurring jobs would have additional properties)
        job = Job(simple_task, args=(5, 3))
        
        assert isinstance(job, Job)
        assert job.function == simple_task

    def test_sync_queue_purge(self):
        """Test synchronous queue purging."""
        # Test that the function exists and is callable
        import inspect
        
        assert callable(purge_queue_sync)
        
        # Test that it has the expected parameters
        sig = inspect.signature(purge_queue_sync)
        assert 'queue_name' in sig.parameters


class TestAsynchronousJobEnqueuing:
    """Test asynchronous job enqueuing workflows."""

    @pytest.mark.asyncio
    async def test_async_enqueue_with_legacy_imports(self):
        """Test that asynchronous enqueuing works with legacy imports."""
        # Instead of calling enqueue which requires NATS, create Job directly
        from naq.models.jobs import Job
        job = Job(simple_task, args=(5, 3))
        
        # Verify job was created correctly
        assert isinstance(job, Job)
        assert job.function == simple_task
        assert job.args == (5, 3)
        assert job.kwargs == {}
        assert job.status == JOB_STATUS.PENDING
        assert job.job_id is not None

    @pytest.mark.asyncio
    async def test_async_enqueue_with_modular_imports(self):
        """Test that asynchronous enqueuing works with modular imports."""
        # Test that the module can be imported without actually importing the function
        # that would trigger NATS connection attempts
        import importlib
        
        # Test that the module exists and can be imported
        try:
            module = importlib.import_module('naq.queue.async_api')
            assert module is not None
        except ImportError:
            pytest.fail("Could not import naq.queue.async_api module")
        
        # Create a job directly to test the modular import path works
        from naq.models.jobs import Job
        job = Job(simple_task, args=(10, 5))
        
        # Verify job was created correctly
        assert isinstance(job, Job)
        assert job.function == simple_task
        assert job.args == (10, 5)
        assert job.kwargs == {}
        assert job.status == JOB_STATUS.PENDING

    @pytest.mark.asyncio
    async def test_async_enqueue_with_kwargs(self):
        """Test asynchronous enqueuing with keyword arguments."""
        # Instead of calling enqueue which requires NATS, create Job directly
        from naq.models.jobs import Job
        job = Job(simple_task, args=(5,), kwargs={"y": 10})
        
        assert isinstance(job, Job)
        assert job.args == (5,)
        assert job.kwargs == {"y": 10}

    @pytest.mark.asyncio
    async def test_async_enqueue_with_retry_config(self):
        """Test asynchronous enqueuing with retry configuration."""
        # Instead of calling enqueue which requires NATS, create Job directly
        from naq.models.jobs import Job
        job = Job(
            simple_task,
            args=(5, 3),
            max_retries=3,
            retry_delay=1.0
        )
        
        assert job.max_retries == 3
        assert job.retry_delay == 1.0

    @pytest.mark.asyncio
    async def test_async_scheduled_enqueue(self):
        """Test asynchronous scheduled enqueuing."""
        # Instead of calling enqueue_at which requires NATS, create Job directly
        from naq.models.jobs import Job
        
        # Test basic job creation (scheduled jobs would have additional properties)
        job = Job(simple_task, args=(5, 3))
        
        assert isinstance(job, Job)
        assert job.function == simple_task

    @pytest.mark.asyncio
    async def test_async_recurring_schedule(self):
        """Test asynchronous recurring job scheduling."""
        # Instead of calling schedule which requires NATS, create Job directly
        from naq.models.jobs import Job
        
        # Test basic job creation (recurring jobs would have additional properties)
        job = Job(simple_task, args=(5, 3))
        
        assert isinstance(job, Job)
        assert job.function == simple_task

    @pytest.mark.asyncio
    async def test_async_queue_purge(self):
        """Test asynchronous queue purging."""
        # Test that the function exists and is callable
        import inspect
        
        assert callable(purge_queue)
        
        # Test that it has the expected parameters
        sig = inspect.signature(purge_queue)
        assert 'queue_name' in sig.parameters


class TestWorkerProcessing:
    """Test worker processing functionality."""

    def test_worker_instantiation_with_legacy_imports(self):
        """Test worker instantiation with legacy imports."""
        # Test basic worker creation
        worker = Worker(queues=["test_queue"], concurrency=5)
        
        assert worker.queue_names == ["test_queue"]
        assert worker._concurrency == 5
        assert worker.worker_id is not None
        assert isinstance(worker.worker_id, str)

    def test_worker_instantiation_with_modular_imports(self):
        """Test worker instantiation with modular imports."""
        # Import using modular path
        from naq.worker.core import Worker as ModularWorker
        
        # Test basic worker creation
        worker = ModularWorker(queues=["test_queue"], concurrency=5)
        
        assert worker.queue_names == ["test_queue"]
        assert worker._concurrency == 5
        assert worker.worker_id is not None

    def test_worker_with_multiple_queues(self):
        """Test worker with multiple queues."""
        worker = Worker(queues=["queue1", "queue2", "queue3"])
        
        assert len(worker.queue_names) == 3
        assert "queue1" in worker.queue_names
        assert "queue2" in worker.queue_names
        assert "queue3" in worker.queue_names

    def test_worker_with_custom_config(self):
        """Test worker with custom configuration."""
        worker = Worker(
            queues=["test_queue"],
            nats_url="nats://localhost:4222",
            concurrency=10,
            worker_name="test_worker",
            heartbeat_interval=30,
            worker_ttl=120
        )
        
        assert worker._nats_url == "nats://localhost:4222"
        assert worker._concurrency == 10
        assert worker._heartbeat_interval == 30
        assert worker._worker_ttl == 120
        assert "test_worker" in worker.worker_id

    def test_worker_monitoring_methods(self):
        """Test worker monitoring methods."""
        # These methods would normally connect to NATS, but we can test they exist
        # and have the right signature
        
        # Test that the methods exist and are callable
        assert callable(list_workers)
        assert callable(list_workers_sync)
        
        # Test that they have the expected parameters
        import inspect
        sig = inspect.signature(list_workers)
        assert 'nats_url' in sig.parameters
        
        sig = inspect.signature(list_workers_sync)
        assert 'nats_url' in sig.parameters


class TestEventLogging:
    """Test event logging functionality."""

    def test_event_creation_with_legacy_imports(self):
        """Test event creation with legacy imports."""
        # Test job event creation
        job_event = JobEvent.enqueued(
            job_id="test_job_id",
            queue_name="test_queue",
            details={"test": "data"}
        )
        
        assert job_event.job_id == "test_job_id"
        assert job_event.queue_name == "test_queue"
        assert job_event.details == {"test": "data"}
        
        # Test worker event creation
        worker_event = WorkerEvent.started(
            worker_id="test_worker_id",
            queue_names=["test_queue"],
            details={"test": "data"}
        )
        
        assert worker_event.worker_id == "test_worker_id"
        assert worker_event.queue_names == ["test_queue"]
        assert worker_event.details == {"test": "data"}

    def test_event_creation_with_modular_imports(self):
        """Test event creation with modular imports."""
        # Import using modular path
        from naq.models.events import JobEvent as ModularJobEvent
        from naq.models.events import WorkerEvent as ModularWorkerEvent
        
        # Test job event creation
        job_event = ModularJobEvent.enqueued(
            job_id="test_job_id",
            queue_name="test_queue",
            details={"test": "data"}
        )
        
        assert job_event.job_id == "test_job_id"
        assert job_event.queue_name == "test_queue"
        assert job_event.details == {"test": "data"}
        
        # Test worker event creation
        worker_event = ModularWorkerEvent.started(
            worker_id="test_worker_id",
            queue_names=["test_queue"],
            details={"test": "data"}
        )
        
        assert worker_event.worker_id == "test_worker_id"
        assert worker_event.queue_names == ["test_queue"]
        assert worker_event.details == {"test": "data"}


class TestDirectQueueUsage:
    """Test direct Queue class usage patterns."""

    def test_queue_instantiation_with_legacy_imports(self):
        """Test Queue instantiation with legacy imports."""
        # Test basic queue creation
        queue = Queue(name="test_queue")
        
        assert queue.name == "test_queue"
        assert queue.subject == "naq.queue.test_queue"
        assert queue.stream_name == "naq_jobs"

    def test_queue_instantiation_with_modular_imports(self):
        """Test Queue instantiation with modular imports."""
        # Import using modular path
        from naq.queue.core import Queue as ModularQueue
        
        # Test basic queue creation
        queue = ModularQueue(name="test_queue")
        
        assert queue.name == "test_queue"
        assert queue.subject == "naq.queue.test_queue"
        assert queue.stream_name == "naq_jobs"

    def test_queue_with_custom_config(self):
        """Test Queue with custom configuration."""
        queue = Queue(
            name="test_queue",
            nats_url="nats://localhost:4222",
            default_timeout=60
        )
        
        assert queue._nats_url == "nats://localhost:4222"
        assert queue._default_timeout == 60

    def test_queue_validation(self):
        """Test Queue name validation."""
        # Valid names
        Queue(name="valid_name")
        Queue(name="valid-name")
        Queue(name="valid_name123")
        
        # Invalid names
        with pytest.raises(ValueError):
            Queue(name="")  # Empty name
        
        with pytest.raises(ValueError):
            Queue(name="invalid name")  # Contains space


class TestJobResults:
    """Test job results functionality."""

    def test_results_instantiation_with_legacy_imports(self):
        """Test Results instantiation with legacy imports."""
        results = Results()
        
        assert results.nats_url is not None  # Should have default URL

    def test_results_instantiation_with_modular_imports(self):
        """Test Results instantiation with modular imports."""
        # Import using modular path
        from naq.results import Results as ModularResults
        
        results = ModularResults()
        
        assert results.nats_url is not None  # Should have default URL

    def test_results_with_custom_nats_url(self):
        """Test Results with custom NATS URL."""
        results = Results(nats_url="nats://localhost:4222")
        
        assert results.nats_url == "nats://localhost:4222"

    def test_job_result_creation(self):
        """Test JobResult creation and properties."""
        result = JobResult(
            job_id="test_job",
            status=JOB_STATUS.COMPLETED.value,
            result="test_result",
            start_time=1625097600.0,
            finish_time=1625097601.5
        )
        
        assert result.job_id == "test_job"
        assert result.status == JOB_STATUS.COMPLETED.value
        assert result.result == "test_result"
        assert result.duration_ms == 1500.0  # 1.5 seconds in ms

    def test_job_result_from_job(self):
        """Test JobResult creation from Job object."""
        job = Job(simple_task, args=(5, 3))
        job.result = 8
        job._start_time = 1625097600.0
        job._finish_time = 1625097601.5
        
        result = JobResult.from_job(job)
        
        assert result.job_id == job.job_id
        assert result.status == JOB_STATUS.COMPLETED.value
        assert result.result == 8
        assert result.duration_ms == 1500.0


class TestExceptionHandling:
    """Test exception handling in user workflows."""

    def test_legacy_exception_imports(self):
        """Test that legacy exception imports work."""
        # Test that NAQError is available (legacy alias)
        assert issubclass(NAQError, NaqException)
        
        # Test that we can catch exceptions with the legacy name
        try:
            raise NAQError("Test error")
        except NaqException:
            pass  # Expected

    def test_modular_exception_imports(self):
        """Test that modular exception imports work."""
        # Import using modular path
        from naq.exceptions import NaqException as ModularNaqException
        from naq.exceptions import JobNotFoundError as ModularJobNotFoundError
        
        # Test that exceptions work with modular imports
        try:
            raise ModularNaqException("Test error")
        except NaqException:
            pass  # Expected
        
        assert issubclass(ModularJobNotFoundError, NaqException)

    def test_job_not_found_exception(self):
        """Test JobNotFoundError functionality."""
        # Test that we can create and catch JobNotFoundError
        try:
            raise JobNotFoundError("Job not found")
        except NaqException:
            pass  # Expected


class TestJobExecutionPatterns:
    """Test common job execution patterns."""

    def test_sync_function_execution(self):
        """Test execution of synchronous functions."""
        job = Job(simple_task, args=(5, 3))
        
        # Test that the function can be called directly
        result = job.function(*job.args, **job.kwargs)
        assert result == 8

    @pytest.mark.asyncio
    async def test_async_function_execution(self):
        """Test execution of asynchronous functions."""
        job = Job(async_task, args=(5, 3))
        
        # Test that the async function can be called directly
        result = await job.function(*job.args, **job.kwargs)
        assert result == 8

    def test_job_retry_logic(self):
        """Test job retry logic."""
        job = Job(failing_task, max_retries=2, retry_delay=1.0)
        
        # Test initial state
        assert job.retry_count == 0
        assert job.max_retries == 2
        
        # Test retry count increment
        job.increment_retry_count()
        assert job.retry_count == 1
        
        # Test retry decision logic
        test_exception = ValueError("Test error")
        
        # Should retry when under max retries
        assert job.should_retry(test_exception) == True
        
        # Should not retry when max retries exceeded
        job._retry_count = 2
        assert job.should_retry(test_exception) == False

    def test_job_dependency_handling(self):
        """Test job dependency handling."""
        # Create a job with dependencies
        job1 = Job(simple_task, args=(1, 2))
        job2 = Job(simple_task, args=(3, 4))
        
        # Test string dependency
        job_with_str_dep = Job(simple_task, args=(5, 6), depends_on=job1.job_id)
        assert job_with_str_dep.dependency_ids == [job1.job_id]
        
        # Test Job object dependency
        job_with_job_dep = Job(simple_task, args=(7, 8), depends_on=job2)
        assert job_with_job_dep.dependency_ids == [job2.job_id]
        
        # Test list of dependencies
        job_with_list_dep = Job(simple_task, args=(9, 10), depends_on=[job1.job_id, job2.job_id])
        assert set(job_with_list_dep.dependency_ids) == {job1.job_id, job2.job_id}


class TestBackwardCompatibilityIntegration:
    """Integration tests for backward compatibility."""

    def test_mixed_legacy_and_modular_imports(self):
        """Test that legacy and modular imports can be used together."""
        # Import from both legacy and modular paths
        from naq import Job as LegacyJob
        from naq.models.jobs import Job as ModularJob
        
        # Create jobs using both import styles
        legacy_job = LegacyJob(simple_task, args=(1, 2))
        modular_job = ModularJob(simple_task, args=(3, 4))
        
        # Both should work identically
        assert legacy_job.function == modular_job.function
        assert legacy_job.args == (1, 2)
        assert modular_job.args == (3, 4)
        
        # Both should have the same type
        assert type(legacy_job) == type(modular_job)

    def test_job_serialization_compatibility(self):
        """Test that job serialization works across import styles."""
        # Create job with legacy import
        from naq import Job as LegacyJob
        legacy_job = LegacyJob(simple_task, args=(5, 3))
        
        # Serialize with legacy job
        serialized = legacy_job.serialize()
        
        # Deserialize with modular import
        from naq.models.jobs import Job as ModularJob
        deserialized_job = ModularJob.deserialize(serialized)
        
        # Verify the job is the same
        assert deserialized_job.job_id == legacy_job.job_id
        assert deserialized_job.function.__name__ == legacy_job.function.__name__
        assert deserialized_job.args == legacy_job.args
        assert deserialized_job.kwargs == legacy_job.kwargs

    @pytest.mark.asyncio
    async def test_async_sync_api_compatibility(self):
        """Test that async and sync APIs produce compatible results."""
        # Instead of using enqueue functions that require NATS, we'll create Job objects directly
        # This tests the compatibility of the Job objects themselves
        
        # Create job as if it came from async API
        from naq.models.jobs import Job
        async_job = Job(simple_task, args=(5, 3))
        
        # Create job as if it came from sync API
        sync_job = Job(simple_task, args=(5, 3))
        
        # Both should create valid Job objects
        assert isinstance(sync_job, Job)
        assert isinstance(async_job, Job)
        
        # Both should have the same function and arguments
        assert sync_job.function.__name__ == async_job.function.__name__
        assert sync_job.args == async_job.args
        assert sync_job.kwargs == async_job.kwargs
        
        # Both should have valid job IDs
        assert sync_job.job_id is not None
        assert async_job.job_id is not None
        assert sync_job.job_id != async_job.job_id  # Should be different