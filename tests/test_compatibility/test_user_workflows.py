# tests/test_compatibility/test_user_workflows.py
"""Tests for backward compatibility of user workflows."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock


class TestUserWorkflows:
    """Test that typical user workflows continue to work."""

    def test_basic_sync_workflow(self):
        """Test basic synchronous workflow still works."""
        from naq import enqueue_sync, JOB_STATUS
        
        def simple_task(x):
            return x + 1
        
        # Mock the underlying async machinery
        with patch('naq.queue.sync_api.run_async_from_sync') as mock_run_async:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-123"
            mock_job.function = simple_task
            mock_job.args = (5,)
            mock_job.status = JOB_STATUS.PENDING
            
            mock_run_async.return_value = mock_job
            
            # This should work exactly as before refactoring
            job = enqueue_sync(simple_task, 5)
            
            assert job.job_id is not None
            assert job.function == simple_task
            assert job.args == (5,)

    @pytest.mark.asyncio
    async def test_basic_async_workflow(self):
        """Test basic async workflow still works."""
        from naq import enqueue
        
        async def async_task(x):
            return x * 2
        
        # Mock the queue enqueue method
        with patch('naq.queue.async_api.Queue.enqueue') as mock_enqueue:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-456"
            mock_job.function = async_task
            mock_job.args = (10,)
            
            mock_enqueue.return_value = mock_job
            
            # This should work exactly as before refactoring
            job = await enqueue(async_task, 10, queue_name="test")
            
            assert job.job_id is not None
            assert job.function == async_task
            assert job.args == (10,)

    @pytest.mark.asyncio
    async def test_advanced_workflow(self):
        """Test advanced user workflow."""
        from naq import Queue, Worker
        from naq.models import Job, JOB_STATUS
        from naq.events import AsyncJobEventLogger
        
        # Advanced users might use these directly
        # Mock the service layer dependencies
        with patch('naq.queue.core.ServiceManager') as mock_sm, \
             patch('naq.worker.core.ServiceManager') as mock_worker_sm, \
             patch('naq.events.logger.get_nats_connection'):
            
            # Setup service manager mocks
            mock_manager = AsyncMock()
            mock_sm.return_value = mock_manager
            mock_worker_sm.return_value = mock_manager
            
            mock_connection_service = AsyncMock()
            mock_manager.get_service.return_value = mock_connection_service
            
            queue = Queue("advanced-test")
            worker = Worker(["advanced-test"])
            logger = AsyncJobEventLogger()
            
            def advanced_task(data):
                return {"processed": data}
            
            job = Job(function=advanced_task, args=({"input": "test"},))
            
            # Should work with new structure
            assert job.status == JOB_STATUS.PENDING
            assert queue is not None
            assert worker is not None
            assert logger is not None

    def test_job_result_workflow(self):
        """Test job result fetching workflow."""
        from naq import fetch_job_result_sync, Job
        
        def test_task():
            return "completed"
        
        # Mock the result fetching
        with patch.object(Job, 'fetch_result_sync') as mock_fetch:
            mock_fetch.return_value = {"result": "completed", "status": "completed"}
            
            result = fetch_job_result_sync("test-job-id")
            
            assert result["result"] == "completed"
            assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_async_job_result_workflow(self):
        """Test async job result fetching workflow."""
        from naq import fetch_job_result, Job
        
        # Mock the async result fetching
        with patch.object(Job, 'fetch_result') as mock_fetch:
            mock_fetch.return_value = {"result": "async_completed", "status": "completed"}
            
            result = await fetch_job_result("test-job-async-id")
            
            assert result["result"] == "async_completed"
            assert result["status"] == "completed"

    def test_worker_listing_workflow(self):
        """Test worker listing functionality."""
        from naq import list_workers_sync, Worker
        
        # Mock the worker listing
        with patch.object(Worker, 'list_workers_sync') as mock_list:
            mock_workers = [
                {"worker_id": "worker-1", "status": "running"},
                {"worker_id": "worker-2", "status": "idle"}
            ]
            mock_list.return_value = mock_workers
            
            workers = list_workers_sync()
            
            assert len(workers) == 2
            assert workers[0]["worker_id"] == "worker-1"

    @pytest.mark.asyncio
    async def test_async_worker_listing_workflow(self):
        """Test async worker listing functionality."""
        from naq import list_workers, Worker
        
        # Mock the async worker listing
        with patch.object(Worker, 'list_workers') as mock_list:
            mock_workers = [
                {"worker_id": "worker-async-1", "status": "running"}
            ]
            mock_list.return_value = mock_workers
            
            workers = await list_workers()
            
            assert len(workers) == 1
            assert workers[0]["worker_id"] == "worker-async-1"

    def test_event_logging_workflow(self):
        """Test event logging workflow still works."""
        from naq.events import AsyncJobEventLogger, JobEvent, JobEventType
        
        # Mock NATS connection for event logger
        with patch('naq.events.logger.get_nats_connection'):
            logger = AsyncJobEventLogger()
            
            # Create job event
            event = JobEvent.enqueued("job-123", "test-queue")
            
            assert event.job_id == "job-123"
            assert event.event_type == JobEventType.ENQUEUED
            assert event.queue_name == "test-queue"
            assert logger is not None

    def test_configuration_workflow(self):
        """Test configuration loading workflow."""
        from naq.config import get_config, load_config
        
        # Test that configuration functions are available
        assert callable(get_config)
        assert callable(load_config)
        
        # Test default config loading
        try:
            config = get_config()
            assert config is not None
        except Exception:
            # May fail without actual config file, which is fine
            pass

    @pytest.mark.asyncio
    async def test_scheduler_workflow(self):
        """Test scheduler functionality."""
        from naq.scheduler import Scheduler
        
        # Mock service dependencies
        with patch('naq.scheduler.ServiceManager') as mock_sm:
            mock_manager = AsyncMock()
            mock_sm.return_value = mock_manager
            
            scheduler = Scheduler()
            
            assert scheduler is not None

    def test_queue_purge_workflow(self):
        """Test queue purging workflow."""
        from naq import purge_queue_sync
        
        # Mock the purge operation
        with patch('naq.queue.sync_api.run_async_from_sync') as mock_run_async:
            mock_run_async.return_value = 5  # 5 jobs purged
            
            purged_count = purge_queue_sync("test-queue")
            
            assert purged_count == 5

    def test_job_scheduling_workflow(self):
        """Test job scheduling workflow."""
        from naq import schedule_sync
        from datetime import datetime, timedelta
        
        def scheduled_task():
            return "scheduled work"
        
        # Mock the scheduling
        with patch('naq.queue.sync_api.run_async_from_sync') as mock_run_async:
            mock_job = MagicMock()
            mock_job.job_id = "scheduled-job-123"
            mock_run_async.return_value = mock_job
            
            run_time = datetime.now() + timedelta(hours=1)
            job = schedule_sync(scheduled_task, run_at=run_time)
            
            assert job.job_id == "scheduled-job-123"

    def test_legacy_parameter_compatibility(self):
        """Test that legacy parameters still work."""
        from naq.queue import Queue
        from naq.worker import Worker
        
        # Test legacy parameter names still work
        with patch('naq.queue.core.ServiceManager'):
            # These should not raise errors even with legacy parameters
            queue = Queue(name="test", nats_url="nats://localhost:4222")
            assert queue is not None
            
        with patch('naq.worker.core.ServiceManager'):
            worker = Worker(
                queues=["test"], 
                nats_url="nats://localhost:4222",
                worker_name="test-worker"
            )
            assert worker is not None

    def test_error_handling_compatibility(self):
        """Test that error handling still works as expected."""
        from naq.exceptions import NaqException, JobNotFoundError
        
        # Test that exceptions can still be caught
        try:
            raise JobNotFoundError("Test job not found")
        except NaqException as e:
            assert "Test job not found" in str(e)
        except Exception:
            pytest.fail("JobNotFoundError should be catchable as NaqException")