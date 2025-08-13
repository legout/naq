# tests/test_worker/test_worker_core.py
"""Tests for worker core functionality."""

import asyncio
import pytest
import signal
import time
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from naq.worker.core import Worker
from naq.worker.status import WorkerStatusManager
from naq.worker.jobs import JobStatusManager
from naq.worker.failed import FailedJobHandler
from naq import Job, JOB_STATUS


class TestWorkerCore:
    """Test core worker functionality."""

    @pytest.fixture
    def mock_services(self):
        """Mock service dependencies."""
        mock_status_manager = AsyncMock(spec=WorkerStatusManager)
        mock_job_manager = AsyncMock(spec=JobStatusManager)
        mock_failed_handler = AsyncMock(spec=FailedJobHandler)
        
        return {
            "status_manager": mock_status_manager,
            "job_manager": mock_job_manager,
            "failed_handler": mock_failed_handler
        }

    @pytest.fixture
    def worker_config(self):
        """Basic worker configuration."""
        return {
            "queues": ["test-queue"],
            "concurrency": 2,
            "heartbeat_interval": 5,
            "max_jobs": 100
        }

    @pytest.mark.asyncio
    async def test_worker_initialization(self, worker_config, mock_services):
        """Test worker initialization."""
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            assert worker.queue_names == ["test-queue"]
            assert worker.concurrency == 2
            assert worker.heartbeat_interval == 5
            assert worker.max_jobs == 100
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_worker_start_stop(self, worker_config, mock_services):
        """Test worker start and stop lifecycle."""
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Start worker
            start_task = asyncio.create_task(worker.start())
            await asyncio.sleep(0.1)  # Let it start
            
            # Stop worker
            await worker.stop()
            
            # Wait for start task to complete
            try:
                await asyncio.wait_for(start_task, timeout=1.0)
            except asyncio.TimeoutError:
                start_task.cancel()
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_job_processing(self, worker_config, mock_services):
        """Test job processing functionality."""
        def test_function(x):
            return x * 2
        
        job = Job(
            function=test_function,
            args=(5,),
            job_id="test-job-123",
            queue_name="test-queue"
        )
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Process the job
            await worker._process_job(job)
            
            # Verify job execution
            assert job.status == JOB_STATUS.COMPLETED
            assert job.result == 10
            
            # Verify status manager calls
            mock_services["status_manager"].update_status.assert_called()
            mock_services["job_manager"].store_result.assert_called_once_with(job)
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_job_processing_failure(self, worker_config, mock_services):
        """Test job processing with failure."""
        def failing_function():
            raise ValueError("Test error")
        
        job = Job(
            function=failing_function,
            job_id="failing-job-123",
            queue_name="test-queue"
        )
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Process the failing job
            await worker._process_job(job)
            
            # Verify job failure
            assert job.status == JOB_STATUS.FAILED
            assert job.error == "Test error"
            
            # Verify failed job handler was called
            mock_services["failed_handler"].handle_failed_job.assert_called_once_with(job)
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_concurrency_limit(self, worker_config, mock_services):
        """Test worker concurrency limiting."""
        async def slow_function():
            await asyncio.sleep(0.2)
            return "done"
        
        jobs = [
            Job(function=slow_function, job_id=f"job-{i}", queue_name="test-queue")
            for i in range(4)
        ]
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)  # concurrency = 2
            await worker.initialize()
            
            start_time = time.time()
            
            # Process jobs concurrently
            tasks = [worker._process_job(job) for job in jobs]
            await asyncio.gather(*tasks)
            
            duration = time.time() - start_time
            
            # With concurrency of 2, 4 jobs should take at least 2 cycles
            assert duration >= 0.35  # Two cycles of 0.2s each
            
            # All jobs should complete
            for job in jobs:
                assert job.status == JOB_STATUS.COMPLETED
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_heartbeat_functionality(self, worker_config, mock_services):
        """Test worker heartbeat functionality."""
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            worker.heartbeat_interval = 0.1  # Fast heartbeat for testing
            await worker.initialize()
            
            # Start heartbeat
            heartbeat_task = asyncio.create_task(worker._heartbeat_loop())
            
            # Let it run for a bit
            await asyncio.sleep(0.25)
            
            # Stop heartbeat
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            
            # Verify heartbeats were sent
            assert mock_services["status_manager"].send_heartbeat.call_count >= 2
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_signal_handling(self, worker_config, mock_services):
        """Test worker signal handling."""
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Install signal handlers
            worker.install_signal_handlers()
            
            # Simulate signal
            worker.signal_handler(signal.SIGTERM, None)
            
            # Verify shutdown event is set
            assert worker._shutdown_event.is_set()
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, worker_config, mock_services):
        """Test worker graceful shutdown."""
        async def long_running_job():
            await asyncio.sleep(0.3)
            return "completed"
        
        job = Job(
            function=long_running_job,
            job_id="long-job",
            queue_name="test-queue"
        )
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Start job processing
            job_task = asyncio.create_task(worker._process_job(job))
            
            # Signal shutdown after a short delay
            async def delayed_shutdown():
                await asyncio.sleep(0.1)
                worker.signal_handler(signal.SIGTERM, None)
            
            shutdown_task = asyncio.create_task(delayed_shutdown())
            
            # Wait for both tasks
            await asyncio.gather(job_task, shutdown_task)
            
            # Job should still complete despite shutdown signal
            assert job.status == JOB_STATUS.COMPLETED
            assert job.result == "completed"
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_max_jobs_limit(self, worker_config, mock_services):
        """Test worker max jobs limit."""
        def simple_job():
            return "done"
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            worker.max_jobs = 3  # Set low limit for testing
            await worker.initialize()
            
            # Process jobs up to the limit
            for i in range(3):
                job = Job(function=simple_job, job_id=f"job-{i}", queue_name="test-queue")
                await worker._process_job(job)
                worker._jobs_processed += 1
            
            # Worker should indicate it's ready to stop
            assert worker._jobs_processed == 3
            assert worker._should_stop_after_max_jobs()
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_worker_status_transitions(self, worker_config, mock_services):
        """Test worker status transitions during job processing."""
        def test_job():
            time.sleep(0.1)  # Simulate work
            return "result"
        
        job = Job(function=test_job, job_id="status-test", queue_name="test-queue")
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**worker_config)
            await worker.initialize()
            
            # Process job
            await worker._process_job(job)
            
            # Verify status transitions
            status_calls = mock_services["status_manager"].update_status.call_args_list
            
            # Should have called update_status multiple times
            assert len(status_calls) >= 2
            
            # Should have transitioned to busy and back to idle
            status_values = [call[0][0] for call in status_calls]
            assert "busy" in status_values
            assert "idle" in status_values
            
            await worker.close()


class TestWorkerErrorHandling:
    """Test worker error handling scenarios."""

    @pytest.fixture
    def mock_services(self):
        """Mock service dependencies."""
        return {
            "status_manager": AsyncMock(spec=WorkerStatusManager),
            "job_manager": AsyncMock(spec=JobStatusManager),
            "failed_handler": AsyncMock(spec=FailedJobHandler)
        }

    @pytest.mark.asyncio
    async def test_job_timeout_handling(self, mock_services):
        """Test job timeout handling."""
        async def timeout_job():
            await asyncio.sleep(1.0)  # Long running job
            return "completed"
        
        job = Job(
            function=timeout_job,
            job_id="timeout-test",
            queue_name="test-queue",
            timeout=0.1  # Short timeout
        )
        
        config = {"queues": ["test-queue"], "concurrency": 1}
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**config)
            await worker.initialize()
            
            # Process job (should timeout)
            await worker._process_job(job)
            
            # Job should be marked as failed due to timeout
            assert job.status == JOB_STATUS.FAILED
            assert "timeout" in job.error.lower()
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_connection_error_recovery(self, mock_services):
        """Test worker recovery from connection errors."""
        def test_job():
            return "success"
        
        job = Job(function=test_job, job_id="conn-test", queue_name="test-queue")
        
        config = {"queues": ["test-queue"], "concurrency": 1}
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            # Simulate connection error during status update
            mock_services["status_manager"].update_status.side_effect = [
                ConnectionError("Connection lost"),  # First call fails
                None  # Second call succeeds
            ]
            
            worker = Worker(**config)
            await worker.initialize()
            
            # Process job (should handle connection error)
            await worker._process_job(job)
            
            # Job should still complete successfully
            assert job.status == JOB_STATUS.COMPLETED
            assert job.result == "success"
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_serialization_error_handling(self, mock_services):
        """Test handling of job serialization errors."""
        class UnserializableResult:
            def __reduce__(self):
                raise TypeError("Cannot serialize")
        
        def job_with_bad_result():
            return UnserializableResult()
        
        job = Job(
            function=job_with_bad_result,
            job_id="serial-test",
            queue_name="test-queue"
        )
        
        config = {"queues": ["test-queue"], "concurrency": 1}
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**config)
            await worker.initialize()
            
            # Process job (should handle serialization error)
            await worker._process_job(job)
            
            # Job should be marked as failed
            assert job.status == JOB_STATUS.FAILED
            assert "serialization" in job.error.lower() or "serialize" in job.error.lower()
            
            await worker.close()


class TestWorkerConfiguration:
    """Test worker configuration options."""

    @pytest.mark.asyncio
    async def test_custom_worker_name(self):
        """Test worker with custom name."""
        config = {
            "queues": ["test-queue"],
            "worker_name": "custom-worker-123"
        }
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(),
            JobStatusManager=AsyncMock(),
            FailedJobHandler=AsyncMock()
        ):
            worker = Worker(**config)
            assert "custom-worker-123" in worker.worker_id

    @pytest.mark.asyncio
    async def test_multiple_queues(self):
        """Test worker with multiple queues."""
        config = {
            "queues": ["queue1", "queue2", "queue3"],
            "concurrency": 2
        }
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(),
            JobStatusManager=AsyncMock(),
            FailedJobHandler=AsyncMock()
        ):
            worker = Worker(**config)
            assert worker.queue_names == ["queue1", "queue2", "queue3"]
            assert len(worker.subjects) == 3

    @pytest.mark.asyncio
    async def test_custom_heartbeat_interval(self):
        """Test worker with custom heartbeat interval."""
        config = {
            "queues": ["test-queue"],
            "heartbeat_interval": 15
        }
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(),
            JobStatusManager=AsyncMock(),
            FailedJobHandler=AsyncMock()
        ):
            worker = Worker(**config)
            assert worker.heartbeat_interval == 15


class TestWorkerMetrics:
    """Test worker metrics and monitoring."""

    @pytest.fixture
    def mock_services(self):
        """Mock service dependencies."""
        return {
            "status_manager": AsyncMock(spec=WorkerStatusManager),
            "job_manager": AsyncMock(spec=JobStatusManager),
            "failed_handler": AsyncMock(spec=FailedJobHandler)
        }

    @pytest.mark.asyncio
    async def test_job_processing_metrics(self, mock_services):
        """Test job processing metrics collection."""
        def test_job():
            return "result"
        
        jobs = [
            Job(function=test_job, job_id=f"metric-job-{i}", queue_name="test-queue")
            for i in range(5)
        ]
        
        config = {"queues": ["test-queue"], "concurrency": 1}
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**config)
            await worker.initialize()
            
            # Process all jobs
            for job in jobs:
                await worker._process_job(job)
                worker._jobs_processed += 1
            
            # Verify metrics
            assert worker._jobs_processed == 5
            assert worker._jobs_completed == 5
            assert worker._jobs_failed == 0
            
            await worker.close()

    @pytest.mark.asyncio
    async def test_performance_metrics(self, mock_services):
        """Test worker performance metrics."""
        def varying_duration_job(duration):
            time.sleep(duration)
            return f"slept for {duration}"
        
        # Jobs with different durations
        durations = [0.01, 0.02, 0.03, 0.04, 0.05]
        jobs = [
            Job(
                function=varying_duration_job,
                args=(duration,),
                job_id=f"perf-job-{i}",
                queue_name="test-queue"
            )
            for i, duration in enumerate(durations)
        ]
        
        config = {"queues": ["test-queue"], "concurrency": 1}
        
        with patch.multiple(
            'naq.worker.core',
            WorkerStatusManager=AsyncMock(return_value=mock_services["status_manager"]),
            JobStatusManager=AsyncMock(return_value=mock_services["job_manager"]),
            FailedJobHandler=AsyncMock(return_value=mock_services["failed_handler"])
        ):
            worker = Worker(**config)
            await worker.initialize()
            
            start_time = time.time()
            
            # Process all jobs
            for job in jobs:
                await worker._process_job(job)
            
            total_time = time.time() - start_time
            
            # Verify all jobs completed
            for job in jobs:
                assert job.status == JOB_STATUS.COMPLETED
                assert job._start_time is not None
                assert job._finish_time is not None
            
            # Total time should be approximately sum of durations
            expected_min_time = sum(durations)
            assert total_time >= expected_min_time
            
            await worker.close()