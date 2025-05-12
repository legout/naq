import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from nats.js.kv import KeyValue
import asyncio
import cloudpickle
from datetime import datetime, timezone
import socket
import os

from naq.worker import Worker, WorkerStatusManager, JobStatusManager, FailedJobHandler
from naq.job import Job, JobExecutionError, JobStatus
from naq.settings import (
    WORKER_STATUS_STARTING,
    WORKER_STATUS_IDLE,
    WORKER_STATUS_BUSY,
    WORKER_STATUS_STOPPING,
    NAQ_PREFIX,
    WORKER_KV_NAME,
    RESULT_KV_NAME,
    JOB_STATUS_KV_NAME,
)

@pytest_asyncio.fixture
async def worker(mock_nats, mocker, settings_with_valid_queue, mock_queue_manager,
                 mock_job_status_manager, mock_worker_status_manager, mock_failed_job_handler):
    """Setup a test worker with mocked NATS and managers."""
    mock_nc, mock_js = mock_nats

    # Patch worker dependencies with our mocks
    mocker.patch('naq.worker.get_nats_connection', return_value=mock_nc)
    mocker.patch('naq.worker.get_jetstream_context', return_value=mock_js)
    mocker.patch('naq.worker.ensure_stream')
    mocker.patch('naq.worker.JobStatusManager', return_value=mock_job_status_manager)
    mocker.patch('naq.worker.WorkerStatusManager', return_value=mock_worker_status_manager)
    mocker.patch('naq.worker.FailedJobHandler', return_value=mock_failed_job_handler)

    # Create worker instance with required arguments
    worker_instance = Worker(
        queues=[settings_with_valid_queue['DEFAULT_QUEUE_NAME']],
        worker_name="test_worker",
    )

    # Setup mock KV stores
    mock_job_status_kv = AsyncMock(name="mock_job_status_kv")
    mock_result_kv = AsyncMock(name="mock_result_kv")
    mock_worker_kv = AsyncMock(name="mock_worker_kv")
    
    async def kv_side_effect(bucket, **kwargs):
        if bucket == JOB_STATUS_KV_NAME:
            return mock_job_status_kv
        elif bucket == RESULT_KV_NAME:
            return mock_result_kv
        elif bucket == WORKER_KV_NAME:
            return mock_worker_kv
        raise ValueError(f"Unexpected bucket name for mock_js.key_value: {bucket}")
    
    mock_js.key_value.side_effect = kv_side_effect

    # Connect the worker to establish JetStream context
    await worker_instance._connect()

    return worker_instance

class TestWorker:
    """Test cases for the Worker class."""

    @pytest.mark.asyncio
    async def test_init_defaults(self):
        """Test worker initialization with default parameters."""
        worker = Worker(queues="test_queue")
        
        assert isinstance(worker.queue_names, list)
        assert worker.queue_names == ["test_queue"]
        assert worker.subjects == [f"{NAQ_PREFIX}.queue.test_queue"]
        assert worker._concurrency == 10  # Default concurrency
        assert worker.worker_id.startswith(f"naq-worker-{socket.gethostname()}-{os.getpid()}")
    
    @pytest.mark.asyncio
    async def test_init_custom_params(self):
        """Test worker initialization with custom parameters."""
        worker = Worker(
            queues=["queue1", "queue2"],
            nats_url="nats://custom:4222",
            concurrency=5,
            worker_name="custom_worker"
        )
        
        assert worker.queue_names == ["queue1", "queue2"]
        assert worker.subjects == [f"{NAQ_PREFIX}.queue.queue1", f"{NAQ_PREFIX}.queue.queue2"]
        assert worker._nats_url == "nats://custom:4222"
        assert worker._concurrency == 5
        assert worker.worker_id.startswith("custom_worker-")

    @pytest.mark.asyncio
    async def test_fetch_job_success(self, worker, mock_nats):
        """Test successful job fetching."""
        mock_nc, mock_js = mock_nats
        
        # Create a test job
        job_func = lambda: "test"
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=job_func,
            args=(),
            kwargs={}
        )

        # Create mock message with job data
        mock_msg = AsyncMock()
        mock_msg.data = cloudpickle.dumps(job.__dict__)
        
        # Process the message
        await worker.process_message(mock_msg)
        
        assert isinstance(job, Job)
        assert job.job_id == "test_job"
        assert job.queue_name == "test_queue"
    
    @pytest.mark.asyncio
    async def test_fetch_job_empty(self, worker, mock_nats):
        """Test behavior when no jobs are available."""
        mock_nc, mock_js = mock_nats
        
        # Create mock message with empty data
        mock_msg = AsyncMock()
        mock_msg.data = cloudpickle.dumps({})
        
        # Process the empty message
        await worker.process_message(mock_msg)
        
        # Verify no processing occurred
        mock_js.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_job_success(self, worker):
        """Test successful job execution."""
        # Create a mock job with a function that returns a value
        mock_result = "success"
        mock_func = AsyncMock(return_value=mock_result)
        
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=mock_func,
            args=(),
            kwargs={}
        )
        
        await worker.process_message(job)
        
        assert job.result == mock_result
        assert job.status == JobStatus.COMPLETED
        assert job.error is None
    
    @pytest.mark.asyncio
    async def test_execute_job_failure(self, worker):
        """Test job execution failure handling."""
        # Create a mock job with a function that raises an exception
        error_msg = "Test error"
        mock_func = AsyncMock(side_effect=ValueError(error_msg))
        
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=mock_func,
            args=(),
            kwargs={},
            max_retries=0  # No retries for this test
        )
        
        await worker.process_message(job)
        
        assert job.status == JobStatus.FAILED
        assert job.error == error_msg

    @pytest.mark.asyncio
    async def test_handle_successful_job(self, worker_instance_dict):
        """Test successful job completion handling with mock manager instances"""
        # Arrange
        worker_instance = worker_instance_dict["worker"]
        mock_jsm = worker_instance_dict["job_status_manager"]
        mock_wsm = worker_instance_dict["worker_status_manager"]
    
        mock_func = AsyncMock(return_value="success")
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=mock_func,
            args=(),
            kwargs={}
        )
    
        # Act
        await worker_instance.process_message(job)
    
        # Assert
        # Verify JobStatusManager interactions
        mock_jsm.store_result.assert_awaited_with(job)
        assert job.status == JobStatus.COMPLETED
    
        # Verify WorkerStatusManager interactions
        # Check that update_status was called for BUSY and then IDLE
        wsm_calls = mock_wsm.update_status.await_args_list
        
        busy_call_found = any(
            call.args == (WORKER_STATUS_BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in wsm_calls
        )
        idle_call_found = any(
            call.args == (WORKER_STATUS_IDLE,) and call.kwargs.get("job_id") is None # After processing, job_id might be None for IDLE
            for call in wsm_calls
        )
        assert busy_call_found, "WorkerStatusManager was not set to BUSY"
        assert idle_call_found, "WorkerStatusManager was not set to IDLE"

    @pytest.mark.asyncio
    async def test_handle_failed_job(self, worker_instance_dict):
        """Test failed job handling with mock manager instances"""
        # Arrange
        worker_instance = worker_instance_dict["worker"]
        mock_jsm = worker_instance_dict["job_status_manager"]
        mock_wsm = worker_instance_dict["worker_status_manager"]
        mock_fjh = worker_instance_dict["failed_job_handler"]
        
        error_msg = "Test error"
        mock_func = AsyncMock(side_effect=ValueError(error_msg))
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=mock_func,
            args=(),
            kwargs={},
            max_retries=0
        )
        
        # Act
        await worker_instance.process_message(job)
        
        # Assert
        # Verify job status updates
        mock_jsm.store_result.assert_awaited_with(job)
        assert job.status == JobStatus.FAILED
        
        # Verify failed job handler was called
        mock_fjh.handle_failed_job.assert_awaited_with(job)
        
        # Verify WorkerStatusManager interactions
        wsm_calls = mock_wsm.update_status.await_args_list
        busy_call_found = any(
            call.args == (WORKER_STATUS_BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in wsm_calls
        )
        idle_call_found = any(
            call.args == (WORKER_STATUS_IDLE,) and call.kwargs.get("job_id") is None
            for call in wsm_calls
        )
        assert busy_call_found, "WorkerStatusManager was not set to BUSY for failed job"
        assert idle_call_found, "WorkerStatusManager was not set to IDLE for failed job"
    
    @pytest.mark.asyncio
    async def test_state_transitions(self, worker_instance_dict):
        """Test worker state transitions during job processing"""
        # Arrange
        worker_instance = worker_instance_dict["worker"]
        mock_wsm = worker_instance_dict["worker_status_manager"]
    
        async def test_func():
            await asyncio.sleep(0.1)
            return "success"
            
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=test_func,
            args=(),
            kwargs={}
        )
    
        # Act
        await worker_instance.process_message(job)
    
        # Assert
        wsm_calls = mock_wsm.update_status.await_args_list
    
        # Verify BUSY state was set
        # WorkerStatusManager.update_status is called with (status, job_id=..., queue_name=...)
        assert any(
            call.args == (WORKER_STATUS_BUSY,) and
            call.kwargs.get("job_id") == job.job_id
            # queue_name is not passed by Worker.process_message to update_status
            for call in wsm_calls
        ), "WorkerStatusManager was not set to BUSY with correct job_id"
    
        # Verify IDLE state was set
        # WorkerStatusManager.update_status is called with (status, job_id=None, queue_name=None) in finally block
        assert any(
            call.args == (WORKER_STATUS_IDLE,) and
            call.kwargs.get("job_id") is None and # job_id is typically None for general IDLE state
            call.kwargs.get("queue_name") is None
            for call in wsm_calls
        ), "WorkerStatusManager was not set to IDLE correctly"
        
        # Verify state transition order (BUSY before IDLE)
        busy_indices = [i for i, call in enumerate(wsm_calls) if call.args == (WORKER_STATUS_BUSY,) and call.kwargs.get("job_id") == job.job_id]
        idle_indices = [i for i, call in enumerate(wsm_calls) if call.args == (WORKER_STATUS_IDLE,)]
    
        assert busy_indices, "BUSY state not found in WorkerStatusManager calls"
        assert idle_indices, "IDLE state not found in WorkerStatusManager calls"
        assert min(busy_indices) < max(idle_indices), "BUSY state did not occur before final IDLE state"
    
        @pytest.mark.asyncio
        async def test_concurrency_limit(self, worker_instance_dict, mock_nats):
            worker_instance = worker_instance_dict["worker"]
            mock_wsm = worker_instance_dict["worker_status_manager"]
            mock_nc, mock_js = mock_nats
            mock_nc, mock_js = mock_nats
            worker_kv = await mock_js.key_value(bucket=WORKER_KV_NAME)
            
            # Set up worker with concurrency of 2
            worker._concurrency = 2
            worker._semaphore = asyncio.Semaphore(2)
    
            # Create mock jobs that take time to process
            async def slow_job():
                await asyncio.sleep(0.1)
                return "done"
    
            jobs = []
            for i in range(4):  # Create 4 jobs
                job = Job(
                    job_id=f"test_job_{i}",
                    queue_name="test_queue",
                    function=slow_job,
                    args=(),
                    kwargs={}
                )
                jobs.append(job)
    
            # Process jobs concurrently
            start_time = datetime.now(timezone.utc)
            tasks = [worker.process_message(job) for job in jobs]
            await asyncio.gather(*tasks)
            end_time = datetime.now(timezone.utc)
    
            # With concurrency of 2, processing 4 jobs should take at least 2 cycles
            duration = (end_time - start_time).total_seconds()
            assert duration >= 0.18  # At least 2 cycles of 0.1 seconds with buffer
    
            # Get all status updates
            wsm_calls = mock_wsm.update_status.await_args_list
            status_updates_from_manager = []
            for call in wsm_calls:
                status_updates_from_manager.append({
                    "status": call.args[1],
                    "job_id": call.kwargs.get("job_id"),
                })
            
            max_concurrent = 0
            current_busy = 0
            for update in status_updates_from_manager:
                if update["status"] == WORKER_STATUS_BUSY:
                    current_busy += 1
                    max_concurrent = max(max_concurrent, current_busy)
                elif update["status"] == WORKER_STATUS_IDLE and update.get("job_id") is not None:
                    current_busy -= 1
            
            assert max_concurrent <= worker_instance._concurrency
            assert current_busy == 0
    
            # Track maximum concurrent busy states
            max_concurrent = 0
            current_busy = 0
            for update in status_updates_from_manager:
                if update["status"] == WORKER_STATUS_BUSY:
                    current_busy += 1
                    max_concurrent = max(max_concurrent, current_busy)
                elif update["status"] == WORKER_STATUS_IDLE:
                    current_busy -= 1
    
            # Verify concurrency limit was respected
            assert max_concurrent <= worker._concurrency
            assert current_busy == 0  # Should end with all jobs complete
            
            # Verify all jobs completed
            for job in jobs:
                assert job.status == JobStatus.COMPLETED
                assert job.result == "done"
        

    @pytest.mark.asyncio
    async def test_graceful_shutdown_flag(self, worker_instance_dict, mock_nats):
        """Test shutdown flag handling."""
        worker_instance = worker_instance_dict["worker"]
        mock_wsm = worker_instance_dict["worker_status_manager"]
        mock_nc, mock_js = mock_nats
        
        worker_instance.install_signal_handlers()
        
        # Create a test job
        mock_func = AsyncMock(return_value="success")
        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=mock_func,
            args=(),
            kwargs={}
        )
        
        # Simulate shutdown signal
        worker_instance.signal_handler(None, None)
        assert worker_instance._shutdown_event.is_set() is True
        
        # Process message after shutdown signal
        await worker_instance.process_message(job)
        
        # Verify job wasn't executed
        mock_func.assert_not_awaited()
        
        # Verify worker status was not set to BUSY for this job, or if it was, it was before the shutdown check.
        # The current logic in process_message sets to BUSY *after* shutdown check if job is processed.
        # If job is not processed due to shutdown, BUSY for *this* job_id should not be set.
        # The finally block will set it to IDLE.
        
        busy_for_this_job_found = any(
            call.args == (WORKER_STATUS_BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in mock_wsm.update_status.await_args_list
        )
        assert not busy_for_this_job_found, "Worker status was set to BUSY for a job that should not have been processed due to shutdown."
        
        # Verify job status remained PENDING (its initial state)
        assert job.status == JobStatus.PENDING
    
    @pytest.mark.asyncio
    async def test_shutdown_during_execution(self, worker):
        """Test shutdown handling during job execution."""
        async def long_running_job():
            await asyncio.sleep(0.2)
            return "done"

        job = Job(
            job_id="test_job",
            queue_name="test_queue",
            function=long_running_job,
            args=(),
            kwargs={}
        )

        # Start job processing
        process_task = asyncio.create_task(worker.process_message(job))

        # Wait briefly then trigger shutdown
        await asyncio.sleep(0.1)
        worker.signal_handler(None, None)

        # Wait for job to complete
        await process_task

        # Verify job completed despite shutdown
        assert job.status == JobStatus.COMPLETED
        assert job.result == "done"