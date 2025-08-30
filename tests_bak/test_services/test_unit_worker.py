import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
import asyncio
import cloudpickle
from datetime import datetime, timezone
import socket
import os

from naq.worker import Worker
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS
from naq.models.enums import WORKER_STATUS
from naq.settings import (
    NAQ_PREFIX,
    WORKER_KV_NAME,
    RESULT_KV_NAME,
    JOB_STATUS_KV_NAME,
)

@pytest_asyncio.fixture
async def worker(service_test_config, mocker, settings_with_valid_queue, mock_queue_manager,
                 mock_job_status_manager, mock_worker_status_manager, mock_failed_job_handler,
                 service_aware_nats_mock):
    """Setup a test worker with ServiceManager architecture and mocked services."""
    
    # Patch manager classes before Worker instantiation
    mocker.patch('naq.worker.JobStatusManager', return_value=mock_job_status_manager)
    mocker.patch('naq.worker.WorkerStatusManager', return_value=mock_worker_status_manager)
    mocker.patch('naq.worker.FailedJobHandler', return_value=mock_failed_job_handler)
    
    # Patch NATS connections for _connect
    mock_nc, mock_js = service_aware_nats_mock
    mocker.patch('naq.connection.get_nats_connection', return_value=mock_nc)
    mocker.patch('naq.connection.get_jetstream_context', return_value=mock_js)
    mocker.patch('naq.connection.ensure_stream')

    # Create a service manager and register all required services
    from naq.services.base import ServiceManager, ServiceConfig
    from naq.services.connection import ConnectionService
    from naq.services.streams import StreamService
    from naq.services.kv_stores import KVStoreService
    from naq.services.jobs import JobService
    from naq.services.events import EventService
    
    # Create service manager with service_test_config
    service_config = ServiceConfig(**service_test_config["service_config"])
    service_manager = ServiceManager(service_config)
    
    # Register connection service with mocked NATS
    connection_service = ConnectionService(config=service_config)
    connection_service._nc = mock_nc
    connection_service._js = mock_js
    connection_service._is_initialized = True
    
    # Register stream service with mocked NATS and connection service
    stream_service = StreamService(config=service_config, connection_service=connection_service)
    stream_service._nc = mock_nc
    stream_service._js = mock_js
    stream_service._is_initialized = True
    
    # Register kv_store service with mocked NATS and connection service
    kv_store_service = KVStoreService(config=service_config, connection_service=connection_service)
    kv_store_service._nc = mock_nc
    kv_store_service._js = mock_js
    kv_store_service._is_initialized = True
    
    # Register job service with mocked NATS and connection service
    job_service = JobService(config=service_config, connection_service=connection_service)
    job_service._nc = mock_nc
    job_service._js = mock_js
    job_service._is_initialized = True
    
    # Register event service with mocked NATS and connection service
    event_service = EventService(config=service_config, connection_service=connection_service)
    event_service._nc = mock_nc
    event_service._js = mock_js
    event_service._is_initialized = True
    
    # Manually register all services
    service_manager._services["connection"] = connection_service
    service_manager._service_configs["connection"] = service_config
    service_manager._services["stream"] = stream_service
    service_manager._service_configs["stream"] = service_config
    service_manager._services["kv_store"] = kv_store_service
    service_manager._service_configs["kv_store"] = service_config
    service_manager._services["jobs"] = job_service
    service_manager._service_configs["jobs"] = service_config
    service_manager._services["events"] = event_service
    service_manager._service_configs["events"] = service_config

    # Create worker with config from service_test_config
    worker_args = {
        "queues": [settings_with_valid_queue['DEFAULT_QUEUE_NAME']],
        "nats_url": service_test_config["nats_url"],
        "concurrency": service_test_config["concurrency"],
        "worker_name": service_test_config["worker_name"],
        "service_manager": service_manager,
    }

    worker_instance = Worker(**worker_args)
    
    # Setup mock KV stores on the mock_js that worker_instance will use
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

    # Run _connect as the fixture is async and pytest-asyncio handles the loop
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
    async def test_fetch_job_success(self, worker, service_aware_nats_mock):
        """Test successful job fetching."""
        mock_nc, mock_js = service_aware_nats_mock
        
        # Create a test job
        job_func = lambda: "test"
        job = Job(
            function=job_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )

        # Create mock message with job data
        from naq.serializers import PickleSerializer
        mock_msg = AsyncMock()
        mock_msg.data = PickleSerializer.serialize_job(job)
        
        # Process the message
        await worker.job_processor.process_message(mock_msg)
        
        assert isinstance(job, Job)
        assert job.job_id == "test_job"
        assert job.queue_name == "test_queue"
    
    @pytest.mark.asyncio
    async def test_fetch_job_empty(self, worker, service_aware_nats_mock):
        """Test behavior when no jobs are available."""
        mock_nc, mock_js = service_aware_nats_mock
        
        # Create mock message with empty data
        mock_msg = AsyncMock()
        mock_msg.data = cloudpickle.dumps({})
        
        # Process the empty message
        await worker.job_processor.process_message(mock_msg)
        
        # Verify no processing occurred
        mock_js.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_job_success(self, worker):
        """Test successful job execution."""
        # Create a mock job with a function that returns a value
        mock_result = "success"
        mock_func = AsyncMock(return_value=mock_result)
        
        job = Job(
            function=mock_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )
        
        await worker.job_processor.process_message(job)
        
        assert job.result == mock_result
        assert job.status == JOB_STATUS.COMPLETED
        assert job.error is None
    
    @pytest.mark.asyncio
    async def test_execute_job_failure(self, worker):
        """Test job execution failure handling."""
        # Create a mock job with a function that raises an exception
        error_msg = "Test error"
        mock_func = AsyncMock(side_effect=ValueError(error_msg))
        
        job = Job(
            function=mock_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={},
            max_retries=0  # No retries for this test
        )
        
        await worker.job_processor.process_message(job)
        
        assert job.status == JOB_STATUS.FAILED
        assert job.error == error_msg

    @pytest.mark.asyncio
    async def test_handle_successful_job(self, worker, mock_job_status_manager, mock_worker_status_manager):
        """Test successful job completion handling with mock manager instances"""
        # Arrange
        mock_func = AsyncMock(return_value="success")
        job = Job(
            function=mock_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )
    
        # Act
        await worker.job_processor.process_message(job)
    
        # Assert
        # Verify JobStatusManager interactions
        mock_job_status_manager.store_result.assert_awaited_with(job)
        assert job.status == JOB_STATUS.COMPLETED
    
        # Verify WorkerStatusManager interactions
        # Check that update_status was called for BUSY and then IDLE
        wsm_calls = mock_worker_status_manager.update_status.await_args_list
        
        busy_call_found = any(
            call.args == (WORKER_STATUS.BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in wsm_calls
        )
        idle_call_found = any(
            call.args == (WORKER_STATUS.IDLE,) and call.kwargs.get("job_id") is None # After processing, job_id might be None for IDLE
            for call in wsm_calls
        )
        assert busy_call_found, "WorkerStatusManager was not set to BUSY"
        assert idle_call_found, "WorkerStatusManager was not set to IDLE"

    @pytest.mark.asyncio
    async def test_handle_failed_job(self, worker, mock_job_status_manager, mock_worker_status_manager, mock_failed_job_handler):
        """Test failed job handling with mock manager instances"""
        # Arrange
        error_msg = "Test error"
        mock_func = AsyncMock(side_effect=ValueError(error_msg))
        job = Job(
            function=mock_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={},
            max_retries=0
        )
        
        # Act
        await worker.job_processor.process_message(job)
    
        # Assert
        mock_job_status_manager.store_result.assert_awaited_with(job)
        assert job.status == JOB_STATUS.FAILED
        
        # Verify failed job handler was called
        mock_failed_job_handler.handle_failed_job.assert_awaited_with(job)
        
        # Verify WorkerStatusManager interactions
        wsm_calls = mock_worker_status_manager.update_status.await_args_list
        busy_call_found = any(
            call.args == (WORKER_STATUS.BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in wsm_calls
        )
        idle_call_found = any(
            call.args == (WORKER_STATUS.IDLE,) and call.kwargs.get("job_id") is None
            for call in wsm_calls
        )
        assert busy_call_found, "WorkerStatusManager was not set to BUSY for failed job"
        assert idle_call_found, "WorkerStatusManager was not set to IDLE for failed job"
    
    @pytest.mark.asyncio
    async def test_state_transitions(self, worker, mock_worker_status_manager):
        """Test worker state transitions during job processing"""
        # Arrange
        async def test_func():
            await asyncio.sleep(0.1)
            return "success"
            
        job = Job(
            function=test_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )
    
        # Act
        await worker.job_processor.process_message(job)
    
        # Assert
        wsm_calls = mock_worker_status_manager.update_status.await_args_list
    
        # Verify BUSY state was set
        # WorkerStatusManager.update_status is called with (status, job_id=..., queue_name=...)
        assert any(
            call.args == (WORKER_STATUS.BUSY,) and
            call.kwargs.get("job_id") == job.job_id
            # queue_name is not passed by Worker.process_message to update_status
            for call in wsm_calls
        ), "WorkerStatusManager was not set to BUSY with correct job_id"
    
        # Verify IDLE state was set
        # WorkerStatusManager.update_status is called with (status, job_id=None, queue_name=None) in finally block
        assert any(
            call.args == (WORKER_STATUS.IDLE,) and
            call.kwargs.get("job_id") is None and # job_id is typically None for general IDLE state
            call.kwargs.get("queue_name") is None
            for call in wsm_calls
        ), "WorkerStatusManager was not set to IDLE correctly"
        
        # Verify state transition order (BUSY before IDLE)
        busy_indices = [i for i, call in enumerate(wsm_calls) if call.args == (WORKER_STATUS.BUSY,) and call.kwargs.get("job_id") == job.job_id]
        idle_indices = [i for i, call in enumerate(wsm_calls) if call.args == (WORKER_STATUS.IDLE,)]
    
        assert busy_indices, "BUSY state not found in WorkerStatusManager calls"
        assert idle_indices, "IDLE state not found in WorkerStatusManager calls"
        assert min(busy_indices) < max(idle_indices), "BUSY state did not occur before final IDLE state"
    
    @pytest.mark.asyncio
    async def test_concurrency_limit(self, worker, mock_worker_status_manager, service_aware_nats_mock):
        mock_nc, mock_js = service_aware_nats_mock
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
                function=slow_job,
                job_id=f"test_job_{i}",
                queue_name="test_queue",
                args=(),
                kwargs={}
            )
            jobs.append(job)

        # Process jobs concurrently
        start_time = datetime.now(timezone.utc)
        tasks = [worker.job_processor.process_message(job) for job in jobs]
        await asyncio.gather(*tasks)
        end_time = datetime.now(timezone.utc)

        # With concurrency of 2, processing 4 jobs should take at least 2 cycles
        duration = (end_time - start_time).total_seconds()
        assert duration >= 0.18  # At least 2 cycles of 0.1 seconds with buffer

        # Get all status updates
        wsm_calls = mock_worker_status_manager.update_status.await_args_list
        status_updates_from_manager = []
        for call in wsm_calls:
            status_updates_from_manager.append({
                "status": call.args[1],
                "job_id": call.kwargs.get("job_id"),
            })
            
        max_concurrent = 0
        current_busy = 0
        for update in status_updates_from_manager:
            if update["status"] == WORKER_STATUS.BUSY:
                current_busy += 1
                max_concurrent = max(max_concurrent, current_busy)
            elif update["status"] == WORKER_STATUS.IDLE and update.get("job_id") is not None:
                current_busy -= 1
            
        assert max_concurrent <= worker_instance._concurrency
        assert current_busy == 0

        # Track maximum concurrent busy states
        max_concurrent = 0
        current_busy = 0
        for update in status_updates_from_manager:
            if update["status"] == WORKER_STATUS.BUSY:
                current_busy += 1
                max_concurrent = max(max_concurrent, current_busy)
            elif update["status"] == WORKER_STATUS.IDLE:
                current_busy -= 1

        # Verify concurrency limit was respected
        assert max_concurrent <= worker._concurrency
        assert current_busy == 0  # Should end with all jobs complete
            
        # Verify all jobs completed
        for job in jobs:
            assert job.status == JOB_STATUS.COMPLETED
            assert job.result == "done"
        

    @pytest.mark.asyncio
    async def test_graceful_shutdown_flag(self, worker, mock_worker_status_manager):
        """Test shutdown flag handling."""
        worker.install_signal_handlers()
        
        # Create a test job
        mock_func = AsyncMock(return_value="success")
        job = Job(
            function=mock_func,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )
        
        # Simulate shutdown signal
        worker_instance.signal_handler(None, None)
        assert worker_instance._shutdown_event.is_set() is True
        
        # Process message after shutdown signal
        await worker.job_processor.process_message(job)
        
        # Verify job wasn't executed
        mock_func.assert_not_awaited()
        
        # Verify worker status was not set to BUSY for this job, or if it was, it was before the shutdown check.
        # The current logic in process_message sets to BUSY *after* shutdown check if job is processed.
        # If job is not processed due to shutdown, BUSY for *this* job_id should not be set.
        # The finally block will set it to IDLE.
        
        busy_for_this_job_found = any(
            call.args == (WORKER_STATUS.BUSY,) and call.kwargs.get("job_id") == job.job_id
            for call in mock_worker_status_manager.update_status.await_args_list
        )
        assert not busy_for_this_job_found, "Worker status was set to BUSY for a job that should not have been processed due to shutdown."
        
        # Verify job status remained PENDING (its initial state)
        assert job.status == JOB_STATUS.PENDING
    
    @pytest.mark.asyncio
    async def test_shutdown_during_execution(self, worker):
        """Test shutdown handling during job execution."""
        async def long_running_job():
            await asyncio.sleep(0.2)
            return "done"

        job = Job(
            function=long_running_job,
            job_id="test_job",
            queue_name="test_queue",
            args=(),
            kwargs={}
        )

        # Start job processing
        process_task = asyncio.create_task(worker.job_processor.process_message(job))

        # Wait briefly then trigger shutdown
        await asyncio.sleep(0.1)
        worker.signal_handler(None, None)

        # Wait for job to complete
        await process_task

        # Verify job completed despite shutdown
        assert job.status == JOB_STATUS.COMPLETED
        assert job.result == "done"