import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
import cloudpickle

from naq.worker import Worker
from naq.job import Job
from naq.models import JOB_STATUS
from naq.models import WORKER_STATUS
from naq.settings import (
    NAQ_PREFIX,
    RESULT_KV_NAME,
)


async def noop_job() -> str:
    """A simple async job that does nothing."""
    return "success"


# Move the async fixture outside the class and use function-level pytest.mark.asyncio


@pytest_asyncio.fixture
async def mock_worker(mock_nats):
    """Setup a test worker with mocked NATS connection."""
    mock_nc, mock_js = mock_nats
    with (
        patch("naq.worker.get_nats_connection", return_value=mock_nc),
        patch("naq.worker.get_jetstream_context", return_value=mock_js),
        patch("naq.worker.ensure_stream"),
    ):
        worker = Worker(queues="test_queue", worker_name="test_worker")
        await worker._connect()  # Establish mock connections
        yield worker


class TestWorkerSmoke:
    """Smoke tests for the Worker class."""

    @pytest.mark.asyncio
    async def test_basic_worker_instantiation(self):
        """Test that a Worker can be created with default parameters."""
        # Create worker with minimal parameters
        worker = Worker(queues="test_queue")

        # Verify basic attributes are set correctly
        assert worker.queue_names == ["test_queue"]
        assert worker.subjects == [f"{NAQ_PREFIX}.queue.test_queue"]
        assert worker._concurrency == 10  # Default concurrency
        assert isinstance(worker.worker_id, str)

    @pytest.mark.asyncio
    async def test_worker_custom_settings(self):
        """Test that a Worker can be created with custom settings."""
        # Create worker with custom parameters
        worker = Worker(
            queues=["queue1", "queue2"], concurrency=5, worker_name="custom_worker"
        )

        # Verify custom settings are applied
        assert worker.queue_names == ["queue1", "queue2"]
        assert worker._concurrency == 5
        assert worker.worker_id.startswith("custom_worker")

    @pytest.mark.asyncio
    async def test_queue_connection_simulation(self, mock_worker, mock_nats):
        """Test that worker can connect to queues and setup consumers."""
        mock_nc, mock_js = mock_nats

        # Mock consumer setup
        mock_consumer = AsyncMock()
        mock_js.pull_subscribe.return_value = mock_consumer

        # Subscribe to queue
        await mock_worker._subscribe_to_queue("test_queue")

        # Verify consumer setup
        mock_js.pull_subscribe.assert_awaited_once()
        assert mock_worker._consumers

    @pytest.mark.asyncio
    async def test_simple_job_processing(self, mock_worker):
        """Test processing of a single simple job."""
        # Create a simple job that will succeed
        job = Job(noop_job, queue_name="test_queue")

        # Create mock message with the job
        mock_msg = AsyncMock()
        mock_msg.data = job.serialize()

        # Process the job
        await mock_worker.process_message(mock_msg)

        # Get the mock KV store and verify persisted job status
        mock_kv_store = await mock_worker._js.key_value(RESULT_KV_NAME)
        
        # Debug output
        print(f"Mock KV store: {mock_kv_store}")
        print(f"Mock KV store put method: {mock_kv_store.put}")
        print(f"Mock KV store put call count: {mock_kv_store.put.call_count}")
        print(f"Mock KV store put mock calls: {mock_kv_store.put.mock_calls}")
        
        mock_kv_store.put.assert_called()

        # Find the call with matching job ID
        persisted_data = None
        for call in mock_kv_store.put.mock_calls:
            print(f"Call args: {call.args}")
            if call.args[0] == job.job_id.encode("utf-8"):
                persisted_data = cloudpickle.loads(call.args[1])
                break

        assert persisted_data is not None
        assert persisted_data["status"] == JOB_STATUS.COMPLETED.value
        assert persisted_data["result"] == "success"
        assert persisted_data.get("error") is None

        # Verify message was acknowledged
        mock_msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_basic_lifecycle(self, mock_worker):
        """Test the basic worker lifecycle - start, process one job, prepare for shutdown."""
        # Create a job that will complete quickly
        job = Job(noop_job, queue_name="test_queue")
        mock_msg = AsyncMock()
        mock_msg.data = job.serialize()

        # Start worker status tracking
        await mock_worker._worker_status_manager.start_heartbeat_loop()

        # Process one job
        await mock_worker.process_message(mock_msg)

        # Request shutdown
        mock_worker.signal_handler(None, None)

        # Stop worker status tracking
        await mock_worker._worker_status_manager.stop_heartbeat_loop()

        # Verify expected behavior
        assert mock_worker._shutdown_event.is_set()  # Shutdown flag is set

        # Verify persisted job status
        mock_kv_store = await mock_worker._js.key_value(RESULT_KV_NAME)
        mock_kv_store.put.assert_called()

        persisted_data = None
        for call in mock_kv_store.put.mock_calls:
            if call.args[0] == job.job_id.encode("utf-8"):
                persisted_data = cloudpickle.loads(call.args[1])
                break

        assert persisted_data is not None
        assert persisted_data["status"] == JOB_STATUS.COMPLETED.value

        mock_msg.ack.assert_awaited_once()  # Message was acknowledged

    @pytest.mark.asyncio
    async def test_error_free_operation(self, mock_worker):
        """Test complete error-free worker operation flow."""
        # Create a simple successful job
        job = Job(noop_job, queue_name="test_queue")
        mock_msg = AsyncMock()
        mock_msg.data = job.serialize()

        # Start worker
        await mock_worker._worker_status_manager.start_heartbeat_loop()
        assert (
            mock_worker._worker_status_manager._current_status
            == WORKER_STATUS.IDLE.value
        )

        # Process job
        await mock_worker.process_message(mock_msg)

        # Verify persisted job status
        mock_kv_store = await mock_worker._js.key_value(RESULT_KV_NAME)
        mock_kv_store.put.assert_called()

        persisted_data = None
        for call in mock_kv_store.put.mock_calls:
            if call.args[0] == job.job_id.encode("utf-8"):
                persisted_data = cloudpickle.loads(call.args[1])
                break

        assert persisted_data is not None
        assert persisted_data["status"] == JOB_STATUS.COMPLETED.value
        assert persisted_data["result"] == "success"
        assert (
            mock_worker._worker_status_manager._current_status
            == WORKER_STATUS.IDLE.value
        )

        # Clean shutdown
        await mock_worker._worker_status_manager.stop_heartbeat_loop()
        await mock_worker._close()

        # Verify no errors occurred in persisted data
        assert not persisted_data.get("error")
        assert not persisted_data.get("traceback")
        mock_msg.ack.assert_awaited_once()
