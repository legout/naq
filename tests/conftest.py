import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from nats.js.kv import KeyValue
from nats.js import JetStreamContext
import nats

import socket

from naq.settings import (
    WORKER_KV_NAME,
    JOB_STATUS_KV_NAME,
    RESULT_KV_NAME,
    DEFAULT_QUEUE_NAME,
)

from naq.worker import Worker


@pytest.fixture
def mock_job_status_manager():
    """Fixture for a mock JobStatusManager with async initialize, update_job_status, store_result and _result_kv_store, and a mock worker._js."""
    from unittest.mock import MagicMock, AsyncMock
    from naq.settings import RESULT_KV_NAME

    mock = MagicMock(name="JobStatusManager")
    mock.initialize = AsyncMock(name="initialize")
    mock.update_job_status = AsyncMock(name="update_job_status") # Changed from set_status
    mock.store_result = AsyncMock(name="store_result")
    
    mock_actual_result_kv_store = AsyncMock(name="actual_result_kv_store_on_jsm")
    mock._result_kv_store = mock_actual_result_kv_store
    
    mock_worker_on_jsm = MagicMock(name="worker_on_jsm")
    mock_js_on_worker = AsyncMock(name="js_on_worker_on_jsm")
    
    async def kv_side_effect(bucket=None, **kwargs):
        if bucket == RESULT_KV_NAME:
            return mock_actual_result_kv_store
        return AsyncMock(name=f"kv_store_for_{bucket}")

    mock_js_on_worker.key_value = AsyncMock(side_effect=kv_side_effect)
    
    mock_worker_on_jsm._js = mock_js_on_worker
    mock.worker = mock_worker_on_jsm
    return mock


@pytest.fixture
def worker_dict():
    """Fixture for a minimal Worker constructor argument dictionary."""
    return {
        "queues": ["default"],
        "nats_url": "nats://localhost:4222",
        "concurrency": 2,
        "worker_name": "test-worker",
        # Add other params if needed by tests
    }


def is_port_in_use(port: int) -> bool:
    """Check if a port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


# Removed deprecated custom event_loop fixture to avoid pytest-asyncio warning.


@pytest_asyncio.fixture(scope="function")
async def nats_server():
    """
    Ensure a NATS server is running on localhost:4222 for integration tests.

    This fixture does NOT start or stop NATS. You must run:
        docker compose up -d nats
    before running tests.

    If NATS is not available, tests using this fixture will be skipped.
    """
    if not is_port_in_use(4222):
        pytest.skip(
            "NATS server (localhost:4222) is not running. Please run 'docker compose up -d nats' before testing."
        )
    # Optionally, test connection
    try:
        nc = await nats.connect()
        await nc.close()
    except Exception:
        pytest.skip(
            "Could not connect to NATS server at localhost:4222. Please ensure it is running."
        )
    yield "nats://localhost:4222"


@pytest_asyncio.fixture
async def mock_nats(mocker):
    """Provide a mock NATS client with full JetStream support for testing"""
    # Create mock JetStream context with proper spec
    mock_js = AsyncMock(spec=JetStreamContext)
    mock_js.publish = AsyncMock(return_value=MagicMock(stream="test_stream", seq=1))
    mock_js.purge_stream = AsyncMock(return_value=5)
    mock_js.stream = AsyncMock()
    mock_js.consumer = AsyncMock()

    # Create distinct, fully mocked KeyValue store instances
    mock_worker_kv = AsyncMock(spec=KeyValue)
    mock_worker_kv.put = AsyncMock(name=f"{WORKER_KV_NAME}.put")
    mock_worker_kv.get = AsyncMock(name=f"{WORKER_KV_NAME}.get", return_value=None)
    mock_worker_kv.delete = AsyncMock(name=f"{WORKER_KV_NAME}.delete")
    mock_worker_kv.keys = AsyncMock(name=f"{WORKER_KV_NAME}.keys", return_value=[])

    mock_job_status_kv = AsyncMock(spec=KeyValue)
    mock_job_status_kv.put = AsyncMock(name=f"{JOB_STATUS_KV_NAME}.put")
    mock_job_status_kv.get = AsyncMock(
        name=f"{JOB_STATUS_KV_NAME}.get", return_value=None
    )
    mock_job_status_kv.delete = AsyncMock(name=f"{JOB_STATUS_KV_NAME}.delete")
    mock_job_status_kv.keys = AsyncMock(
        name=f"{JOB_STATUS_KV_NAME}.keys", return_value=[]
    )

    mock_result_kv = AsyncMock(spec=KeyValue)
    mock_result_kv.put = AsyncMock(name=f"{RESULT_KV_NAME}.put")
    mock_result_kv.get = AsyncMock(name=f"{RESULT_KV_NAME}.get", return_value=None)
    mock_result_kv.delete = AsyncMock(name=f"{RESULT_KV_NAME}.delete")
    mock_result_kv.keys = AsyncMock(name=f"{RESULT_KV_NAME}.keys", return_value=[])

    # Configure key_value to return appropriate KV store based on bucket name
    async def get_key_value_store_side_effect(bucket=None, **kwargs):
        print(f"DEBUG: mock_js.key_value called for bucket: {bucket}")  # Debug print
        if bucket == WORKER_KV_NAME:
            return mock_worker_kv
        elif bucket == JOB_STATUS_KV_NAME:
            return mock_job_status_kv
        elif bucket == RESULT_KV_NAME:
            return mock_result_kv
        raise ValueError(f"mock_js.key_value called with unexpected bucket: {bucket}")

    # Set up key_value with the side effect
    mock_js.key_value = AsyncMock(side_effect=get_key_value_store_side_effect)
    mock_js.create_key_value = AsyncMock(return_value=AsyncMock(spec=KeyValue))

    # Create NATS client mock with properly configured JetStream
    mock_nc = AsyncMock()
    mock_nc.jetstream = AsyncMock(
        return_value=mock_js
    )  # Make this an AsyncMock for consistency

    print(
        f"DEBUG: mock_nats fixture returning mock_js: {mock_js}, mock_js.key_value: {mock_js.key_value}"
    )

    return mock_nc, mock_js


@pytest_asyncio.fixture(scope="function")
async def nats_client(nats_server):
    """
    Provide a properly managed NATS client for testing.

    This fixture ensures the NATS client is created and cleaned up within
    the same event loop, preventing "Event loop is closed" errors.
    """
    nc = None
    try:
        nc = await nats.connect(nats_server)
        yield nc
    finally:
        if nc:
            try:
                await nc.drain()
            except Exception as e:
                print(f"Error during NATS client drain: {e}")
            try:
                await nc.close()
            except Exception as e:
                print(f"Error during NATS client close: {e}")


@pytest.fixture
def settings_with_valid_queue():
    """Provide settings with a valid queue configuration."""
    return {"DEFAULT_QUEUE_NAME": DEFAULT_QUEUE_NAME}


@pytest_asyncio.fixture
async def mock_queue_manager():
    """Provide a mock queue manager for testing."""
    mock_manager = AsyncMock()
    # Add any necessary mock methods that Worker might call
    mock_manager.get_js = AsyncMock()
    mock_manager.enqueue = AsyncMock()
    mock_manager.purge = AsyncMock()
    mock_manager.cancel_scheduled_job = AsyncMock()
    return mock_manager


@pytest.fixture
def mock_worker_status_manager():
    """Fixture for a mock WorkerStatusManager with async heartbeat, update_status, and set_status."""
    from unittest.mock import MagicMock, AsyncMock

    mock = MagicMock(name="WorkerStatusManager")
    mock.start_heartbeat_loop = AsyncMock(name="start_heartbeat_loop")
    mock.update_status = AsyncMock(name="update_status")
    mock.set_status = AsyncMock(name="set_status")
    return mock


@pytest_asyncio.fixture # Change to async fixture
async def worker_instance_dict( # Add async keyword
    mocker, # Add mocker fixture
    worker_dict,
    mock_job_status_manager,
    mock_worker_status_manager,
    mock_queue_manager,
    mock_failed_job_handler,
    mock_nats, # Add mock_nats for _connect
    settings_with_valid_queue # Add for queues argument
):
    """Fixture that returns a dict with a 'worker' key containing a Worker instance (with patched managers) and mock managers."""
    
    # Patch manager classes before Worker instantiation
    mocker.patch('naq.worker.core.WorkerStatusManager', return_value=mock_worker_status_manager)
    mocker.patch('naq.worker.core.JobStatusManager', return_value=mock_job_status_manager)
    mocker.patch('naq.worker.core.FailedJobHandler', return_value=mock_failed_job_handler)
    
    # Patch NATS connections for _connect
    mock_nc, mock_js = mock_nats
    mocker.patch('naq.worker.core.get_nats_connection', return_value=mock_nc)
    mocker.patch('naq.worker.core.get_jetstream_context', return_value=mock_js)
    mocker.patch('naq.worker.core.ensure_stream')


    # Create worker with basic args from worker_dict, but ensure queues is correct for this context
    # worker_dict might not have the right queue name if settings_with_valid_queue is different
    worker_args = worker_dict.copy()
    worker_args["queues"] = [settings_with_valid_queue['DEFAULT_QUEUE_NAME']]


    worker_instance = Worker(**worker_args)
    
    # Run _connect manually as it's not part of the constructor and tests might rely on it
    # Need to ensure worker_instance._js is set up for _get_kv_store if real managers were used (though they are mocked)
    # The mock_js from mock_nats should be used by the worker instance.
    # The worker._connect() method will use the patched get_nats_connection and get_jetstream_context.
    
    # Setup mock KV stores on the mock_js that worker_instance will use
    # This is similar to what the 'worker' fixture does.
    mock_job_status_kv_for_dict = AsyncMock(name="mock_job_status_kv_for_dict")
    mock_result_kv_for_dict = AsyncMock(name="mock_result_kv_for_dict")
    mock_worker_kv_for_dict = AsyncMock(name="mock_worker_kv_for_dict")

    async def kv_side_effect_for_dict(bucket, **kwargs):
        if bucket == JOB_STATUS_KV_NAME:
            return mock_job_status_kv_for_dict
        elif bucket == RESULT_KV_NAME:
            return mock_result_kv_for_dict
        elif bucket == WORKER_KV_NAME:
            return mock_worker_kv_for_dict
        raise ValueError(f"Unexpected bucket name for mock_js.key_value in worker_instance_dict: {bucket}")
    
    mock_js.key_value.side_effect = kv_side_effect_for_dict # Configure the mock_js from mock_nats

    # Run _connect as the fixture is async and pytest-asyncio handles the loop.
    await worker_instance._connect()

    result = dict(worker_dict) # Start with original worker_dict for other params
    result["worker"] = worker_instance # This worker instance now has mocked managers
    result["job_status_manager"] = mock_job_status_manager # The mock itself
    result["worker_status_manager"] = mock_worker_status_manager # The mock itself
    result["queue_manager"] = mock_queue_manager # The mock itself
    result["failed_job_handler"] = mock_failed_job_handler # The mock itself
    result["mock_js"] = mock_js # Pass along the configured mock_js
    return result


@pytest.fixture
def mock_failed_job_handler():
    """Fixture for a mock FailedJobHandler with async initialize and handle_failed_job."""
    from unittest.mock import MagicMock, AsyncMock

    mock = MagicMock(name="FailedJobHandler")
    mock.initialize = AsyncMock(name="initialize")
    mock.handle_failed_job = AsyncMock(name="handle_failed_job")
    return mock
