import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from nats.js.kv import KeyValue
from nats.js import JetStreamContext
import nats
import tempfile
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, AsyncIterator

import socket

from naq.settings import (
    WORKER_KV_NAME,
    JOB_STATUS_KV_NAME,
    RESULT_KV_NAME,
    DEFAULT_QUEUE_NAME,
)

from naq.worker import Worker
from naq.services.base import ServiceManager, ServiceConfig
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS


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
        # Increased timeouts for more robust connection testing in fixtures
        print("DEBUG: conftest: Attempting to connect to NATS server for fixture check.")
        nc = await nats.connect(connect_timeout=30, flush_timeout=30)
        print("DEBUG: conftest: NATS connection successful for fixture check. Closing.")
        await nc.close()
        print("DEBUG: conftest: NATS connection closed for fixture check.")
    except Exception as e:
        print(f"DEBUG: conftest: NATS connection failed for fixture check: {e}")
        pytest.skip(
            "Could not connect to NATS server at localhost:4222. Please ensure it is running."
        )
    yield "nats://localhost:4222"
    print("DEBUG: conftest: NATS server fixture tearing down.")


@pytest_asyncio.fixture
async def mock_nats(mocker):
    """Provide a mock NATS client with full JetStream support for testing (deprecated - use service_aware_nats_mock instead)"""
    # This fixture is deprecated. Use service_aware_nats_mock instead.
    # For backward compatibility, we'll delegate to the new fixture.
    return await service_aware_nats_mock(mocker)


@pytest_asyncio.fixture(scope="function")
async def nats_client(nats_server):
    """
    Provide a properly managed NATS client for testing.

    This fixture ensures the NATS client is created and cleaned up within
    the same event loop, preventing "Event loop is closed" errors.
    """
    nc = None
    try:
        print(f"DEBUG: conftest: nats_client fixture: Attempting to connect to {nats_server}")
        # Increased timeouts for more robust connection testing in fixtures
        nc = await nats.connect(nats_server, connect_timeout=30, flush_timeout=30)
        print(f"DEBUG: conftest: nats_client fixture: Connection to {nats_server} established.")
        yield nc
    finally:
        if nc:
            print(f"DEBUG: conftest: nats_client fixture: Tearing down connection to {nats_server}.")
            try:
                await nc.drain()
                print(f"DEBUG: conftest: nats_client fixture: Connection to {nats_server} drained.")
            except Exception as e:
                print(f"ERROR: conftest: nats_client fixture: Error during NATS client drain to {nats_server}: {e}")
            try:
                await nc.close()
                print(f"DEBUG: conftest: nats_client fixture: Connection to {nats_server} closed.")
            except Exception as e:
                print(f"ERROR: conftest: nats_client fixture: Error during NATS client close to {nats_server}: {e}")


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
    service_test_config, # Replace worker_dict with service_test_config
    mock_job_status_manager,
    mock_worker_status_manager,
    mock_queue_manager,
    mock_failed_job_handler,
    service_aware_nats_mock, # Replace mock_nats with service_aware_nats_mock
    settings_with_valid_queue # Add for queues argument
):
    """Fixture that returns a dict with a 'worker' key containing a Worker instance (with patched managers) and mock managers."""
    
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

    # Create worker with basic args from service_test_config, but ensure queues is correct for this context
    # service_test_config might not have the right queue name if settings_with_valid_queue is different
    worker_args = service_test_config.copy()
    worker_args["queues"] = [settings_with_valid_queue['DEFAULT_QUEUE_NAME']]
    worker_args["service_manager"] = service_manager

    worker_instance = Worker(**worker_args)
    
    # Run _connect manually as it's not part of the constructor and tests might rely on it
    # Need to ensure worker_instance._js is set up for _get_kv_store if real managers were used (though they are mocked)
    # The mock_js from service_aware_nats_mock should be used by the worker instance.
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
    
    mock_js.key_value.side_effect = kv_side_effect_for_dict # Configure the mock_js from service_aware_nats_mock

    # Run _connect as the fixture is async and pytest-asyncio handles the loop.
    await worker_instance._connect()

    result = dict(service_test_config) # Start with original service_test_config for other params
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


@pytest_asyncio.fixture
async def service_manager():
    """Fixture for a real ServiceManager instance for testing."""
    # Create service config with test settings
    service_config = ServiceConfig(
        nats_url="nats://localhost:4222",
        custom_settings={
            "log_level": "DEBUG",
            "test_mode": True
        }
    )
    
    # Create service manager
    manager = ServiceManager(service_config)
    
    # Initialize the service manager
    try:
        # Register core services
        await manager.register_service("connection", ConnectionService, initialize=False)
        await manager.register_service("stream", StreamService, initialize=False)
        await manager.register_service("kv_store", KVStoreService, initialize=False)
        await manager.register_service("jobs", JobService, initialize=False)
        await manager.register_service("events", EventService, initialize=False)
        
        yield manager
        
    finally:
        # Cleanup all services
        await manager.cleanup_all()


@pytest.fixture
def mock_service_manager():
    """Fixture for a mock ServiceManager with all services mocked."""
    # Create mock service manager
    manager = AsyncMock(spec=ServiceManager)
    manager._services = {}
    manager._service_configs = {}
    manager.has_service = MagicMock(return_value=True)
    manager.get_service = AsyncMock()
    
    # Create mock services
    mock_connection_service = AsyncMock(spec=ConnectionService)
    mock_connection_service._is_initialized = True
    mock_connection_service.get_connection = AsyncMock()
    mock_connection_service.get_jetstream = AsyncMock()
    
    # Add jetstream_scope context manager mocking
    @asynccontextmanager
    async def mock_jetstream_scope(url: Optional[str] = None) -> AsyncIterator[Any]:
        """Mock jetstream_scope context manager."""
        mock_nc = AsyncMock()
        mock_js = AsyncMock(spec=JetStreamContext)
        yield mock_nc
    
    mock_connection_service.jetstream_scope = mock_jetstream_scope
    
    mock_job_service = AsyncMock(spec=JobService)
    mock_job_service._is_initialized = True
    mock_job_service.enqueue_job = AsyncMock()
    mock_job_service.execute_job = AsyncMock()
    mock_job_service.store_result = AsyncMock()
    mock_job_service.get_result = AsyncMock()
    
    mock_event_service = AsyncMock(spec=EventService)
    mock_event_service._is_initialized = True
    mock_event_service.log_job_event = AsyncMock()
    mock_event_service.log_worker_event = AsyncMock()
    mock_event_service.get_job_events = AsyncMock(return_value=[])
    mock_event_service.get_worker_events = AsyncMock(return_value=[])
    
    mock_stream_service = AsyncMock(spec=StreamService)
    mock_stream_service._is_initialized = True
    mock_stream_service.ensure_stream = AsyncMock()
    mock_stream_service.get_stream_info = AsyncMock()
    
    mock_kv_store_service = AsyncMock(spec=KVStoreService)
    mock_kv_store_service._is_initialized = True
    mock_kv_store_service.put = AsyncMock()
    mock_kv_store_service.get = AsyncMock()
    mock_kv_store_service.delete = AsyncMock()
    mock_kv_store_service.get_kv_store = AsyncMock()
    
    # Configure service manager to return mock services
    async def mock_get_service(name: str, service_class: Optional[type] = None):
        """Mock get_service method."""
        if name == "connection" and (service_class is None or issubclass(service_class, ConnectionService)):
            return mock_connection_service
        elif name == "jobs" and (service_class is None or issubclass(service_class, JobService)):
            return mock_job_service
        elif name == "events" and (service_class is None or issubclass(service_class, EventService)):
            return mock_event_service
        elif name == "stream" and (service_class is None or issubclass(service_class, StreamService)):
            return mock_stream_service
        elif name == "kv_store" and (service_class is None or issubclass(service_class, KVStoreService)):
            return mock_kv_store_service
        else:
            raise ValueError(f"Unknown service: {name}")
    
    manager.get_service.side_effect = mock_get_service
    
    # Store mock services for direct access
    manager._mock_connection_service = mock_connection_service
    manager._mock_job_service = mock_job_service
    manager._mock_event_service = mock_event_service
    manager._mock_stream_service = mock_stream_service
    manager._mock_kv_store_service = mock_kv_store_service
    
    return manager


@pytest.fixture
def service_test_config():
    """Fixture for service test configuration."""
    return {
        "queues": ["default"],
        "nats_url": "nats://localhost:4222",
        "concurrency": 2,
        "worker_name": "test-worker",
        "service_config": {
            "nats_url": "nats://localhost:4222",
            "log_level": "DEBUG",
            "custom_settings": {
                "test_mode": True,
                "auto_create_buckets": True,
                "enable_event_logging": True,
                "enable_job_execution": True,
                "enable_result_storage": True
            }
        }
    }


@pytest.fixture
def temp_config_file():
    """Fixture for a temporary configuration file."""
    # Create a temporary config file
    config_content = """
nats:
  servers:
    - "nats://localhost:4222"
  client_name: naq-test-client
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
  connection_timeout: 30.0
  drain_timeout: 30.0

workers:
  concurrency: 4
  heartbeat_interval: 30.0
  ttl: 300.0
  max_job_duration: 3600.0
  shutdown_timeout: 60.0

events:
  enabled: true
  batch_size: 100
  flush_interval: 5.0
  max_buffer_size: 1000
  stream: naq_events

streams:
  stream_name: naq_stream
  subjects:
    - naq.>
  retention_limit: null
  max_age: null
  max_msgs: null
  max_bytes: null
  replicas: 1
  storage: file

kv_store:
  bucket_name: naq_kv_store
  history: 10
  replicas: 1
  stream_name: null
  ttl: null

job_service:
  enable_job_execution: true
  enable_result_storage: true
  enable_event_logging: true
  max_job_execution_time: 3600.0
  default_result_ttl: 86400.0
  results_bucket_name: naq_results
  auto_create_buckets: true
  default_queue: default
  default_max_retries: 3
  default_retry_delay: 60.0

worker_service:
  worker_name: test_worker
  queues:
    - default
  max_concurrent_jobs: 4
  heartbeat_interval: 30.0
  ttl: 300.0
  max_job_duration: 3600.0
  shutdown_timeout: 60.0
  status_bucket_name: naq_worker_status
  auto_create_buckets: true

scheduler_service:
  scheduler_name: test_scheduler
  check_interval: 1.0
  max_concurrent_schedules: 100
  schedules_bucket_name: naq_schedules
  lock_bucket_name: naq_locks
  lock_ttl: 30.0
  lock_renew_interval: 10.0
  auto_create_buckets: true

logging:
  level: INFO
  to_file_enabled: false
  file_path: naq.log
"""
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Clean up
    os.unlink(temp_file_path)


@pytest_asyncio.fixture
async def service_aware_nats_mock(mocker):
    """Provide a mock NATS client with full JetStream support for service-aware testing."""
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
        print(f"DEBUG: service_aware_nats_mock.js.key_value called for bucket: {bucket}")
        if bucket == WORKER_KV_NAME:
            return mock_worker_kv
        elif bucket == JOB_STATUS_KV_NAME:
            return mock_job_status_kv
        elif bucket == RESULT_KV_NAME:
            return mock_result_kv
        raise ValueError(f"service_aware_nats_mock.js.key_value called with unexpected bucket: {bucket}")

    # Set up key_value with the side effect
    mock_js.key_value = AsyncMock(side_effect=get_key_value_store_side_effect)
    mock_js.create_key_value = AsyncMock(return_value=AsyncMock(spec=KeyValue))

    # Create NATS client mock with properly configured JetStream
    mock_nc = AsyncMock()
    mock_nc.jetstream = AsyncMock(return_value=mock_js)

    print(
        f"DEBUG: service_aware_nats_mock fixture returning mock_js: {mock_js}, mock_js.key_value: {mock_js.key_value}"
    )

    return mock_nc, mock_js


@pytest.fixture
def service_test_job():
    """Fixture for a test job aligned with the new Job model structure."""
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
