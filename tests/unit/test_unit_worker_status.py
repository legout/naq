import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import cloudpickle
import socket
import os
import time
from datetime import datetime, timezone

from naq.worker.status import WorkerStatusManager
from naq.models import WORKER_STATUS
from naq.settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    WORKER_KV_NAME,
)


class MockWorker:
    """Mock worker class for testing WorkerStatusManager."""
    
    def __init__(self, worker_id="test-worker-123"):
        self.worker_id = worker_id
        self._js = None
        self._worker_ttl = 300  # 5 minutes


@pytest_asyncio.fixture
async def mock_worker():
    """Create a mock worker for testing."""
    return MockWorker()


@pytest_asyncio.fixture
async def mock_kv_store():
    """Create a mock KV store for testing."""
    kv = AsyncMock()
    kv.put = AsyncMock()
    kv.delete = AsyncMock()
    kv.get = AsyncMock()
    kv.keys = AsyncMock()
    return kv


@pytest_asyncio.fixture
async def mock_js():
    """Create a mock JetStream context for testing."""
    js = AsyncMock()
    js.key_value = AsyncMock()
    return js


@pytest_asyncio.fixture
async def mock_nats_connection():
    """Create a mock NATS connection for testing."""
    nc = AsyncMock()
    return nc


class TestWorkerStatusManager:
    """Test cases for the WorkerStatusManager class."""

    @pytest.mark.asyncio
    async def test_init(self, mock_worker):
        """Test WorkerStatusManager initialization."""
        manager = WorkerStatusManager(mock_worker)
        
        assert manager.worker == mock_worker
        assert manager._current_status == WORKER_STATUS.STARTING
        assert manager._kv_store is None
        assert manager._heartbeat_task is None

    @pytest.mark.asyncio
    async def test_get_kv_store_existing(self, mock_worker, mock_js, mock_kv_store):
        """Test getting an existing KV store."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        kv = await manager._get_kv_store()
        
        assert kv == mock_kv_store
        mock_js.key_value.assert_called_once_with(bucket=WORKER_KV_NAME)
        # Second call should return the same instance
        kv2 = await manager._get_kv_store()
        assert kv2 == mock_kv_store

    @pytest.mark.asyncio
    async def test_get_kv_store_create_new(self, mock_worker, mock_js, mock_kv_store):
        """Test creating a new KV store when it doesn't exist."""
        from nats.js.errors import BucketNotFoundError
        
        mock_worker._js = mock_js
        mock_js.key_value.side_effect = BucketNotFoundError("Bucket not found")
        mock_js.create_key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        kv = await manager._get_kv_store()
        
        assert kv == mock_kv_store
        mock_js.key_value.assert_called_once_with(bucket=WORKER_KV_NAME)
        mock_js.create_key_value.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_kv_store_failure(self, mock_worker, mock_js):
        """Test handling KV store creation failure."""
        mock_worker._js = mock_js
        mock_js.key_value.side_effect = Exception("Connection failed")
        
        manager = WorkerStatusManager(mock_worker)
        kv = await manager._get_kv_store()
        
        assert kv is None

    @pytest.mark.asyncio
    async def test_update_status(self, mock_worker, mock_js, mock_kv_store):
        """Test updating worker status."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        await manager.update_status(WORKER_STATUS.BUSY, job_id="test-job-123")
        
        assert manager._current_status == WORKER_STATUS.BUSY
        mock_kv_store.put.assert_called_once()
        
        # Verify the payload contains correct information
        call_args = mock_kv_store.put.call_args
        assert call_args[0][0] == mock_worker.worker_id  # key
        payload = cloudpickle.loads(call_args[0][1])  # value
        assert payload["worker_id"] == mock_worker.worker_id
        assert payload["status"] == WORKER_STATUS.BUSY.value
        assert "timestamp" in payload
        assert payload["hostname"] == socket.gethostname()
        assert payload["pid"] == os.getpid()
        assert payload["job_id"] == "test-job-123"

    @pytest.mark.asyncio
    async def test_update_status_no_kv_store(self, mock_worker):
        """Test updating status when KV store is not available."""
        manager = WorkerStatusManager(mock_worker)
        # Don't set up JS, so _get_kv_store returns None
        
        # Should not raise an exception
        await manager.update_status(WORKER_STATUS.IDLE)
        assert manager._current_status == WORKER_STATUS.IDLE

    @pytest.mark.asyncio
    async def test_heartbeat_loop(self, mock_worker, mock_js, mock_kv_store):
        """Test the heartbeat loop."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        
        # Start heartbeat but stop it quickly to avoid infinite loop
        await manager.start_heartbeat_loop()
        
        # Give it a moment to run
        await asyncio.sleep(0.1)
        
        # Stop the heartbeat
        await manager.stop_heartbeat_loop()
        
        # Verify heartbeat was called at least once
        mock_kv_store.put.assert_called()

    @pytest.mark.asyncio
    async def test_start_stop_heartbeat(self, mock_worker, mock_js, mock_kv_store):
        """Test starting and stopping heartbeat loop."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        
        # Initially no heartbeat task
        assert manager._heartbeat_task is None
        
        # Start heartbeat
        await manager.start_heartbeat_loop()
        assert manager._heartbeat_task is not None
        assert not manager._heartbeat_task.done()
        
        # Stop heartbeat
        await manager.stop_heartbeat_loop()
        assert manager._heartbeat_task.done()

    @pytest.mark.asyncio
    async def test_unregister_worker(self, mock_worker, mock_js, mock_kv_store):
        """Test unregistering a worker."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        await manager.unregister_worker()
        
        mock_kv_store.delete.assert_called_once_with(mock_worker.worker_id)

    @pytest.mark.asyncio
    async def test_unregister_worker_no_kv_store(self, mock_worker):
        """Test unregistering when KV store is not available."""
        manager = WorkerStatusManager(mock_worker)
        # Don't set up JS, so _get_kv_store returns None
        
        # Should not raise an exception
        await manager.unregister_worker()

    @pytest.mark.asyncio
    async def test_list_workers_success(self, mock_nats_connection, mock_js, mock_kv_store):
        """Test listing workers successfully."""
        # Mock worker data
        worker_data = {
            "worker_id": "test-worker-123",
            "status": WORKER_STATUS.IDLE.value,
            "timestamp": time.time(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
        }
        
        # Mock KV store responses
        mock_kv_store.keys.return_value = [b"test-worker-123"]
        mock_kv_store.get.return_value.value = cloudpickle.dumps(worker_data)
        
        # Mock NATS connection
        with patch('naq.worker.status.get_nats_connection', return_value=mock_nats_connection), \
             patch('naq.worker.status.get_jetstream_context', return_value=mock_js), \
             patch('naq.worker.status.close_nats_connection') as mock_close:
            
            mock_js.key_value.return_value = mock_kv_store
            
            workers = await WorkerStatusManager.list_workers()
            
            assert len(workers) == 1
            assert workers[0] == worker_data
            mock_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_workers_no_store(self, mock_nats_connection, mock_js):
        """Test listing workers when KV store doesn't exist."""
        mock_js.key_value.side_effect = Exception("Store not accessible")
        
        with patch('naq.worker.status.get_nats_connection', return_value=mock_nats_connection), \
             patch('naq.worker.status.get_jetstream_context', return_value=mock_js), \
             patch('naq.worker.status.close_nats_connection'):
            
            workers = await WorkerStatusManager.list_workers()
            
            assert workers == []

    @pytest.mark.asyncio
    async def test_list_workers_connection_error(self, mock_nats_connection):
        """Test listing workers with connection error."""
        from naq.worker.status import NaqConnectionError
        
        with patch('naq.worker.status.get_nats_connection', side_effect=NaqConnectionError("Connection failed")):
            
            with pytest.raises(NaqConnectionError):
                await WorkerStatusManager.list_workers()

    @pytest.mark.asyncio
    async def test_list_workers_generic_error(self, mock_nats_connection, mock_js):
        """Test listing workers with generic error."""
        with patch('naq.worker.status.get_nats_connection', return_value=mock_nats_connection), \
             patch('naq.worker.status.get_jetstream_context', return_value=mock_js), \
             patch('naq.worker.status.close_nats_connection'):
            
            mock_js.key_value.side_effect = Exception("Generic error")
            
            with pytest.raises(Exception) as exc_info:
                await WorkerStatusManager.list_workers()
            
            assert "Error listing workers" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_status_transitions(self, mock_worker, mock_js, mock_kv_store):
        """Test worker status transitions."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        
        # Test status transitions
        await manager.update_status(WORKER_STATUS.STARTING)
        assert manager._current_status == WORKER_STATUS.STARTING
        
        await manager.update_status(WORKER_STATUS.IDLE)
        assert manager._current_status == WORKER_STATUS.IDLE
        
        await manager.update_status(WORKER_STATUS.BUSY, job_id="test-job")
        assert manager._current_status == WORKER_STATUS.BUSY
        
        await manager.update_status(WORKER_STATUS.STOPPING)
        assert manager._current_status == WORKER_STATUS.STOPPING
        
        # Test string status conversion
        await manager.update_status("IDLE")
        assert manager._current_status == WORKER_STATUS.IDLE

    @pytest.mark.asyncio
    async def test_heartbeat_task_cancellation(self, mock_worker, mock_js, mock_kv_store):
        """Test that heartbeat task is properly cancelled."""
        mock_worker._js = mock_js
        mock_js.key_value.return_value = mock_kv_store
        
        manager = WorkerStatusManager(mock_worker)
        
        # Start heartbeat
        await manager.start_heartbeat_loop()
        
        # Verify task is running
        assert manager._heartbeat_task is not None
        assert not manager._heartbeat_task.done()
        
        # Stop heartbeat
        await manager.stop_heartbeat_loop()
        
        # Verify task is cancelled
        assert manager._heartbeat_task.done()
        assert manager._heartbeat_task.cancelled()