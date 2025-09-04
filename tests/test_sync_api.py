"""
Tests for the synchronous API wrapper.

This module tests the synchronous wrapper that provides a sync interface
to the async NatsClient operations.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import threading
import time

from naq.nats_client import NatsClient, NatsClientConfig
from naq.sync_api import SyncNatsClient
from naq.exceptions import NaqConnectionError, NaqException


class TestSyncNatsClient:
    """Test cases for SyncNatsClient class."""

    @pytest.fixture
    def mock_async_client(self):
        """Mock async NatsClient."""
        client = AsyncMock(spec=NatsClient)
        client.is_connected = True
        return client

    @pytest.fixture
    def sync_client(self, mock_async_client):
        """Test sync client with mock async client."""
        with patch('naq.sync_api.NatsClient', return_value=mock_async_client):
            return SyncNatsClient()

    def test_init(self, sync_client, mock_async_client):
        """Test sync client initialization."""
        assert sync_client._async_client == mock_async_client
        assert sync_client._loop is not None

    def test_is_connected_property(self, sync_client, mock_async_client):
        """Test is_connected property."""
        # Connected
        mock_async_client.is_connected = True
        assert sync_client.is_connected is True
        
        # Not connected
        mock_async_client.is_connected = False
        assert sync_client.is_connected is False

    def test_connect(self, sync_client, mock_async_client):
        """Test connection."""
        sync_client.connect()
        
        # Verify async connect was called
        mock_async_client.connect.assert_called_once()

    def test_connect_failure(self, sync_client, mock_async_client):
        """Test connection failure."""
        mock_async_client.connect.side_effect = NaqConnectionError("Connection failed")
        
        with pytest.raises(NaqConnectionError, match="Connection failed"):
            sync_client.connect()

    def test_disconnect(self, sync_client, mock_async_client):
        """Test disconnection."""
        sync_client.disconnect()
        
        # Verify async disconnect was called
        mock_async_client.disconnect.assert_called_once()

    def test_ensure_stream(self, sync_client, mock_async_client):
        """Test ensure stream."""
        sync_client.ensure_stream("test_stream", ["test.subject"])
        
        # Verify async ensure_stream was called
        mock_async_client.ensure_stream.assert_called_once_with(
            "test_stream", ["test.subject"]
        )

    def test_publish(self, sync_client, mock_async_client):
        """Test publish."""
        mock_async_client.publish.return_value = "test_message_id"
        
        result = sync_client.publish("test.subject", b"test_payload")
        
        assert result == "test_message_id"
        mock_async_client.publish.assert_called_once_with(
            "test.subject", b"test_payload"
        )

    def test_jetstream_publish(self, sync_client, mock_async_client):
        """Test JetStream publish."""
        mock_async_client.jetstream_publish.return_value = "test_message_id"
        
        result = sync_client.jetstream_publish("test.subject", b"test_payload")
        
        assert result == "test_message_id"
        mock_async_client.jetstream_publish.assert_called_once_with(
            "test.subject", b"test_payload"
        )

    def test_subscribe(self, sync_client, mock_async_client):
        """Test subscribe."""
        mock_subscription = AsyncMock()
        mock_async_client.subscribe.return_value = mock_subscription
        
        result = sync_client.subscribe("test.subject")
        
        assert result == mock_subscription
        mock_async_client.subscribe.assert_called_once_with("test.subject")

    def test_subscribe_with_queue_group(self, sync_client, mock_async_client):
        """Test subscribe with queue group."""
        mock_subscription = AsyncMock()
        mock_async_client.subscribe.return_value = mock_subscription
        
        result = sync_client.subscribe("test.subject", queue_group="test_queue")
        
        assert result == mock_subscription
        mock_async_client.subscribe.assert_called_once_with(
            "test.subject", queue_group="test_queue"
        )

    def test_pull_subscribe(self, sync_client, mock_async_client):
        """Test pull subscribe."""
        mock_subscription = AsyncMock()
        mock_async_client.pull_subscribe.return_value = mock_subscription
        
        result = sync_client.pull_subscribe("test.subject", "test_durable")
        
        assert result == mock_subscription
        mock_async_client.pull_subscribe.assert_called_once_with(
            "test.subject", "test_durable"
        )

    def test_fetch_messages(self, sync_client, mock_async_client):
        """Test fetch messages."""
        mock_messages = [AsyncMock(), AsyncMock()]
        mock_async_client.fetch_messages.return_value = mock_messages
        
        result = sync_client.fetch_messages(AsyncMock(), batch_size=2, timeout=2.0)
        
        assert result == mock_messages
        mock_async_client.fetch_messages.assert_called_once_with(
            AsyncMock(), batch_size=2, timeout=2.0
        )

    def test_purge_stream(self, sync_client, mock_async_client):
        """Test purge stream."""
        sync_client.purge_stream("test_stream")
        
        mock_async_client.purge_stream.assert_called_once_with("test_stream")

    def test_purge_stream_with_subject(self, sync_client, mock_async_client):
        """Test purge stream with subject."""
        sync_client.purge_stream("test_stream", subject="test.subject")
        
        mock_async_client.purge_stream.assert_called_once_with(
            "test_stream", subject="test.subject"
        )

    def test_get_kv(self, sync_client, mock_async_client):
        """Test get KV store."""
        mock_kv = AsyncMock()
        mock_async_client.get_kv.return_value = mock_kv
        
        result = sync_client.get_kv("test_bucket")
        
        assert result == mock_kv
        mock_async_client.get_kv.assert_called_once_with("test_bucket")

    def test_create_kv(self, sync_client, mock_async_client):
        """Test create KV store."""
        mock_kv = AsyncMock()
        mock_async_client.create_kv.return_value = mock_kv
        
        result = sync_client.create_kv("test_bucket")
        
        assert result == mock_kv
        mock_async_client.create_kv.assert_called_once_with("test_bucket")

    def test_delete_kv(self, sync_client, mock_async_client):
        """Test delete KV store."""
        sync_client.delete_kv("test_bucket")
        
        mock_async_client.delete_kv.assert_called_once_with("test_bucket")

    def test_trigger_due_jobs(self, sync_client, mock_async_client):
        """Test trigger due jobs."""
        mock_async_client.trigger_due_jobs.return_value = (5, 1)
        
        result = sync_client.trigger_due_jobs()
        
        assert result == (5, 1)
        mock_async_client.trigger_due_jobs.assert_called_once()

    def test_context_manager(self, sync_client, mock_async_client):
        """Test sync client as context manager."""
        with sync_client as client:
            assert client == sync_client
        
        # Verify connect and disconnect were called
        mock_async_client.connect.assert_called_once()
        mock_async_client.disconnect.assert_called_once()

    def test_repr(self, sync_client, mock_async_client):
        """Test string representation."""
        mock_async_client.__repr__ = MagicMock(return_value="MockAsyncClient")
        
        result = repr(sync_client)
        
        assert "SyncNatsClient" in result
        assert "MockAsyncClient" in result


class TestSyncQueue:
    """Test cases for synchronous Queue wrapper."""

    @pytest.fixture
    def mock_async_queue(self):
        """Mock async Queue."""
        queue = AsyncMock()
        return queue

    @pytest.fixture
    def sync_queue(self, mock_async_queue):
        """Test sync queue with mock async queue."""
        with patch('naq.sync_api.Queue', return_value=mock_async_queue):
            from naq.sync_api import SyncQueue
            return SyncQueue(name="test_queue")

    def test_init(self, sync_queue, mock_async_queue):
        """Test sync queue initialization."""
        assert sync_queue._async_queue == mock_async_queue
        assert sync_queue._loop is not None

    def test_enqueue(self, sync_queue, mock_async_queue):
        """Test enqueue."""
        mock_async_queue.enqueue.return_value = "job_id"
        
        result = sync_queue.enqueue({"func": "test", "args": [1, 2, 3]})
        
        assert result == "job_id"
        mock_async_queue.enqueue.assert_called_once_with(
            {"func": "test", "args": [1, 2, 3]}
        )

    def test_enqueue_with_delay(self, sync_queue, mock_async_queue):
        """Test enqueue with delay."""
        mock_async_queue.enqueue.return_value = "job_id"
        
        result = sync_queue.enqueue(
            {"func": "test", "args": [1, 2, 3]},
            delay=60
        )
        
        assert result == "job_id"
        mock_async_queue.enqueue.assert_called_once_with(
            {"func": "test", "args": [1, 2, 3]},
            delay=60
        )

    def test_dequeue(self, sync_queue, mock_async_queue):
        """Test dequeue."""
        mock_jobs = [{"job": "data1"}, {"job": "data2"}]
        mock_async_queue.dequeue.return_value = mock_jobs
        
        result = sync_queue.dequeue(batch_size=2, timeout=1.0)
        
        assert result == mock_jobs
        mock_async_queue.dequeue.assert_called_once_with(
            batch_size=2, timeout=1.0
        )

    def test_job_count(self, sync_queue, mock_async_queue):
        """Test job count."""
        mock_async_queue.job_count.return_value = 42
        
        result = sync_queue.job_count()
        
        assert result == 42
        mock_async_queue.job_count.assert_called_once()

    def test_purge(self, sync_queue, mock_async_queue):
        """Test purge."""
        sync_queue.purge()
        
        mock_async_queue.purge.assert_called_once()

    def test_close(self, sync_queue, mock_async_queue):
        """Test close."""
        sync_queue.close()
        
        mock_async_queue.close.assert_called_once()


class TestSyncWorker:
    """Test cases for synchronous Worker wrapper."""

    @pytest.fixture
    def mock_async_worker(self):
        """Mock async Worker."""
        worker = AsyncMock()
        return worker

    @pytest.fixture
    def sync_worker(self, mock_async_worker):
        """Test sync worker with mock async worker."""
        with patch('naq.sync_api.Worker', return_value=mock_async_worker):
            from naq.sync_api import SyncWorker
            return SyncWorker(queue_names=["test_queue"])

    def test_init(self, sync_worker, mock_async_worker):
        """Test sync worker initialization."""
        assert sync_worker._async_worker == mock_async_worker
        assert sync_worker._loop is not None

    def test_start(self, sync_worker, mock_async_worker):
        """Test start."""
        sync_worker.start()
        
        mock_async_worker.start.assert_called_once()

    def test_stop(self, sync_worker, mock_async_worker):
        """Test stop."""
        sync_worker.stop()
        
        mock_async_worker.stop.assert_called_once()

    def test_run_once(self, sync_worker, mock_async_worker):
        """Test run once."""
        sync_worker.run_once()
        
        mock_async_worker.run_once.assert_called_once()

    def test_context_manager(self, sync_worker, mock_async_worker):
        """Test sync worker as context manager."""
        with sync_worker as worker:
            assert worker == sync_worker
        
        # Verify start and stop were called
        mock_async_worker.start.assert_called_once()
        mock_async_worker.stop.assert_called_once()


class TestSyncIntegration:
    """Test cases for synchronous API integration."""

    @pytest.fixture
    def mock_async_client(self):
        """Mock async NatsClient."""
        client = AsyncMock(spec=NatsClient)
        client.is_connected = True
        return client

    @pytest.fixture
    def sync_client(self, mock_async_client):
        """Test sync client with mock async client."""
        with patch('naq.sync_api.NatsClient', return_value=mock_async_client):
            return SyncNatsClient()

    def test_enqueue_dequeue_cycle(self, sync_client, mock_async_client):
        """Test enqueue and dequeue cycle with sync API."""
        from naq.sync_api import SyncQueue
        
        # Create sync queue
        with patch('naq.sync_api.Queue') as mock_queue_class:
            mock_async_queue = AsyncMock()
            mock_queue_class.return_value = mock_async_queue
            
            sync_queue = SyncQueue(name="test_queue", client=sync_client)
            
            # Enqueue job
            job_data = {"func": "test", "args": [1, 2, 3]}
            mock_async_queue.enqueue.return_value = "job_id"
            
            result = sync_queue.enqueue(job_data)
            
            assert result == "job_id"
            mock_async_queue.enqueue.assert_called_once_with(job_data)
            
            # Dequeue job
            mock_jobs = [job_data]
            mock_async_queue.dequeue.return_value = mock_jobs
            
            result = sync_queue.dequeue()
            
            assert result == mock_jobs
            mock_async_queue.dequeue.assert_called_once()

    def test_worker_processing(self, sync_client, mock_async_client):
        """Test worker processing with sync API."""
        from naq.sync_api import SyncWorker
        
        # Create sync worker
        with patch('naq.sync_api.Worker') as mock_worker_class:
            mock_async_worker = AsyncMock()
            mock_worker_class.return_value = mock_async_worker
            
            sync_worker = SyncWorker(queue_names=["test_queue"], client=sync_client)
            
            # Start worker
            sync_worker.start()
            
            mock_async_worker.start.assert_called_once()
            
            # Stop worker
            sync_worker.stop()
            
            mock_async_worker.stop.assert_called_once()

    def test_thread_safety(self, sync_client, mock_async_client):
        """Test thread safety of sync API."""
        from naq.sync_api import SyncQueue
        
        # Create sync queue
        with patch('naq.sync_api.Queue') as mock_queue_class:
            mock_async_queue = AsyncMock()
            mock_queue_class.return_value = mock_async_queue
            
            sync_queue = SyncQueue(name="test_queue", client=sync_client)
            
            # Test concurrent access
            results = []
            errors = []
            
            def enqueue_job(i):
                try:
                    result = sync_queue.enqueue({"job": i})
                    results.append(result)
                except Exception as e:
                    errors.append(e)
            
            threads = []
            for i in range(10):
                thread = threading.Thread(target=enqueue_job, args=(i,))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            # Verify all operations completed without errors
            assert len(errors) == 0
            assert len(results) == 10
            assert mock_async_queue.enqueue.call_count == 10