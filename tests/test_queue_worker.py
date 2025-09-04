"""
Tests for the Queue and Worker classes with unified NATS client.

This module tests the integration between Queue, Worker, and the unified NatsClient.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.nats_client import NatsClient, NatsClientConfig
from naq.queue import Queue
from naq.worker import Worker
from naq.exceptions import NaqException, NaqConnectionError
from naq.schemas import Job, JobStatus, JobResult
from naq.serializers import JobSerializer
from naq.config import get_config


class TestQueueWithUnifiedClient:
    """Test cases for Queue class with unified NatsClient."""

    @pytest.fixture
    def mock_client(self):
        """Mock NatsClient."""
        client = AsyncMock(spec=NatsClient)
        client.is_connected = True
        return client

    @pytest.fixture
    def queue(self, mock_client):
        """Test Queue with mock client."""
        return Queue(name="test_queue", client=mock_client)

    @pytest.mark.asyncio
    async def test_init_with_client(self, mock_client):
        """Test Queue initialization with client."""
        queue = Queue(name="test_queue", client=mock_client)
        
        assert queue.name == "test_queue"
        assert queue._client == mock_client
        assert queue._serializer is not None

    @pytest.mark.asyncio
    async def test_init_without_client(self):
        """Test Queue initialization without client."""
        with patch('naq.nats_client.NatsClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.is_connected = True
            mock_client_class.return_value = mock_client
            
            queue = Queue(name="test_queue")
            
            assert queue.name == "test_queue"
            assert queue._client == mock_client
            mock_client_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_enqueue(self, queue, mock_client):
        """Test job enqueue."""
        job_data = {"func": "test_func", "args": [1, 2, 3]}
        
        with patch.object(queue._serializer, 'serialize', return_value=b'serialized_job') as mock_serialize:
            result = await queue.enqueue(job_data)
            
            mock_serialize.assert_called_once_with(job_data)
            mock_client.jetstream_publish.assert_called_once_with(
                "naq.jobs.test_queue", b'serialized_job'
            )
            assert result == mock_client.jetstream_publish.return_value

    @pytest.mark.asyncio
    async def test_enqueue_with_delay(self, queue, mock_client):
        """Test job enqueue with delay."""
        job_data = {"func": "test_func", "args": [1, 2, 3]}
        
        with patch.object(queue._serializer, 'serialize', return_value=b'serialized_job') as mock_serialize:
            with patch('naq.queue.time.time', return_value=1000):
                result = await queue.enqueue(job_data, delay=60)
                
                # Verify job data includes scheduled time
                call_args = mock_serialize.call_args[0][0]
                assert "scheduled_time" in call_args
                assert call_args["scheduled_time"] == 1060
                
                mock_client.jetstream_publish.assert_called_once_with(
                    "naq.jobs.test_queue", b'serialized_job'
                )

    @pytest.mark.asyncio
    async def test_enqueue_failure(self, queue, mock_client):
        """Test job enqueue failure."""
        job_data = {"func": "test_func", "args": [1, 2, 3]}
        
        mock_client.jetstream_publish.side_effect = Exception("Publish failed")
        
        with pytest.raises(NaqException, match="Failed to enqueue job"):
            await queue.enqueue(job_data)

    @pytest.mark.asyncio
    async def test_dequeue(self, queue, mock_client):
        """Test job dequeue."""
        mock_msg = AsyncMock()
        mock_msg.data = b'serialized_job'
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        
        mock_subscription = AsyncMock()
        mock_subscription.fetch.return_value = [mock_msg]
        
        mock_client.pull_subscribe.return_value = mock_subscription
        
        with patch.object(queue._serializer, 'deserialize', return_value={"job": "data"}) as mock_deserialize:
            result = await queue.dequeue(batch_size=1, timeout=1.0)
            
            mock_client.pull_subscribe.assert_called_once_with(
                "naq.jobs.test_queue", "test_queue_consumer"
            )
            mock_subscription.fetch.assert_called_once_with(batch=1, timeout=1.0)
            mock_deserialize.assert_called_once_with(b'serialized_job')
            assert result == [{"job": "data"}]

    @pytest.mark.asyncio
    async def test_dequeue_empty(self, queue, mock_client):
        """Test job dequeue with no messages."""
        mock_subscription = AsyncMock()
        mock_subscription.fetch.return_value = []
        
        mock_client.pull_subscribe.return_value = mock_subscription
        
        result = await queue.dequeue(batch_size=1, timeout=1.0)
        
        assert result == []

    @pytest.mark.asyncio
    async def test_dequeue_failure(self, queue, mock_client):
        """Test job dequeue failure."""
        mock_client.pull_subscribe.side_effect = Exception("Subscribe failed")
        
        with pytest.raises(NaqException, match="Failed to dequeue job"):
            await queue.dequeue()

    @pytest.mark.asyncio
    async def test_job_count(self, queue, mock_client):
        """Test job count."""
        mock_stream_info = AsyncMock()
        mock_stream_info.state.messages = 42
        mock_client.js.stream_info.return_value = mock_stream_info
        
        result = await queue.job_count()
        
        assert result == 42
        mock_client.js.stream_info.assert_called_once_with("test_queue")

    @pytest.mark.asyncio
    async def test_purge(self, queue, mock_client):
        """Test queue purge."""
        await queue.purge()
        
        mock_client.purge_stream.assert_called_once_with("test_queue")

    @pytest.mark.asyncio
    async def test_close(self, queue, mock_client):
        """Test queue close."""
        await queue.close()
        
        mock_client.disconnect.assert_called_once()


class TestWorkerWithUnifiedClient:
    """Test cases for Worker class with unified NatsClient."""

    @pytest.fixture
    def mock_client(self):
        """Mock NatsClient."""
        client = AsyncMock(spec=NatsClient)
        client.is_connected = True
        return client

    @pytest.fixture
    def worker(self, mock_client):
        """Test Worker with mock client."""
        return Worker(queue_names=["test_queue"], client=mock_client)

    @pytest.mark.asyncio
    async def test_init_with_client(self, mock_client):
        """Test Worker initialization with client."""
        worker = Worker(queue_names=["test_queue"], client=mock_client)
        
        assert worker.queue_names == ["test_queue"]
        assert worker._client == mock_client
        assert worker._running is False

    @pytest.mark.asyncio
    async def test_init_without_client(self):
        """Test Worker initialization without client."""
        with patch('naq.nats_client.NatsClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.is_connected = True
            mock_client_class.return_value = mock_client
            
            worker = Worker(queue_names=["test_queue"])
            
            assert worker.queue_names == ["test_queue"]
            assert worker._client == mock_client
            mock_client_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_start(self, worker, mock_client):
        """Test worker start."""
        mock_queue = AsyncMock()
        mock_queue.dequeue.return_value = []
        
        with patch('naq.worker.Queue', return_value=mock_queue):
            await worker.start()
            
            assert worker._running is True
            mock_queue.dequeue.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self, worker):
        """Test worker stop."""
        worker._running = True
        
        await worker.stop()
        
        assert worker._running is False

    @pytest.mark.asyncio
    async def test_process_job(self, worker, mock_client):
        """Test job processing."""
        job_data = {
            "func": "test_func",
            "args": [1, 2, 3],
            "kwargs": {"key": "value"},
            "job_id": "test_job_id"
        }
        
        with patch('naq.worker.import_function', return_value=lambda x, y, key: x + y + key) as mock_import:
            with patch('naq.worker.JobResult') as mock_result_class:
                mock_result = AsyncMock()
                mock_result_class.return_value = mock_result
                
                await worker._process_job(job_data, "test_queue")
                
                mock_import.assert_called_once_with("test_func")
                mock_result_class.assert_called_once()
                mock_result.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_job_with_error(self, worker, mock_client):
        """Test job processing with error."""
        job_data = {
            "func": "test_func",
            "args": [1, 2, 3],
            "job_id": "test_job_id"
        }
        
        with patch('naq.worker.import_function', side_effect=Exception("Function failed")):
            with patch('naq.worker.JobResult') as mock_result_class:
                mock_result = AsyncMock()
                mock_result_class.return_value = mock_result
                
                await worker._process_job(job_data, "test_queue")
                
                mock_result_class.assert_called_once()
                assert mock_result.status == JobStatus.FAILED
                assert "Function failed" in mock_result.error
                mock_result.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_job_with_timeout(self, worker, mock_client):
        """Test job processing with timeout."""
        job_data = {
            "func": "test_func",
            "args": [1, 2, 3],
            "job_id": "test_job_id",
            "timeout": 1.0
        }
        
        async def slow_func(*args):
            await asyncio.sleep(2)
            return "result"
        
        with patch('naq.worker.import_function', return_value=slow_func):
            with patch('naq.worker.JobResult') as mock_result_class:
                mock_result = AsyncMock()
                mock_result_class.return_value = mock_result
                
                await worker._process_job(job_data, "test_queue")
                
                mock_result_class.assert_called_once()
                assert mock_result.status == JobStatus.FAILED
                assert "timeout" in mock_result.error.lower()
                mock_result.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_once(self, worker, mock_client):
        """Test worker run once."""
        mock_queue = AsyncMock()
        mock_queue.dequeue.return_value = [{"job": "data"}]
        
        with patch('naq.worker.Queue', return_value=mock_queue):
            with patch.object(worker, '_process_job') as mock_process:
                await worker.run_once()
                
                mock_queue.dequeue.assert_called_once()
                mock_process.assert_called_once_with({"job": "data"}, "test_queue")

    @pytest.mark.asyncio
    async def test_run_once_no_jobs(self, worker, mock_client):
        """Test worker run once with no jobs."""
        mock_queue = AsyncMock()
        mock_queue.dequeue.return_value = []
        
        with patch('naq.worker.Queue', return_value=mock_queue):
            with patch.object(worker, '_process_job') as mock_process:
                await worker.run_once()
                
                mock_queue.dequeue.assert_called_once()
                mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_with_error(self, worker, mock_client):
        """Test worker run once with error."""
        mock_queue = AsyncMock()
        mock_queue.dequeue.side_effect = Exception("Dequeue failed")
        
        with patch('naq.worker.Queue', return_value=mock_queue):
            with patch.object(worker, '_process_job') as mock_process:
                await worker.run_once()
                
                mock_queue.dequeue.assert_called_once()
                mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_context_manager(self, worker, mock_client):
        """Test worker as context manager."""
        with patch.object(worker, 'start') as mock_start:
            with patch.object(worker, 'stop') as mock_stop:
                async with worker:
                    pass
                
                mock_start.assert_called_once()
                mock_stop.assert_called_once()


class TestQueueWorkerIntegration:
    """Test cases for Queue and Worker integration."""

    @pytest.fixture
    def mock_client(self):
        """Mock NatsClient."""
        client = AsyncMock(spec=NatsClient)
        client.is_connected = True
        return client

    @pytest.mark.asyncio
    async def test_enqueue_dequeue_cycle(self, mock_client):
        """Test enqueue and dequeue cycle."""
        queue = Queue(name="test_queue", client=mock_client)
        worker = Worker(queue_names=["test_queue"], client=mock_client)
        
        job_data = {"func": "test_func", "args": [1, 2, 3]}
        
        # Enqueue job
        with patch.object(queue._serializer, 'serialize', return_value=b'serialized_job') as mock_serialize:
            await queue.enqueue(job_data)
            
            mock_serialize.assert_called_once_with(job_data)
            mock_client.jetstream_publish.assert_called_once_with(
                "naq.jobs.test_queue", b'serialized_job'
            )
        
        # Dequeue job
        mock_msg = AsyncMock()
        mock_msg.data = b'serialized_job'
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        
        mock_subscription = AsyncMock()
        mock_subscription.fetch.return_value = [mock_msg]
        
        mock_client.pull_subscribe.return_value = mock_subscription
        
        with patch.object(queue._serializer, 'deserialize', return_value=job_data) as mock_deserialize:
            result = await queue.dequeue(batch_size=1, timeout=1.0)
            
            assert result == [job_data]
            mock_deserialize.assert_called_once_with(b'serialized_job')
        
        # Process job
        with patch('naq.worker.import_function', return_value=lambda *args: sum(args)) as mock_import:
            with patch('naq.worker.JobResult') as mock_result_class:
                mock_result = AsyncMock()
                mock_result_class.return_value = mock_result
                
                await worker._process_job(job_data, "test_queue")
                
                mock_import.assert_called_once_with("test_func")
                mock_result_class.assert_called_once()
                assert mock_result.status == JobStatus.COMPLETED
                assert mock_result.result == 6
                mock_result.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_queues(self, mock_client):
        """Test worker with multiple queues."""
        worker = Worker(queue_names=["queue1", "queue2"], client=mock_client)
        
        mock_queue1 = AsyncMock()
        mock_queue1.dequeue.return_value = []
        
        mock_queue2 = AsyncMock()
        mock_queue2.dequeue.return_value = []
        
        with patch('naq.worker.Queue') as mock_queue_class:
            mock_queue_class.side_effect = [mock_queue1, mock_queue2]
            
            await worker.run_once()
            
            assert mock_queue_class.call_count == 2
            mock_queue1.dequeue.assert_called_once()
            mock_queue2.dequeue.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_with_custom_serializer(self, mock_client):
        """Test worker with custom serializer."""
        custom_serializer = AsyncMock()
        custom_serializer.serialize.return_value = b'custom_serialized'
        custom_serializer.deserialize.return_value = {"custom": "job"}
        
        queue = Queue(name="test_queue", client=mock_client, serializer=custom_serializer)
        worker = Worker(queue_names=["test_queue"], client=mock_client, serializer=custom_serializer)
        
        job_data = {"func": "test_func", "args": [1, 2, 3]}
        
        # Enqueue with custom serializer
        await queue.enqueue(job_data)
        
        custom_serializer.serialize.assert_called_once_with(job_data)
        mock_client.jetstream_publish.assert_called_once_with(
            "naq.jobs.test_queue", b'custom_serialized'
        )
        
        # Dequeue with custom serializer
        mock_msg = AsyncMock()
        mock_msg.data = b'custom_serialized'
        mock_msg.ack = AsyncMock()
        
        mock_subscription = AsyncMock()
        mock_subscription.fetch.return_value = [mock_msg]
        
        mock_client.pull_subscribe.return_value = mock_subscription
        
        result = await queue.dequeue(batch_size=1, timeout=1.0)
        
        assert result == [{"custom": "job"}]
        custom_serializer.deserialize.assert_called_once_with(b'custom_serialized')