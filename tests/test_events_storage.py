# tests/test_events_storage.py
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.events.storage import NATSJobEventStorage
from naq.models import JobEvent, JobEventType


class TestNATSJobEventStorage:
    """Test cases for NATSJobEventStorage class."""

    @pytest.fixture
    def storage(self):
        """Create a NATSJobEventStorage instance for testing."""
        return NATSJobEventStorage(
            nats_url="nats://localhost:4222",
            stream_name="TEST_JOB_EVENTS",
            subject_prefix="test.jobs.events"
        )

    @pytest.fixture
    def sample_event(self):
        """Create a sample JobEvent for testing."""
        return JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.ENQUEUED,
            queue_name="test-queue",
            worker_id="worker-abc"
        )

    @pytest.mark.asyncio
    async def test_store_event(self, storage, sample_event):
        """Test storing an event."""
        with patch.object(storage, '_js') as mock_js:
            mock_js.publish = AsyncMock()
            
            await storage.store_event(sample_event)
            
            # Verify publish was called
            mock_js.publish.assert_called_once()
            args = mock_js.publish.call_args
            assert args[0][0] == "test.jobs.events.test-job-123.worker.enqueued"
            assert args[1]['data'] == sample_event

    @pytest.mark.asyncio
    async def test_get_events(self, storage):
        """Test retrieving events for a job."""
        job_id = "test-job-123"
        
        # Mock messages
        mock_msg1 = AsyncMock()
        mock_msg1.data = b'{"job_id": "test-job-123", "event_type": "enqueued"}'
        mock_msg1.ack = AsyncMock()
        
        mock_msg2 = AsyncMock()
        mock_msg2.data = b'{"job_id": "test-job-123", "event_type": "started"}'
        mock_msg2.ack = AsyncMock()
        
        with patch.object(storage, '_js') as mock_js:
            mock_consumer = AsyncMock()
            mock_consumer.fetch_messages = AsyncMock()
            mock_consumer.fetch_messages.return_value = [mock_msg1, mock_msg2]
            
            mock_js.add_consumer = AsyncMock(return_value=mock_consumer)
            mock_js.stream_info = AsyncMock()
            
            events = await storage.get_events(job_id)
            
            # Verify events were retrieved
            assert len(events) == 2
            assert events[0].job_id == job_id
            assert events[1].job_id == job_id

    @pytest.mark.asyncio
    async def test_stream_events(self, storage):
        """Test streaming events for a job."""
        job_id = "test-job-123"
        
        # Mock messages
        mock_msg1 = AsyncMock()
        mock_msg1.data = b'{"job_id": "test-job-123", "event_type": "enqueued"}'
        mock_msg1.ack = AsyncMock()
        
        mock_msg2 = AsyncMock()
        mock_msg2.data = b'{"job_id": "test-job-123", "event_type": "started"}'
        mock_msg2.ack = AsyncMock()
        
        with patch.object(storage, '_js') as mock_js:
            mock_consumer = AsyncMock()
            mock_consumer.consume_messages = AsyncMock()
            mock_consumer.consume_messages.return_value = [mock_msg1, mock_msg2]
            
            mock_js.add_consumer = AsyncMock(return_value=mock_consumer)
            mock_js.stream_info = AsyncMock()
            
            events = []
            async for event in storage.stream_events(job_id):
                events.append(event)
            
            # Verify events were streamed
            assert len(events) == 2
            assert events[0].job_id == job_id
            assert events[1].job_id == job_id

    @pytest.mark.asyncio
    async def test_setup_stream(self, storage):
        """Test stream setup functionality."""
        with patch.object(storage, '_js') as mock_js:
            # Test stream doesn't exist
            mock_js.stream_info.side_effect = Exception("Stream not found")
            mock_js.add_stream = AsyncMock()
            
            await storage._setup_stream()
            
            # Verify stream was created
            mock_js.add_stream.assert_called_once()
            
            # Test stream exists
            mock_js.stream_info.reset_mock()
            mock_js.stream_info.side_effect = None
            
            await storage._setup_stream()
            
            # Verify stream was not recreated
            mock_js.add_stream.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, storage):
        """Test connection and disconnection."""
        with patch('naq.events.storage.get_jetstream_context') as mock_get_js:
            mock_js = AsyncMock()
            mock_get_js.return_value = mock_js
            
            # Test connect
            await storage._connect()
            
            # Verify connection was established
            assert storage._js is not None
            mock_get_js.assert_called_once()
            
            # Test disconnect
            await storage._disconnect()
            
            # Verify connection was closed
            assert storage._js is None

    @pytest.mark.asyncio
    async def test_context_manager(self, storage):
        """Test async context manager functionality."""
        with patch.object(storage, '_connect') as mock_connect, \
             patch.object(storage, '_disconnect') as mock_disconnect:
            
            async with storage:
                pass
            
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()