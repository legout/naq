"""
Tests for Queue event logging functionality.
"""
import asyncio
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, Optional, AsyncIterator

from naq.events import JobEvent, JobEventType
from naq.events.shared_logger import get_shared_sync_logger, configure_shared_logger
from naq.events.logger import JobEventLogger, AsyncJobEventLogger
from naq.events.storage import BaseEventStorage, NATSJobEventStorage
from naq.models import Job
from naq.queue import Queue, ScheduledJobManager
from naq.settings import DEFAULT_QUEUE_NAME, DEFAULT_NATS_URL, SCHEDULED_JOB_STATUS


class MockEventStorage(BaseEventStorage):
    """Mock event storage for testing."""
    
    def __init__(self):
        self.events = []
        self.setup_called = False
        self.close_called = False
    
    async def setup(self) -> None:
        """Setup the storage."""
        self.setup_called = True
    
    async def close(self) -> None:
        """Close the storage."""
        self.close_called = True
    
    async def store_event(self, event: JobEvent) -> None:
        """Store an event."""
        self.events.append(event)
    
    async def get_events(
        self,
        job_id: Optional[str] = None,
        event_type: Optional[JobEventType] = None,
        queue_name: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[JobEvent]:
        """Get events with optional filtering."""
        filtered_events = self.events
        
        if job_id:
            filtered_events = [e for e in filtered_events if e.job_id == job_id]
        
        if event_type:
            filtered_events = [e for e in filtered_events if e.event_type == event_type]
        
        if queue_name:
            filtered_events = [e for e in filtered_events if e.queue_name == queue_name]
        
        if limit:
            filtered_events = filtered_events[:limit]
        
        return filtered_events
    
    async def get_job_events(self, job_id: str) -> list[JobEvent]:
        """Get all events for a specific job."""
        return await self.get_events(job_id=job_id)
    
    async def stream_events(self, job_id: str, context: Optional[str] = None, event_type: Optional[JobEventType] = None):  # type: ignore
        """Stream events for a specific job ID in real-time."""
        # Mock implementation for testing
        for event in self.events:
            if event.job_id == job_id:
                if event_type and event.event_type != event_type:
                    continue
                if context and event.worker_id and not event.worker_id.startswith(context):
                    continue
                yield event


@pytest.fixture
def mock_storage():
    """Create a mock event storage."""
    return MockEventStorage()


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    def sample_function(x, y):
        return x + y
    
    return Job(
        function=sample_function,
        args=(1, 2),
        kwargs={},
        queue_name="test-queue",
        max_retries=3,
        retry_delay=1.0,
    )


@pytest.mark.asyncio
async def test_queue_enqueued_event_logging(mock_storage, sample_job):
    """Test that ENQUEUED events are logged when jobs are enqueued."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue and enqueue job
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the JetStream publish operation
    mock_ack = MagicMock()
    mock_ack.stream = "test-stream"
    mock_ack.seq = 123
    
    with patch.object(queue, '_get_js') as mock_get_js:
        mock_js = AsyncMock()
        mock_js.publish.return_value = mock_ack
        mock_get_js.return_value = mock_js
        
        # Enqueue the job
        await queue.enqueue(sample_job.function, *sample_job.args, **sample_job.kwargs)
    
    # Verify ENQUEUED event was logged
    enqueued_events = [e for e in mock_storage.events if e.event_type == JobEventType.ENQUEUED]
    assert len(enqueued_events) == 1
    
    event = enqueued_events[0]
    assert event.job_id == sample_job.job_id
    assert event.queue_name == "test-queue"
    assert event.nats_subject == f"naq.queue.test-queue"
    assert event.nats_sequence == 123
    assert event.details["function_name"] == "sample_function"


@pytest.mark.asyncio
async def test_queue_scheduled_event_logging_enqueue_at(mock_storage):
    """Test that SCHEDULED events are logged when jobs are scheduled with enqueue_at."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Schedule a job
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    
    def sample_function():
        return "test"
    
    # Mock the scheduled job manager
    with patch.object(queue._scheduled_job_manager, 'store_job') as mock_store:
        mock_store.return_value = None
        
        await queue.enqueue_at(scheduled_time, sample_function)
    
    # Verify SCHEDULED event was logged
    scheduled_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULED]
    assert len(scheduled_events) == 1
    
    event = scheduled_events[0]
    assert event.queue_name == "test-queue"
    assert event.details["function_name"] == "sample_function"
    assert "scheduled_timestamp_utc" in event.details


@pytest.mark.asyncio
async def test_queue_scheduled_event_logging_schedule(mock_storage):
    """Test that SCHEDULED events are logged when jobs are scheduled with schedule()."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Schedule a recurring job
    def sample_function():
        return "test"
    
    # Mock the scheduled job manager
    with patch.object(queue._scheduled_job_manager, 'store_job') as mock_store:
        mock_store.return_value = None
        
        await queue.schedule(sample_function, cron="0 * * * *")
    
    # Verify SCHEDULED event was logged
    scheduled_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULED]
    assert len(scheduled_events) == 1
    
    event = scheduled_events[0]
    assert event.queue_name == "test-queue"
    assert event.details["function_name"] == "sample_function"
    assert event.details["cron"] == "0 * * * *"
    assert "scheduled_timestamp_utc" in event.details


@pytest.mark.asyncio
async def test_queue_cancelled_event_logging(mock_storage):
    """Test that CANCELLED events are logged when scheduled jobs are cancelled."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the KV store and entry
    mock_kv = AsyncMock()
    mock_entry = MagicMock()
    mock_entry.value = b'cloudpickle_value'  # Mock serialized data
    
    # Mock the schedule data that would be loaded from the entry
    schedule_data = {
        "job_id": "test-job-id",
        "queue_name": "test-queue",
        "status": SCHEDULED_JOB_STATUS.ACTIVE,
    }
    
    with patch.object(queue._scheduled_job_manager, 'get_kv') as mock_get_kv, \
         patch('cloudpickle.loads') as mock_cloudpickle_loads:
        
        mock_get_kv.return_value = mock_kv
        mock_kv.get.return_value = mock_entry
        mock_cloudpickle_loads.return_value = schedule_data
        
        # Cancel the scheduled job
        result = await queue.cancel_scheduled_job("test-job-id")
        
        assert result is True
    
    # Verify CANCELLED event was logged
    cancelled_events = [e for e in mock_storage.events if e.event_type == JobEventType.CANCELLED]
    assert len(cancelled_events) == 1
    
    event = cancelled_events[0]
    assert event.job_id == "test-job-id"
    assert event.queue_name == "test-queue"
    assert event.details["cancelled_by"] == "user"
    assert event.details["reason"] == "scheduled_job_cancellation"


@pytest.mark.asyncio
async def test_queue_status_changed_event_logging_pause(mock_storage):
    """Test that STATUS_CHANGED events are logged when scheduled jobs are paused."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the KV store and entry
    mock_kv = AsyncMock()
    mock_entry = MagicMock()
    mock_entry.value = b'cloudpickle_value'  # Mock serialized data
    mock_entry.revision = 1
    
    # Mock the schedule data that would be loaded from the entry
    schedule_data = {
        "job_id": "test-job-id",
        "queue_name": "test-queue",
        "status": SCHEDULED_JOB_STATUS.ACTIVE,
    }
    
    with patch.object(queue._scheduled_job_manager, 'get_kv') as mock_get_kv, \
         patch('cloudpickle.loads') as mock_cloudpickle_loads, \
         patch('cloudpickle.dumps') as mock_cloudpickle_dumps:
        
        mock_get_kv.return_value = mock_kv
        mock_kv.get.return_value = mock_entry
        mock_cloudpickle_loads.return_value = schedule_data
        mock_cloudpickle_dumps.return_value = b'serialized_data'
        
        # Pause the scheduled job
        result = await queue.pause_scheduled_job("test-job-id")
        
        assert result is True
    
    # Verify STATUS_CHANGED event was logged
    status_changed_events = [e for e in mock_storage.events if e.event_type == JobEventType.STATUS_CHANGED]
    assert len(status_changed_events) == 1
    
    event = status_changed_events[0]
    assert event.job_id == "test-job-id"
    assert event.queue_name == "test-queue"
    assert event.details["old_status"] == SCHEDULED_JOB_STATUS.ACTIVE
    assert event.details["new_status"] == SCHEDULED_JOB_STATUS.PAUSED
    assert event.details["updated_by"] == "user"
    assert event.details["reason"] == "scheduled_job_status_update"


@pytest.mark.asyncio
async def test_queue_status_changed_event_logging_resume(mock_storage):
    """Test that STATUS_CHANGED events are logged when scheduled jobs are resumed."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the KV store and entry
    mock_kv = AsyncMock()
    mock_entry = MagicMock()
    mock_entry.value = b'cloudpickle_value'  # Mock serialized data
    mock_entry.revision = 1
    
    # Mock the schedule data that would be loaded from the entry
    schedule_data = {
        "job_id": "test-job-id",
        "queue_name": "test-queue",
        "status": SCHEDULED_JOB_STATUS.PAUSED,
    }
    
    with patch.object(queue._scheduled_job_manager, 'get_kv') as mock_get_kv, \
         patch('cloudpickle.loads') as mock_cloudpickle_loads, \
         patch('cloudpickle.dumps') as mock_cloudpickle_dumps:
        
        mock_get_kv.return_value = mock_kv
        mock_kv.get.return_value = mock_entry
        mock_cloudpickle_loads.return_value = schedule_data
        mock_cloudpickle_dumps.return_value = b'serialized_data'
        
        # Resume the scheduled job
        result = await queue.resume_scheduled_job("test-job-id")
        
        assert result is True
    
    # Verify STATUS_CHANGED event was logged
    status_changed_events = [e for e in mock_storage.events if e.event_type == JobEventType.STATUS_CHANGED]
    assert len(status_changed_events) == 1
    
    event = status_changed_events[0]
    assert event.job_id == "test-job-id"
    assert event.queue_name == "test-queue"
    assert event.details["old_status"] == SCHEDULED_JOB_STATUS.PAUSED
    assert event.details["new_status"] == SCHEDULED_JOB_STATUS.ACTIVE
    assert event.details["updated_by"] == "user"
    assert event.details["reason"] == "scheduled_job_status_update"


@pytest.mark.asyncio
async def test_queue_event_logging_with_no_event_logger(sample_job):
    """Test that queue operations work even when event logger is not available."""
    # Create queue without configuring event logger
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the JetStream publish operation
    mock_ack = MagicMock()
    mock_ack.stream = "test-stream"
    mock_ack.seq = 123
    
    with patch.object(queue, '_get_js') as mock_get_js:
        mock_js = AsyncMock()
        mock_js.publish.return_value = mock_ack
        mock_get_js.return_value = mock_js
        
        # Enqueue the job - should not raise an exception
        await queue.enqueue(sample_job.function, *sample_job.args, **sample_job.kwargs)
    
    # Operation should complete successfully even without event logger


@pytest.mark.asyncio
async def test_queue_event_logging_error_handling(mock_storage, sample_job):
    """Test that errors in event logging don't affect queue operations."""
    # Configure shared logger with mock storage that raises an exception
    mock_storage.store_event.side_effect = Exception("Event logging failed")
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    # Mock the JetStream publish operation
    mock_ack = MagicMock()
    mock_ack.stream = "test-stream"
    mock_ack.seq = 123
    
    with patch.object(queue, '_get_js') as mock_get_js:
        mock_js = AsyncMock()
        mock_js.publish.return_value = mock_ack
        mock_get_js.return_value = mock_js
        
        # Enqueue the job - should not raise an exception despite event logging error
        await queue.enqueue(sample_job.function, *sample_job.args, **sample_job.kwargs)
    
    # Operation should complete successfully even if event logging fails


@pytest.mark.asyncio
async def test_queue_event_logging_metadata_completeness(mock_storage):
    """Test that all queue events include proper metadata."""
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create queue
    queue = Queue(name="test-queue", nats_url="nats://localhost:4222")
    
    def sample_function():
        return "test"
    
    # Test ENQUEUED event metadata
    with patch.object(queue, '_get_js') as mock_get_js:
        mock_js = AsyncMock()
        mock_ack = MagicMock()
        mock_ack.stream = "test-stream"
        mock_ack.seq = 123
        mock_js.publish.return_value = mock_ack
        mock_get_js.return_value = mock_js
        
        await queue.enqueue(sample_function)
    
    enqueued_events = [e for e in mock_storage.events if e.event_type == JobEventType.ENQUEUED]
    assert len(enqueued_events) == 1
    event = enqueued_events[0]
    
    # Verify required fields
    assert event.job_id is not None
    assert event.queue_name == "test-queue"
    assert event.nats_subject is not None
    assert event.nats_sequence is not None
    assert event.details is not None
    assert "function_name" in event.details
    assert event.timestamp is not None
    
    # Test SCHEDULED event metadata
    mock_storage.events.clear()
    
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    
    with patch.object(queue._scheduled_job_manager, 'store_job') as mock_store:
        mock_store.return_value = None
        
        await queue.enqueue_at(scheduled_time, sample_function)
    
    scheduled_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULED]
    assert len(scheduled_events) == 1
    event = scheduled_events[0]
    
    # Verify required fields
    assert event.job_id is not None
    assert event.queue_name == "test-queue"
    assert event.details is not None
    assert "function_name" in event.details
    assert "scheduled_timestamp_utc" in event.details
    assert event.timestamp is not None