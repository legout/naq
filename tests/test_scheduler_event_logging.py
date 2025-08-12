# tests/test_scheduler_event_logging.py
import asyncio
import pytest
import cloudpickle
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Optional

from naq.scheduler import Scheduler, ScheduledJobProcessor
from naq.events.shared_logger import get_shared_sync_logger, configure_shared_logger
from naq.events.storage import BaseEventStorage
from naq.models import JobEvent, JobEventType
from naq.settings import SCHEDULED_JOB_STATUS


class MockEventStorage(BaseEventStorage):
    """Mock event storage for testing."""
    
    def __init__(self):
        self.events = []
        self.fail_next = False
        
    async def store_event(self, event: JobEvent) -> None:
        if self.fail_next:
            raise Exception("Storage failure")
        self.events.append(event)
        
    async def get_events(self, job_id: str) -> list[JobEvent]:
        return [e for e in self.events if e.job_id == job_id]
    
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


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging():
    """Test that SCHEDULE_TRIGGERED events are logged when scheduled jobs are enqueued."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry
        mock_entry = MagicMock()
        # Create a proper job dict and pickle it
        job_data = {
            "job_id": "test-job-123",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "active"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-123"])
        mock_kv.put = AsyncMock()
        mock_kv.delete = AsyncMock()
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job was processed
        assert processed == 1
        assert errors == 0
        
        # Verify SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 1
        
        event = triggered_events[0]
        assert event.job_id == "test-job-123"
        assert event.queue_name == "test-queue"
        assert event.details["scheduled_timestamp_utc"] == 1640995200.0
        assert event.details["cron"] == "0 * * * *"


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_with_failure():
    """Test that SCHEDULE_TRIGGERED events are not logged when enqueue fails."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return failure
    with patch.object(processor, '_enqueue_job', return_value=False) as mock_enqueue:
        # Mock KV store entry
        mock_entry = MagicMock()
        # Create a proper job dict and pickle it
        job_data = {
            "job_id": "test-job-123",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "active"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-123"])
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job processing failed
        assert processed == 0
        assert errors == 1
        
        # Verify no SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 0


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_with_metadata():
    """Test that SCHEDULE_TRIGGERED events include proper metadata."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry with interval-based scheduling
        mock_entry = MagicMock()
        # Create a proper job dict with interval scheduling and pickle it
        job_data = {
            "job_id": "test-job-456",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": None,
            "interval_seconds": 3600,
            "repeat": 5,
            "_orig_job_payload": b"test-payload",
            "status": "active"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-456"])
        mock_kv.put = AsyncMock()
        mock_kv.delete = AsyncMock()
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job was processed
        assert processed == 1
        assert errors == 0
        
        # Verify SCHEDULE_TRIGGERED event was logged with correct metadata
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 1
        
        event = triggered_events[0]
        assert event.job_id == "test-job-456"
        assert event.queue_name == "test-queue"
        assert event.details["scheduled_timestamp_utc"] == 1640995200.0
        assert event.details["interval_seconds"] == 3600
        assert event.details["repeat"] == 5
        assert "cron" not in event.details  # Should not be present for interval-based jobs


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_paused_job():
    """Test that SCHEDULE_TRIGGERED events are not logged for paused jobs."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry for paused job
        mock_entry = MagicMock()
        # Create a proper job dict with paused status and pickle it
        job_data = {
            "job_id": "test-job-789",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "paused"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-789"])
        mock_kv.put = AsyncMock()
        mock_kv.delete = AsyncMock()
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job was not processed
        assert processed == 0
        assert errors == 0
        
        # Verify no SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 0


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_failed_job():
    """Test that SCHEDULE_TRIGGERED events are not logged for failed jobs."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry for failed job
        mock_entry = MagicMock()
        # Create a proper job dict with failed status and pickle it
        job_data = {
            "job_id": "test-job-999",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "failed"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-999"])
        mock_kv.put = AsyncMock()
        mock_kv.delete = AsyncMock()
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job was not processed
        assert processed == 0
        assert errors == 0
        
        # Verify no SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 0


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_future_job():
    """Test that SCHEDULE_TRIGGERED events are not logged for jobs scheduled in the future."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry for job scheduled in the future
        future_timestamp = 9999999999.0  # Far in the future
        mock_entry = MagicMock()
        # Create a proper job dict with future timestamp and pickle it
        job_data = {
            "job_id": "test-job-future",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": future_timestamp,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "active"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-future"])
        
        # Process the job
        processed, errors = await processor.process_jobs(is_leader=True)
        
        # Verify job was not processed
        assert processed == 0
        assert errors == 0
        
        # Verify no SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 0


@pytest.mark.asyncio
async def test_scheduler_schedule_triggered_event_logging_non_leader():
    """Test that SCHEDULE_TRIGGERED events are not logged when not leader."""
    # Create mock storage
    mock_storage = MockEventStorage()
    
    # Configure shared logger with mock storage
    configure_shared_logger(storage_instance=mock_storage)
    
    # Create mock JetStream context and KV store
    mock_js = MagicMock()
    mock_kv = MagicMock()
    
    # Create job processor
    processor = ScheduledJobProcessor(mock_js, mock_kv, "nats://localhost:4222")
    
    # Mock the _enqueue_job method to return success
    with patch.object(processor, '_enqueue_job', return_value=True) as mock_enqueue:
        # Mock KV store entry
        mock_entry = MagicMock()
        # Create a proper job dict and pickle it
        job_data = {
            "job_id": "test-job-123",
            "queue_name": "test-queue",
            "scheduled_timestamp_utc": 1640995200.0,
            "cron": "0 * * * *",
            "interval_seconds": None,
            "repeat": None,
            "_orig_job_payload": b"test-payload",
            "status": "active"
        }
        mock_entry.value = cloudpickle.dumps(job_data)
        mock_kv.get = AsyncMock(return_value=mock_entry)
        mock_kv.keys = AsyncMock(return_value=[b"test-job-123"])
        
        # Process the job as non-leader
        processed, errors = await processor.process_jobs(is_leader=False)
        
        # Verify job was not processed
        assert processed == 0
        assert errors == 0
        
        # Verify no SCHEDULE_TRIGGERED event was logged
        triggered_events = [e for e in mock_storage.events if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
        assert len(triggered_events) == 0


if __name__ == "__main__":
    # Run basic tests
    print("Running basic scheduler event logging tests...")
    
    # Test async functions
    asyncio.run(test_scheduler_schedule_triggered_event_logging())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_with_failure())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_with_metadata())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_paused_job())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_failed_job())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_future_job())
    asyncio.run(test_scheduler_schedule_triggered_event_logging_non_leader())
    
    print("All scheduler event logging tests passed!")