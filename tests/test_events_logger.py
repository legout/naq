# tests/test_events_logger.py
import asyncio
from typing import AsyncIterator, Optional
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from naq.events.logger import AsyncJobEventLogger, JobEventLogger
from naq.events.storage import BaseEventStorage
from naq.models import JobEvent, JobEventType


class MockStorage(BaseEventStorage):
    """Mock storage backend for testing."""
    
    def __init__(self):
        self.events = []
        self.fail_next = False
        
    async def store_event(self, event: JobEvent) -> None:
        if self.fail_next:
            raise Exception("Storage failure")
        self.events.append(event)
        
    async def get_events(self, job_id: str) -> list[JobEvent]:
        return [e for e in self.events if e.job_id == job_id]
        
    async def stream_events(self, job_id: str, context: Optional[str] = None, event_type: Optional[JobEventType] = None) -> AsyncIterator[JobEvent]:
        # Mock implementation for testing
        for event in self.events:
            if event.job_id == job_id:
                if event_type and event.event_type != event_type:
                    continue
                if context and event.worker_id and not event.worker_id.startswith(context):
                    continue
                yield event


@pytest.mark.asyncio
async def test_async_logger_initialization():
    """Test AsyncJobEventLogger initialization."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    assert logger.storage is storage
    assert logger.batch_size == 10
    assert logger.flush_interval == 1.0
    assert logger.max_buffer_size == 10000
    assert len(logger._buffer) == 0
    assert not logger._running


@pytest.mark.asyncio
async def test_async_logger_log_event():
    """Test basic event logging."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    event = JobEvent(
        job_id="test-job-123",
        event_type=JobEventType.ENQUEUED,
        queue_name="test-queue",
        worker_id="worker-abc"
    )
    
    await logger.log_event(event)
    
    assert len(logger._buffer) == 1
    assert logger._buffer[0] == event
    assert logger._stats["events_logged"] == 1


@pytest.mark.asyncio
async def test_async_logger_batch_flush():
    """Test automatic flushing when batch size is reached."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=3, flush_interval=1.0)
    
    # Log events that should trigger a flush
    events = []
    for i in range(3):
        event = JobEvent(
            job_id=f"test-job-{i}",
            event_type=JobEventType.ENQUEUED,
            queue_name="test-queue",
            worker_id="worker-abc"
        )
        events.append(event)
        await logger.log_event(event)
    
    # Buffer should be empty after flush
    assert len(logger._buffer) == 0
    assert len(storage.events) == 3
    assert all(e in storage.events for e in events)


@pytest.mark.asyncio
async def test_async_logger_convenience_methods():
    """Test convenience methods for different event types."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    # Test convenience methods
    await logger.log_job_enqueued("job-1", "queue-1", "worker-1")
    await logger.log_job_started("job-1", "worker-1", "queue-1")
    await logger.log_job_completed("job-1", "worker-1", 1000.0, "queue-1")
    await logger.log_job_failed("job-2", "worker-1", "ValueError", "Test error", 500.0, "queue-1")
    await logger.log_job_retry_scheduled("job-2", "worker-1", 30.0, "queue-1")
    await logger.log_job_scheduled("job-3", "queue-1", 1640995200.0, "worker-1")
    await logger.log_job_schedule_triggered("job-3", "queue-1", "worker-1")
    
    # Check that all events were logged
    assert len(logger._buffer) == 7
    
    # Check specific event types
    enqueued_events = [e for e in logger._buffer if e.event_type == JobEventType.ENQUEUED]
    started_events = [e for e in logger._buffer if e.event_type == JobEventType.STARTED]
    completed_events = [e for e in logger._buffer if e.event_type == JobEventType.COMPLETED]
    failed_events = [e for e in logger._buffer if e.event_type == JobEventType.FAILED]
    retry_events = [e for e in logger._buffer if e.event_type == JobEventType.RETRY_SCHEDULED]
    scheduled_events = [e for e in logger._buffer if e.event_type == JobEventType.SCHEDULED]
    triggered_events = [e for e in logger._buffer if e.event_type == JobEventType.SCHEDULE_TRIGGERED]
    
    assert len(enqueued_events) == 1
    assert len(started_events) == 1
    assert len(completed_events) == 1
    assert len(failed_events) == 1
    assert len(retry_events) == 1
    assert len(scheduled_events) == 1
    assert len(triggered_events) == 1


@pytest.mark.asyncio
async def test_async_logger_start_stop():
    """Test logger start and stop functionality."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=0.1)
    
    # Start the logger
    await logger.start()
    assert logger._running
    assert logger._flush_task is not None
    assert not logger._flush_task.done()
    
    # Stop the logger
    await logger.stop()
    assert not logger._running
    assert logger._flush_task.done()


@pytest.mark.asyncio
async def test_async_logger_flush_retry():
    """Test retry logic when storage fails."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    # Make storage fail
    storage.fail_next = True
    
    event = JobEvent(
        job_id="test-job-123",
        event_type=JobEventType.ENQUEUED,
        queue_name="test-queue",
        worker_id="worker-abc"
    )
    
    await logger.log_event(event)
    
    # Buffer should still contain the event after failed flush
    assert len(logger._buffer) == 1
    
    # Reset storage and try again
    storage.fail_next = False
    await logger._flush_events()
    
    # Buffer should be empty after successful flush
    assert len(logger._buffer) == 0
    assert len(storage.events) == 1


@pytest.mark.asyncio
async def test_async_logger_stats():
    """Test logger statistics."""
    storage = MockStorage()
    logger = AsyncJobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    # Log some events
    for i in range(5):
        event = JobEvent(
            job_id=f"test-job-{i}",
            event_type=JobEventType.ENQUEUED,
            queue_name="test-queue",
            worker_id="worker-abc"
        )
        await logger.log_event(event)
    
    # Check initial stats
    stats = logger.get_stats()
    assert stats["events_logged"] == 5
    assert stats["events_flushed"] == 0
    assert stats["flush_attempts"] == 0
    assert stats["flush_failures"] == 0
    
    # Flush events
    await logger.flush()
    
    # Check updated stats
    stats = logger.get_stats()
    assert stats["events_logged"] == 5
    assert stats["events_flushed"] == 5
    assert stats["flush_attempts"] == 1
    assert stats["flush_failures"] == 0


def test_sync_logger_initialization():
    """Test JobEventLogger initialization."""
    storage = MockStorage()
    logger = JobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    assert logger._async_logger.storage is storage
    assert logger._async_logger.batch_size == 10
    assert logger._async_logger.flush_interval == 1.0


def test_sync_logger_methods():
    """Test synchronous wrapper methods."""
    storage = MockStorage()
    logger = JobEventLogger(storage, batch_size=10, flush_interval=1.0)
    
    # Test convenience methods
    logger.log_job_enqueued("job-1", "queue-1", "worker-1")
    logger.log_job_started("job-1", "worker-1", "queue-1")
    logger.log_job_completed("job-1", "worker-1", 1000.0, "queue-1")
    logger.log_job_failed("job-2", "worker-1", "ValueError", "Test error", 500.0, "queue-1")
    logger.log_job_retry_scheduled("job-2", "worker-1", 30.0, "queue-1")
    logger.log_job_scheduled("job-3", "queue-1", 1640995200.0, "worker-1")
    logger.log_job_schedule_triggered("job-3", "queue-1", "worker-1")
    
    # Check that all events were logged
    assert len(logger._async_logger._buffer) == 7
    
    # Test stats
    stats = logger.get_stats()
    assert stats["events_logged"] == 7
    
    # Test buffer size
    assert logger.get_buffer_size() == 7


def test_sync_logger_start_stop():
    """Test synchronous start and stop."""
    storage = MockStorage()
    logger = JobEventLogger(storage, batch_size=10, flush_interval=0.1)
    
    # Start and stop should work without errors
    logger.start()
    logger.stop()
    
    # Check that the async logger was started and stopped
    assert not logger._async_logger._running


if __name__ == "__main__":
    # Run basic tests
    print("Running basic tests...")
    
    # Test async logger
    asyncio.run(test_async_logger_initialization())
    asyncio.run(test_async_logger_log_event())
    asyncio.run(test_async_logger_convenience_methods())
    asyncio.run(test_async_logger_start_stop())
    asyncio.run(test_async_logger_flush_retry())
    asyncio.run(test_async_logger_stats())
    
    # Test sync logger
    test_sync_logger_initialization()
    test_sync_logger_methods()
    test_sync_logger_start_stop()
    
    print("All tests passed!")