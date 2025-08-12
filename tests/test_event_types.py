# tests/test_event_types.py
"""
Comprehensive tests for the new event types.

This test file covers:
2. Tests for the new event types:
   - Test CANCELLED and STATUS_CHANGED events
   - Test that all event types work correctly with the shared logger
   - Test that details field is properly populated for all event types
"""

import asyncio
import pytest
from typing import AsyncIterator, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from naq.events.shared_logger import get_shared_event_logger_manager, configure_shared_logger
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
        
    async def stream_events(self, job_id: str, context: Optional[str] = None, event_type: Optional[JobEventType] = None):
        # Mock implementation for testing
        for event in self.events:
            if event.job_id == job_id:
                if event_type and event.event_type != event_type:
                    continue
                if context and event.worker_id and not event.worker_id.startswith(context):
                    continue
                yield event


class TestCancelledEvent:
    """Test cases for CANCELLED event type."""

    def test_cancelled_event_creation(self):
        """Test basic CANCELLED event creation."""
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue",
            worker_id="worker-abc",
            details={"reason": "User requested cancellation"}
        )
        
        assert event.job_id == "test-job-123"
        assert event.event_type == JobEventType.CANCELLED
        assert event.queue_name == "test-queue"
        assert event.worker_id == "worker-abc"
        assert event.message == "Job cancelled"
        assert event.details == {"reason": "User requested cancellation"}

    def test_cancelled_event_with_nats_info(self):
        """Test CANCELLED event creation with NATS information."""
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue",
            worker_id="worker-abc",
            nats_subject="naq.jobs.events.test-job-123.worker.cancelled",
            nats_sequence=42,
            details={"reason": "User requested cancellation"}
        )
        
        assert event.nats_subject == "naq.jobs.events.test-job-123.worker.cancelled"
        assert event.nats_sequence == 42

    def test_cancelled_event_minimal_parameters(self):
        """Test CANCELLED event creation with minimal parameters."""
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue"
        )
        
        assert event.job_id == "test-job-123"
        assert event.event_type == JobEventType.CANCELLED
        assert event.queue_name == "test-queue"
        assert event.worker_id is None
        assert event.details == {}

    def test_cancelled_event_with_none_details(self):
        """Test CANCELLED event creation with None details."""
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue",
            details=None
        )
        
        assert event.details == {}

    @pytest.mark.asyncio
    async def test_cancelled_event_with_async_logger(self):
        """Test CANCELLED event logging with async logger."""
        storage = MockStorage()
        logger = AsyncJobEventLogger(storage, batch_size=1, flush_interval=0.1)
        
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue",
            worker_id="worker-abc",
            details={"reason": "User requested cancellation"}
        )
        
        await logger.log_event(event)
        await logger.flush()
        
        assert len(storage.events) == 1
        stored_event = storage.events[0]
        assert stored_event.job_id == "test-job-123"
        assert stored_event.event_type == JobEventType.CANCELLED
        assert stored_event.details == {"reason": "User requested cancellation"}

    def test_cancelled_event_with_sync_logger(self):
        """Test CANCELLED event logging with sync logger."""
        storage = MockStorage()
        logger = JobEventLogger(storage, batch_size=1, flush_interval=0.1)
        
        event = JobEvent.cancelled(
            job_id="test-job-123",
            queue_name="test-queue",
            worker_id="worker-abc",
            details={"reason": "User requested cancellation"}
        )
        
        logger.log_event(event)
        logger.flush()
        
        assert len(storage.events) == 1
        stored_event = storage.events[0]
        assert stored_event.job_id == "test-job-123"
        assert stored_event.event_type == JobEventType.CANCELLED
        assert stored_event.details == {"reason": "User requested cancellation"}

    @pytest.mark.asyncio
    async def test_cancelled_event_with_shared_logger(self):
        """Test CANCELLED event logging with shared logger."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Get shared async logger
                shared_logger = await get_shared_event_logger_manager().get_async_logger()
                assert shared_logger is not None
                
                # Create and log CANCELLED event
                event = JobEvent.cancelled(
                    job_id="test-job-123",
                    queue_name="test-queue",
                    worker_id="worker-abc",
                    details={"reason": "User requested cancellation"}
                )
                
                await shared_logger.log_event(event)
                await shared_logger.flush()
                
                # Verify event was stored
                assert len(mock_storage.events) == 1
                stored_event = mock_storage.events[0]
                assert stored_event.job_id == "test-job-123"
                assert stored_event.event_type == JobEventType.CANCELLED
                assert stored_event.details == {"reason": "User requested cancellation"}
        finally:
            # Clean up
            await manager.async_reset()


class TestStatusChangedEvent:
    """Test cases for STATUS_CHANGED event type."""

    def test_status_changed_event_creation(self):
        """Test basic STATUS_CHANGED event creation."""
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="pending",
            new_status="running",
            worker_id="worker-abc",
            details={"trigger": "worker_start"}
        )
        
        assert event.job_id == "test-job-123"
        assert event.event_type == JobEventType.STATUS_CHANGED
        assert event.queue_name == "test-queue"
        assert event.worker_id == "worker-abc"
        assert event.message == "Job status changed from pending to running"
        assert event.details == {
            "old_status": "pending",
            "new_status": "running",
            "trigger": "worker_start"
        }

    def test_status_changed_event_with_nats_info(self):
        """Test STATUS_CHANGED event creation with NATS information."""
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="running",
            new_status="completed",
            worker_id="worker-abc",
            nats_subject="naq.jobs.events.test-job-123.worker.status_changed",
            nats_sequence=42,
            details={"trigger": "job_completion"}
        )
        
        assert event.nats_subject == "naq.jobs.events.test-job-123.worker.status_changed"
        assert event.nats_sequence == 42

    def test_status_changed_event_minimal_parameters(self):
        """Test STATUS_CHANGED event creation with minimal parameters."""
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="pending",
            new_status="running"
        )
        
        assert event.job_id == "test-job-123"
        assert event.event_type == JobEventType.STATUS_CHANGED
        assert event.queue_name == "test-queue"
        assert event.worker_id is None
        assert event.details == {
            "old_status": "pending",
            "new_status": "running"
        }

    def test_status_changed_event_with_none_details(self):
        """Test STATUS_CHANGED event creation with None details."""
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="pending",
            new_status="running",
            details=None
        )
        
        assert event.details == {
            "old_status": "pending",
            "new_status": "running"
        }

    def test_status_changed_event_details_merge(self):
        """Test that custom details are merged with status information."""
        custom_details = {
            "trigger": "manual intervention",
            "user": "admin",
            "timestamp": 1640995200.0
        }
        
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="running",
            new_status="failed",
            details=custom_details
        )
        
        expected_details = {
            "old_status": "running",
            "new_status": "failed",
            "trigger": "manual intervention",
            "user": "admin",
            "timestamp": 1640995200.0
        }
        
        assert event.details == expected_details

    def test_status_changed_event_details_override(self):
        """Test that custom details don't override status information."""
        custom_details = {
            "old_status": "wrong_old",  # This should be overridden
            "new_status": "wrong_new",  # This should be overridden
            "trigger": "manual intervention"
        }
        
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="running",
            new_status="completed",
            details=custom_details
        )
        
        # Status information should take precedence
        assert event.details is not None
        assert event.details["old_status"] == "running"
        assert event.details["new_status"] == "completed"
        assert event.details["trigger"] == "manual intervention"

    @pytest.mark.asyncio
    async def test_status_changed_event_with_async_logger(self):
        """Test STATUS_CHANGED event logging with async logger."""
        storage = MockStorage()
        logger = AsyncJobEventLogger(storage, batch_size=1, flush_interval=0.1)
        
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="running",
            new_status="completed",
            worker_id="worker-abc",
            details={"trigger": "job_completion"}
        )
        
        await logger.log_event(event)
        await logger.flush()
        
        assert len(storage.events) == 1
        stored_event = storage.events[0]
        assert stored_event.job_id == "test-job-123"
        assert stored_event.event_type == JobEventType.STATUS_CHANGED
        assert stored_event.details == {
            "old_status": "running",
            "new_status": "completed",
            "trigger": "job_completion"
        }

    def test_status_changed_event_with_sync_logger(self):
        """Test STATUS_CHANGED event logging with sync logger."""
        storage = MockStorage()
        logger = JobEventLogger(storage, batch_size=1, flush_interval=0.1)
        
        event = JobEvent.status_changed(
            job_id="test-job-123",
            queue_name="test-queue",
            old_status="running",
            new_status="completed",
            worker_id="worker-abc",
            details={"trigger": "job_completion"}
        )
        
        logger.log_event(event)
        logger.flush()
        
        assert len(storage.events) == 1
        stored_event = storage.events[0]
        assert stored_event.job_id == "test-job-123"
        assert stored_event.event_type == JobEventType.STATUS_CHANGED
        assert stored_event.details == {
            "old_status": "running",
            "new_status": "completed",
            "trigger": "job_completion"
        }

    @pytest.mark.asyncio
    async def test_status_changed_event_with_shared_logger(self):
        """Test STATUS_CHANGED event logging with shared logger."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Get shared async logger
                shared_logger = await get_shared_event_logger_manager().get_async_logger()
                
                # Create and log STATUS_CHANGED event
                event = JobEvent.status_changed(
                    job_id="test-job-123",
                    queue_name="test-queue",
                    old_status="running",
                    new_status="completed",
                    worker_id="worker-abc",
                    details={"trigger": "job_completion"}
                )
                
                await shared_logger.log_event(event)
                await shared_logger.flush()
                
                # Verify event was stored
                assert len(mock_storage.events) == 1
                stored_event = mock_storage.events[0]
                assert stored_event.job_id == "test-job-123"
                assert stored_event.event_type == JobEventType.STATUS_CHANGED
                assert stored_event.details == {
                    "old_status": "running",
                    "new_status": "completed",
                    "trigger": "job_completion"
                }
        finally:
            # Clean up
            await manager.async_reset()


class TestAllEventTypesWithSharedLogger:
    """Test that all event types work correctly with the shared logger."""

    @pytest.mark.asyncio
    async def test_all_event_types_with_shared_async_logger(self):
        """Test that all event types work with shared async logger."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=10, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Get shared async logger
                shared_logger = await get_shared_event_logger_manager().get_async_logger()
                assert shared_logger is not None
                
                # Create events of all types
                events = [
                    JobEvent.enqueued("job-1", "queue-1", "worker-1"),
                    JobEvent.started("job-1", "worker-1", "queue-1"),
                    JobEvent.completed("job-1", "worker-1", 1000.0, "queue-1"),
                    JobEvent.failed("job-2", "worker-1", "ValueError", "Test error", 500.0, "queue-1"),
                    JobEvent.retry_scheduled("job-2", "worker-1", 30.0, "queue-1"),
                    JobEvent.scheduled("job-3", "queue-1", 1640995200.0, "worker-1"),
                    JobEvent.schedule_triggered("job-3", "queue-1", "worker-1"),
                    JobEvent.cancelled("job-4", "queue-1", "worker-1", details={"reason": "user_cancel"}),
                    JobEvent.status_changed("job-1", "queue-1", "running", "completed", "worker-1")
                ]
                
                # Log all events
                for event in events:
                    await shared_logger.log_event(event)
                
                await shared_logger.flush()
                
                # Verify all events were stored
                assert len(mock_storage.events) == len(events)
                
                # Verify event types
                stored_event_types = [e.event_type for e in mock_storage.events]
                expected_event_types = [e.event_type for e in events]
                assert stored_event_types == expected_event_types
                
                # Verify details field is properly populated for all event types
                for event in mock_storage.events:
                    assert event.details is not None
                    assert isinstance(event.details, dict)
                    
                    # Check specific details for each event type
                    if event.event_type == JobEventType.COMPLETED:
                        assert "duration_ms" in event.details
                    elif event.event_type == JobEventType.FAILED:
                        assert "duration_ms" in event.details
                        assert "error_type" in event.details
                        assert "error_message" in event.details
                    elif event.event_type == JobEventType.RETRY_SCHEDULED:
                        assert "delay_seconds" in event.details
                    elif event.event_type == JobEventType.SCHEDULED:
                        assert "scheduled_timestamp_utc" in event.details
                    elif event.event_type == JobEventType.CANCELLED:
                        # Should have custom details if provided
                        if event.job_id == "job-4":
                            assert "reason" in event.details
                    elif event.event_type == JobEventType.STATUS_CHANGED:
                        assert "old_status" in event.details
                        assert "new_status" in event.details
        finally:
            # Clean up
            await manager.async_reset()

    def test_all_event_types_with_shared_sync_logger(self):
        """Test that all event types work with shared sync logger."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.JobEventLogger') as mock_logger_class:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = JobEventLogger(mock_storage, batch_size=10, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Get shared sync logger
                shared_logger = get_shared_event_logger_manager().get_sync_logger()
                assert shared_logger is not None
                
                # Create events of all types
                events = [
                    JobEvent.enqueued("job-1", "queue-1", "worker-1"),
                    JobEvent.started("job-1", "worker-1", "queue-1"),
                    JobEvent.completed("job-1", "worker-1", 1000.0, "queue-1"),
                    JobEvent.failed("job-2", "worker-1", "ValueError", "Test error", 500.0, "queue-1"),
                    JobEvent.retry_scheduled("job-2", "worker-1", 30.0, "queue-1"),
                    JobEvent.scheduled("job-3", "queue-1", 1640995200.0, "worker-1"),
                    JobEvent.schedule_triggered("job-3", "queue-1", "worker-1"),
                    JobEvent.cancelled("job-4", "queue-1", "worker-1", details={"reason": "user_cancel"}),
                    JobEvent.status_changed("job-1", "queue-1", "running", "completed", "worker-1")
                ]
                
                # Log all events
                for event in events:
                    shared_logger.log_event(event)
                
                shared_logger.flush()
                
                # Verify all events were stored
                assert len(mock_storage.events) == len(events)
                
                # Verify event types
                stored_event_types = [e.event_type for e in mock_storage.events]
                expected_event_types = [e.event_type for e in events]
                assert stored_event_types == expected_event_types
                
                # Verify details field is properly populated for all event types
                for event in mock_storage.events:
                    assert event.details is not None
                    assert isinstance(event.details, dict)
                    
                    # Check specific details for each event type
                    if event.event_type == JobEventType.COMPLETED:
                        assert "duration_ms" in event.details
                    elif event.event_type == JobEventType.FAILED:
                        assert "duration_ms" in event.details
                        assert "error_type" in event.details
                        assert "error_message" in event.details
                    elif event.event_type == JobEventType.RETRY_SCHEDULED:
                        assert "delay_seconds" in event.details
                    elif event.event_type == JobEventType.SCHEDULED:
                        assert "scheduled_timestamp_utc" in event.details
                    elif event.event_type == JobEventType.CANCELLED:
                        # Should have custom details if provided
                        if event.job_id == "job-4":
                            assert "reason" in event.details
                    elif event.event_type == JobEventType.STATUS_CHANGED:
                        assert "old_status" in event.details
                        assert "new_status" in event.details
        finally:
            # Clean up
            asyncio.run(manager.async_reset())

    @pytest.mark.asyncio
    async def test_event_details_field_population(self):
        """Test that details field is properly populated for all event types."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Get shared async logger
                shared_logger = await get_shared_event_logger_manager().get_async_logger()
                assert shared_logger is not None
                
                # Test each event type with specific details
                test_cases = [
                    (JobEventType.ENQUEUED, lambda: JobEvent.enqueued(
                        "job-1", "queue-1", "worker-1", details={"priority": "high"}
                    )),
                    (JobEventType.STARTED, lambda: JobEvent.started(
                        "job-1", "worker-1", "queue-1", details={"start_time": 1640995200.0}
                    )),
                    (JobEventType.COMPLETED, lambda: JobEvent.completed(
                        "job-1", "worker-1", 1000.0, "queue-1", details={"result_size": 1024}
                    )),
                    (JobEventType.FAILED, lambda: JobEvent.failed(
                        "job-2", "worker-1", "ValueError", "Test error", 500.0, "queue-1", 
                        details={"retry_count": 2}
                    )),
                    (JobEventType.RETRY_SCHEDULED, lambda: JobEvent.retry_scheduled(
                        "job-2", "worker-1", 30.0, "queue-1", details={"backoff_factor": 2.0}
                    )),
                    (JobEventType.SCHEDULED, lambda: JobEvent.scheduled(
                        "job-3", "queue-1", 1640995200.0, "worker-1", details={"cron": "0 0 * * *"}
                    )),
                    (JobEventType.SCHEDULE_TRIGGERED, lambda: JobEvent.schedule_triggered(
                        "job-3", "queue-1", "worker-1", details={"trigger_time": 1640995200.0}
                    )),
                    (JobEventType.CANCELLED, lambda: JobEvent.cancelled(
                        "job-4", "queue-1", "worker-1", details={"reason": "timeout"}
                    )),
                    (JobEventType.STATUS_CHANGED, lambda: JobEvent.status_changed(
                        "job-1", "queue-1", "running", "completed", "worker-1", 
                        details={"trigger": "natural_completion"}
                    ))
                ]
                
                for event_type, event_factory in test_cases:
                    # Clear previous events
                    mock_storage.events.clear()
                    
                    # Create and log event
                    event = event_factory()
                    await shared_logger.log_event(event)
                    await shared_logger.flush()
                    
                    # Verify event was stored
                    assert len(mock_storage.events) == 1
                    stored_event = mock_storage.events[0]
                    
                    # Verify event type
                    assert stored_event.event_type == event_type
                    
                    # Verify details field is properly populated
                    assert stored_event.details is not None
                    assert isinstance(stored_event.details, dict)
                    
                    # Verify specific details for each event type
                    if event_type == JobEventType.ENQUEUED:
                        assert stored_event.details["priority"] == "high"
                    elif event_type == JobEventType.STARTED:
                        assert stored_event.details["start_time"] == 1640995200.0
                    elif event_type == JobEventType.COMPLETED:
                        assert stored_event.details["duration_ms"] == 1000.0
                        assert stored_event.details["result_size"] == 1024
                    elif event_type == JobEventType.FAILED:
                        assert stored_event.details["duration_ms"] == 500.0
                        assert stored_event.details["error_type"] == "ValueError"
                        assert stored_event.details["error_message"] == "Test error"
                        assert stored_event.details["retry_count"] == 2
                    elif event_type == JobEventType.RETRY_SCHEDULED:
                        assert stored_event.details["delay_seconds"] == 30.0
                        assert stored_event.details["backoff_factor"] == 2.0
                    elif event_type == JobEventType.SCHEDULED:
                        assert stored_event.details["scheduled_timestamp_utc"] == 1640995200.0
                        assert stored_event.details["cron"] == "0 0 * * *"
                    elif event_type == JobEventType.SCHEDULE_TRIGGERED:
                        assert stored_event.details["trigger_time"] == 1640995200.0
                    elif event_type == JobEventType.CANCELLED:
                        assert stored_event.details["reason"] == "timeout"
                    elif event_type == JobEventType.STATUS_CHANGED:
                        assert stored_event.details["old_status"] == "running"
                        assert stored_event.details["new_status"] == "completed"
                        assert stored_event.details["trigger"] == "natural_completion"
        finally:
            # Clean up
            await manager.async_reset()