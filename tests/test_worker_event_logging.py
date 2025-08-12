# tests/test_worker_event_logging.py
"""
Comprehensive tests for Worker event logging.

This test file covers:
3. Tests for Worker event logging:
   - Test that STARTED events are logged when jobs begin processing
   - Test that COMPLETED events are logged when jobs finish successfully
   - Test that FAILED events are logged when jobs fail
   - Test that RETRY_SCHEDULED events are logged when jobs are retried
   - Test that all events include proper metadata
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, AsyncIterator

from naq.worker import Worker
from naq.models import Job, JOB_STATUS
from typing import Optional
from naq.events.shared_logger import get_shared_event_logger_manager, configure_shared_logger
from naq.events.logger import AsyncJobEventLogger
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
        
    async def stream_events(self, job_id: str, context: Optional[str] = None, event_type: Optional[JobEventType] = None):  # type: ignore
        # Mock implementation for testing
        for event in self.events:
            if event.job_id == job_id:
                if event_type and event.event_type != event_type:
                    continue
                if context and event.worker_id and not event.worker_id.startswith(context):
                    continue
                yield event


class MockJob:
    """Mock job for testing."""
    
    def __init__(self, job_id="test-job-123", queue_name="test-queue", function_name="test_func"):
        self.job_id = job_id
        self.queue_name = queue_name
        self.function_name = function_name
        self.status = JOB_STATUS.PENDING
        self.result = None
        self.error = None
        self.traceback = None
        self.timeout = 30
        self.max_retries = 3
        self.kwargs = {"test": "value"}
        self.dependency_ids = []
        self._start_time = time.time()
        
    def serialize(self):
        return {
            "job_id": self.job_id,
            "queue_name": self.queue_name,
            "function_name": self.function_name,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "traceback": self.traceback,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "kwargs": self.kwargs,
            "dependency_ids": self.dependency_ids,
        }
        
    @classmethod
    def deserialize(cls, data):
        job = cls()
        job.job_id = data["job_id"]
        job.queue_name = data["queue_name"]
        job.function_name = data["function_name"]
        job.status = JOB_STATUS(data["status"])
        job.result = data.get("result")
        job.error = data.get("error")
        job.traceback = data.get("traceback")
        job.timeout = data.get("timeout", 30)
        job.max_retries = data.get("max_retries", 3)
        job.kwargs = data.get("kwargs", {})
        job.dependency_ids = data.get("dependency_ids", [])
        return job
        
    async def execute(self):
        self._start_time = time.time()
        self.status = JOB_STATUS.RUNNING
        
        # Simulate work
        await asyncio.sleep(0.1)
        
        # Simulate successful completion
        self.result = "Job completed successfully"
        self.status = JOB_STATUS.COMPLETED
        
    def get_retry_delay(self, attempt):
        # Simple exponential backoff
        return min(2 ** attempt, 300)


class MockMessage:
    """Mock NATS message for testing."""
    
    def __init__(self, subject="naq.queue.test-queue", data=None):
        self.subject = subject
        self.data = data or MockJob().serialize()
        self.ack_called = False
        self.nak_called = False
        self.term_called = False
        self.metadata = MagicMock()
        self.metadata.num_delivered = 1
        
    async def ack(self):
        self.ack_called = True
        
    async def nak(self, delay=None):
        self.nak_called = True
        
    async def term(self):
        self.term_called = True


class TestWorkerEventLogging:
    """Test cases for Worker event logging."""
    
    @pytest.mark.asyncio
    async def test_started_event_logged_when_job_begins(self):
        """Test that STARTED events are logged when jobs begin processing."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job and message
                job = MockJob()
                msg = MockMessage(data=job.serialize())
                
                # Process the message
                await worker.process_message(msg)
                
                # Verify STARTED event was logged
                started_events = [e for e in mock_storage.events if e.event_type == JobEventType.STARTED]
                assert len(started_events) == 1
                
                started_event = started_events[0]
                assert started_event.job_id == job.job_id
                assert started_event.worker_id == worker.worker_id
                assert started_event.queue_name == job.queue_name
                
                # Verify event details
                assert started_event.details is not None
                assert started_event.details["function_name"] == job.function_name
                assert started_event.details["job_timeout"] == job.timeout
                assert started_event.details["max_retries"] == job.max_retries
                assert started_event.details["job_kwargs"] == job.kwargs
                assert started_event.details["dependency_ids"] == job.dependency_ids
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_completed_event_logged_when_job_finishes(self):
        """Test that COMPLETED events are logged when jobs finish successfully."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job and message
                job = MockJob()
                msg = MockMessage(data=job.serialize())
                
                # Process the message
                await worker.process_message(msg)
                
                # Verify COMPLETED event was logged
                completed_events = [e for e in mock_storage.events if e.event_type == JobEventType.COMPLETED]
                assert len(completed_events) == 1
                
                completed_event = completed_events[0]
                assert completed_event.job_id == job.job_id
                assert completed_event.worker_id == worker.worker_id
                assert completed_event.queue_name == job.queue_name
                
                # Verify event details
                assert completed_event.details is not None
                assert completed_event.details["function_name"] == job.function_name
                assert completed_event.details["job_timeout"] == job.timeout
                assert completed_event.details["max_retries"] == job.max_retries
                assert completed_event.details["job_kwargs"] == job.kwargs
                assert completed_event.details["dependency_ids"] == job.dependency_ids
                assert "duration_ms" in completed_event.details
                assert completed_event.details["result_type"] == "str"  # job.result is a string
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_failed_event_logged_when_job_fails(self):
        """Test that FAILED events are logged when jobs fail."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job that will fail
                job = MockJob()
                
                # Override execute to simulate failure
                async def failing_execute():
                    job._start_time = time.time()
                    job.status = JOB_STATUS.RUNNING
                    await asyncio.sleep(0.1)
                    raise ValueError("Simulated job failure")
                
                job.execute = failing_execute
                
                msg = MockMessage(data=job.serialize())
                
                # Process the message - should fail and log FAILED event
                await worker.process_message(msg)
                
                # Verify FAILED event was logged
                failed_events = [e for e in mock_storage.events if e.event_type == JobEventType.FAILED]
                assert len(failed_events) == 1
                
                failed_event = failed_events[0]
                assert failed_event.job_id == job.job_id
                assert failed_event.worker_id == worker.worker_id
                assert failed_event.queue_name == job.queue_name
                
                # Verify event details
                assert failed_event.details is not None
                assert failed_event.details["function_name"] == job.function_name
                assert failed_event.details["job_timeout"] == job.timeout
                assert failed_event.details["max_retries"] == job.max_retries
                assert failed_event.details["job_kwargs"] == job.kwargs
                assert failed_event.details["dependency_ids"] == job.dependency_ids
                assert "duration_ms" in failed_event.details
                assert failed_event.details["error_type"] == "ValueError"
                assert failed_event.details["error_message"] == "Simulated job failure"
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_retry_scheduled_event_logged_when_job_retried(self):
        """Test that RETRY_SCHEDULED events are logged when jobs are retried."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job that will fail but can be retried
                job = MockJob()
                job.max_retries = 2
                
                # Override execute to simulate failure
                async def failing_execute():
                    job._start_time = time.time()
                    job.status = JOB_STATUS.RUNNING
                    await asyncio.sleep(0.1)
                    raise ValueError("Simulated job failure")
                
                job.execute = failing_execute
                
                msg = MockMessage(data=job.serialize())
                
                # Mock message metadata to simulate first delivery
                msg.metadata.num_delivered = 1
                
                # Process the message - should fail and schedule retry
                # We need to call the private method with proper typing
                await worker._handle_job_execution_error(job, msg)  # type: ignore
                
                # Verify RETRY_SCHEDULED event was logged
                retry_events = [e for e in mock_storage.events if e.event_type == JobEventType.RETRY_SCHEDULED]
                assert len(retry_events) == 1
                
                retry_event = retry_events[0]
                assert retry_event.job_id == job.job_id
                assert retry_event.worker_id == worker.worker_id
                assert retry_event.queue_name == job.queue_name
                
                # Verify event details
                assert retry_event.details is not None
                assert retry_event.details["function_name"] == job.function_name
                assert retry_event.details["job_timeout"] == job.timeout
                assert retry_event.details["max_retries"] == job.max_retries
                assert retry_event.details["job_kwargs"] == job.kwargs
                assert retry_event.details["dependency_ids"] == job.dependency_ids
                assert "delay_seconds" in retry_event.details
                assert retry_event.details["retry_attempt"] == 1
                assert retry_event.details["error_message"] == "Simulated job failure"
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_all_events_include_proper_metadata(self):
        """Test that all events include proper metadata."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage and logger
                mock_storage = MockStorage()
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job and message
                job = MockJob()
                msg = MockMessage(data=job.serialize())
                
                # Process the message
                await worker.process_message(msg)
                
                # Verify all events have proper metadata
                for event in mock_storage.events:
                    # Basic metadata
                    assert event.job_id == job.job_id
                    assert event.worker_id == worker.worker_id
                    assert event.queue_name == job.queue_name
                    
                    # All events should have details
                    assert event.details is not None
                    assert isinstance(event.details, dict)
                    
                    # Common details should be present
                    assert "function_name" in event.details
                    assert "job_timeout" in event.details
                    assert "max_retries" in event.details
                    assert "job_kwargs" in event.details
                    assert "dependency_ids" in event.details
                    
                    # Verify values
                    assert event.details["function_name"] == job.function_name
                    assert event.details["job_timeout"] == job.timeout
                    assert event.details["max_retries"] == job.max_retries
                    assert event.details["job_kwargs"] == job.kwargs
                    assert event.details["dependency_ids"] == job.dependency_ids
                    
                    # Event-specific details
                    if event.event_type == JobEventType.STARTED:
                        # Started events should not have duration yet
                        assert "duration_ms" not in event.details
                    elif event.event_type == JobEventType.COMPLETED:
                        # Completed events should have duration
                        assert "duration_ms" in event.details
                        assert event.details["duration_ms"] > 0
                        assert "result_type" in event.details
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_event_logging_with_disabled_logger(self):
        """Test that worker continues to function when event logging is disabled."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Configure shared logger as disabled
                configure_shared_logger(enabled=False)
                
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job and message
                job = MockJob()
                msg = MockMessage(data=job.serialize())
                
                # Process the message - should not raise any errors
                await worker.process_message(msg)
                
                # Verify message was acknowledged
                assert msg.ack_called
                
                # Verify no events were logged (logger was disabled)
                event_logger = await worker._get_event_logger()
                assert event_logger is None
        finally:
            # Clean up
            await manager.async_reset()

    @pytest.mark.asyncio
    async def test_event_logging_with_storage_failure(self):
        """Test that worker continues to function when event storage fails."""
        # Reset shared logger manager
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        try:
            with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
                 patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class, \
                 patch('naq.worker.get_nats_connection') as mock_get_conn, \
                 patch('naq.worker.get_jetstream_context') as mock_get_js:
                
                # Mock the storage to fail
                mock_storage = MockStorage()
                mock_storage.fail_next = True
                mock_logger = AsyncJobEventLogger(mock_storage, batch_size=1, flush_interval=0.1)
                
                mock_storage_class.return_value = mock_storage
                mock_logger_class.return_value = mock_logger
                mock_get_conn.return_value = AsyncMock()
                mock_get_js.return_value = AsyncMock()
                
                # Configure shared logger
                configure_shared_logger(enabled=True)
                
                # Create worker
                worker = Worker(queues=["test-queue"], worker_name="test-worker")
                
                # Create mock job and message
                job = MockJob()
                msg = MockMessage(data=job.serialize())
                
                # Process the message - should not raise any errors despite storage failure
                await worker.process_message(msg)
                
                # Verify message was acknowledged
                assert msg.ack_called
                
                # Verify no events were stored (storage failed)
                assert len(mock_storage.events) == 0
        finally:
            # Clean up
            await manager.async_reset()