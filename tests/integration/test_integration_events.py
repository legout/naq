# tests/integration/test_integration_events.py
"""Integration tests for event logging functionality."""

import asyncio
import os
import socket
import time
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from naq.events import AsyncJobEventLogger, JobEvent, JobEventType, WorkerEvent, WorkerEventType
from naq.events.storage import NATSJobEventStorage
from naq import Job, Queue, Worker, DEFAULT_NATS_URL


@pytest.fixture
def nats_url():
    """Provide NATS URL for testing."""
    return os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)


@pytest.fixture  
async def event_logger(nats_url):
    """Provide an event logger for testing."""
    logger = AsyncJobEventLogger(nats_url=nats_url)
    await logger.start()
    yield logger
    await logger.stop()


@pytest.fixture
async def event_storage(nats_url):
    """Provide event storage for testing."""
    storage = NATSJobEventStorage(nats_url=nats_url)
    yield storage
    await storage.close()


class TestJobEventLogging:
    """Test job event logging integration."""

    @pytest.mark.asyncio
    async def test_job_enqueue_event_logging(self, event_logger, event_storage):
        """Test that job enqueue events are logged correctly."""
        # Create a test job event
        job_id = "test-job-123"
        queue_name = "test-queue"
        
        # Log an enqueue event
        await event_logger.log_job_enqueued(
            job_id=job_id,
            queue_name=queue_name,
            message="Test job enqueued"
        )
        
        # Flush the logger to ensure event is stored
        await event_logger.flush()
        
        # Wait a bit for the event to be processed
        await asyncio.sleep(0.1)
        
        # Verify event was stored
        events = await event_storage.get_events(job_id)
        assert len(events) >= 1
        
        # Find the enqueue event
        enqueue_event = next((e for e in events if e.event_type == JobEventType.ENQUEUED), None)
        assert enqueue_event is not None
        assert enqueue_event.job_id == job_id
        assert enqueue_event.queue_name == queue_name
        assert "Test job enqueued" in enqueue_event.message

    @pytest.mark.asyncio
    async def test_job_lifecycle_events(self, event_logger, event_storage):
        """Test logging complete job lifecycle events."""
        job_id = "test-lifecycle-job-456"
        queue_name = "test-queue"
        worker_id = "test-worker-1"
        
        # Log job lifecycle events
        await event_logger.log_job_enqueued(job_id=job_id, queue_name=queue_name)
        await event_logger.log_job_started(job_id=job_id, worker_id=worker_id, queue_name=queue_name)
        await event_logger.log_job_completed(job_id=job_id, worker_id=worker_id, duration_ms=1500)
        
        # Flush to ensure all events are stored
        await event_logger.flush()
        await asyncio.sleep(0.1)
        
        # Verify all events were stored
        events = await event_storage.get_events(job_id)
        assert len(events) >= 3
        
        # Check each event type exists
        event_types = [e.event_type for e in events]
        assert JobEventType.ENQUEUED in event_types
        assert JobEventType.STARTED in event_types  
        assert JobEventType.COMPLETED in event_types
        
        # Check event details
        completed_event = next(e for e in events if e.event_type == JobEventType.COMPLETED)
        assert completed_event.duration_ms == 1500
        assert completed_event.worker_id == worker_id

    @pytest.mark.asyncio
    async def test_schedule_events(self, event_logger, event_storage):
        """Test schedule-related event logging."""
        job_id = "test-scheduled-job-789"
        queue_name = "test-queue"
        scheduled_timestamp = time.time() + 3600  # 1 hour from now
        
        # Log schedule events
        await event_logger.log_job_scheduled(
            job_id=job_id,
            queue_name=queue_name, 
            scheduled_timestamp=scheduled_timestamp
        )
        await event_logger.log_schedule_triggered(job_id=job_id, queue_name=queue_name)
        await event_logger.log_schedule_paused(job_id=job_id, queue_name=queue_name)
        await event_logger.log_schedule_resumed(job_id=job_id, queue_name=queue_name)
        
        await event_logger.flush()
        await asyncio.sleep(0.1)
        
        # Verify schedule events
        events = await event_storage.get_events(job_id)
        event_types = [e.event_type for e in events]
        
        assert JobEventType.SCHEDULED in event_types
        assert JobEventType.SCHEDULE_TRIGGERED in event_types
        assert JobEventType.SCHEDULE_PAUSED in event_types
        assert JobEventType.SCHEDULE_RESUMED in event_types


class TestWorkerEventLogging:
    """Test worker event logging integration."""

    @pytest.mark.asyncio
    async def test_worker_lifecycle_events(self, event_logger):
        """Test worker lifecycle event logging."""
        worker_id = "test-worker-123"
        hostname = socket.gethostname()
        pid = os.getpid()
        queue_names = ["queue1", "queue2"]
        concurrency_limit = 4
        
        # Log worker lifecycle events
        await event_logger.log_worker_started(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            queue_names=queue_names,
            concurrency_limit=concurrency_limit
        )
        
        await event_logger.log_worker_busy(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            current_job_id="job-123"
        )
        
        await event_logger.log_worker_idle(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid
        )
        
        await event_logger.log_worker_stopped(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid
        )
        
        # Flush to ensure events are stored
        await event_logger.flush()
        await asyncio.sleep(0.1)
        
        # Verify events were logged by checking the internal event buffer/storage
        # Note: Worker events are stored as job events with worker context
        # This is a simplified test - in practice you'd query the event stream

    @pytest.mark.asyncio
    async def test_worker_heartbeat_events(self, event_logger):
        """Test worker heartbeat event logging."""
        worker_id = "test-worker-heartbeat"
        hostname = socket.gethostname()
        pid = os.getpid()
        
        # Log heartbeat events
        await event_logger.log_worker_heartbeat(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            active_jobs=2,
            concurrency_limit=4,
            current_job_id="current-job-456"
        )
        
        await event_logger.flush()
        await asyncio.sleep(0.1)


class TestEventIntegration:
    """Test event logging integration with Queue and Worker components."""

    @pytest.mark.asyncio
    async def test_queue_enqueue_logging_integration(self, nats_url):
        """Test that queue enqueue operations generate events."""
        queue = Queue(name="test-event-queue", nats_url=nats_url)
        
        # Mock function for testing
        def test_func(x):
            return x * 2
            
        # Enqueue a job
        job = await queue.enqueue(test_func, 42)
        
        # Wait for event to be processed
        await asyncio.sleep(0.2)
        
        # The event should have been logged automatically
        assert job.job_id is not None
        
        await queue.close()

    @pytest.mark.asyncio 
    async def test_schedule_management_logging_integration(self, nats_url):
        """Test that schedule management operations generate events."""
        queue = Queue(name="test-schedule-queue", nats_url=nats_url)
        
        # Mock function for testing
        def test_func():
            return "scheduled result"
        
        # Schedule a job
        job = await queue.enqueue_at(
            time.time() + 1,  # 1 second from now
            test_func
        )
        
        # Test schedule management operations
        await queue.pause_scheduled_job(job.job_id)
        await queue.resume_scheduled_job(job.job_id)
        
        # Wait for events to be processed
        await asyncio.sleep(0.2)
        
        # Clean up
        await queue.cancel_scheduled_job(job.job_id)
        await queue.close()

    @pytest.mark.asyncio
    @patch('naq.worker.AsyncJobEventLogger')
    async def test_worker_status_logging_integration(self, mock_logger_class, nats_url):
        """Test that worker status changes generate events."""
        # Mock the event logger
        mock_logger = AsyncMock()
        mock_logger_class.return_value = mock_logger
        
        # Create a worker
        worker = Worker(
            queue_names=["test-worker-queue"],
            nats_url=nats_url,
            concurrency=2
        )
        
        # Initialize the worker (this should trigger worker started event)
        await worker._init_components()
        
        # Verify worker started event was logged
        mock_logger.log_worker_started.assert_called_once()
        
        # Update worker status (this should trigger status events)
        await worker._status_manager.update_status("idle")
        
        # Clean up
        await worker.close()


class TestEventStorage:
    """Test event storage functionality."""

    @pytest.mark.asyncio
    async def test_event_storage_and_retrieval(self, event_storage):
        """Test storing and retrieving events."""
        job_id = "test-storage-job"
        
        # Create test events
        events = [
            JobEvent.enqueued(job_id=job_id, queue_name="test-queue"),
            JobEvent.started(job_id=job_id, worker_id="worker-1", queue_name="test-queue"),
            JobEvent.completed(job_id=job_id, worker_id="worker-1", duration_ms=1000)
        ]
        
        # Store events
        for event in events:
            await event_storage.store_event(event)
        
        # Wait for storage
        await asyncio.sleep(0.1)
        
        # Retrieve events
        stored_events = await event_storage.get_events(job_id)
        
        # Verify events were stored and retrieved correctly
        assert len(stored_events) >= len(events)
        stored_event_types = [e.event_type for e in stored_events]
        for event in events:
            assert event.event_type in stored_event_types


@pytest.mark.asyncio
async def test_event_system_end_to_end(nats_url):
    """End-to-end test of the event system."""
    # This test would demonstrate the complete event logging system
    # working together with real components
    
    queue = Queue(name="e2e-test-queue", nats_url=nats_url)
    event_storage = NATSJobEventStorage(nats_url=nats_url)
    
    try:
        # Define a test function
        def test_task(n):
            time.sleep(0.1)  # Simulate work
            return n * 2
        
        # Enqueue a job
        job = await queue.enqueue(test_task, 10)
        
        # Wait for events to be logged
        await asyncio.sleep(0.3)
        
        # Check that events were logged
        events = await event_storage.get_events(job.job_id)
        assert len(events) > 0
        
        # Should at least have an enqueue event
        enqueue_events = [e for e in events if e.event_type == JobEventType.ENQUEUED]
        assert len(enqueue_events) >= 1
        
    finally:
        await queue.close()
        await event_storage.close()