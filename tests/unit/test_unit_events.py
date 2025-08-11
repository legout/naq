# tests/unit/test_unit_events.py
"""Unit tests for event models and functionality."""

import os
import socket
import time
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from naq.events import (
    AsyncJobEventLogger, 
    JobEvent, 
    JobEventType, 
    WorkerEvent, 
    WorkerEventType
)
from naq.events.storage import NATSJobEventStorage
from naq.models import JOB_STATUS


class TestJobEvent:
    """Test JobEvent model and factory methods."""

    def test_job_event_creation(self):
        """Test basic JobEvent creation."""
        job_id = "test-job-123"
        event_type = JobEventType.ENQUEUED
        queue_name = "test-queue"
        
        event = JobEvent(
            job_id=job_id,
            event_type=event_type,
            queue_name=queue_name,
            message="Test message"
        )
        
        assert event.job_id == job_id
        assert event.event_type == event_type
        assert event.queue_name == queue_name
        assert event.message == "Test message"
        assert event.timestamp > 0

    def test_job_event_enqueued_factory(self):
        """Test JobEvent.enqueued factory method."""
        job_id = "test-job-456"
        queue_name = "test-queue"
        
        event = JobEvent.enqueued(job_id=job_id, queue_name=queue_name)
        
        assert event.job_id == job_id
        assert event.event_type == JobEventType.ENQUEUED
        assert event.queue_name == queue_name
        assert "enqueued" in event.message.lower()

    def test_job_event_started_factory(self):
        """Test JobEvent.started factory method."""
        job_id = "test-job-789"
        worker_id = "test-worker-1"
        queue_name = "test-queue"
        
        event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id, 
            queue_name=queue_name
        )
        
        assert event.job_id == job_id
        assert event.event_type == JobEventType.STARTED
        assert event.worker_id == worker_id
        assert event.queue_name == queue_name
        assert "started" in event.message.lower()

    def test_job_event_completed_factory(self):
        """Test JobEvent.completed factory method."""
        job_id = "test-job-completed"
        worker_id = "test-worker-2"
        duration_ms = 1500
        
        event = JobEvent.completed(
            job_id=job_id,
            worker_id=worker_id,
            duration_ms=duration_ms,
            result="test result"
        )
        
        assert event.job_id == job_id
        assert event.event_type == JobEventType.COMPLETED
        assert event.worker_id == worker_id
        assert event.duration_ms == duration_ms
        assert event.details["result"] == "test result"

    def test_job_event_failed_factory(self):
        """Test JobEvent.failed factory method.""" 
        job_id = "test-job-failed"
        worker_id = "test-worker-3"
        error_type = "ValueError"
        error_message = "Test error"
        
        event = JobEvent.failed(
            job_id=job_id,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message
        )
        
        assert event.job_id == job_id
        assert event.event_type == JobEventType.FAILED
        assert event.worker_id == worker_id
        assert event.error_type == error_type
        assert event.error_message == error_message

    def test_job_event_scheduled_factory(self):
        """Test JobEvent.scheduled factory method."""
        job_id = "test-job-scheduled"
        queue_name = "test-queue"
        scheduled_timestamp = time.time() + 3600
        
        event = JobEvent.scheduled(
            job_id=job_id,
            queue_name=queue_name,
            scheduled_timestamp=scheduled_timestamp,
            cron="0 */6 * * *"
        )
        
        assert event.job_id == job_id
        assert event.event_type == JobEventType.SCHEDULED
        assert event.queue_name == queue_name
        assert event.details["scheduled_timestamp"] == scheduled_timestamp
        assert event.details["cron"] == "0 */6 * * *"

    def test_schedule_management_events(self):
        """Test schedule management event factory methods."""
        job_id = "test-schedule-job"
        queue_name = "test-queue"
        
        # Test schedule paused
        paused_event = JobEvent.schedule_paused(job_id=job_id, queue_name=queue_name)
        assert paused_event.event_type == JobEventType.SCHEDULE_PAUSED
        assert "paused" in paused_event.message.lower()
        
        # Test schedule resumed  
        resumed_event = JobEvent.schedule_resumed(job_id=job_id, queue_name=queue_name)
        assert resumed_event.event_type == JobEventType.SCHEDULE_RESUMED
        assert "resumed" in resumed_event.message.lower()
        
        # Test schedule cancelled
        cancelled_event = JobEvent.schedule_cancelled(job_id=job_id, queue_name=queue_name)
        assert cancelled_event.event_type == JobEventType.SCHEDULE_CANCELLED
        assert "cancelled" in cancelled_event.message.lower()
        
        # Test schedule modified
        modifications = {"cron": "0 */8 * * *", "repeat": 5}
        modified_event = JobEvent.schedule_modified(
            job_id=job_id,
            queue_name=queue_name,
            modifications=modifications
        )
        assert modified_event.event_type == JobEventType.SCHEDULE_MODIFIED
        assert modified_event.details["modifications"] == modifications

    def test_job_event_to_dict(self):
        """Test JobEvent to_dict conversion."""
        event = JobEvent.enqueued(job_id="test", queue_name="queue")
        event_dict = event.to_dict()
        
        assert isinstance(event_dict, dict)
        assert event_dict["job_id"] == "test"
        assert event_dict["event_type"] == "enqueued"
        assert event_dict["queue_name"] == "queue"
        assert "timestamp" in event_dict


class TestWorkerEvent:
    """Test WorkerEvent model and factory methods."""

    def test_worker_event_creation(self):
        """Test basic WorkerEvent creation."""
        worker_id = "test-worker-123"
        event_type = WorkerEventType.WORKER_STARTED
        hostname = socket.gethostname()
        pid = os.getpid()
        
        event = WorkerEvent(
            worker_id=worker_id,
            event_type=event_type,
            hostname=hostname,
            pid=pid,
            message="Test worker message"
        )
        
        assert event.worker_id == worker_id
        assert event.event_type == event_type
        assert event.hostname == hostname
        assert event.pid == pid
        assert event.message == "Test worker message"
        assert event.timestamp > 0

    def test_worker_started_factory(self):
        """Test WorkerEvent.worker_started factory method."""
        worker_id = "test-worker-456"
        hostname = "test-host"
        pid = 12345
        queue_names = ["queue1", "queue2"]
        concurrency_limit = 4
        
        event = WorkerEvent.worker_started(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            queue_names=queue_names,
            concurrency_limit=concurrency_limit
        )
        
        assert event.worker_id == worker_id
        assert event.event_type == WorkerEventType.WORKER_STARTED
        assert event.hostname == hostname
        assert event.pid == pid
        assert event.queue_names == queue_names
        assert event.concurrency_limit == concurrency_limit
        assert "started" in event.message.lower()

    def test_worker_stopped_factory(self):
        """Test WorkerEvent.worker_stopped factory method."""
        worker_id = "test-worker-789"
        hostname = "test-host"
        pid = 12345
        
        event = WorkerEvent.worker_stopped(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid
        )
        
        assert event.worker_id == worker_id
        assert event.event_type == WorkerEventType.WORKER_STOPPED
        assert event.hostname == hostname
        assert event.pid == pid
        assert "stopped" in event.message.lower()

    def test_worker_status_events(self):
        """Test worker status event factory methods."""
        worker_id = "test-worker-status"
        hostname = "test-host"  
        pid = 12345
        job_id = "current-job-123"
        
        # Test worker idle
        idle_event = WorkerEvent.worker_idle(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid
        )
        assert idle_event.event_type == WorkerEventType.WORKER_IDLE
        assert "idle" in idle_event.message.lower()
        
        # Test worker busy
        busy_event = WorkerEvent.worker_busy(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            current_job_id=job_id
        )
        assert busy_event.event_type == WorkerEventType.WORKER_BUSY
        assert busy_event.current_job_id == job_id
        assert "busy" in busy_event.message.lower()

    def test_worker_heartbeat_factory(self):
        """Test WorkerEvent.worker_heartbeat factory method."""
        worker_id = "test-worker-heartbeat"
        hostname = "test-host"
        pid = 12345
        active_jobs = 2
        concurrency_limit = 4
        current_job_id = "job-456"
        
        event = WorkerEvent.worker_heartbeat(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            active_jobs=active_jobs,
            concurrency_limit=concurrency_limit,
            current_job_id=current_job_id
        )
        
        assert event.worker_id == worker_id
        assert event.event_type == WorkerEventType.WORKER_HEARTBEAT
        assert event.active_jobs == active_jobs
        assert event.concurrency_limit == concurrency_limit
        assert event.current_job_id == current_job_id
        assert "heartbeat" in event.message.lower()

    def test_worker_error_factory(self):
        """Test WorkerEvent.worker_error factory method."""
        worker_id = "test-worker-error"
        hostname = "test-host"
        pid = 12345
        error_type = "ConnectionError"
        error_message = "Failed to connect to NATS"
        
        event = WorkerEvent.worker_error(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            error_type=error_type,
            error_message=error_message
        )
        
        assert event.worker_id == worker_id
        assert event.event_type == WorkerEventType.WORKER_ERROR
        assert event.error_type == error_type
        assert event.error_message == error_message
        assert "error" in event.message.lower()

    def test_worker_event_to_dict(self):
        """Test WorkerEvent to_dict conversion."""
        event = WorkerEvent.worker_started(
            worker_id="test",
            hostname="host",
            pid=123,
            queue_names=["q1"],
            concurrency_limit=2
        )
        event_dict = event.to_dict()
        
        assert isinstance(event_dict, dict)
        assert event_dict["worker_id"] == "test"
        assert event_dict["event_type"] == "worker_started"
        assert event_dict["hostname"] == "host" 
        assert event_dict["pid"] == 123
        assert "timestamp" in event_dict


class TestAsyncJobEventLogger:
    """Test AsyncJobEventLogger functionality."""

    @pytest.mark.asyncio
    async def test_event_logger_initialization(self):
        """Test event logger initialization."""
        with patch('naq.events.logger.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger(nats_url="nats://localhost:4222")
            
            # Should not be started initially
            assert not logger._started
            
            # Start logger
            await logger.start()
            assert logger._started
            
            # Stop logger
            await logger.stop()
            assert not logger._started

    @pytest.mark.asyncio
    async def test_job_event_logging_methods(self):
        """Test job event logging methods."""
        with patch('naq.events.logger.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger()
            await logger.start()
            
            try:
                # Test logging various job events
                await logger.log_job_enqueued("job1", "queue1")
                await logger.log_job_started("job1", "worker1", "queue1")
                await logger.log_job_completed("job1", "worker1", duration_ms=1000)
                await logger.log_job_failed("job1", "worker1", "Error", "Test error")
                await logger.log_job_scheduled("job2", "queue1", time.time())
                await logger.log_schedule_triggered("job2", "queue1")
                
                # Flush to ensure events are processed
                await logger.flush()
                
                # Verify events were logged (would need to check storage calls)
                
            finally:
                await logger.stop()

    @pytest.mark.asyncio
    async def test_worker_event_logging_methods(self):
        """Test worker event logging methods.""" 
        with patch('naq.events.logger.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger()
            await logger.start()
            
            try:
                # Test logging worker events
                await logger.log_worker_started(
                    "worker1", "host1", 123, ["queue1"], 4
                )
                await logger.log_worker_busy("worker1", "host1", 123, "job1")
                await logger.log_worker_idle("worker1", "host1", 123)
                await logger.log_worker_heartbeat(
                    "worker1", "host1", 123, 1, 4, "job1"
                )
                await logger.log_worker_stopped("worker1", "host1", 123)
                
                # Flush to ensure events are processed
                await logger.flush()
                
            finally:
                await logger.stop()

    @pytest.mark.asyncio
    async def test_schedule_management_event_logging(self):
        """Test schedule management event logging methods."""
        with patch('naq.events.logger.NATSJobEventStorage') as mock_storage_class:
            mock_storage = AsyncMock()
            mock_storage_class.return_value = mock_storage
            
            logger = AsyncJobEventLogger()
            await logger.start()
            
            try:
                # Test schedule management events
                await logger.log_schedule_paused("job1", "queue1")
                await logger.log_schedule_resumed("job1", "queue1") 
                await logger.log_schedule_cancelled("job1", "queue1")
                await logger.log_schedule_modified(
                    "job1", "queue1", {"cron": "0 */6 * * *"}
                )
                
                # Flush to ensure events are processed
                await logger.flush()
                
            finally:
                await logger.stop()


class TestEventTypes:
    """Test event type enums."""

    def test_job_event_type_enum(self):
        """Test JobEventType enum values."""
        assert JobEventType.ENQUEUED == "enqueued"
        assert JobEventType.STARTED == "started"
        assert JobEventType.COMPLETED == "completed"
        assert JobEventType.FAILED == "failed"
        assert JobEventType.RETRY_SCHEDULED == "retry_scheduled"
        assert JobEventType.SCHEDULED == "scheduled"
        assert JobEventType.SCHEDULE_TRIGGERED == "schedule_triggered"
        assert JobEventType.CANCELLED == "cancelled"
        assert JobEventType.PAUSED == "paused"
        assert JobEventType.RESUMED == "resumed"
        
        # New schedule management events
        assert JobEventType.SCHEDULE_PAUSED == "schedule_paused"
        assert JobEventType.SCHEDULE_RESUMED == "schedule_resumed"
        assert JobEventType.SCHEDULE_CANCELLED == "schedule_cancelled"
        assert JobEventType.SCHEDULE_MODIFIED == "schedule_modified"

    def test_worker_event_type_enum(self):
        """Test WorkerEventType enum values."""
        assert WorkerEventType.WORKER_STARTED == "worker_started"
        assert WorkerEventType.WORKER_STOPPED == "worker_stopped"
        assert WorkerEventType.WORKER_IDLE == "worker_idle"
        assert WorkerEventType.WORKER_BUSY == "worker_busy"
        assert WorkerEventType.WORKER_HEARTBEAT == "worker_heartbeat"
        assert WorkerEventType.WORKER_ERROR == "worker_error"

    def test_event_type_conversions(self):
        """Test event type string conversions."""
        # Test JobEventType
        assert JobEventType("enqueued") == JobEventType.ENQUEUED
        assert str(JobEventType.STARTED) == "started"
        
        # Test WorkerEventType  
        assert WorkerEventType("worker_started") == WorkerEventType.WORKER_STARTED
        assert str(WorkerEventType.WORKER_BUSY) == "worker_busy"

    def test_invalid_event_types(self):
        """Test invalid event type handling."""
        with pytest.raises(ValueError):
            JobEventType("invalid_event_type")
            
        with pytest.raises(ValueError):
            WorkerEventType("invalid_worker_event")