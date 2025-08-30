"""
Unit tests for JobEvent and WorkerEvent models.

This module contains comprehensive tests for the JobEvent and WorkerEvent classes,
including creation, serialization, deserialization, and factory method functionality.
"""

import time
from typing import Any, Dict, Optional

import msgspec
import pytest

from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType


class TestJobEvent:
    """Test cases for the JobEvent class."""

    def test_job_event_creation_minimal(self) -> None:
        """Test minimal JobEvent creation."""
        job_id = "test-job-123"
        event_type = JobEventType.STARTED

        job_event = JobEvent(job_id=job_id, event_type=event_type)

        assert job_event.job_id == job_id
        assert job_event.event_type == event_type
        assert job_event.timestamp > 0
        assert job_event.worker_id is None
        assert job_event.queue_name is None
        assert job_event.message is None
        assert job_event.details is None
        assert job_event.error_type is None
        assert job_event.error_message is None
        assert job_event.duration_ms is None
        assert job_event.nats_subject is None
        assert job_event.nats_sequence is None

    def test_job_event_creation_with_all_params(self) -> None:
        """Test JobEvent creation with all parameters."""
        job_id = "test-job-123"
        event_type = JobEventType.COMPLETED
        timestamp = 1625097600.0
        worker_id = "worker-1"
        queue_name = "test-queue"
        message = "Job completed successfully"
        details = {"result": "success", "output": 42}
        error_type = None
        error_message = None
        duration_ms = 1500.0
        nats_subject = "naq.jobs.test"
        nats_sequence = 123

        job_event = JobEvent(
            job_id=job_id,
            event_type=event_type,
            timestamp=timestamp,
            worker_id=worker_id,
            queue_name=queue_name,
            message=message,
            details=details,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == event_type
        assert job_event.timestamp == timestamp
        assert job_event.worker_id == worker_id
        assert job_event.queue_name == queue_name
        assert job_event.message == message
        assert job_event.details == details
        assert job_event.error_type == error_type
        assert job_event.error_message == error_message
        assert job_event.duration_ms == duration_ms
        assert job_event.nats_subject == nats_subject
        assert job_event.nats_sequence == nats_sequence

    def test_job_event_creation_with_error(self) -> None:
        """Test JobEvent creation with error information."""
        job_id = "test-job-123"
        event_type = JobEventType.FAILED
        error_type = "ValueError"
        error_message = "Invalid input data"
        duration_ms = 500.0

        job_event = JobEvent(
            job_id=job_id,
            event_type=event_type,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == event_type
        assert job_event.error_type == error_type
        assert job_event.error_message == error_message
        assert job_event.duration_ms == duration_ms

    def test_job_event_enqueued_factory_method(self) -> None:
        """Test JobEvent.enqueued factory method."""
        job_id = "test-job-123"
        queue_name = "test-queue"
        worker_id = "worker-1"
        nats_subject = "naq.jobs.test"
        nats_sequence = 123
        details = {"priority": "high"}

        job_event = JobEvent.enqueued(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.ENQUEUED
        assert job_event.queue_name == queue_name
        assert job_event.worker_id == worker_id
        assert job_event.nats_subject == nats_subject
        assert job_event.nats_sequence == nats_sequence
        assert job_event.details == details
        assert job_event.message is None

    def test_job_event_started_factory_method(self) -> None:
        """Test JobEvent.started factory method."""
        job_id = "test-job-123"
        worker_id = "worker-1"
        queue_name = "test-queue"
        details = {"timeout": 30}

        job_event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.STARTED
        assert job_event.worker_id == worker_id
        assert job_event.queue_name == queue_name
        assert job_event.details == details

    def test_job_event_completed_factory_method(self) -> None:
        """Test JobEvent.completed factory method."""
        job_id = "test-job-123"
        worker_id = "worker-1"
        duration_ms = 1500.0
        queue_name = "test-queue"
        details = {"result": "success"}

        job_event = JobEvent.completed(
            job_id=job_id,
            worker_id=worker_id,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.COMPLETED
        assert job_event.worker_id == worker_id
        assert job_event.duration_ms == duration_ms
        assert job_event.queue_name == queue_name
        assert job_event.details["duration_ms"] == duration_ms
        assert job_event.details["result"] == "success"

    def test_job_event_failed_factory_method(self) -> None:
        """Test JobEvent.failed factory method."""
        job_id = "test-job-123"
        worker_id = "worker-1"
        error_type = "ValueError"
        error_message = "Invalid input data"
        duration_ms = 500.0
        queue_name = "test-queue"
        details = {"input": "invalid"}

        job_event = JobEvent.failed(
            job_id=job_id,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.FAILED
        assert job_event.worker_id == worker_id
        assert job_event.error_type == error_type
        assert job_event.error_message == error_message
        assert job_event.duration_ms == duration_ms
        assert job_event.queue_name == queue_name
        assert job_event.details["duration_ms"] == duration_ms
        assert job_event.details["error_type"] == error_type
        assert job_event.details["error_message"] == error_message
        assert job_event.details["input"] == "invalid"

    def test_job_event_retry_scheduled_factory_method(self) -> None:
        """Test JobEvent.retry_scheduled factory method."""
        job_id = "test-job-123"
        worker_id = "worker-1"
        delay_seconds = 30.0
        queue_name = "test-queue"
        details = {"retry_count": 2}

        job_event = JobEvent.retry_scheduled(
            job_id=job_id,
            worker_id=worker_id,
            delay_seconds=delay_seconds,
            queue_name=queue_name,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.RETRY_SCHEDULED
        assert job_event.worker_id == worker_id
        assert job_event.message == f"Retry scheduled in {delay_seconds} seconds"
        assert job_event.queue_name == queue_name
        assert job_event.details["delay_seconds"] == delay_seconds
        assert job_event.details["retry_count"] == 2

    def test_job_event_scheduled_factory_method(self) -> None:
        """Test JobEvent.scheduled factory method."""
        job_id = "test-job-123"
        queue_name = "test-queue"
        scheduled_timestamp_utc = 1625097600.0
        worker_id = "scheduler-1"
        details = {"cron": "0 0 * * *"}

        job_event = JobEvent.scheduled(
            job_id=job_id,
            queue_name=queue_name,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            worker_id=worker_id,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.SCHEDULED
        assert job_event.queue_name == queue_name
        assert job_event.worker_id == worker_id
        assert job_event.details["scheduled_timestamp_utc"] == scheduled_timestamp_utc
        assert job_event.details["cron"] == "0 0 * * *"

    def test_job_event_schedule_triggered_factory_method(self) -> None:
        """Test JobEvent.schedule_triggered factory method."""
        job_id = "test-job-123"
        queue_name = "test-queue"
        worker_id = "scheduler-1"
        details = {"trigger_time": 1625097600.0}

        job_event = JobEvent.schedule_triggered(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.SCHEDULE_TRIGGERED
        assert job_event.queue_name == queue_name
        assert job_event.worker_id == worker_id
        assert job_event.details == details

    def test_job_event_cancelled_factory_method(self) -> None:
        """Test JobEvent.cancelled factory method."""
        job_id = "test-job-123"
        queue_name = "test-queue"
        worker_id = "admin-1"
        details = {"reason": "user request"}

        job_event = JobEvent.cancelled(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.CANCELLED
        assert job_event.queue_name == queue_name
        assert job_event.worker_id == worker_id
        assert job_event.message == "Job cancelled"
        assert job_event.details == details

    def test_job_event_status_changed_factory_method(self) -> None:
        """Test JobEvent.status_changed factory method."""
        job_id = "test-job-123"
        queue_name = "test-queue"
        old_status = "pending"
        new_status = "running"
        worker_id = "worker-1"
        details = {"reason": "timeout"}

        job_event = JobEvent.status_changed(
            job_id=job_id,
            queue_name=queue_name,
            old_status=old_status,
            new_status=new_status,
            worker_id=worker_id,
            details=details
        )

        assert job_event.job_id == job_id
        assert job_event.event_type == JobEventType.STATUS_CHANGED
        assert job_event.queue_name == queue_name
        assert job_event.worker_id == worker_id
        assert job_event.message == f"Job status changed from {old_status} to {new_status}"
        assert job_event.details["old_status"] == old_status
        assert job_event.details["new_status"] == new_status
        assert job_event.details["reason"] == "timeout"

    def test_job_event_serialization(self) -> None:
        """Test JobEvent serialization and deserialization."""
        job_event = JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.COMPLETED,
            worker_id="worker-1",
            duration_ms=1500.0,
            details={"result": "success"}
        )

        # Test msgspec serialization
        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(JobEvent)
        
        serialized = encoder.encode(job_event)
        deserialized = decoder.decode(serialized)
        
        assert deserialized.job_id == job_event.job_id
        assert deserialized.event_type == job_event.event_type
        assert deserialized.worker_id == job_event.worker_id
        assert deserialized.duration_ms == job_event.duration_ms
        assert deserialized.details == job_event.details

    def test_job_event_repr(self) -> None:
        """Test JobEvent.__repr__ method."""
        job_event = JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.STARTED,
            worker_id="worker-1"
        )
        
        repr_str = repr(job_event)
        
        assert "JobEvent" in repr_str
        assert "test-job-123" in repr_str
        assert "started" in repr_str

    def test_job_event_to_dict_minimal(self) -> None:
        """Test JobEvent.to_dict method with minimal parameters."""
        job_event = JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.STARTED
        )

        result = job_event.to_dict()

        assert result["job_id"] == "test-job-123"
        assert result["event_type"] == "started"
        assert "timestamp" in result
        assert result["timestamp"] > 0
        # None values should be filtered out
        assert "worker_id" not in result
        assert "queue_name" not in result
        assert "message" not in result
        assert "details" not in result
        assert "error_type" not in result
        assert "error_message" not in result
        assert "duration_ms" not in result
        assert "nats_subject" not in result
        assert "nats_sequence" not in result

    def test_job_event_to_dict_with_all_params(self) -> None:
        """Test JobEvent.to_dict method with all parameters."""
        job_event = JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.COMPLETED,
            timestamp=1625097600.0,
            worker_id="worker-1",
            queue_name="test-queue",
            message="Job completed successfully",
            details={"result": "success", "output": 42},
            error_type=None,
            error_message=None,
            duration_ms=1500.0,
            nats_subject="naq.jobs.test",
            nats_sequence=123
        )

        result = job_event.to_dict()

        assert result["job_id"] == "test-job-123"
        assert result["event_type"] == "completed"
        assert result["timestamp"] == 1625097600.0
        assert result["worker_id"] == "worker-1"
        assert result["queue_name"] == "test-queue"
        assert result["message"] == "Job completed successfully"
        assert result["details"] == {"result": "success", "output": 42}
        assert result["duration_ms"] == 1500.0
        assert result["nats_subject"] == "naq.jobs.test"
        assert result["nats_sequence"] == 123
        # None values should be filtered out
        assert "error_type" not in result
        assert "error_message" not in result

    def test_job_event_to_dict_with_error(self) -> None:
        """Test JobEvent.to_dict method with error information."""
        job_event = JobEvent(
            job_id="test-job-123",
            event_type=JobEventType.FAILED,
            error_type="ValueError",
            error_message="Invalid input data",
            duration_ms=500.0
        )

        result = job_event.to_dict()

        assert result["job_id"] == "test-job-123"
        assert result["event_type"] == "failed"
        assert result["error_type"] == "ValueError"
        assert result["error_message"] == "Invalid input data"
        assert result["duration_ms"] == 500.0
        # None values should be filtered out
        assert "worker_id" not in result
        assert "queue_name" not in result
        assert "message" not in result
        assert "details" not in result
        assert "nats_subject" not in result
        assert "nats_sequence" not in result

    def test_job_event_to_dict_with_factory_method(self) -> None:
        """Test JobEvent.to_dict method with event created by factory method."""
        job_event = JobEvent.completed(
            job_id="test-job-123",
            worker_id="worker-1",
            duration_ms=1500.0,
            queue_name="test-queue",
            details={"result": "success"}
        )

        result = job_event.to_dict()

        assert result["job_id"] == "test-job-123"
        assert result["event_type"] == "completed"
        assert result["worker_id"] == "worker-1"
        assert result["duration_ms"] == 1500.0
        assert result["queue_name"] == "test-queue"
        assert result["details"]["duration_ms"] == 1500.0
        assert result["details"]["result"] == "success"


class TestWorkerEvent:
    """Test cases for the WorkerEvent class."""

    def test_worker_event_creation_minimal(self) -> None:
        """Test minimal WorkerEvent creation."""
        worker_id = "test-worker-123"
        event_type = WorkerEventType.STARTED

        worker_event = WorkerEvent(worker_id=worker_id, event_type=event_type)

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == event_type
        assert worker_event.timestamp > 0
        assert worker_event.queue_names is None
        assert worker_event.message is None
        assert worker_event.details is None
        assert worker_event.job_id is None
        assert worker_event.duration_ms is None
        assert worker_event.cpu_usage is None
        assert worker_event.memory_usage is None

    def test_worker_event_creation_with_all_params(self) -> None:
        """Test WorkerEvent creation with all parameters."""
        worker_id = "test-worker-123"
        event_type = WorkerEventType.HEARTBEAT
        timestamp = 1625097600.0
        queue_names = ["queue1", "queue2"]
        message = "Worker heartbeat"
        details = {"status": "healthy"}
        job_id = "job-456"
        duration_ms = 100.0
        cpu_usage = 45.5
        memory_usage = 512.0

        worker_event = WorkerEvent(
            worker_id=worker_id,
            event_type=event_type,
            timestamp=timestamp,
            queue_names=queue_names,
            message=message,
            details=details,
            job_id=job_id,
            duration_ms=duration_ms,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == event_type
        assert worker_event.timestamp == timestamp
        assert worker_event.queue_names == queue_names
        assert worker_event.message == message
        assert worker_event.details == details
        assert worker_event.job_id == job_id
        assert worker_event.duration_ms == duration_ms
        assert worker_event.cpu_usage == cpu_usage
        assert worker_event.memory_usage == memory_usage

    def test_worker_event_started_factory_method(self) -> None:
        """Test WorkerEvent.started factory method."""
        worker_id = "test-worker-123"
        queue_names = ["queue1", "queue2"]
        details = {"concurrency": 4}

        worker_event = WorkerEvent.started(
            worker_id=worker_id,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.STARTED
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} started"
        assert worker_event.details == details

    def test_worker_event_stopped_factory_method(self) -> None:
        """Test WorkerEvent.stopped factory method."""
        worker_id = "test-worker-123"
        queue_names = ["queue1"]
        details = {"reason": "shutdown"}

        worker_event = WorkerEvent.stopped(
            worker_id=worker_id,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.STOPPED
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} stopped"
        assert worker_event.details == details

    def test_worker_event_heartbeat_factory_method(self) -> None:
        """Test WorkerEvent.heartbeat factory method."""
        worker_id = "test-worker-123"
        queue_names = ["queue1", "queue2"]
        cpu_usage = 45.5
        memory_usage = 512.0
        details = {"status": "healthy"}

        worker_event = WorkerEvent.heartbeat(
            worker_id=worker_id,
            queue_names=queue_names,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.HEARTBEAT
        assert worker_event.queue_names == queue_names
        assert worker_event.cpu_usage == cpu_usage
        assert worker_event.memory_usage == memory_usage
        assert worker_event.details == details

    def test_worker_event_job_started_factory_method(self) -> None:
        """Test WorkerEvent.job_started factory method."""
        worker_id = "test-worker-123"
        job_id = "job-456"
        queue_names = ["queue1"]
        details = {"timeout": 30}

        worker_event = WorkerEvent.job_started(
            worker_id=worker_id,
            job_id=job_id,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.JOB_STARTED
        assert worker_event.job_id == job_id
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} started job {job_id}"
        assert worker_event.details == details

    def test_worker_event_job_completed_factory_method(self) -> None:
        """Test WorkerEvent.job_completed factory method."""
        worker_id = "test-worker-123"
        job_id = "job-456"
        duration_ms = 1500.0
        queue_names = ["queue1"]
        details = {"result": "success"}

        worker_event = WorkerEvent.job_completed(
            worker_id=worker_id,
            job_id=job_id,
            duration_ms=duration_ms,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.JOB_COMPLETED
        assert worker_event.job_id == job_id
        assert worker_event.duration_ms == duration_ms
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} completed job {job_id}"
        assert worker_event.details["duration_ms"] == duration_ms
        assert worker_event.details["result"] == "success"

    def test_worker_event_job_failed_factory_method(self) -> None:
        """Test WorkerEvent.job_failed factory method."""
        worker_id = "test-worker-123"
        job_id = "job-456"
        duration_ms = 500.0
        queue_names = ["queue1"]
        details = {"error": "timeout"}

        worker_event = WorkerEvent.job_failed(
            worker_id=worker_id,
            job_id=job_id,
            duration_ms=duration_ms,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.JOB_FAILED
        assert worker_event.job_id == job_id
        assert worker_event.duration_ms == duration_ms
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} failed job {job_id}"
        assert worker_event.details["duration_ms"] == duration_ms
        assert worker_event.details["error"] == "timeout"

    def test_worker_event_paused_factory_method(self) -> None:
        """Test WorkerEvent.paused factory method."""
        worker_id = "test-worker-123"
        queue_names = ["queue1"]
        details = {"reason": "maintenance"}

        worker_event = WorkerEvent.paused(
            worker_id=worker_id,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.PAUSED
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} paused"
        assert worker_event.details == details

    def test_worker_event_resumed_factory_method(self) -> None:
        """Test WorkerEvent.resumed factory method."""
        worker_id = "test-worker-123"
        queue_names = ["queue1"]
        details = {"reason": "maintenance complete"}

        worker_event = WorkerEvent.resumed(
            worker_id=worker_id,
            queue_names=queue_names,
            details=details
        )

        assert worker_event.worker_id == worker_id
        assert worker_event.event_type == WorkerEventType.RESUMED
        assert worker_event.queue_names == queue_names
        assert worker_event.message == f"Worker {worker_id} resumed"
        assert worker_event.details == details

    def test_worker_event_serialization(self) -> None:
        """Test WorkerEvent serialization and deserialization."""
        worker_event = WorkerEvent(
            worker_id="test-worker-123",
            event_type=WorkerEventType.HEARTBEAT,
            queue_names=["queue1", "queue2"],
            cpu_usage=45.5,
            memory_usage=512.0,
            details={"status": "healthy"}
        )

        # Test msgspec serialization
        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder(WorkerEvent)
        
        serialized = encoder.encode(worker_event)
        deserialized = decoder.decode(serialized)
        
        assert deserialized.worker_id == worker_event.worker_id
        assert deserialized.event_type == worker_event.event_type
        assert deserialized.queue_names == worker_event.queue_names
        assert deserialized.cpu_usage == worker_event.cpu_usage
        assert deserialized.memory_usage == worker_event.memory_usage
        assert deserialized.details == worker_event.details

    def test_worker_event_repr(self) -> None:
        """Test WorkerEvent.__repr__ method."""
        worker_event = WorkerEvent(
            worker_id="test-worker-123",
            event_type=WorkerEventType.STARTED,
            queue_names=["queue1", "queue2"]
        )
        
        repr_str = repr(worker_event)
        
        assert "WorkerEvent" in repr_str
        assert "test-worker-123" in repr_str
        assert "started" in repr_str

    def test_worker_event_with_none_queue_names(self) -> None:
        """Test WorkerEvent with None queue_names defaults to empty list."""
        worker_event = WorkerEvent.started(worker_id="test-worker-123", queue_names=None)
        
        assert worker_event.queue_names == []

    def test_worker_event_with_none_details(self) -> None:
        """Test WorkerEvent with None details defaults to empty dict."""
        worker_event = WorkerEvent.started(worker_id="test-worker-123", details=None)
        
        assert worker_event.details == {}

    def test_worker_event_to_dict_minimal(self) -> None:
        """Test WorkerEvent.to_dict method with minimal parameters."""
        worker_event = WorkerEvent(
            worker_id="test-worker-123",
            event_type=WorkerEventType.STARTED
        )

        result = worker_event.to_dict()

        assert result["worker_id"] == "test-worker-123"
        assert result["event_type"] == "started"
        assert "timestamp" in result
        assert result["timestamp"] > 0
        # None values should be filtered out
        assert "queue_names" not in result
        assert "message" not in result
        assert "details" not in result
        assert "job_id" not in result
        assert "duration_ms" not in result
        assert "cpu_usage" not in result
        assert "memory_usage" not in result

    def test_worker_event_to_dict_with_all_params(self) -> None:
        """Test WorkerEvent.to_dict method with all parameters."""
        worker_event = WorkerEvent(
            worker_id="test-worker-123",
            event_type=WorkerEventType.HEARTBEAT,
            timestamp=1625097600.0,
            queue_names=["queue1", "queue2"],
            message="Worker heartbeat",
            details={"status": "healthy"},
            job_id="job-456",
            duration_ms=100.0,
            cpu_usage=45.5,
            memory_usage=512.0
        )

        result = worker_event.to_dict()

        assert result["worker_id"] == "test-worker-123"
        assert result["event_type"] == "heartbeat"
        assert result["timestamp"] == 1625097600.0
        assert result["queue_names"] == ["queue1", "queue2"]
        assert result["message"] == "Worker heartbeat"
        assert result["details"] == {"status": "healthy"}
        assert result["job_id"] == "job-456"
        assert result["duration_ms"] == 100.0
        assert result["cpu_usage"] == 45.5
        assert result["memory_usage"] == 512.0

    def test_worker_event_to_dict_with_job_event(self) -> None:
        """Test WorkerEvent.to_dict method with job-related event."""
        worker_event = WorkerEvent.job_completed(
            worker_id="test-worker-123",
            job_id="job-456",
            duration_ms=1500.0,
            queue_names=["queue1"],
            details={"result": "success"}
        )

        result = worker_event.to_dict()

        assert result["worker_id"] == "test-worker-123"
        assert result["event_type"] == "job_completed"
        assert result["job_id"] == "job-456"
        assert result["duration_ms"] == 1500.0
        assert result["queue_names"] == ["queue1"]
        assert result["message"] == "Worker test-worker-123 completed job job-456"
        assert result["details"]["duration_ms"] == 1500.0
        assert result["details"]["result"] == "success"
        # None values should be filtered out
        assert "cpu_usage" not in result
        assert "memory_usage" not in result

    def test_worker_event_to_dict_with_system_event(self) -> None:
        """Test WorkerEvent.to_dict method with system event."""
        worker_event = WorkerEvent.heartbeat(
            worker_id="test-worker-123",
            queue_names=["queue1", "queue2"],
            cpu_usage=45.5,
            memory_usage=512.0,
            details={"status": "healthy"}
        )

        result = worker_event.to_dict()

        assert result["worker_id"] == "test-worker-123"
        assert result["event_type"] == "heartbeat"
        assert result["queue_names"] == ["queue1", "queue2"]
        assert result["cpu_usage"] == 45.5
        assert result["memory_usage"] == 512.0
        assert result["details"] == {"status": "healthy"}
        # None values should be filtered out
        assert "message" not in result
        assert "job_id" not in result
        assert "duration_ms" not in result