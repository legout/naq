# src/naq/models/events.py
import time
from typing import Any, Dict, Optional

import msgspec

from .enums import JobEventType, WorkerEventType

class JobEvent(msgspec.Struct):
    """
    Represents an event in the lifecycle of a job.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    job_id: str
    event_type: JobEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    worker_id: Optional[str] = None
    queue_name: Optional[str] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[float] = None
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None

    @classmethod
    def enqueued(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create an ENQUEUED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.ENQUEUED,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def started(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a STARTED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STARTED,
            worker_id=worker_id,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def completed(
        cls,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a COMPLETED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.COMPLETED,
            worker_id=worker_id,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details={"duration_ms": duration_ms, **(details or {})},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def failed(
        cls,
        job_id: str,
        worker_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a FAILED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.FAILED,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details={
                "duration_ms": duration_ms,
                "error_type": error_type,
                "error_message": error_message,
                **(details or {}),
            },
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def retry_scheduled(
        cls,
        job_id: str,
        worker_id: str,
        delay_seconds: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a RETRY_SCHEDULED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.RETRY_SCHEDULED,
            worker_id=worker_id,
            message=f"Retry scheduled in {delay_seconds} seconds",
            queue_name=queue_name,
            details={"delay_seconds": delay_seconds, **(details or {})},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def scheduled(
        cls,
        job_id: str,
        queue_name: str,
        scheduled_timestamp_utc: float,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a SCHEDULED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULED,
            queue_name=queue_name,
            worker_id=worker_id,
            details={
                "scheduled_timestamp_utc": scheduled_timestamp_utc,
                **(details or {}),
            },
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def schedule_triggered(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a SCHEDULE_TRIGGERED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_TRIGGERED,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def cancelled(
        cls,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a CANCELLED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.CANCELLED,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            message=f"Job cancelled",
            details=details or {},
        )

    @classmethod
    def status_changed(
        cls,
        job_id: str,
        queue_name: str,
        old_status: str,
        new_status: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "JobEvent":
        """Create a STATUS_CHANGED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STATUS_CHANGED,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            message=f"Job status changed from {old_status} to {new_status}",
            details={
                "old_status": old_status,
                "new_status": new_status,
                **(details or {}),
            },
        )