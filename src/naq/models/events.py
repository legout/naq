# src/naq/models/events.py
"""
Event model classes for NAQ job and worker lifecycle events.

This module contains event classes that capture information about job
and worker lifecycle events for monitoring and observability.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import msgspec

from .enums import JobEventType, WorkerEventType


class JobEvent(msgspec.Struct):
    """
    Represents a job lifecycle event.
    
    This structure captures all relevant information about events that occur
    during a job's lifecycle, including metadata for monitoring and debugging.
    """
    
    # Core event identification
    job_id: str
    event_type: JobEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    
    # Context information
    worker_id: Optional[str] = None
    queue_name: Optional[str] = None
    
    # Event details
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    
    # Error information (for FAILED events)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    
    # Performance metrics
    duration_ms: Optional[int] = None
    
    # NATS metadata
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None

    @classmethod
    def enqueued(
        cls,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create an ENQUEUED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.ENQUEUED,
            queue_name=queue_name,
            message=message or f"Job {job_id} enqueued to {queue_name}",
            **kwargs
        )

    @classmethod
    def started(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a STARTED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STARTED,
            worker_id=worker_id,
            queue_name=queue_name,
            message=message or f"Job {job_id} started by worker {worker_id}",
            **kwargs
        )

    @classmethod
    def completed(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a COMPLETED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.COMPLETED,
            worker_id=worker_id,
            queue_name=queue_name,
            duration_ms=duration_ms,
            message=message or f"Job {job_id} completed successfully in {duration_ms}ms",
            **kwargs
        )

    @classmethod
    def failed(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: str,
        error_type: str,
        error_message: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a FAILED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.FAILED,
            worker_id=worker_id,
            queue_name=queue_name,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            message=message or f"Job {job_id} failed after {duration_ms}ms: {error_message}",
            **kwargs
        )

    @classmethod
    def retry_scheduled(
        cls,
        job_id: str,
        worker_id: str,
        queue_name: str,
        retry_count: int,
        retry_delay: float,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a RETRY_SCHEDULED event."""
        details = kwargs.get('details', {})
        details.update({
            'retry_count': retry_count,
            'retry_delay': retry_delay
        })
        
        return cls(
            job_id=job_id,
            event_type=JobEventType.RETRY_SCHEDULED,
            worker_id=worker_id,
            queue_name=queue_name,
            details=details,
            message=message or f"Job {job_id} retry #{retry_count} scheduled in {retry_delay}s",
            **kwargs
        )

    @classmethod
    def scheduled(
        cls,
        job_id: str,
        queue_name: str,
        scheduled_timestamp: float,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULED event."""
        scheduled_dt = datetime.fromtimestamp(scheduled_timestamp, tz=timezone.utc)
        details = kwargs.get('details', {})
        details.update({
            'scheduled_timestamp': scheduled_timestamp,
            'scheduled_datetime': scheduled_dt.isoformat()
        })
        
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULED,
            queue_name=queue_name,
            details=details,
            message=message or f"Job {job_id} scheduled for {scheduled_dt}",
            **kwargs
        )

    @classmethod
    def schedule_triggered(
        cls,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULE_TRIGGERED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_TRIGGERED,
            queue_name=queue_name,
            message=message or f"Scheduled job {job_id} triggered and enqueued",
            **kwargs
        )

    @classmethod
    def schedule_paused(
        cls,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULE_PAUSED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_PAUSED,
            queue_name=queue_name,
            message=message or f"Scheduled job {job_id} paused",
            **kwargs
        )

    @classmethod
    def schedule_resumed(
        cls,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULE_RESUMED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_RESUMED,
            queue_name=queue_name,
            message=message or f"Scheduled job {job_id} resumed",
            **kwargs
        )

    @classmethod
    def schedule_cancelled(
        cls,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULE_CANCELLED event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_CANCELLED,
            queue_name=queue_name,
            message=message or f"Scheduled job {job_id} cancelled",
            **kwargs
        )

    @classmethod
    def schedule_modified(
        cls,
        job_id: str,
        queue_name: str,
        modifications: Dict[str, Any],
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULE_MODIFIED event."""
        details = kwargs.get('details', {})
        details.update({'modifications': modifications})
        
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULE_MODIFIED,
            queue_name=queue_name,
            details=details,
            message=message or f"Scheduled job {job_id} modified",
            **kwargs
        )

    @classmethod
    def scheduler_error(
        cls,
        error_type: str,
        error_message: str,
        schedule_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs
    ) -> "JobEvent":
        """Create a SCHEDULER_ERROR event."""
        return cls(
            job_id=schedule_id or "unknown",
            event_type=JobEventType.SCHEDULER_ERROR,
            queue_name=queue_name,
            error_type=error_type,
            error_message=error_message,
            message=message or f"Scheduler error: {error_type}",
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a dictionary representation."""
        return {
            'job_id': self.job_id,
            'event_type': self.event_type.value,
            'timestamp': self.timestamp,
            'worker_id': self.worker_id,
            'queue_name': self.queue_name,
            'message': self.message,
            'details': self.details,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'duration_ms': self.duration_ms,
            'nats_subject': self.nats_subject,
            'nats_sequence': self.nats_sequence
        }


class WorkerEvent(msgspec.Struct):
    """
    Represents a worker lifecycle event.
    
    This structure captures all relevant information about events that occur
    during a worker's lifecycle, including metadata for monitoring and debugging.
    """
    
    # Core event identification
    worker_id: str
    event_type: WorkerEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    
    # Context information
    hostname: Optional[str] = None
    pid: Optional[int] = None
    queue_names: Optional[List[str]] = None
    
    # Event details
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    
    # Job context (for WORKER_BUSY events)
    current_job_id: Optional[str] = None
    
    # Performance/status metrics
    active_jobs: Optional[int] = None
    concurrency_limit: Optional[int] = None
    
    # Error information (for WORKER_ERROR events)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    
    # NATS metadata
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None

    @classmethod
    def worker_started(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        queue_names: List[str],
        concurrency_limit: int,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_STARTED event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_STARTED,
            hostname=hostname,
            pid=pid,
            queue_names=queue_names,
            concurrency_limit=concurrency_limit,
            message=message or f"Worker {worker_id} started on {hostname} (PID: {pid})",
            **kwargs
        )

    @classmethod
    def worker_stopped(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_STOPPED event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_STOPPED,
            hostname=hostname,
            pid=pid,
            message=message or f"Worker {worker_id} stopped on {hostname} (PID: {pid})",
            **kwargs
        )

    @classmethod
    def worker_idle(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_IDLE event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_IDLE,
            hostname=hostname,
            pid=pid,
            message=message or f"Worker {worker_id} is now idle",
            **kwargs
        )

    @classmethod
    def worker_busy(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        current_job_id: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_BUSY event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_BUSY,
            hostname=hostname,
            pid=pid,
            current_job_id=current_job_id,
            message=message or f"Worker {worker_id} is now busy with job {current_job_id}",
            **kwargs
        )

    @classmethod
    def worker_heartbeat(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        active_jobs: int,
        concurrency_limit: int,
        current_job_id: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_HEARTBEAT event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_HEARTBEAT,
            hostname=hostname,
            pid=pid,
            active_jobs=active_jobs,
            concurrency_limit=concurrency_limit,
            current_job_id=current_job_id,
            message=message or f"Worker {worker_id} heartbeat: {active_jobs}/{concurrency_limit} active jobs",
            **kwargs
        )

    @classmethod
    def worker_error(
        cls,
        worker_id: str,
        hostname: str,
        pid: int,
        error_type: str,
        error_message: str,
        message: Optional[str] = None,
        **kwargs
    ) -> "WorkerEvent":
        """Create a WORKER_ERROR event."""
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.WORKER_ERROR,
            hostname=hostname,
            pid=pid,
            error_type=error_type,
            error_message=error_message,
            message=message or f"Worker {worker_id} error: {error_message}",
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a dictionary representation."""
        return {
            'worker_id': self.worker_id,
            'event_type': self.event_type.value,
            'timestamp': self.timestamp,
            'hostname': self.hostname,
            'pid': self.pid,
            'queue_names': self.queue_names,
            'message': self.message,
            'details': self.details,
            'current_job_id': self.current_job_id,
            'active_jobs': self.active_jobs,
            'concurrency_limit': self.concurrency_limit,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'nats_subject': self.nats_subject,
            'nats_sequence': self.nats_sequence
        }