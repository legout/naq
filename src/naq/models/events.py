"""
NAQ Event Models Module

This module contains JobEvent and WorkerEvent model definitions for the NAQ job queue system.
"""

import time
from typing import Any, Dict, Optional

import msgspec

from .enums import JobEventType, WorkerEventType


class JobEvent(msgspec.Struct):
    """
    Represents an event in the lifecycle of a job.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    
    Example:
        ```python
        from naq.models import JobEvent, JobEventType
        
        # Create an enqueued event
        event = JobEvent.enqueued(
            job_id="job-123",
            queue_name="default",
            worker_id="worker-456"
        )
        
        # Create a started event
        started_event = JobEvent.started(
            job_id="job-123",
            worker_id="worker-456"
        )
        
        # Check event type
        if event.event_type == JobEventType.ENQUEUED:
            print("Job was enqueued")
        ```
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
        """Create an ENQUEUED event.
        
        Args:
            job_id: The ID of the job that was enqueued
            queue_name: The name of the queue the job was added to
            worker_id: Optional ID of the worker that enqueued the job
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new ENQUEUED event instance
            
        Example:
            ```python
            event = JobEvent.enqueued(
                job_id="job-123",
                queue_name="default",
                worker_id="worker-456",
                details={"priority": "high"}
            )
            ```
        """
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
        """Create a STARTED event.
        
        Args:
            job_id: The ID of the job that started
            worker_id: The ID of the worker that started processing the job
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new STARTED event instance
            
        Example:
            ```python
            event = JobEvent.started(
                job_id="job-123",
                worker_id="worker-456",
                queue_name="default"
            )
            ```
        """
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
        """Create a COMPLETED event.
        
        Args:
            job_id: The ID of the job that completed
            worker_id: The ID of the worker that completed the job
            duration_ms: The duration of the job execution in milliseconds
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new COMPLETED event instance
            
        Example:
            ```python
            event = JobEvent.completed(
                job_id="job-123",
                worker_id="worker-456",
                duration_ms=1500.5,
                details={"result": "success"}
            )
            ```
        """
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
        """Create a FAILED event.
        
        Args:
            job_id: The ID of the job that failed
            worker_id: The ID of the worker that was processing the job
            error_type: The type of error that occurred
            error_message: The error message describing what went wrong
            duration_ms: The duration of the job execution before failure in milliseconds
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new FAILED event instance
            
        Example:
            ```python
            event = JobEvent.failed(
                job_id="job-123",
                worker_id="worker-456",
                error_type="ValueError",
                error_message="Invalid input data",
                duration_ms=500.0,
                details={"input": {"data": "invalid"}}
            )
            ```
        """
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
                **(details or {})
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
        """Create a RETRY_SCHEDULED event.
        
        Args:
            job_id: The ID of the job that was scheduled for retry
            worker_id: The ID of the worker that scheduled the retry
            delay_seconds: The delay before the retry attempt in seconds
            queue_name: Optional name of the queue the job will be retried on
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new RETRY_SCHEDULED event instance
            
        Example:
            ```python
            event = JobEvent.retry_scheduled(
                job_id="job-123",
                worker_id="worker-456",
                delay_seconds=30.0,
                details={"retry_count": 2}
            )
            ```
        """
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
        """Create a SCHEDULED event.
        
        Args:
            job_id: The ID of the job that was scheduled
            queue_name: The name of the queue the job was scheduled for
            scheduled_timestamp_utc: The UTC timestamp when the job is scheduled to run
            worker_id: Optional ID of the worker that scheduled the job
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new SCHEDULED event instance
            
        Example:
            ```python
            import time
            future_time = time.time() + 3600  # 1 hour from now
            
            event = JobEvent.scheduled(
                job_id="job-123",
                queue_name="default",
                scheduled_timestamp_utc=future_time,
                details={"cron": "0 * * * *"}
            )
            ```
        """
        return cls(
            job_id=job_id,
            event_type=JobEventType.SCHEDULED,
            queue_name=queue_name,
            worker_id=worker_id,
            details={"scheduled_timestamp_utc": scheduled_timestamp_utc, **(details or {})},
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
        """Create a SCHEDULE_TRIGGERED event.
        
        Args:
            job_id: The ID of the job whose schedule was triggered
            queue_name: The name of the queue the job was triggered for
            worker_id: Optional ID of the worker that triggered the schedule
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new SCHEDULE_TRIGGERED event instance
            
        Example:
            ```python
            event = JobEvent.schedule_triggered(
                job_id="job-123",
                queue_name="default",
                worker_id="scheduler-001",
                details={"trigger_type": "cron"}
            )
            ```
        """
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
        """Create a CANCELLED event.
        
        Args:
            job_id: The ID of the job that was cancelled
            queue_name: The name of the queue the job was cancelled from
            worker_id: Optional ID of the worker that cancelled the job
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new CANCELLED event instance
            
        Example:
            ```python
            event = JobEvent.cancelled(
                job_id="job-123",
                queue_name="default",
                worker_id="admin-001",
                details={"reason": "user_request"}
            )
            ```
        """
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
        """Create a STATUS_CHANGED event.
        
        Args:
            job_id: The ID of the job whose status changed
            queue_name: The name of the queue the job belongs to
            old_status: The previous status of the job
            new_status: The new status of the job
            worker_id: Optional ID of the worker that changed the status
            nats_subject: Optional NATS subject where the job was published
            nats_sequence: Optional NATS sequence number for the job
            details: Optional additional details about the event
            
        Returns:
            JobEvent: A new STATUS_CHANGED event instance
            
        Example:
            ```python
            event = JobEvent.status_changed(
                job_id="job-123",
                queue_name="default",
                old_status="pending",
                new_status="running",
                worker_id="worker-456"
            )
            ```
        """
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
                **(details or {})
            },
        )


class WorkerEvent(msgspec.Struct):
    """
    Represents an event in the lifecycle of a worker.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    
    Example:
        ```python
        from naq.models import WorkerEvent, WorkerEventType
        
        # Create a started event
        event = WorkerEvent.started(
            worker_id="worker-123",
            worker_type="default"
        )
        
        # Create a heartbeat event
        heartbeat_event = WorkerEvent.heartbeat(
            worker_id="worker-123",
            jobs_processed=10
        )
        
        # Check event type
        if event.event_type == WorkerEventType.STARTED:
            print("Worker started")
        ```
    """

    worker_id: str
    event_type: WorkerEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    worker_type: Optional[str] = None
    queue_name: Optional[str] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None

    @classmethod
    def started(
        cls,
        worker_id: str,
        worker_type: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a STARTED event.
        
        Args:
            worker_id: The ID of the worker that started
            worker_type: The type of the worker (e.g., "default", "priority")
            queue_name: Optional name of the queue the worker is processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new STARTED event instance
            
        Example:
            ```python
            event = WorkerEvent.started(
                worker_id="worker-123",
                worker_type="default",
                queue_name="high_priority",
                details={"max_concurrent_jobs": 5}
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.STARTED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def stopped(
        cls,
        worker_id: str,
        worker_type: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a STOPPED event.
        
        Args:
            worker_id: The ID of the worker that stopped
            worker_type: The type of the worker
            queue_name: Optional name of the queue the worker was processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new STOPPED event instance
            
        Example:
            ```python
            event = WorkerEvent.stopped(
                worker_id="worker-123",
                worker_type="default",
                details={"reason": "graceful_shutdown", "jobs_processed": 150}
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.STOPPED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def heartbeat(
        cls,
        worker_id: str,
        worker_type: str,
        jobs_processed: Optional[int] = None,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a HEARTBEAT event.
        
        Args:
            worker_id: The ID of the worker sending the heartbeat
            worker_type: The type of the worker
            jobs_processed: Optional count of jobs processed by the worker
            queue_name: Optional name of the queue the worker is processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new HEARTBEAT event instance
            
        Example:
            ```python
            event = WorkerEvent.heartbeat(
                worker_id="worker-123",
                worker_type="default",
                jobs_processed=25,
                details={"memory_usage": "45MB", "cpu_usage": "12%"}
            )
            ```
        """
        heartbeat_details = {"jobs_processed": jobs_processed, **(details or {})}
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.HEARTBEAT,
            worker_type=worker_type,
            queue_name=queue_name,
            details=heartbeat_details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def job_assigned(
        cls,
        worker_id: str,
        worker_type: str,
        job_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a JOB_ASSIGNED event.
        
        Args:
            worker_id: The ID of the worker that was assigned the job
            worker_type: The type of the worker
            job_id: The ID of the job that was assigned
            queue_name: Optional name of the queue the job was assigned from
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new JOB_ASSIGNED event instance
            
        Example:
            ```python
            event = WorkerEvent.job_assigned(
                worker_id="worker-123",
                worker_type="default",
                job_id="job-456",
                queue_name="default",
                details={"priority": "high", "assigned_at": "2023-01-01T12:00:00Z"}
            )
            ```
        """
        assignment_details = {"job_id": job_id, **(details or {})}
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_ASSIGNED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=assignment_details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def job_started(
        cls,
        worker_id: str,
        worker_type: str,
        job_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a JOB_STARTED event.
        
        Args:
            worker_id: The ID of the worker that started the job
            worker_type: The type of the worker
            job_id: The ID of the job that was started
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new JOB_STARTED event instance
            
        Example:
            ```python
            event = WorkerEvent.job_started(
                worker_id="worker-123",
                worker_type="default",
                job_id="job-456",
                queue_name="default",
                details={"started_at": "2023-01-01T12:00:00Z"}
            )
            ```
        """
        job_details = {"job_id": job_id, **(details or {})}
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_STARTED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=job_details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def job_completed(
        cls,
        worker_id: str,
        worker_type: str,
        job_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a JOB_COMPLETED event.
        
        Args:
            worker_id: The ID of the worker that completed the job
            worker_type: The type of the worker
            job_id: The ID of the job that was completed
            duration_ms: The duration of the job execution in milliseconds
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new JOB_COMPLETED event instance
            
        Example:
            ```python
            event = WorkerEvent.job_completed(
                worker_id="worker-123",
                worker_type="default",
                job_id="job-456",
                duration_ms=1500.5,
                details={"result": "success", "completed_at": "2023-01-01T12:00:15Z"}
            )
            ```
        """
        completion_details = {
            "job_id": job_id,
            "duration_ms": duration_ms,
            **(details or {})
        }
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_COMPLETED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=completion_details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def job_failed(
        cls,
        worker_id: str,
        worker_type: str,
        job_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a JOB_FAILED event.
        
        Args:
            worker_id: The ID of the worker that failed to complete the job
            worker_type: The type of the worker
            job_id: The ID of the job that failed
            error_type: The type of error that occurred
            error_message: The error message describing what went wrong
            duration_ms: The duration of the job execution before failure in milliseconds
            queue_name: Optional name of the queue the job was taken from
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new JOB_FAILED event instance
            
        Example:
            ```python
            event = WorkerEvent.job_failed(
                worker_id="worker-123",
                worker_type="default",
                job_id="job-456",
                error_type="ValueError",
                error_message="Invalid input data",
                duration_ms=500.0,
                details={"failed_at": "2023-01-01T12:00:05Z"}
            )
            ```
        """
        failure_details = {
            "job_id": job_id,
            "error_type": error_type,
            "error_message": error_message,
            "duration_ms": duration_ms,
            **(details or {})
        }
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_FAILED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=failure_details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def error(
        cls,
        worker_id: str,
        worker_type: str,
        error_type: str,
        error_message: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create an ERROR event.
        
        Args:
            worker_id: The ID of the worker that encountered the error
            worker_type: The type of the worker
            error_type: The type of error that occurred
            error_message: The error message describing what went wrong
            queue_name: Optional name of the queue the worker was processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new ERROR event instance
            
        Example:
            ```python
            event = WorkerEvent.error(
                worker_id="worker-123",
                worker_type="default",
                error_type="ConnectionError",
                error_message="Failed to connect to NATS server",
                details={"retry_count": 3, "server": "nats://localhost:4222"}
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.ERROR,
            worker_type=worker_type,
            queue_name=queue_name,
            error_type=error_type,
            error_message=error_message,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def paused(
        cls,
        worker_id: str,
        worker_type: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a PAUSED event.
        
        Args:
            worker_id: The ID of the worker that was paused
            worker_type: The type of the worker
            queue_name: Optional name of the queue the worker was processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new PAUSED event instance
            
        Example:
            ```python
            event = WorkerEvent.paused(
                worker_id="worker-123",
                worker_type="default",
                queue_name="default",
                details={"reason": "maintenance", "paused_at": "2023-01-01T12:00:00Z"}
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.PAUSED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    @classmethod
    def resumed(
        cls,
        worker_id: str,
        worker_type: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """Create a RESUMED event.
        
        Args:
            worker_id: The ID of the worker that was resumed
            worker_type: The type of the worker
            queue_name: Optional name of the queue the worker is processing
            nats_subject: Optional NATS subject where the event was published
            nats_sequence: Optional NATS sequence number for the event
            details: Optional additional details about the event
            
        Returns:
            WorkerEvent: A new RESUMED event instance
            
        Example:
            ```python
            event = WorkerEvent.resumed(
                worker_id="worker-123",
                worker_type="default",
                queue_name="default",
                details={"resumed_at": "2023-01-01T12:30:00Z", "downtime": 1800}
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.RESUMED,
            worker_type=worker_type,
            queue_name=queue_name,
            details=details or {},
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )

    