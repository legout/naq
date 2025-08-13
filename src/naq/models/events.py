# src/naq/models/events.py
"""
Event model definitions for the NAQ job queue system.

This module contains event classes that track the lifecycle of jobs and workers
throughout the NAQ system. These events are used for monitoring, logging, and
debugging purposes.
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

    Job events are used to track the execution flow of jobs through the system,
    providing visibility into when jobs are enqueued, started, completed, failed,
    or undergo other state transitions.

    Example:
        ```python
        import time
        from naq.models.events import JobEvent
        from naq.models.enums import JobEventType

        # Create a job started event
        event = JobEvent.started(
            job_id="job-123",
            worker_id="worker-1",
            queue_name="default"
        )

        # Access event properties
        print(f"Job {event.job_id} {event.event_type.value} at {event.timestamp}")
        ```

    Attributes:
        job_id: Unique identifier for the job
        event_type: Type of event that occurred
        timestamp: Unix timestamp when the event occurred (defaults to current time)
        worker_id: ID of the worker processing the job (if applicable)
        queue_name: Name of the queue the job belongs to
        message: Human-readable message describing the event
        details: Additional event-specific details as a dictionary
        error_type: Type of error (if the event represents an error)
        error_message: Error message (if the event represents an error)
        duration_ms: Duration of the operation in milliseconds (if applicable)
        nats_subject: NATS subject associated with the event
        nats_sequence: NATS sequence number associated with the event
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
        """
        Create an ENQUEUED event.

        This event is generated when a job is added to a queue and is ready
        to be processed by a worker.

        Args:
            job_id: Unique identifier for the job
            queue_name: Name of the queue the job was added to
            worker_id: ID of the worker that enqueued the job (if applicable)
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new ENQUEUED event instance

        Example:
            ```python
            event = JobEvent.enqueued(
                job_id="job-123",
                queue_name="default",
                worker_id="worker-1"
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
        """
        Create a STARTED event.

        This event is generated when a worker begins processing a job.

        Args:
            job_id: Unique identifier for the job
            worker_id: ID of the worker that started the job
            queue_name: Name of the queue the job belongs to
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new STARTED event instance

        Example:
            ```python
            event = JobEvent.started(
                job_id="job-123",
                worker_id="worker-1",
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
        """
        Create a COMPLETED event.

        This event is generated when a job finishes successfully.

        Args:
            job_id: Unique identifier for the job
            worker_id: ID of the worker that completed the job
            duration_ms: Time taken to complete the job in milliseconds
            queue_name: Name of the queue the job belongs to
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new COMPLETED event instance

        Example:
            ```python
            event = JobEvent.completed(
                job_id="job-123",
                worker_id="worker-1",
                duration_ms=1500.0,
                queue_name="default"
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
        """
        Create a FAILED event.

        This event is generated when a job fails to complete successfully.

        Args:
            job_id: Unique identifier for the job
            worker_id: ID of the worker that failed to complete the job
            error_type: Type of error that occurred
            error_message: Human-readable error message
            duration_ms: Time taken before failure in milliseconds
            queue_name: Name of the queue the job belongs to
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new FAILED event instance

        Example:
            ```python
            event = JobEvent.failed(
                job_id="job-123",
                worker_id="worker-1",
                error_type="ValueError",
                error_message="Invalid input data",
                duration_ms=500.0,
                queue_name="default"
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
        """
        Create a RETRY_SCHEDULED event.

        This event is generated when a failed job is scheduled for a retry attempt.

        Args:
            job_id: Unique identifier for the job
            worker_id: ID of the worker that scheduled the retry
            delay_seconds: Number of seconds to wait before retrying
            queue_name: Name of the queue the job belongs to
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new RETRY_SCHEDULED event instance

        Example:
            ```python
            event = JobEvent.retry_scheduled(
                job_id="job-123",
                worker_id="worker-1",
                delay_seconds=30.0,
                queue_name="default"
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
        """
        Create a SCHEDULED event.

        This event is generated when a job is scheduled for future execution.

        Args:
            job_id: Unique identifier for the job
            queue_name: Name of the queue the job belongs to
            scheduled_timestamp_utc: Unix timestamp when the job should run
            worker_id: ID of the worker that scheduled the job (if applicable)
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new SCHEDULED event instance

        Example:
            ```python
            import time
            future_time = time.time() + 3600  # 1 hour from now
            event = JobEvent.scheduled(
                job_id="job-123",
                queue_name="default",
                scheduled_timestamp_utc=future_time
            )
            ```
        """
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
        """
        Create a SCHEDULE_TRIGGERED event.

        This event is generated when a scheduled job is triggered for execution.

        Args:
            job_id: Unique identifier for the job
            queue_name: Name of the queue the job belongs to
            worker_id: ID of the worker that triggered the schedule (if applicable)
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new SCHEDULE_TRIGGERED event instance

        Example:
            ```python
            event = JobEvent.schedule_triggered(
                job_id="job-123",
                queue_name="default",
                worker_id="scheduler-1"
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
        """
        Create a CANCELLED event.

        This event is generated when a job is cancelled before completion.

        Args:
            job_id: Unique identifier for the job
            queue_name: Name of the queue the job belongs to
            worker_id: ID of the worker that cancelled the job (if applicable)
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new CANCELLED event instance

        Example:
            ```python
            event = JobEvent.cancelled(
                job_id="job-123",
                queue_name="default",
                worker_id="admin-1"
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
            message="Job cancelled",
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
        """
        Create a STATUS_CHANGED event.

        This event is generated when a job's status changes.

        Args:
            job_id: Unique identifier for the job
            queue_name: Name of the queue the job belongs to
            old_status: Previous status of the job
            new_status: New status of the job
            worker_id: ID of the worker that changed the status (if applicable)
            nats_subject: NATS subject where the job was published
            nats_sequence: NATS sequence number for the message
            details: Additional event-specific details

        Returns:
            JobEvent: A new STATUS_CHANGED event instance

        Example:
            ```python
            event = JobEvent.status_changed(
                job_id="job-123",
                queue_name="default",
                old_status="pending",
                new_status="running",
                worker_id="worker-1"
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
                **(details or {}),
            },
        )


class WorkerEvent(msgspec.Struct):
    """
    Represents an event in the lifecycle of a worker.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.

    Worker events are used to track the state and health of workers throughout
    the system, providing visibility into worker lifecycle events, job processing,
    and system health monitoring.

    Example:
        ```python
        import time
        from naq.models.events import WorkerEvent
        from naq.models.enums import WorkerEventType

        # Create a worker heartbeat event
        event = WorkerEvent.heartbeat(
            worker_id="worker-1",
            queue_names=["default", "high-priority"]
        )

        # Access event properties
        print(f"Worker {event.worker_id} sent {event.event_type.value} at {event.timestamp}")
        ```

    Attributes:
        worker_id: Unique identifier for the worker
        event_type: Type of event that occurred
        timestamp: Unix timestamp when the event occurred (defaults to current time)
        queue_names: List of queue names the worker is processing
        message: Human-readable message describing the event
        details: Additional event-specific details as a dictionary
        job_id: ID of the job associated with the event (if applicable)
        duration_ms: Duration of the operation in milliseconds (if applicable)
        cpu_usage: CPU usage percentage (if applicable)
        memory_usage: Memory usage percentage (if applicable)
    """

    worker_id: str
    event_type: WorkerEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    queue_names: Optional[list[str]] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = None
    duration_ms: Optional[float] = None
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None

    @classmethod
    def started(
        cls,
        worker_id: str,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a STARTED event.

        This event is generated when a worker starts running.

        Args:
            worker_id: Unique identifier for the worker
            queue_names: List of queue names the worker will process
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new STARTED event instance

        Example:
            ```python
            event = WorkerEvent.started(
                worker_id="worker-1",
                queue_names=["default", "high-priority"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.STARTED,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} started",
            details=details or {},
        )

    @classmethod
    def stopped(
        cls,
        worker_id: str,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a STOPPED event.

        This event is generated when a worker stops running.

        Args:
            worker_id: Unique identifier for the worker
            queue_names: List of queue names the worker was processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new STOPPED event instance

        Example:
            ```python
            event = WorkerEvent.stopped(
                worker_id="worker-1",
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.STOPPED,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} stopped",
            details=details or {},
        )

    @classmethod
    def heartbeat(
        cls,
        worker_id: str,
        queue_names: Optional[list[str]] = None,
        cpu_usage: Optional[float] = None,
        memory_usage: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a HEARTBEAT event.

        This event is generated periodically by workers to indicate they are
        still alive and healthy.

        Args:
            worker_id: Unique identifier for the worker
            queue_names: List of queue names the worker is processing
            cpu_usage: Current CPU usage percentage
            memory_usage: Current memory usage percentage
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new HEARTBEAT event instance

        Example:
            ```python
            event = WorkerEvent.heartbeat(
                worker_id="worker-1",
                queue_names=["default"],
                cpu_usage=45.2,
                memory_usage=512.5
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.HEARTBEAT,
            queue_names=queue_names or [],
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            details=details or {},
        )

    @classmethod
    def job_started(
        cls,
        worker_id: str,
        job_id: str,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a JOB_STARTED event.

        This event is generated when a worker starts processing a job.

        Args:
            worker_id: Unique identifier for the worker
            job_id: ID of the job being started
            queue_names: List of queue names the worker is processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new JOB_STARTED event instance

        Example:
            ```python
            event = WorkerEvent.job_started(
                worker_id="worker-1",
                job_id="job-123",
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_STARTED,
            job_id=job_id,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} started job {job_id}",
            details=details or {},
        )

    @classmethod
    def job_completed(
        cls,
        worker_id: str,
        job_id: str,
        duration_ms: float,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a JOB_COMPLETED event.

        This event is generated when a worker completes a job successfully.

        Args:
            worker_id: Unique identifier for the worker
            job_id: ID of the job that was completed
            duration_ms: Time taken to complete the job in milliseconds
            queue_names: List of queue names the worker is processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new JOB_COMPLETED event instance

        Example:
            ```python
            event = WorkerEvent.job_completed(
                worker_id="worker-1",
                job_id="job-123",
                duration_ms=1500.0,
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_COMPLETED,
            job_id=job_id,
            duration_ms=duration_ms,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} completed job {job_id}",
            details={"duration_ms": duration_ms, **(details or {})},
        )

    @classmethod
    def job_failed(
        cls,
        worker_id: str,
        job_id: str,
        duration_ms: float,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a JOB_FAILED event.

        This event is generated when a worker fails to complete a job.

        Args:
            worker_id: Unique identifier for the worker
            job_id: ID of the job that failed
            duration_ms: Time taken before failure in milliseconds
            queue_names: List of queue names the worker is processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new JOB_FAILED event instance

        Example:
            ```python
            event = WorkerEvent.job_failed(
                worker_id="worker-1",
                job_id="job-123",
                duration_ms=500.0,
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.JOB_FAILED,
            job_id=job_id,
            duration_ms=duration_ms,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} failed job {job_id}",
            details={"duration_ms": duration_ms, **(details or {})},
        )

    @classmethod
    def paused(
        cls,
        worker_id: str,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a PAUSED event.

        This event is generated when a worker is paused.

        Args:
            worker_id: Unique identifier for the worker
            queue_names: List of queue names the worker was processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new PAUSED event instance

        Example:
            ```python
            event = WorkerEvent.paused(
                worker_id="worker-1",
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.PAUSED,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} paused",
            details=details or {},
        )

    @classmethod
    def resumed(
        cls,
        worker_id: str,
        queue_names: Optional[list[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> "WorkerEvent":
        """
        Create a RESUMED event.

        This event is generated when a paused worker is resumed.

        Args:
            worker_id: Unique identifier for the worker
            queue_names: List of queue names the worker is processing
            details: Additional event-specific details

        Returns:
            WorkerEvent: A new RESUMED event instance

        Example:
            ```python
            event = WorkerEvent.resumed(
                worker_id="worker-1",
                queue_names=["default"]
            )
            ```
        """
        return cls(
            worker_id=worker_id,
            event_type=WorkerEventType.RESUMED,
            queue_names=queue_names or [],
            message=f"Worker {worker_id} resumed",
            details=details or {},
        )
