"""
NAQ Enums Module

This module contains all enum definitions used throughout the NAQ job queue system.
"""

from enum import Enum
from typing import Set, Union, Sequence

# Import retry strategy from settings
from ..settings import RETRY_STRATEGY


class JOB_STATUS(Enum):
    """
    Enum representing the possible states of a job.
    
    This enum defines the lifecycle states that a job can be in during its execution.
    Each status represents a specific phase in the job's lifecycle from creation
    to completion or failure.
    
    Values:
        PENDING: Job has been created but not yet started
        RUNNING: Job is currently being executed
        COMPLETED: Job finished successfully
        FAILED: Job finished with an error
        RETRY: Job is scheduled for retry after a failure
        SCHEDULED: Job is scheduled for future execution
        PAUSED: Job execution is temporarily paused
        CANCELLED: Job was cancelled before completion
    
    Example:
        ```python
        from naq.models import JOB_STATUS, Job
        
        # Create a new job
        job = Job(function=lambda: print("Hello"))
        
        # Check job status
        if job.status == JOB_STATUS.PENDING:
            print("Job is waiting to be processed")
        
        # Use enum values for comparisons
        if job.status.value == "completed":
            print("Job finished successfully")
        ```
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    SCHEDULED = "scheduled"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class JobEventType(str, Enum):
    """
    Enum representing the types of job events.
    
    This enum defines all possible event types that can occur during a job's lifecycle.
    These events are used for logging, monitoring, and tracking job execution progress.
    
    Values:
        ENQUEUED: Job was added to a queue
        STARTED: Job began execution
        COMPLETED: Job finished successfully
        FAILED: Job finished with an error
        RETRY_SCHEDULED: Job was scheduled for retry
        SCHEDULED: Job was scheduled for future execution
        SCHEDULE_TRIGGERED: Scheduled job was triggered for execution
        CANCELLED: Job was cancelled
        STATUS_CHANGED: Job status changed from one state to another
    
    Example:
        ```python
        from naq.models import JobEventType, JobEvent
        
        # Create different types of job events
        enqueued_event = JobEvent.enqueued(
            job_id="job-123",
            queue_name="default"
        )
        
        started_event = JobEvent.started(
            job_id="job-123",
            worker_id="worker-456"
        )
        
        # Check event type
        if enqueued_event.event_type == JobEventType.ENQUEUED:
            print("Job was added to queue")
        
        # Use enum values for filtering
        completed_events = [e for e in events 
                          if e.event_type == JobEventType.COMPLETED]
        ```
    """

    ENQUEUED = "enqueued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    SCHEDULED = "scheduled"
    SCHEDULE_TRIGGERED = "schedule_triggered"
    CANCELLED = "cancelled"
    STATUS_CHANGED = "status_changed"


class WorkerEventType(str, Enum):
    """
    Enum representing the types of worker events.
    
    This enum defines all possible event types that can occur during a worker's lifecycle.
    These events are used for monitoring worker health, performance, and state changes.
    
    Values:
        STARTED: Worker started and is ready to process jobs
        STOPPED: Worker stopped gracefully
        HEARTBEAT: Worker sent a heartbeat signal
        JOB_ASSIGNED: Worker was assigned a job
        JOB_STARTED: Worker started processing a job
        JOB_COMPLETED: Worker completed a job successfully
        JOB_FAILED: Worker failed to complete a job
        ERROR: Worker encountered an error
        PAUSED: Worker was paused
        RESUMED: Worker was resumed after being paused
    
    Example:
        ```python
        from naq.models import WorkerEventType, WorkerEvent
        
        # Create worker events
        started_event = WorkerEvent.started(
            worker_id="worker-123",
            worker_type="default"
        )
        
        heartbeat_event = WorkerEvent.heartbeat(
            worker_id="worker-123",
            jobs_processed=10
        )
        
        # Check event type
        if started_event.event_type == WorkerEventType.STARTED:
            print("Worker is now active")
        
        # Filter events by type
        error_events = [e for e in worker_events 
                       if e.event_type == WorkerEventType.ERROR]
        ```
    """

    STARTED = "started"
    STOPPED = "stopped"
    HEARTBEAT = "heartbeat"
    JOB_ASSIGNED = "job_assigned"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    ERROR = "error"
    PAUSED = "paused"
    RESUMED = "resumed"


# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES: Set[str] = {"linear", "exponential"}

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]