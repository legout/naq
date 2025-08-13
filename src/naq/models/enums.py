# src/naq/models/enums.py
"""
Enum definitions for the NAQ job queue system.

This module contains all Enum classes used throughout the NAQ system,
proposing a centralized location for these shared constants.
"""

from enum import Enum
from typing import Set


class JOB_STATUS(Enum):
    """
    Enum representing the possible states of a job.

    This enum tracks the lifecycle of a job from creation through completion.
    Each status represents a distinct phase in the job's execution.

    Values:
        PENDING: Job has been created but not yet started
        RUNNING: Job is currently being executed by a worker
        COMPLETED: Job finished successfully
        FAILED: Job finished with an error
        RETRY: Job is scheduled for a retry attempt
        SCHEDULED: Job is scheduled for future execution
        PAUSED: Job execution has been temporarily paused
        CANCELLED: Job was cancelled before completion

    Example:
        ```python
        from naq.models.enums import JOB_STATUS

        # Check if a job is completed
        if job.status == JOB_STATUS.COMPLETED:
            print("Job finished successfully")

        # Set a job to failed status
        job.status = JOB_STATUS.FAILED
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

    This enum defines all possible event types that can occur during
    a job's lifecycle. These events are used for tracking, logging,
    and monitoring job execution.

    Values:
        ENQUEUED: Job was added to a queue
        STARTED: Job began execution
        COMPLETED: Job finished successfully
        FAILED: Job finished with an error
        RETRY_SCHEDULED: Job was scheduled for a retry
        SCHEDULED: Job was scheduled for future execution
        SCHEDULE_TRIGGERED: A scheduled job was triggered for execution
        CANCELLED: Job was cancelled
        STATUS_CHANGED: Job status changed
        PAUSED: Job execution was paused
        RESUMED: Job execution was resumed
        SCHEDULE_PAUSED: Schedule was paused
        SCHEDULE_RESUMED: Schedule was resumed
        SCHEDULE_CANCELLED: Schedule was cancelled
        SCHEDULE_MODIFIED: Schedule was modified
        SCHEDULER_ERROR: An error occurred in the scheduler

    Example:
        ```python
        from naq.models.enums import JobEventType
        from naq.models.events import JobEvent

        # Create a job event
        event = JobEvent(
            job_id="123",
            event_type=JobEventType.STARTED,
            timestamp=time.time()
        )

        # Check event type
        if event.event_type == JobEventType.COMPLETED:
            print("Job completed successfully")
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
    PAUSED = "paused"
    RESUMED = "resumed"
    # Schedule management events
    SCHEDULE_PAUSED = "schedule_paused"
    SCHEDULE_RESUMED = "schedule_resumed"
    SCHEDULE_CANCELLED = "schedule_cancelled"
    SCHEDULE_MODIFIED = "schedule_modified"
    SCHEDULER_ERROR = "scheduler_error"


class WorkerEventType(str, Enum):
    """
    Enum representing the types of worker events.

    This enum defines all possible event types that can occur during
    a worker's lifecycle. These events are used for tracking, logging,
    and monitoring worker status and health.

    Values:
        STARTED: Worker started running
        STOPPED: Worker stopped running
        HEARTBEAT: Worker sent a heartbeat signal
        JOB_STARTED: Worker started processing a job
        JOB_COMPLETED: Worker completed a job successfully
        JOB_FAILED: Worker failed to complete a job
        PAUSED: Worker was paused
        RESUMED: Worker was resumed after being paused

    Example:
        ```python
        from naq.models.enums import WorkerEventType
        from naq.models.events import WorkerEvent

        # Create a worker event
        event = WorkerEvent(
            worker_id="worker-1",
            event_type=WorkerEventType.HEARTBEAT,
            timestamp=time.time()
        )

        # Check event type
        if event.event_type == WorkerEventType.JOB_COMPLETED:
            print("Worker completed a job")
        ```
    """

    STARTED = "started"
    STOPPED = "stopped"
    HEARTBEAT = "heartbeat"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    PAUSED = "paused"
    RESUMED = "resumed"


class RETRY_STRATEGY(Enum):
    """
    Enum representing the available retry strategies for jobs.

    This enum defines the different strategies that can be used when
    retrying failed jobs. Each strategy implements a different approach
    to calculating the delay between retry attempts.

    Values:
        LINEAR: Retry with a constant delay between attempts
        EXPONENTIAL: Retry with exponentially increasing delays

    Example:
        ```python
        from naq.models.enums import RETRY_STRATEGY

        # Set a job to use exponential backoff
        job.retry_strategy = RETRY_STRATEGY.EXPONENTIAL

        # Check if a job uses linear retry
        if job.retry_strategy == RETRY_STRATEGY.LINEAR:
            print("Using linear retry strategy")
        ```
    """

    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class SCHEDULED_JOB_STATUS(Enum):
    """Enum representing the possible states of a scheduled job."""

    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Normalize valid strategies to string values for consistent comparison
# and error messages
VALID_RETRY_STRATEGIES: Set[str] = {strategy.value for strategy in RETRY_STRATEGY}


# --- Worker Monitoring Settings ---
class WORKER_STATUS(Enum):
    """Enum representing the possible states of a worker."""

    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    STOPPING = "stopping"
