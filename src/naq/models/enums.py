# src/naq/models/enums.py
"""
Status enums, event types, and constants for NAQ.

This module contains all enum definitions and constants used throughout
the NAQ system for job statuses, event types, and retry strategies.
"""

from enum import Enum
from typing import Sequence, Union

# Define a type hint for retry delays
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]


class JOB_STATUS(Enum):
    """Enum representing the possible states of a job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    SCHEDULED = "scheduled"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class JobEventType(str, Enum):
    """Enum representing the different types of job lifecycle events."""
    
    ENQUEUED = "enqueued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    SCHEDULED = "scheduled"
    SCHEDULE_TRIGGERED = "schedule_triggered"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    RESUMED = "resumed"
    # Schedule management events
    SCHEDULE_PAUSED = "schedule_paused"
    SCHEDULE_RESUMED = "schedule_resumed"  
    SCHEDULE_CANCELLED = "schedule_cancelled"
    SCHEDULE_MODIFIED = "schedule_modified"


class WorkerEventType(str, Enum):
    """Enum representing the different types of worker lifecycle events."""
    
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"
    WORKER_IDLE = "worker_idle"
    WORKER_BUSY = "worker_busy"
    WORKER_HEARTBEAT = "worker_heartbeat"
    WORKER_ERROR = "worker_error"


# Define retry strategies - imported from settings
from ..settings import RETRY_STRATEGY

# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}