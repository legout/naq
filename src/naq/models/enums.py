# src/naq/models/enums.py
from enum import Enum
from typing import Sequence, Union

from ..settings import RETRY_STRATEGY

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
    """Enum representing the types of job events."""
    
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
    """Enum representing the types of worker events."""
    
    STARTED = "started"
    STOPPED = "stopped"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    HEARTBEAT = "heartbeat"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_RESTORED = "connection_restored"

# Define retry strategies
# Normalize valid strategies to string values for consistent comparison and error messages
VALID_RETRY_STRATEGIES = {"linear", "exponential"}

# Define a type hint for retry delays (same as in job.py)
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]