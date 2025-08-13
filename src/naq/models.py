# src/naq/models.py
# This file is deprecated and will be removed in a future version.
# All models have been moved to the naq.models package:
# - Enums: naq.models.enums
# - Events: naq.models.events
# - Jobs: naq.models.jobs
# - Schedules: naq.models.schedules

# For backward compatibility, re-export the main classes
from .models.enums import (
    JOB_STATUS,
    JobEventType,
    WorkerEventType,
    VALID_RETRY_STRATEGIES,
    RetryDelayType,
)
from .models.events import JobEvent
from .models.jobs import Job, JobResult
from .models.schedules import Schedule

__all__ = [
    "JOB_STATUS",
    "JobEventType",
    "WorkerEventType",
    "VALID_RETRY_STRATEGIES",
    "RetryDelayType",
    "JobEvent",
    "Job",
    "JobResult",
    "Schedule",
]