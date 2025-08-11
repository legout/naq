# src/naq/models/__init__.py
"""
NAQ Models Package

This package contains all model classes and enums for the NAQ job queue system.
Models are organized into focused modules for better maintainability while
preserving backward compatibility through this init file.

The models are split into:
- enums: Status enums, event types, and constants
- jobs: Job and JobResult classes for job execution
- events: JobEvent and WorkerEvent classes for observability
- schedules: Schedule class for job scheduling
"""

# Import all models for backward compatibility
from .enums import (
    JOB_STATUS,
    JobEventType,
    WorkerEventType,
    RetryDelayType,
    VALID_RETRY_STRATEGIES,
)
from .jobs import Job, JobResult
from .events import JobEvent, WorkerEvent
from .schedules import Schedule

# Define explicit exports for clarity
__all__ = [
    # Status and event enums
    "JOB_STATUS",
    "JobEventType",
    "WorkerEventType", 
    "RetryDelayType",
    "VALID_RETRY_STRATEGIES",
    # Job models
    "Job",
    "JobResult",
    # Event models
    "JobEvent",
    "WorkerEvent",
    # Schedule models
    "Schedule",
]