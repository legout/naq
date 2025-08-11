# src/naq/models.py
"""
NAQ Models - Backward Compatibility Module

This module maintains backward compatibility by importing from the new
modular structure while providing the same API as before the refactoring.

All model classes have been split into focused modules:
- models.enums: Status enums, event types, and constants  
- models.jobs: Job and JobResult classes for job execution
- models.events: JobEvent and WorkerEvent classes for observability
- models.schedules: Schedule class for job scheduling

Import this module as before: from naq.models import Job, JOB_STATUS, etc.
"""

# Import everything from the new modular structure for backward compatibility
from .models import *

# Ensure all the original imports still work exactly as before
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