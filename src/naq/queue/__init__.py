"""NAQ Queue Package.

This package provides queue management functionality for the NAQ job queue system.
"""

# Import all functions from the unified API
from .api import (
    # Async functions
    cancel_scheduled_job,
    enqueue,
    enqueue_at,
    enqueue_in,
    modify_scheduled_job,
    pause_scheduled_job,
    purge_queue,
    resume_scheduled_job,
    schedule,
    # Sync functions
    cancel_scheduled_job_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    enqueue_sync,
    modify_scheduled_job_sync,
    pause_scheduled_job_sync,
    purge_queue_sync,
    resume_scheduled_job_sync,
    schedule_sync,
)
from .core import Queue
from .scheduled import ScheduledJobManager

__all__ = [
    # Classes
    "Queue",
    "ScheduledJobManager",
    # Async functions
    "enqueue",
    "enqueue_at",
    "enqueue_in",
    "schedule",
    "purge_queue",
    "cancel_scheduled_job",
    "pause_scheduled_job",
    "resume_scheduled_job",
    "modify_scheduled_job",
    # Sync functions
    "enqueue_sync",
    "enqueue_at_sync",
    "enqueue_in_sync",
    "schedule_sync",
    "purge_queue_sync",
    "cancel_scheduled_job_sync",
    "pause_scheduled_job_sync",
    "resume_scheduled_job_sync",
    "modify_scheduled_job_sync",
]
