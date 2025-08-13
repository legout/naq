# src/naq/queue/__init__.py
"""
NAQ Queue Package

This package provides queue functionality for NAQ, organized into focused modules:
- core: Main Queue class
- scheduled: ScheduledJobManager for scheduled job operations  
- async_api: High-level async functions
- sync_api: Synchronous wrapper functions

All public APIs are re-exported from this __init__.py for backward compatibility.
"""

# Import core classes
from .core import Queue
from .scheduled import ScheduledJobManager

# Import async API
from .async_api import (
    enqueue,
    enqueue_at,
    enqueue_in,
    schedule,
    purge_queue,
    cancel_scheduled_job,
    pause_scheduled_job,
    resume_scheduled_job,
    modify_scheduled_job,
)

# Import sync API  
from .sync_api import (
    enqueue_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    schedule_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
    pause_scheduled_job_sync,
    resume_scheduled_job_sync,
    modify_scheduled_job_sync,
    close_sync_connections,
)

__all__ = [
    # Classes
    "Queue",
    "ScheduledJobManager",
    # Async API
    "enqueue",
    "enqueue_at",
    "enqueue_in",
    "schedule",
    "purge_queue",
    "cancel_scheduled_job",
    "pause_scheduled_job",
    "resume_scheduled_job",
    "modify_scheduled_job",
    # Sync API
    "enqueue_sync",
    "enqueue_at_sync",
    "enqueue_in_sync",
    "schedule_sync",
    "purge_queue_sync",
    "cancel_scheduled_job_sync",
    "pause_scheduled_job_sync",
    "resume_scheduled_job_sync",
    "modify_scheduled_job_sync",
    "close_sync_connections",
]