# src/naq/queue/__init__.py
"""
Queue package for naq job queue system.

This package provides the core queue functionality split into focused modules:
- core: Main Queue class
- scheduled: ScheduledJobManager and schedule operations
- async_api: Async functions (enqueue, enqueue_at, etc.)
- sync_api: Sync wrapper functions

This module maintains backward compatibility by re-exporting all public APIs.
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