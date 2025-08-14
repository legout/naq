# src/naq/queue/__init__.py
"""
Queue package for naq.

This package provides functionality for working with job queues backed by NATS JetStream.
"""

# Core classes
from .core import Queue

# Async API functions
from .async_api import (
    cancel_scheduled_job,
    enqueue,
    enqueue_at,
    enqueue_in,
    modify_scheduled_job,
    pause_scheduled_job,
    purge_queue,
    resume_scheduled_job,
    schedule,
)

# Sync API functions
from .sync_api import (
    cancel_scheduled_job_sync,
    close_sync_connections,
    enqueue_at_sync,
    enqueue_in_sync,
    enqueue_sync,
    modify_scheduled_job_sync,
    pause_scheduled_job_sync,
    purge_queue_sync,
    resume_scheduled_job_sync,
    schedule_sync,
)

# Scheduled job management
from .scheduled import ScheduledJobManager

__all__ = [
    # Core classes
    "Queue",
    "ScheduledJobManager",
    
    # Async API functions
    "enqueue",
    "enqueue_at",
    "enqueue_in",
    "schedule",
    "purge_queue",
    "cancel_scheduled_job",
    "pause_scheduled_job",
    "resume_scheduled_job",
    "modify_scheduled_job",
    
    # Sync API functions
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