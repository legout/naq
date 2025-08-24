# src/naq/__init__.py
from .connection import (
    nats_jetstream,
    nats_kv_store,
)
from .models.jobs import Job, RetryDelayType
from .scheduler import Scheduler
from .worker import Worker

# Import worker monitoring functions
from .worker.monitoring import list_workers, list_workers_sync

# Import synchronous API functions
from .sync_api import (
    enqueue_job_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
    list_workers_sync as list_workers_sync_api,
)

__version__ = "0.1.4"  # Bump version for service context patterns

__all__ = [
    "Worker",
    "Scheduler",
    "Job",
    "RetryDelayType",
    "nats_jetstream",
    "nats_kv_store",
    "list_workers",
    "list_workers_sync",
    # Synchronous API functions
    "enqueue_job_sync",
    "enqueue_at_sync",
    "enqueue_in_sync",
    "purge_queue_sync",
    "cancel_scheduled_job_sync",
    "list_workers_sync_api",
]