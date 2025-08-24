# src/naq/__init__.py
# Core models
from .models.jobs import Job, JobResult, RetryDelayType
from .models.enums import JOB_STATUS, JobEventType, WorkerEventType
from .models.events import JobEvent, WorkerEvent
from .models.schedules import Schedule

# Core components
from .scheduler import Scheduler
from .worker.core import Worker
from .queue.core import Queue
from .settings import Config

# Connection management
from .connection import (
    nats_jetstream,
    nats_kv_store,
)

# Queue APIs
from .queue.async_api import (
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

from .queue.sync_api import (
    enqueue_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    schedule_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
    pause_scheduled_job_sync,
    resume_scheduled_job_sync,
    modify_scheduled_job_sync,
)

# Configuration
from .config import get_config, load_config

# Exceptions
from .exceptions import (
    NaqException,
    NaqConnectionError,
    JobExecutionError,
    SerializationError,
    ConfigurationError,
)

# Job results management
from .results import Results

# Worker monitoring
list_workers = Worker.list_workers
list_workers_sync = Worker.list_workers_sync

# Backward compatibility aliases
NAQError = NaqException

__version__ = "0.2.0"  # Bump version for modularization

__all__ = [
    # Core models
    "Job",
    "JobResult",
    "RetryDelayType",
    "JOB_STATUS",
    "JobEventType",
    "WorkerEventType",
    "JobEvent",
    "WorkerEvent",
    "Schedule",
    # Core components
    "Worker",
    "Scheduler",
    "Queue",
    "Config",
    # Connection management
    "nats_jetstream",
    "nats_kv_store",
    # Async queue API
    "enqueue",
    "enqueue_at",
    "enqueue_in",
    "schedule",
    "purge_queue",
    "cancel_scheduled_job",
    "pause_scheduled_job",
    "resume_scheduled_job",
    "modify_scheduled_job",
    # Sync queue API
    "enqueue_sync",
    "enqueue_at_sync",
    "enqueue_in_sync",
    "schedule_sync",
    "purge_queue_sync",
    "cancel_scheduled_job_sync",
    "pause_scheduled_job_sync",
    "resume_scheduled_job_sync",
    "modify_scheduled_job_sync",
    # Worker monitoring
    "list_workers",
    "list_workers_sync",
    # Configuration
    "get_config",
    "load_config",
    # Exceptions
    "NaqException",
    "NaqConnectionError",
    "JobExecutionError",
    "SerializationError",
    "ConfigurationError",
    # Backward compatibility aliases
    "NAQError",
    # Job results management
    "Results",
]