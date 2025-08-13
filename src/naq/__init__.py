# src/naq/__init__.py
import asyncio
import logging
from typing import Optional

from loguru import logger

from .connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
)
from .settings import DEFAULT_NATS_URL
from .exceptions import (
    ConfigurationError,
    NaqConnectionError,
    JobExecutionError,
    JobNotFoundError,
    NaqException,
    SerializationError,
)
from .models.jobs import Job, RetryDelayType
from .results import Results

# Make key classes and functions available directly from the 'naq' package
from .queue import (
    Queue,
    ScheduledJobManager,
    cancel_scheduled_job,
    cancel_scheduled_job_sync,
    enqueue,
    enqueue_at,
    enqueue_at_sync,
    enqueue_in,
    enqueue_in_sync,
    enqueue_sync,
    modify_scheduled_job,
    modify_scheduled_job_sync,
    pause_scheduled_job,
    pause_scheduled_job_sync,
    purge_queue,
    purge_queue_sync,
    resume_scheduled_job,
    resume_scheduled_job_sync,
    schedule,
    schedule_sync,
    close_sync_connections,
)
#from .models import JOB_STATUS
from .scheduler import Scheduler

#from .models.enums import SCHEDULED_JOB_STATUS, WORKER_STATUS
from .worker import Worker

__version__ = "0.1.3"  # Bump version for worker monitoring

__all__ = [
    "Worker",
    "Scheduler",
    "JOB_STATUS",
    "SCHEDULED_JOB_STATUS",
    "WORKER_STATUS",
    "Job",
    "JobResult",
    "RetryDelayType",
    "Schedule",
    "get_nats_connection",
    "get_jetstream_context",
    "close_nats_connection",
]