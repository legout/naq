# src/naq/__init__.py
import asyncio
import logging
from typing import Optional

from loguru import logger

# Import from new locations but expose as before
from .models.jobs import Job, JobResult
from .models.enums import JOB_STATUS, JobEventType, WorkerEventType, RetryDelayType
from .models.events import JobEvent, WorkerEvent
from .models.schedules import Schedule

from .connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
)
from .settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME, SCHEDULED_JOB_STATUS, WORKER_STATUS
from .exceptions import (
    ConfigurationError,
    NaqConnectionError,
    JobExecutionError,
    JobNotFoundError,
    NaqException,
    SerializationError,
)
from .results import Results

# Make key classes and functions available directly from the 'naq' package
from .queue import (
    Queue,
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
)
from .scheduler import Scheduler
from .worker import Worker

# Import events module for event logging capabilities
from . import events

# Configuration (new)
from .config import get_config, load_config

__version__ = "0.2.0"  # Bump version for major refactoring


# Basic configuration/convenience
# setup_logging is now centralized in src/naq/utils.py

# Global connection management (optional convenience)
_default_loop = None


def _get_loop():
    global _default_loop
    if _default_loop is None:
        try:
            _default_loop = asyncio.get_running_loop()
        except RuntimeError:
            _default_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_default_loop)
    return _default_loop


async def connect(url: str = DEFAULT_NATS_URL):
    """Convenience function to establish default NATS connection."""
    return await get_nats_connection(url=url)


async def disconnect():
    """Convenience function to close default NATS connection."""
    await close_nats_connection()


# Existing convenience functions maintained
fetch_job_result = Job.fetch_result
fetch_job_result_sync = Job.fetch_result_sync
list_workers = Worker.list_workers
list_workers_sync = Worker.list_workers_sync
