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
from .models import Job, RetryDelayType, JOB_STATUS
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

from .settings import SCHEDULED_JOB_STATUS, WORKER_STATUS
from .worker import Worker

# Import events module to make it available via naq.events
from . import events

__version__ = "0.1.3"  # Bump version for worker monitoring


# Basic configuration/convenience
# setup_logging is now centralized in src/naq/utils.py



async def connect(url: str = DEFAULT_NATS_URL):
    """Convenience function to establish default NATS connection."""
    return await get_nats_connection(url=url)


async def disconnect():
    """Convenience function to close default NATS connection."""
    await close_nats_connection()


# --- Make result fetching available ---
# Expose static methods directly if desired, or users can use Job.fetch_result
# fetch_job_result = Job.fetch_result
# fetch_job_result_sync = Job.fetch_result_sync

# # Make Results class available for direct use
# Results = Results

# # --- Make worker listing available ---
# list_workers = Worker.list_workers
# list_workers_sync = Worker.list_workers_sync
