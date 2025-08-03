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
from .exceptions import (
    ConfigurationError,
    NaqConnectionError,
    JobExecutionError,
    JobNotFoundError,
    NaqException,
    SerializationError,
)
from .job import Job, RetryDelayType

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
from .settings import JOB_STATUS

from .settings import SCHEDULED_JOB_STATUS, WORKER_STATUS
from .worker import Worker

__version__ = "0.1.3"  # Bump version for worker monitoring


# Basic configuration/convenience
def setup_logging(level=logging.INFO):
    """Basic logging setup using loguru."""
    logger.add(
        "naq_{time}.log",  # File to log to (optional)
        rotation="10 MB",  # Rotate log file every 10 MB (optional)
        level=level,  # Set the logging level
        format="{time} - {name} - {level} - {message}",  # Customize the format
    )


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


async def connect(url: Optional[str] = None):
    """Convenience function to establish default NATS connection."""
    return await get_nats_connection(url=url)


async def disconnect():
    """Convenience function to close default NATS connection."""
    await close_nats_connection()


# --- Make result fetching available ---
# Expose static methods directly if desired, or users can use Job.fetch_result
fetch_job_result = Job.fetch_result
fetch_job_result_sync = Job.fetch_result_sync

# --- Make worker listing available ---
list_workers = Worker.list_workers
list_workers_sync = Worker.list_workers_sync
