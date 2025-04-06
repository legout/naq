# src/naq/__init__.py
import logging
import asyncio
from typing import Optional

# Make key classes and functions available directly from the 'naq' package
from .queue import (
    Queue,
    enqueue,
    enqueue_at,
    enqueue_in,
    schedule,
    purge_queue,
    cancel_scheduled_job,
    pause_scheduled_job,
    resume_scheduled_job,
    modify_scheduled_job,
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
from .job import Job, RetryDelayType
from .worker import Worker
from .scheduler import Scheduler
from .exceptions import (
    NaqException, 
    ConnectionError, 
    ConfigurationError, 
    SerializationError, 
    JobExecutionError,
    JobNotFoundError,
)
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .settings import (
    SCHEDULED_JOB_STATUS_ACTIVE,
    SCHEDULED_JOB_STATUS_PAUSED,
    SCHEDULED_JOB_STATUS_FAILED,
)
from loguru import logger

__version__ = "0.1.0"  # Bump version for scheduler HA and management features

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
