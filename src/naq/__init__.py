# src/naq/__init__.py
import importlib.metadata
import asyncio
import logging
from typing import Optional

from loguru import logger

from .connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
    nats_connection,
    Config,
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
from .models import RetryDelayType, JOB_STATUS
from .models import Job
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

# Import CLI app to make it available via naq.cli
from .cli import app

__version__ = importlib.metadata.version("naq")  # Bump version for worker monitoring


# Basic configuration/convenience
# setup_logging is now centralized in src/naq/utils.py



async def connect(url: str = DEFAULT_NATS_URL):
    """Convenience function to establish default NATS connection."""
    # Create a config with the specific NATS URL
    config = Config()
    config.nats.servers = [url]
    
    # Create a connection using the new connection approach
    import nats
    conn = await nats.connect(
        servers=config.nats.servers,
        name=config.nats.client_name,
        max_reconnect_attempts=config.nats.max_reconnect_attempts,
        reconnect_time_wait=config.nats.reconnect_time_wait,
    )
    return conn


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
