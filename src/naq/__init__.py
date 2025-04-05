# src/naq/__init__.py
import os
import asyncio

# Make key classes and functions available directly from the 'naq' package
from .queue import Queue, enqueue
from .job import Job
from .worker import Worker
from .exceptions import NaqException, ConnectionError, ConfigurationError, SerializationError, JobExecutionError
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection

__version__ = "0.0.1"

# Basic configuration/convenience
def setup_logging(level=logging.INFO):
    """Basic logging setup"""
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
