# src/naq/connection.py
"""
Central connection management module for NAQ.

This module provides backward compatibility functions and utilities for NATS connection
management, now integrated with the new connection context managers and utilities.
"""

import warnings
from typing import Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from .exceptions import NaqConnectionError
from .settings import DEFAULT_NATS_URL
from .connection import (
    context_managers,
    decorators,
    manager,
    utils,
)
from .services.config import GlobalServiceConfig

# Import the new connection management components
ConnectionManager = manager.ConnectionManager
ConnectionMetrics = utils.ConnectionMetrics
ConnectionMonitor = utils.ConnectionMonitor
connection_monitor = utils.connection_monitor

# Import context managers
nats_connection = context_managers.nats_connection
jetstream_context = context_managers.jetstream_context
nats_jetstream = context_managers.nats_jetstream
nats_kv_store = context_managers.nats_kv_store

# Import decorators
with_nats_connection = decorators.with_nats_connection
with_jetstream_context = decorators.with_jetstream_context

# Import utilities
test_nats_connection = utils.test_nats_connection
wait_for_nats_connection = utils.wait_for_nats_connection

# Create a singleton instance of the connection manager
_manager = ConnectionManager()


# Backward compatibility functions with deprecation warnings
async def get_nats_connection(
    url: str = DEFAULT_NATS_URL, *, prefer_thread_local: bool = False
) -> NATSClient:
    """
    Gets a NATS client connection, reusing if possible.

    .. deprecated::
        Use the `nats_connection` context manager instead for better resource management.
        This function will be removed in a future version.

    Args:
        url: NATS server URL
        prefer_thread_local: When True, reuse a thread-local connection (for sync helpers)

    Returns:
        A connected NATS client

    Raises:
        NaqConnectionError: If connection fails
    """
    warnings.warn(
        "get_nats_connection is deprecated. Use the nats_connection context manager instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return await _manager.get_connection(url, prefer_thread_local=prefer_thread_local)


async def get_jetstream_context(
    nc: Optional[NATSClient] = None, *, prefer_thread_local: bool = False
) -> JetStreamContext:
    """
    Gets a JetStream context from a NATS connection.

    .. deprecated::
        Use the `jetstream_context` or `nats_jetstream` context managers instead
        for better resource management. This function will be removed in a future version.

    Args:
        nc: Optional NATS client connection. If None, a new connection will be established.
        prefer_thread_local: When True, reuse a thread-local JS context (for sync helpers)

    Returns:
        A JetStream context

    Raises:
        NaqConnectionError: If getting JetStream context fails
    """
    warnings.warn(
        "get_jetstream_context is deprecated. Use the jetstream_context or "
        "nats_jetstream context managers instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if nc is not None:
        # If a connection is provided directly, use it
        try:
            return nc.jetstream()
        except Exception as e:
            raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e

    # Otherwise use the connection manager
    return await _manager.get_jetstream(prefer_thread_local=prefer_thread_local)


async def close_nats_connection(
    url: str = DEFAULT_NATS_URL, *, thread_local: bool = False
):
    """
    Closes a specific NATS connection.

    .. deprecated::
        Use the `nats_connection` context manager instead for automatic resource cleanup.
        This function will be removed in a future version.

    Args:
        url: NATS server URL to close
        thread_local: When True, closes the thread-local connection if present
    """
    warnings.warn(
        "close_nats_connection is deprecated. Use the nats_connection context manager "
        "instead for automatic resource cleanup.",
        DeprecationWarning,
        stacklevel=2,
    )
    await _manager.close_connection(url, thread_local=thread_local)


async def close_all_connections():
    """
    Closes all NATS connections managed by the connection pool.

    .. deprecated::
        Use the `nats_connection` context manager instead for automatic resource cleanup.
        This function will be removed in a future version.
    """
    warnings.warn(
        "close_all_connections is deprecated. Use the nats_connection context manager "
        "instead for automatic resource cleanup.",
        DeprecationWarning,
        stacklevel=2,
    )
    await _manager.close_all()


async def ensure_stream(
    js: Optional[JetStreamContext] = None,
    stream_name: str = "naq_jobs",  # Default stream name
    subjects: Optional[list[str]] = None,
    config: Optional[GlobalServiceConfig] = None,
) -> None:
    """
    Ensures a JetStream stream exists.

    .. deprecated::
        Use the `nats_jetstream` context manager instead for better resource management.
        This function will be removed in a future version.

    Args:
        js: Optional JetStream context. If None, a new one will be obtained.
        stream_name: Name of the stream to ensure exists
        subjects: List of subjects for the stream. If None, defaults to [f"{stream_name}.*"]
        config: Optional configuration object for the NATS connection.

    Raises:
        NaqConnectionError: If stream creation or verification fails
    """
    warnings.warn(
        "ensure_stream is deprecated. Use the nats_jetstream context manager instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if js is None:
        js = await get_jetstream_context()

    if subjects is None:
        subjects = [f"{stream_name}.*"]  # Default subject pattern

    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        logger.info(f"Stream '{stream_name}' already exists.")
    except nats.js.errors.NotFoundError:
        # Create stream if it doesn't exist
        logger.info(f"Stream '{stream_name}' not found, creating...")
        await js.add_stream(
            name=stream_name,
            subjects=subjects,
            storage=nats.js.api.StorageType.FILE,  # Use File storage
            retention=nats.js.api.RetentionPolicy.WORK_QUEUE,  # Consume then delete
        )
        logger.info(f"Stream '{stream_name}' created with subjects {subjects}.")
    except Exception as e:
        raise NaqConnectionError(f"Failed to ensure stream '{stream_name}': {e}") from e
