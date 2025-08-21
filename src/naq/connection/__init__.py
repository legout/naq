"""
Connection management utilities for NAQ.

This package provides utilities for managing NATS connections, including
context managers for proper resource handling and connection lifecycle management.
"""

from .context_managers import nats_connection, jetstream_context, nats_jetstream
from .decorators import with_nats_connection, with_jetstream_context
from .manager import (
    ConnectionManager,
    close_nats_connection,
    close_all_connections,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from .utils import ConnectionMetrics, ConnectionMonitor, connection_monitor

__all__ = [
    "nats_connection",
    "jetstream_context",
    "nats_jetstream",
    "with_nats_connection",
    "with_jetstream_context",
    "ConnectionManager",
    "ConnectionMetrics",
    "ConnectionMonitor",
    "connection_monitor",
    "close_nats_connection",
    "close_all_connections",
    "ensure_stream",
    "get_jetstream_context",
    "get_nats_connection",
]
