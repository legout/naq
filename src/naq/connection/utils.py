# Backward compatibility imports from nats_helpers
from ..utils.nats_helpers import (
    ConnectionMetrics,
    ConnectionMonitor,
    connection_monitor,
    test_nats_connection,
    test_nats_connection_sync,
    wait_for_nats_connection,
    wait_for_nats_connection_sync,
    get_connection_metrics,
    reset_connection_metrics,
    is_jetstream_enabled,
    is_jetstream_enabled_sync,
)

# Re-export for backward compatibility
__all__ = [
    "ConnectionMetrics",
    "ConnectionMonitor",
    "connection_monitor",
    "test_nats_connection",
    "test_nats_connection_sync",
    "wait_for_nats_connection",
    "wait_for_nats_connection_sync",
    "get_connection_metrics",
    "reset_connection_metrics",
    "is_jetstream_enabled",
    "is_jetstream_enabled_sync",
]