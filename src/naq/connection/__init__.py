"""
NATS connection management utilities.
"""

# Import the legacy connection functions
from .legacy import (
    get_nats_connection,
    get_jetstream_context,
    close_nats_connection,
    close_all_connections,
    ensure_stream,
    ConnectionManager
)

# Import the new context managers
from .context_managers import (
    nats_connection,
    jetstream_context,
    nats_jetstream,
    nats_kv_store,
    Config
)

# Import connection utilities and monitoring
from .utils import (
    ConnectionMetrics,
    ConnectionMonitor,
    connection_monitor,
    test_nats_connection,
    wait_for_nats_connection
)

# Import connection decorators
from .decorators import (
    with_nats_connection,
    with_jetstream_context
)

__all__ = [
    # Legacy connection functions
    'get_nats_connection',
    'get_jetstream_context',
    'close_nats_connection',
    'close_all_connections',
    'ensure_stream',
    'ConnectionManager',
    
    # New context managers
    'nats_connection',
    'jetstream_context',
    'nats_jetstream',
    'nats_kv_store',
    'Config',
    
    # Connection utilities and monitoring
    'ConnectionMetrics',
    'ConnectionMonitor',
    'connection_monitor',
    'test_nats_connection',
    'wait_for_nats_connection',
    
    # Connection decorators
    'with_nats_connection',
    'with_jetstream_context'
]