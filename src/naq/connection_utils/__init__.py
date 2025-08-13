# src/naq/connection/__init__.py
"""
NATS Connection Management Package for NAQ.

This package provides comprehensive NATS connection management including:
- Context managers for safe resource handling
- Connection utilities for monitoring and testing
- Decorators for automatic connection injection
- Connection pooling and lifecycle management

The package supports both standalone context managers and service-based
connection management for maximum flexibility.
"""

# Core connection management (imported later to avoid circular imports)

# Context managers for standalone usage
from .context_managers import (
    nats_connection,
    jetstream_context,
    nats_jetstream,
    nats_kv_store,
    nats_object_store,
)

# Connection utilities and monitoring
from .utils import (
    ConnectionMetrics,
    ConnectionMonitor,
    connection_monitor,
    test_nats_connection,
    wait_for_nats_connection,
    ping_nats_server,
    get_nats_server_info,
    get_connection_metrics,
    reset_connection_metrics,
    diagnose_connection_issues,
)

# Function decorators
from .decorators import (
    with_nats_connection,
    with_jetstream_context,
    with_kv_store,
    connection_retry,
    nats_publisher,
    jetstream_publisher,
)

# Service-based connection management (from services package)
try:
    from ..services.connection import ConnectionService
    __all_services = ['ConnectionService']
except ImportError:
    __all_services = []

__all__ = [
    # Context managers
    'nats_connection',
    'jetstream_context',
    'nats_jetstream',
    'nats_kv_store',
    'nats_object_store',
    
    # Utilities and monitoring
    'ConnectionMetrics',
    'ConnectionMonitor',
    'connection_monitor',
    'test_nats_connection',
    'wait_for_nats_connection',
    'ping_nats_server',
    'get_nats_server_info',
    'get_connection_metrics',
    'reset_connection_metrics',
    'diagnose_connection_issues',
    
    # Decorators
    'with_nats_connection',
    'with_jetstream_context',
    'with_kv_store',
    'connection_retry',
    'nats_publisher',
    'jetstream_publisher',
] + __all_services

# Provide access to core connection management if needed
def get_connection_manager():
    """
    Get the singleton connection manager from parent module.
    
    Returns:
        ConnectionManager instance
    """
    from .. import connection as parent_conn
    return getattr(parent_conn, '_manager', None)

def get_nats_connection(*args, **kwargs):
    """Get NATS connection using parent module function."""
    from .. import connection as parent_conn
    return parent_conn.get_nats_connection(*args, **kwargs)

def get_jetstream_context(*args, **kwargs):
    """Get JetStream context using parent module function."""
    from .. import connection as parent_conn
    return parent_conn.get_jetstream_context(*args, **kwargs)

def close_nats_connection(*args, **kwargs):
    """Close NATS connection using parent module function."""
    from .. import connection as parent_conn
    return parent_conn.close_nats_connection(*args, **kwargs)

# Module-level convenience functions
async def health_check(config=None, timeout=10.0):
    """
    Perform a comprehensive connection health check.
    
    Args:
        config: Optional connection configuration
        timeout: Health check timeout in seconds
        
    Returns:
        Dictionary containing health check results
    """
    return await diagnose_connection_issues(config)


def get_manager():
    """
    Get the singleton connection manager instance.
    
    Returns:
        ConnectionManager instance
    """
    return get_connection_manager()


# Version and metadata
__version__ = '1.0.0'
__author__ = 'NAQ Team'
__description__ = 'Comprehensive NATS connection management for NAQ'