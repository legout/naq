"""
NAQ Services Package

This package contains the service infrastructure for the NAQ job queue system.
It provides base classes and management functionality for all services in the system.

The service infrastructure follows a consistent pattern with:
- BaseService abstract base class for all services
- ServiceManager for service registry and dependency management
- Proper lifecycle management with initialize/cleanup methods
- Context manager support for resource management
"""

from .base import BaseService, ServiceManager
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .streams import StreamService
from .jobs import JobService

__all__ = [
    "BaseService",
    "ServiceManager",
    "ConnectionService",
    "KVStoreService",
    "StreamService",
    "JobService",
]