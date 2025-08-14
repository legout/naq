"""
NAQ Service Infrastructure

This package provides the foundational classes for all NAQ services, including
base service classes and service management functionality.
"""

from .base import BaseService, ServiceManager, ServiceConfig
from .config import (
    GlobalServiceConfig,
    ConnectionServiceConfig,
    JobServiceConfig,
    WorkerServiceConfig,
    SchedulerServiceConfig,
    StreamServiceConfig,
    KVStoreServiceConfig,
    EventServiceConfig,
    create_global_config,
    create_config_from_env,
    merge_configs,
)
from .connection import ConnectionService
from .streams import StreamService
from .kv_stores import KVStoreService, KVTransaction
from .events import EventService
from .jobs import JobService
from .scheduler import SchedulerService
from .worker import WorkerService

__all__ = [
    "BaseService",
    "ServiceConfig",
    "ServiceManager",
    "GlobalServiceConfig",
    "ConnectionService",
    "ConnectionServiceConfig",
    "StreamService",
    "StreamServiceConfig",
    "KVStoreService",
    "KVStoreServiceConfig",
    "KVTransaction",
    "EventService",
    "EventServiceConfig",
    "JobService",
    "JobServiceConfig",
    "SchedulerService",
    "SchedulerServiceConfig",
    "WorkerService",
    "WorkerServiceConfig",
    "create_global_config",
    "create_config_from_env",
    "merge_configs",
]
