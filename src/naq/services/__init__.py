# src/naq/services/__init__.py
"""
NAQ Service Layer

This package provides a centralized service layer for NAQ, eliminating code duplication
and providing consistent interfaces for common operations.

Services:
- ConnectionService: Centralized NATS connection management
- StreamService: JetStream stream operations
- KVStoreService: KeyValue store operations  
- JobService: Job execution and result services
- EventService: Event logging and processing
- SchedulerService: Scheduler operations

Usage:
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        result = await job_service.execute_job(job)
"""

from .base import (
    BaseService,
    ServiceManager,
    ServiceError,
    ServiceNotInitializedError,
    ServiceDependencyError,
    create_service_manager_from_config,
)
from .connection import ConnectionService
from .jobs import JobService
from .events import EventService
from .streams import StreamService
from .kv_stores import KVStoreService
from .scheduler import SchedulerService

__all__ = [
    # Base classes
    "BaseService",
    "ServiceManager",
    # Services
    "ConnectionService",
    "JobService", 
    "EventService",
    "StreamService",
    "KVStoreService",
    "SchedulerService",
    # Functions
    "create_service_manager_from_config",
    # Exceptions
    "ServiceError",
    "ServiceNotInitializedError", 
    "ServiceDependencyError",
]