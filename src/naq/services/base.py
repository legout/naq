"""
NAQ Service Base Classes

This module provides the foundation for all services in the NAQ job queue system.
It defines the abstract base class for services and a manager for service registry
and dependency management.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import asyncio
from contextlib import asynccontextmanager

from ..exceptions import NaqException


class ServiceError(NaqException):
    """Raised when a service-related error occurs."""
    pass


class ServiceInitializationError(ServiceError):
    """Raised when service initialization fails."""
    pass


class ServiceDependencyError(ServiceError):
    """Raised when there's an issue with service dependencies."""
    pass


class BaseService(ABC):
    """Base class for all NAQ services."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the service."""
        if not self._initialized:
            await self._do_initialize()
            self._initialized = True
            
    @abstractmethod
    async def _do_initialize(self) -> None:
        """Implement service-specific initialization."""
        
    async def cleanup(self) -> None:
        """Cleanup service resources."""
        self._initialized = False
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()


class ServiceManager:
    """Manages service instances and dependencies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._services: Dict[str, BaseService] = {}
        
    async def get_service(self, service_type: type) -> BaseService:
        """Get or create service instance."""
        # Get the service name from the type
        service_name = service_type.__name__
        
        # Check if service already exists
        if service_name in self._services:
            return self._services[service_name]
        
        # Create new service instance
        try:
            # Handle service dependencies
            from .connection import ConnectionService
            from .streams import StreamService
            from .jobs import JobService
            from .kv_stores import KVStoreService
            
            # Special handling for services with dependencies
            if service_type == StreamService:
                # Get ConnectionService dependency
                connection_service = await self.get_service(ConnectionService)
                service = service_type(self.config, connection_service)
            elif service_type == JobService:
                # Get ConnectionService dependency
                connection_service = await self.get_service(ConnectionService)
                service = service_type(self.config, connection_service)
            elif service_type == KVStoreService:
                # Get ConnectionService dependency
                connection_service = await self.get_service(ConnectionService)
                service = service_type(self.config, connection_service)
            else:
                # Create service with config only
                service = service_type(self.config)
            
            self._services[service_name] = service
            
            # Initialize the service
            await service.initialize()
            
            return service
        except Exception as e:
            raise ServiceInitializationError(
                f"Failed to create and initialize service {service_name}: {e}"
            ) from e
    
    async def cleanup_all(self) -> None:
        """Clean up all registered services."""
        for service in self._services.values():
            try:
                await service.cleanup()
            except Exception as e:
                # Log error but continue with other services
                pass
        
        # Clear the service registry
        self._services.clear()
    
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup_all()
    
    def __repr__(self) -> str:
        """Return a string representation of the service manager."""
        return f"<ServiceManager services={len(self._services)}>"