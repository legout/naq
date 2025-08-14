"""Base service classes for NAQ services.

This module provides the foundational classes for all NAQ services:
- BaseService: Abstract base class for all services with lifecycle management
- ServiceManager: Manager for service instances and dependencies
"""

import abc
import asyncio
import logging
from typing import Any, Dict, Optional, Set

from ..exceptions import NaqException

logger = logging.getLogger(__name__)


class BaseService(abc.ABC):
    """Abstract base class for all NAQ services.
    
    Provides consistent interface for service lifecycle management,
    configuration handling, and resource cleanup.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the base service.
        
        Args:
            config: Configuration dictionary for the service
        """
        self.config = config or {}
        self._initialized = False
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize the service.
        
        This method handles the initialization process including
        configuration validation and resource setup.
        """
        async with self._lock:
            if self._initialized:
                return
            
            try:
                await self._do_initialize()
                self._initialized = True
                logger.debug(f"{self.__class__.__name__} initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize {self.__class__.__name__}: {e}")
                raise
    
    @abc.abstractmethod
    async def _do_initialize(self) -> None:
        """Abstract method for service-specific initialization.
        
        This method should be implemented by subclasses to perform
        service-specific initialization tasks.
        """
        pass
    
    async def cleanup(self) -> None:
        """Clean up service resources.
        
        This method handles resource cleanup and should be called
        when the service is no longer needed.
        """
        async with self._lock:
            if not self._initialized:
                return
            
            try:
                await self._do_cleanup()
                self._initialized = False
                logger.debug(f"{self.__class__.__name__} cleaned up successfully")
            except Exception as e:
                logger.error(f"Error during cleanup of {self.__class__.__name__}: {e}")
                raise
    
    async def _do_cleanup(self) -> None:
        """Service-specific cleanup logic.
        
        This method can be overridden by subclasses to perform
        service-specific cleanup tasks.
        """
        pass
    
    async def __aenter__(self) -> "BaseService":
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.cleanup()


class ServiceManager:
    """Manager for service instances and dependencies.
    
    Handles service registration, retrieval, configuration, and
    dependency injection.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the service manager.
        
        Args:
            config: Global configuration for all services
        """
        self.config = config or {}
        self._services: Dict[str, BaseService] = {}
        self._service_types: Dict[str, type] = {}
        self._dependencies: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()
    
    def register_service_type(self, name: str, service_type: type) -> None:
        """Register a service type.
        
        Args:
            name: Name identifier for the service type
            service_type: The service class to register
        """
        if not issubclass(service_type, BaseService):
            raise NaqException(f"Service type {service_type} must inherit from BaseService")
        
        self._service_types[name] = service_type
        logger.debug(f"Registered service type: {name}")
    
    async def get_service(self, name: str, config: Optional[Dict[str, Any]] = None) -> BaseService:
        """Get a service instance by name.
        
        Args:
            name: Name of the service to retrieve
            config: Optional service-specific configuration
            
        Returns:
            The service instance
            
        Raises:
            NAQError: If service type is not registered
        """
        async with self._lock:
            if name not in self._services:
                if name not in self._service_types:
                    raise NaqException(f"Service type '{name}' not registered")
                
                service_config = self.config.get(name, {})
                if config:
                    service_config.update(config)
                
                # Create service instance - check if it accepts service_manager parameter
                try:
                    service = self._service_types[name](service_config, service_manager=self)
                except TypeError:
                    # Fallback to old constructor if service_manager parameter is not accepted
                    service = self._service_types[name](service_config)
                
                await service.initialize()
                self._services[name] = service
                logger.debug(f"Created and initialized service: {name}")
            
            return self._services[name]
    
    async def cleanup_all(self) -> None:
        """Clean up all managed services."""
        async with self._lock:
            for name, service in self._services.items():
                try:
                    await service.cleanup()
                    logger.debug(f"Cleaned up service: {name}")
                except Exception as e:
                    logger.error(f"Error cleaning up service {name}: {e}")
            
            self._services.clear()