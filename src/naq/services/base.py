# src/naq/services/base.py
"""
Base service classes and service management for NAQ.

This module provides the foundation for all NAQ services, including lifecycle
management, dependency injection, and resource cleanup patterns.
"""

import asyncio
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Type, TypeVar

from loguru import logger

# Import the typed configuration
try:
    from ..config.types import NAQConfig
    _TYPED_CONFIG_AVAILABLE = True
except ImportError:
    _TYPED_CONFIG_AVAILABLE = False

T = TypeVar('T', bound='BaseService')


class BaseService(ABC):
    """
    Base class for all NAQ services.
    
    Provides common functionality for service lifecycle management,
    configuration handling, and resource cleanup.
    """

    def __init__(self, config):
        """
        Initialize the base service.
        
        Args:
            config: Service configuration (NAQConfig instance or dictionary for backward compatibility)
        """
        if _TYPED_CONFIG_AVAILABLE and hasattr(config, '__dataclass_fields__'):
            # New typed configuration
            self.config: 'NAQConfig' = config
            self._typed_config = True
        else:
            # Backward compatibility with dict config
            self.config: Dict[str, Any] = config
            self._typed_config = False
        
        self._initialized = False
        self._cleanup_tasks = []

    async def initialize(self) -> None:
        """Initialize the service if not already initialized."""
        if not self._initialized:
            logger.debug(f"Initializing service: {self.__class__.__name__}")
            await self._do_initialize()
            self._initialized = True
            logger.debug(f"Service initialized: {self.__class__.__name__}")

    @abstractmethod
    async def _do_initialize(self) -> None:
        """Implement service-specific initialization logic."""
        pass

    async def cleanup(self) -> None:
        """Clean up service resources."""
        if self._initialized:
            logger.debug(f"Cleaning up service: {self.__class__.__name__}")
            
            # Run cleanup tasks in reverse order
            for cleanup_task in reversed(self._cleanup_tasks):
                try:
                    if asyncio.iscoroutinefunction(cleanup_task):
                        await cleanup_task()
                    else:
                        cleanup_task()
                except Exception as e:
                    logger.warning(f"Error during cleanup: {e}")
            
            self._cleanup_tasks.clear()
            await self._do_cleanup()
            self._initialized = False
            logger.debug(f"Service cleanup complete: {self.__class__.__name__}")

    async def _do_cleanup(self) -> None:
        """Implement service-specific cleanup logic."""
        pass

    def add_cleanup_task(self, task):
        """Add a cleanup task to be executed during service shutdown."""
        self._cleanup_tasks.append(task)

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()

    @property
    def is_initialized(self) -> bool:
        """Check if the service is initialized."""
        return self._initialized

    def get_config_value(self, path: str, default=None):
        """
        Get configuration value using dot notation path.
        
        Works with both typed and dictionary configurations.
        
        Args:
            path: Configuration path (e.g., "nats.servers[0]", "workers.concurrency")
            default: Default value if path not found
            
        Returns:
            Configuration value or default
        """
        if self._typed_config:
            return self.config.get_nested(path, default)
        else:
            # Dictionary-based access for backward compatibility
            parts = path.split('.')
            current = self.config
            try:
                for part in parts:
                    if '[' in part and part.endswith(']'):
                        # Handle array access
                        attr_name = part[:part.index('[')]
                        index_str = part[part.index('[') + 1:-1]
                        index = int(index_str)
                        current = current[attr_name][index]
                    else:
                        current = current[part]
                return current
            except (KeyError, IndexError, ValueError, TypeError):
                return default

    @property
    def nats_config(self):
        """Get NATS configuration section."""
        if self._typed_config:
            return self.config.nats
        else:
            return self.config.get('nats', {})

    @property
    def workers_config(self):
        """Get workers configuration section."""
        if self._typed_config:
            return self.config.workers
        else:
            return self.config.get('workers', {})

    @property
    def events_config(self):
        """Get events configuration section."""
        if self._typed_config:
            return self.config.events
        else:
            return self.config.get('events', {})


class ServiceManager:
    """
    Manages service instances and their dependencies.
    
    Provides dependency injection, service registration, and lifecycle management
    for all NAQ services.
    """

    def __init__(self, config):
        """
        Initialize the service manager.
        
        Args:
            config: Global configuration (NAQConfig instance or dictionary for backward compatibility)
        """
        self.config = config
        self._services: Dict[Type[BaseService], BaseService] = {}
        self._service_configs: Dict[Type[BaseService], Any] = {}
        self._initialized = False

    def register_service(self, service_type: Type[T], service_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Register a service type with optional specific configuration.
        
        Args:
            service_type: The service class to register
            service_config: Optional service-specific configuration (only for dict-based configs)
        """
        if _TYPED_CONFIG_AVAILABLE and hasattr(self.config, '__dataclass_fields__'):
            # Use typed configuration directly
            self._service_configs[service_type] = self.config
        else:
            # Backward compatibility with dict configs
            if service_config is None:
                service_config = {}
            
            # Merge service-specific config with global config
            merged_config = {**self.config}
            merged_config.update(service_config)
            self._service_configs[service_type] = merged_config
        
        logger.debug(f"Registered service: {service_type.__name__}")

    async def get_service(self, service_type: Type[T]) -> T:
        """
        Get or create a service instance.
        
        Args:
            service_type: The service class to get or create
            
        Returns:
            The service instance
            
        Raises:
            ValueError: If service type is not registered
        """
        if service_type in self._services:
            return self._services[service_type]

        if service_type not in self._service_configs:
            # Auto-register with default config if not explicitly registered
            self.register_service(service_type)

        # Create service instance
        config = self._service_configs[service_type]
        service_instance = await self._create_service_instance(service_type, config)
        
        self._services[service_type] = service_instance
        return service_instance

    async def _create_service_instance(self, service_type: Type[T], config: Dict[str, Any]) -> T:
        """
        Create and initialize a service instance with dependency injection.
        
        Args:
            service_type: The service class to create
            config: Configuration for the service
            
        Returns:
            Initialized service instance
        """
        # Check if service constructor needs other services (basic dependency injection)
        import inspect
        
        constructor_params = inspect.signature(service_type.__init__).parameters
        kwargs = {'config': config}
        
        # Inject known service dependencies
        for param_name, param in constructor_params.items():
            if param_name in ('self', 'config'):
                continue
                
            # Check if parameter is a service type
            if (hasattr(param.annotation, '__module__') and 
                param.annotation.__module__ and
                'services' in param.annotation.__module__):
                
                dependency_service = await self.get_service(param.annotation)
                kwargs[param_name] = dependency_service

        service_instance = service_type(**kwargs)
        await service_instance.initialize()
        
        return service_instance

    async def initialize_all(self) -> None:
        """Initialize all registered services."""
        if self._initialized:
            return
            
        logger.debug("Initializing service manager")
        self._initialized = True

    async def cleanup_all(self) -> None:
        """Clean up all service instances."""
        if not self._initialized:
            return
            
        logger.debug("Cleaning up service manager")
        
        # Clean up services in reverse order of creation
        for service in reversed(list(self._services.values())):
            try:
                await service.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up service {service.__class__.__name__}: {e}")
        
        self._services.clear()
        self._initialized = False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize_all()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup_all()

    @asynccontextmanager
    async def service_scope(self, service_type: Type[T]):
        """
        Context manager for using a single service with automatic cleanup.
        
        Args:
            service_type: The service class to use
            
        Yields:
            The service instance
        """
        service = await self.get_service(service_type)
        try:
            yield service
        finally:
            # Service cleanup is handled by the service manager
            pass


def create_service_manager_from_config(config_path: Optional[str] = None):
    """
    Create a ServiceManager instance using the new typed configuration system.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        ServiceManager instance configured with typed configuration
    """
    if _TYPED_CONFIG_AVAILABLE:
        from ..config import load_config
        config = load_config(config_path)
        return ServiceManager(config)
    else:
        # Fallback to dictionary config
        logger.warning("Typed configuration not available, using default dict config")
        return ServiceManager({})


class ServiceError(Exception):
    """Base exception for service-related errors."""
    pass


class ServiceNotInitializedError(ServiceError):
    """Raised when attempting to use a service that hasn't been initialized."""
    pass


class ServiceDependencyError(ServiceError):
    """Raised when there's an issue with service dependencies."""
    pass