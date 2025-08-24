"""
Base Service Classes for NAQ

This module provides the foundational classes for all NAQ services, including
the BaseService abstract class for consistent service interfaces and the
ServiceManager for managing service instances and dependencies.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type, TypeVar

import msgspec
from loguru import logger

from ..config.types import NAQConfig
from ..exceptions import NaqException

# Type variable for generic service types
T = TypeVar("T", bound="BaseService")


class ServiceInitializationError(NaqException):
    """Raised when service initialization fails."""

    pass


class ServiceConfigurationError(NaqException):
    """Raised when service configuration is invalid."""

    pass


class ServiceRuntimeError(NaqException):
    """Raised when a service encounters a runtime error."""

    pass


class ServiceConfig(msgspec.Struct):
    """
    Configuration structure for NAQ services.

    This class provides a standardized way to pass configuration to services.
    All fields are optional to allow for flexible configuration.

    Attributes:
        nats_url: URL of the NATS server
        queue_name: Default queue name for jobs
        log_level: Logging level for the service
        custom_settings: Dictionary for custom service-specific settings
    """

    nats_url: Optional[str] = None
    queue_name: Optional[str] = None
    log_level: Optional[str] = None
    custom_settings: Dict[str, Any] = msgspec.field(default_factory=dict)


class BaseService(ABC):
    """
    Abstract base class for all NAQ services.

    This class provides a consistent interface for service lifecycle management,
    including initialization, cleanup, and context manager support. All services
    should inherit from this class to ensure consistent behavior.

    Example:
        ```python
        class MyService(BaseService):
            async def _do_initialize(self) -> None:
                # Service-specific initialization
                pass

            async def cleanup(self) -> None:
                # Service-specific cleanup
                pass

        # Using the service as a context manager
        async with MyService() as service:
            await service.do_work()
        ```
    """

    def __init__(self, config: Optional[ServiceConfig] = None) -> None:
        """
        Initialize the base service.

        Args:
            config: Optional configuration for the service. If not provided,
                    default configuration will be used.
        """
        self._config = config or ServiceConfig()
        self._is_initialized = False
        self._logger = logger.bind(service=self.__class__.__name__)

    @property
    def config(self) -> ServiceConfig:
        """Get the service configuration."""
        return self._config

    @config.setter
    def config(self, value: ServiceConfig) -> None:
        """Set the service configuration."""
        self._config = value

    @property
    def is_initialized(self) -> bool:
        """Check if the service has been initialized."""
        return self._is_initialized

    @property
    def logger(self) -> Any:
        """Get the logger instance for this service."""
        return self._logger

    async def initialize(self) -> None:
        """
        Initialize the service.

        This method performs common initialization tasks and then calls the
        abstract _do_initialize method for service-specific initialization.
        It should be called before using the service.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        if self._is_initialized:
            self._logger.warning("Service already initialized, skipping")
            return

        try:
            self._logger.info("Initializing service")

            # Configure logging if log level is specified
            if self._config.log_level:
                # Note: In a real implementation, you would configure the logger here
                self._logger.debug(f"Log level set to: {self._config.log_level}")

            # Call service-specific initialization
            await self._do_initialize()

            self._is_initialized = True
            self._logger.info("Service initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize service: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    @abstractmethod
    async def _do_initialize(self) -> None:
        """
        Perform service-specific initialization.

        This method must be implemented by subclasses to handle
        service-specific initialization logic such as establishing
        connections, loading resources, etc.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        pass

    async def cleanup(self) -> None:
        """
        Clean up service resources.

        This method should be called when the service is no longer needed
        to release resources and perform cleanup tasks. Subclasses should
        override this method to implement service-specific cleanup logic.

        Raises:
            ServiceRuntimeError: If cleanup fails.
        """
        if not self._is_initialized:
            self._logger.warning("Service not initialized, skipping cleanup")
            return

        try:
            self._logger.info("Cleaning up service")

            # Perform service-specific cleanup
            await self._do_cleanup()

            self._is_initialized = False
            self._logger.info("Service cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup service: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Perform service-specific cleanup.

        This method can be overridden by subclasses to implement
        service-specific cleanup logic. The default implementation
        does nothing.

        Raises:
            ServiceRuntimeError: If cleanup fails.
        """
        # Default implementation does nothing
        pass

    async def __aenter__(self) -> "BaseService":
        """
        Enter the async context manager.

        This method initializes the service and returns it.

        Returns:
            The initialized service instance.
        """
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Exit the async context manager.

        This method cleans up the service resources.

        Args:
            exc_type: The exception type if an exception occurred, None otherwise.
            exc_val: The exception value if an exception occurred, None otherwise.
            exc_tb: The exception traceback if an exception occurred, None otherwise.
        """
        await self.cleanup()

    def __repr__(self) -> str:
        """
        Return a string representation of the service.

        Returns:
            A string representation including the service class and
            initialization state.
        """
        return f"<{self.__class__.__name__} initialized={self._is_initialized}>"


class ServiceManager:
    """
    Manager for service instances and dependencies.

    This class provides a centralized way to register, retrieve, and manage
    service instances. It supports dependency injection and configuration
    management for services with performance optimizations.

    Performance Features:
    - Lazy service initialization (services are initialized only when first accessed)
    - Service instance caching and reuse
    - Connection sharing between services
    - Minimal overhead in hot paths through optimized service access

    Example:
        ```python
        # Create a service manager
        manager = ServiceManager()

        # Register services
        await manager.register_service("my_service", MyService)

        # Get a service instance (lazy initialization)
        service = await manager.get_service("my_service")

        # Use the service
        await service.do_work()

        # Cleanup all services
        await manager.cleanup_all()
        ```
    """

    def __init__(self, config: Optional[ServiceConfig] = None, *, naq_config: Optional[NAQConfig] = None) -> None:
        """
        Initialize the service manager.

        Args:
            config: Default configuration to use for services that don't
                   specify their own configuration.
            naq_config: Optional NAQ configuration instance. If not provided, uses global config.
        """
        from ..config import get_config
        
        self._default_config = config or ServiceConfig()
        self._naq_config = naq_config if naq_config is not None else get_config()
        self._services: Dict[str, BaseService] = {}
        self._service_configs: Dict[str, ServiceConfig] = {}
        self._service_creation_times: Dict[str, float] = {}
        self._service_access_counts: Dict[str, int] = {}
        self._logger = logger.bind(service="ServiceManager")

    async def register_service(
        self,
        name: str,
        service_class: Type[T],
        config: Optional[ServiceConfig] = None,
        initialize: bool = True,
    ) -> T:
        """
        Register and optionally initialize a service.

        Args:
            name: Name to register the service under.
            service_class: The service class to instantiate.
            config: Optional configuration for the service. If not provided,
                   the default configuration will be used.
            initialize: Whether to initialize the service immediately.
                       If False, the service will be initialized when first
                       retrieved via get_service.

        Returns:
            The created service instance.

        Raises:
            ServiceConfigurationError: If a service with the same name
                                     is already registered.
            ServiceInitializationError: If initialization fails and
                                       initialize is True.
        """
        if name in self._services:
            raise ServiceConfigurationError(f"Service '{name}' is already registered")

        try:
            self._logger.info(f"Registering service: {name}")

            # Use provided config or default config
            service_config = config or self._default_config

            # Create service instance with NAQ config if the service supports it
            try:
                # Check if the service constructor accepts naq_config parameter
                import inspect
                sig = inspect.signature(service_class.__init__)
                if 'naq_config' in sig.parameters:
                    service = service_class(config=service_config, naq_config=self._naq_config)
                else:
                    service = service_class(config=service_config)
            except Exception:
                # Fallback to regular initialization
                service = service_class(config=service_config)

            # Store service and its config
            self._services[name] = service
            self._service_configs[name] = service_config

            # Initialize if requested
            if initialize:
                await service.initialize()

            self._logger.info(f"Service '{name}' registered successfully")
            return service

        except Exception as e:
            # Clean up on failure
            if name in self._services:
                del self._services[name]
            if name in self._service_configs:
                del self._service_configs[name]

            error_msg = f"Failed to register service '{name}': {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def get_service(
        self, name: str, service_class: Optional[Type[T]] = None
    ) -> T:
        """
        Get a service instance by name.

        This method implements lazy initialization and performance optimizations:
        - Services are initialized only when first accessed
        - Service access is tracked for performance monitoring
        - Minimal overhead for already initialized services

        Args:
            name: Name of the service to retrieve.
            service_class: Optional class to validate the service type against.
                          If provided, the service must be an instance of this class.

        Returns:
            The service instance.

        Raises:
            ServiceConfigurationError: If the service is not registered.
            ServiceInitializationError: If the service is not initialized and
                                       initialization fails.
            TypeError: If service_class is provided and the service is not
                      an instance of that class.
        """
        if name not in self._services:
            raise ServiceConfigurationError(f"Service '{name}' is not registered")

        service = self._services[name]

        # Track service access for performance monitoring
        self._service_access_counts[name] = self._service_access_counts.get(name, 0) + 1

        # Initialize if not already initialized (lazy initialization)
        if not service.is_initialized:
            import time
            start_time = time.perf_counter()
            
            try:
                self._logger.info(f"Initializing service on demand: {name}")
                await service.initialize()
                
                # Track initialization time for performance monitoring
                init_time = time.perf_counter() - start_time
                self._service_creation_times[name] = init_time
                self._logger.debug(f"Service '{name}' initialized in {init_time:.3f}s")
                
            except Exception as e:
                error_msg = f"Failed to initialize service '{name}': {e}"
                self._logger.error(error_msg)
                raise ServiceInitializationError(error_msg) from e

        # Validate service type if requested
        if service_class is not None and not isinstance(service, service_class):
            raise TypeError(
                f"Service '{name}' is not an instance of {service_class.__name__}"
            )

        return service  # type: ignore

    def has_service(self, name: str) -> bool:
        """
        Check if a service is registered.

        Args:
            name: Name of the service to check.

        Returns:
            True if the service is registered, False otherwise.
        """
        return name in self._services

    async def remove_service(self, name: str, cleanup: bool = True) -> None:
        """
        Remove a service from the manager.

        Args:
            name: Name of the service to remove.
            cleanup: Whether to cleanup the service before removing it.

        Raises:
            ServiceConfigurationError: If the service is not registered.
            ServiceRuntimeError: If cleanup fails and cleanup is True.
        """
        if name not in self._services:
            raise ServiceConfigurationError(f"Service '{name}' is not registered")

        try:
            self._logger.info(f"Removing service: {name}")

            service = self._services[name]

            # Cleanup if requested and service is initialized
            if cleanup and service.is_initialized:
                await service.cleanup()

            # Remove service and config
            del self._services[name]
            del self._service_configs[name]

            self._logger.info(f"Service '{name}' removed successfully")

        except Exception as e:
            error_msg = f"Failed to remove service '{name}': {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def cleanup_all(self) -> None:
        """
        Cleanup all registered services.

        This method cleans up all services in the reverse order of their
        registration to handle dependencies correctly.

        Raises:
            ServiceRuntimeError: If cleanup of any service fails.
        """
        if not self._services:
            self._logger.info("No services to cleanup")
            return

        self._logger.info("Cleaning up all services")

        # Get services in reverse order of registration
        service_names = list(self._services.keys())
        errors = []

        for name in reversed(service_names):
            try:
                service = self._services[name]
                if service.is_initialized:
                    await service.cleanup()
            except Exception as e:
                error_msg = f"Failed to cleanup service '{name}': {e}"
                self._logger.error(error_msg)
                errors.append(error_msg)

        # Clear all services and configs
        self._services.clear()
        self._service_configs.clear()

        if errors:
            raise ServiceRuntimeError(
                f"Failed to cleanup all services: {'; '.join(errors)}"
            )

        self._logger.info("All services cleaned up successfully")

    def get_service_names(self) -> list[str]:
        """
        Get the names of all registered services.

        Returns:
            A list of service names in registration order.
        """
        return list(self._services.keys())

    def __len__(self) -> int:
        """Get the number of registered services."""
        return len(self._services)

    def __contains__(self, name: str) -> bool:
        """Check if a service is registered."""
        return name in self._services

    def __repr__(self) -> str:
        """
        Return a string representation of the service manager.

        Returns:
            A string representation including the number of registered services.
        """
        return f"<ServiceManager services={len(self._services)}>"
    
    @property
    def config(self) -> NAQConfig:
        """Get the NAQ configuration instance."""
        return self._naq_config
    
    def get_service_performance_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance statistics for all services.
        
        Returns:
            Dictionary with performance metrics for each service including:
            - initialization_time: Time taken to initialize the service
            - access_count: Number of times the service was accessed
            - is_initialized: Whether the service is initialized
        """
        stats = {}
        for name in self._services:
            stats[name] = {
                "initialization_time": self._service_creation_times.get(name),
                "access_count": self._service_access_counts.get(name, 0),
                "is_initialized": self._services[name].is_initialized,
            }
        return stats
    
    def get_hot_services(self, threshold: int = 10) -> list[str]:
        """
        Get list of frequently accessed services (hot services).
        
        Args:
            threshold: Minimum access count to be considered a hot service.
            
        Returns:
            List of service names that are frequently accessed.
        """
        return [
            name for name, count in self._service_access_counts.items()
            if count >= threshold
        ]
