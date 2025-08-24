"""Service context managers for NAQ.

This module provides context managers for managing service lifecycles in different
usage patterns: short-lived operations (like CLI commands) and long-lived components
(like Worker, Queue, Scheduler).
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from .services import ServiceManager, ServiceConfig
from .services.config import create_global_config, GlobalServiceConfig
from .utils.logging import StructuredLogger


@asynccontextmanager
async def service_context(
    nats_url: Optional[str] = None,
    config: Optional[ServiceConfig] = None,
    global_config: Optional[GlobalServiceConfig] = None,
    custom_settings: Optional[Dict[str, Any]] = None,
    logger_name: str = "naq.service_context"
):
    """Context manager for short-lived service operations.
    
    This context manager provides a clean way to manage services for short-lived
    operations like CLI commands, API calls, or other transient operations.
    It ensures proper initialization and cleanup of services.
    
    Args:
        nats_url: Optional NATS server URL. If not provided, uses default from config.
        config: Optional ServiceConfig instance. If not provided, creates one.
        global_config: Optional GlobalServiceConfig for additional configuration.
        custom_settings: Optional custom settings to merge with service config.
        logger_name: Name for the structured logger.
        
    Yields:
        ServiceManager: The configured service manager instance.
        
    Example:
        async with service_context(nats_url="nats://localhost:4222") as services:
            job_service = await services.get_service("job", JobService)
            # Use the service...
            # Services are automatically cleaned up when exiting the context
    """
    logger = StructuredLogger(logger_name)
    service_manager: Optional[ServiceManager] = None
    
    try:
        with logger.operation_context("service_context_enter"):
            # Create service config if not provided
            if config is None:
                if global_config is None:
                    global_config = create_global_config()
                
                config = ServiceConfig(
                    nats_url=nats_url or global_config.nats_url,
                    custom_settings=custom_settings or {}
                )
            elif custom_settings:
                # Merge custom settings with existing config
                config.custom_settings.update(custom_settings)
            
            # Create service manager
            service_manager = ServiceManager(config)
            
            # Register core services
            from .services.connection import ConnectionService
            from .services.jobs import JobService
            from .services.kv_stores import KVStoreService
            
            try:
                # Register connection service first
                await service_manager.register_service("connection", ConnectionService, initialize=True)
                
                # Get the connection service to pass to other services
                connection_service = await service_manager.get_service("connection", ConnectionService)
                
                # Create job service with connection service directly
                job_service = JobService(
                    config=service_manager._default_config,
                    naq_config=service_manager._naq_config,
                    connection_service=connection_service
                )
                
                # Manually register the already-created service
                service_manager._services["jobs"] = job_service
                service_manager._service_configs["jobs"] = service_manager._default_config
                
                # Now initialize the job service
                await job_service.initialize()
                
                # Create KV store service with connection service directly
                kv_service = KVStoreService(
                    config=service_manager._default_config,
                    naq_config=service_manager._naq_config,
                    connection_service=connection_service
                )
                
                # Manually register the already-created service
                service_manager._services["kv"] = kv_service
                service_manager._service_configs["kv"] = service_manager._default_config
                
                # Now initialize the KV store service
                await kv_service.initialize()
                
            except Exception as e:
                logger.error("Failed to register core services", error=str(e))
                raise
            
            logger.info(
                "Service context initialized",
                nats_url=config.nats_url,
                custom_settings=config.custom_settings
            )
            
            yield service_manager
            
    except Exception as e:
        logger.error(
            "Service context error",
            error=str(e),
            error_type=type(e).__name__
        )
        raise
    finally:
        if service_manager is not None:
            with logger.operation_context("service_context_exit"):
                await service_manager.cleanup_all()
                logger.info("Service context cleaned up")


@asynccontextmanager
async def long_lived_service_context(
    service_manager: ServiceManager,
    logger_name: str = "naq.long_lived_service_context"
):
    """Context manager for long-lived service components.
    
    This context manager is designed for long-lived components like Worker,
    Queue, or Scheduler that manage their own lifecycle but need to ensure
    services are properly initialized and available.
    
    Args:
        service_manager: The ServiceManager instance to use.
        logger_name: Name for the structured logger.
        
    Yields:
        ServiceManager: The same service manager instance.
        
    Example:
        # In a long-lived component like Worker:
        async with long_lived_service_context(self._service_manager) as services:
            # Services are guaranteed to be available
            connection_service = await services.get_service("connection", ConnectionService)
            # Use services for the component's lifetime
    """
    logger = StructuredLogger(logger_name)
    
    try:
        with logger.operation_context("long_lived_service_context_enter"):
            # Ensure all core services are initialized
            core_services = [
                ("connection", "ConnectionService"),
                ("stream", "StreamService"),
                ("kv_store", "KVStoreService"),
                ("job", "JobService"),
                ("event", "EventService"),
            ]
            
            for service_name, service_class_name in core_services:
                try:
                    # Try to get the service - this will initialize it if needed
                    await service_manager.get_service(service_name)
                    logger.debug("Core service available", service=service_name)
                except Exception as e:
                    logger.warning(
                        "Failed to initialize core service",
                        service=service_name,
                        error=str(e)
                    )
            
            logger.info("Long-lived service context initialized")
            yield service_manager
            
    except Exception as e:
        logger.error(
            "Long-lived service context error",
            error=str(e),
            error_type=type(e).__name__
        )
        raise
    finally:
        with logger.operation_context("long_lived_service_context_exit"):
            # For long-lived contexts, we don't cleanup the service manager
            # as it's managed by the component lifecycle
            logger.info("Long-lived service context exited")


def run_with_service_context(
    func,
    *args,
    nats_url: Optional[str] = None,
    config: Optional[ServiceConfig] = None,
    global_config: Optional[GlobalServiceConfig] = None,
    custom_settings: Optional[Dict[str, Any]] = None,
    logger_name: str = "naq.sync_service_context",
    **kwargs
):
    """Run a synchronous function with a service context.
    
    This function provides a synchronous wrapper around the async service context,
    making it easy to use service contexts in synchronous code like CLI commands
    or simple scripts.
    
    Args:
        func: The synchronous function to run.
        *args: Positional arguments to pass to the function.
        nats_url: Optional NATS server URL.
        config: Optional ServiceConfig instance.
        global_config: Optional GlobalServiceConfig for additional configuration.
        custom_settings: Optional custom settings to merge with service config.
        logger_name: Name for the structured logger.
        **kwargs: Keyword arguments to pass to the function.
        
    Returns:
        The result of the function.
        
    Example:
        def my_command(services, arg1, arg2):
            # Use services synchronously
            return do_something()
            
        result = run_with_service_context(
            my_command, 
            "value1", 
            "value2",
            nats_url="nats://localhost:4222"
        )
    """
    async def _async_wrapper():
        async with service_context(
            nats_url=nats_url,
            config=config,
            global_config=global_config,
            custom_settings=custom_settings,
            logger_name=logger_name
        ) as service_manager:
            # Pass the service manager as the first argument
            return await func(service_manager, *args, **kwargs)
    
    return asyncio.run(_async_wrapper())