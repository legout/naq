"""Service context managers for NAQ.

This module provides context managers for managing service lifecycles in different
usage patterns: short-lived operations (like CLI commands) and long-lived components
(like Worker, Queue, Scheduler).
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from .services.base import (
    ServiceManager,
    ServiceConfig,
    ServiceInitializationError,
    ServiceConfigurationError,
    ServiceRuntimeError,
)
from .exceptions import NaqException
from .services.config import create_global_config, GlobalServiceConfig
from .services.connection import ConnectionService
from .services.events import EventService
from .services.jobs import JobService
from .services.kv_stores import KVStoreService
from .services.streams import StreamService
from loguru import logger


async def _prepare_service_config(
    config: Optional[ServiceConfig],
    global_config: Optional[GlobalServiceConfig],
    nats_url: Optional[str],
    custom_settings: Optional[Dict[str, Any]],
) -> ServiceConfig:
    """Prepare service configuration.

    Args:
        config: Optional ServiceConfig instance.
        global_config: Optional GlobalServiceConfig for additional configuration.
        nats_url: Optional NATS server URL.
        custom_settings: Optional custom settings to merge with service config.

    Returns:
        ServiceConfig: The prepared service configuration.
    """
    if config is None:
        if global_config is None:
            global_config = create_global_config()
        config = ServiceConfig(
            nats_url=nats_url or global_config.nats_url,
            custom_settings=custom_settings or {},
        )
    elif custom_settings:
        config.custom_settings.update(custom_settings)
    return config


async def _register_core_services(service_manager: ServiceManager, log):
    """Register all core NAQ services with the service manager."""
    # Register connection service first
    await service_manager.register_service(
        "connection", ConnectionService, initialize=True
    )
    log.debug("Registered and initialized service: connection")

    # Get the connection service to pass to other services
    connection_service = await service_manager.get_service(
        "connection", ConnectionService
    )

    # Register stream service with connection service
    await service_manager.register_service(
        "stream", StreamService, initialize=True, connection_service=connection_service
    )
    log.debug("Registered and initialized service: stream")

    # Register KV store service with connection service
    await service_manager.register_service(
        "kv", KVStoreService, initialize=True, connection_service=connection_service
    )
    log.debug("Registered and initialized service: kv")

    # Register job service with connection service
    await service_manager.register_service(
        "job", JobService, initialize=True, connection_service=connection_service
    )
    log.debug("Registered and initialized service: job")

    # Register event service with connection service
    await service_manager.register_service(
        "event", EventService, initialize=True, connection_service=connection_service
    )
    log.debug("Registered and initialized service: event")

    # Register 'kv_store' alias for backward compatibility
    service_manager.add_alias("kv_store", "kv")


@asynccontextmanager
async def service_context(
    nats_url: Optional[str] = None,
    config: Optional[ServiceConfig] = None,
    global_config: Optional[GlobalServiceConfig] = None,
    custom_settings: Optional[Dict[str, Any]] = None,
    logger_name: str = "naq.service_context",
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
    log = logger.bind(name=logger_name)
    service_manager: Optional[ServiceManager] = None

    try:
        start_time = time.perf_counter()
        log.info(
            "Starting operation: service_context_enter",
            operation="service_context_enter",
            status="started",
        )
        # Create service config using helper function
        config = await _prepare_service_config(
            config, global_config, nats_url, custom_settings
        )

        # Create service manager
        service_manager = ServiceManager(config)

        # Register core services

        try:
            # Register all core services
            await _register_core_services(service_manager, log)
        except (ServiceInitializationError, ServiceConfigurationError) as e:
            log.error(
                "Failed to register core services",
                error=str(e),
                error_type=type(e).__name__,
            )
            # If it's a ServiceInitializationError wrapping another exception, unwrap it
            if isinstance(e, ServiceInitializationError) and e.__cause__ is not None:
                raise e.__cause__
            raise

            log.info(
                "Service context initialized",
                nats_url=config.nats_url,
                custom_settings=config.custom_settings,
            )

            # Log successful completion of enter operation
            duration = time.perf_counter() - start_time
            log.info(
                "Completed operation: service_context_enter",
                operation="service_context_enter",
                status="completed",
                duration_seconds=duration,
            )

        yield service_manager

    except (
        ServiceInitializationError,
        ServiceConfigurationError,
        ServiceRuntimeError,
        NaqException,
    ) as e:
        log.error("Service context error", error=str(e), error_type=type(e).__name__)
        # Log failure of enter operation
        duration = time.perf_counter() - start_time
        log.error(
            "Failed operation: service_context_enter",
            operation="service_context_enter",
            status="failed",
            duration_seconds=duration,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise
    except Exception as e:
        log.error(
            "Unexpected error in service context",
            error=str(e),
            error_type=type(e).__name__,
        )
        # Log failure of enter operation
        duration = time.perf_counter() - start_time
        log.error(
            "Failed operation: service_context_enter",
            operation="service_context_enter",
            status="failed",
            duration_seconds=duration,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise
    finally:
        if service_manager is not None:
            start_time = time.perf_counter()
            log.info(
                "Starting operation: service_context_exit",
                operation="service_context_exit",
                status="started",
            )
            try:
                await service_manager.cleanup_all()
                log.info("Service context cleaned up")
                # Log successful completion of exit operation
                duration = time.perf_counter() - start_time
                log.info(
                    "Completed operation: service_context_exit",
                    operation="service_context_exit",
                    status="completed",
                    duration_seconds=duration,
                )
            except Exception as e:
                # Log failure of exit operation
                duration = time.perf_counter() - start_time
                log.error(
                    "Failed operation: service_context_exit",
                    operation="service_context_exit",
                    status="failed",
                    duration_seconds=duration,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise


@asynccontextmanager
async def long_lived_service_context(
    service_manager: ServiceManager, logger_name: str = "naq.long_lived_service_context"
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
    log = logger.bind(name=logger_name)

    try:
        start_time = time.perf_counter()
        log.info(
            "Starting operation: long_lived_service_context_enter",
            operation="long_lived_service_context_enter",
            status="started",
        )
        # Ensure all core services are initialized
        core_services = [
            ("connection", "ConnectionService"),
            ("stream", "StreamService"),
            (
                "kv",
                "KVStoreService",
            ),  # Use 'kv' instead of 'kv_store' as that's the actual service name
            ("job", "JobService"),
            ("event", "EventService"),
        ]
        for service_name, service_class_name in core_services:
            # Try to get the service - this will initialize it if needed
            log.debug(
                "Attempting to get core service",
                service=service_name,
                available_services=service_manager.get_service_names(),
            )
            await service_manager.get_service(service_name)
            log.debug("Core service available", service=service_name)

            log.info("Long-lived service context initialized")
            # Log successful completion of enter operation
            duration = time.perf_counter() - start_time
            log.info(
                "Completed operation: long_lived_service_context_enter",
                operation="long_lived_service_context_enter",
                status="completed",
                duration_seconds=duration,
            )
        yield service_manager

    except (
        ServiceInitializationError,
        ServiceConfigurationError,
        ServiceRuntimeError,
        NaqException,
    ) as e:
        log.error(
            "Long-lived service context error",
            error=str(e),
            error_type=type(e).__name__,
        )
        # Log failure of enter operation
        duration = time.perf_counter() - start_time
        log.error(
            "Failed operation: long_lived_service_context_enter",
            operation="long_lived_service_context_enter",
            status="failed",
            duration_seconds=duration,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise
    except Exception as e:
        log.error(
            "Unexpected error in long-lived service context",
            error=str(e),
            error_type=type(e).__name__,
        )
        # Log failure of enter operation
        duration = time.perf_counter() - start_time
        log.error(
            "Failed operation: long_lived_service_context_enter",
            operation="long_lived_service_context_enter",
            status="failed",
            duration_seconds=duration,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise
    finally:
        start_time = time.perf_counter()
        log.info(
            "Starting operation: long_lived_service_context_exit",
            operation="long_lived_service_context_exit",
            status="started",
        )
        try:
            # For long-lived contexts, we don't cleanup the service manager
            # as it's managed by the component lifecycle
            log.info("Long-lived service context exited")
            # Log successful completion of exit operation
            duration = time.perf_counter() - start_time
            log.info(
                "Completed operation: long_lived_service_context_exit",
                operation="long_lived_service_context_exit",
                status="completed",
                duration_seconds=duration,
            )
        except Exception as e:
            # Log failure of exit operation
            duration = time.perf_counter() - start_time
            log.error(
                "Failed operation: long_lived_service_context_exit",
                operation="long_lived_service_context_exit",
                status="failed",
                duration_seconds=duration,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise


def run_with_service_context(
    func,
    *args,
    nats_url: Optional[str] = None,
    config: Optional[ServiceConfig] = None,
    global_config: Optional[GlobalServiceConfig] = None,
    custom_settings: Optional[Dict[str, Any]] = None,
    logger_name: str = "naq.sync_service_context",
    **kwargs,
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
            logger_name=logger_name,
        ) as service_manager:
            # Pass the service manager as the first argument
            return func(service_manager, *args, **kwargs)

    # Check if we're already in an event loop
    try:
        asyncio.get_running_loop()
        # If we're in a running loop, we need to run in a separate thread
        import threading

        result = None
        exception = None

        def _run_in_thread():
            nonlocal result, exception
            try:
                result = asyncio.run(_async_wrapper())
            except Exception as e:
                exception = e

        thread = threading.Thread(target=_run_in_thread)
        thread.start()
        thread.join(timeout=30)  # Wait up to 30 seconds

        if exception:
            raise exception

        return result
    except RuntimeError:
        # No running event loop, we can use asyncio.run
        return asyncio.run(_async_wrapper())
