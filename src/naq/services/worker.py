"""
Worker Service for NAQ

This module provides a centralized service for managing worker instances,
including worker registration, status tracking, and lifecycle management.
"""

import time
from typing import Any, Dict, List, Optional

import msgspec

from ..config import get_config
from ..config.types import NAQConfig
from ..exceptions import NaqException
from ..models.enums import WorkerEventType, WORKER_STATUS
from ..models.events import WorkerEvent
from ..worker import Worker
from .base import (
    BaseService,
    ServiceConfig,
    ServiceInitializationError,
    ServiceRuntimeError,
)
from .connection import ConnectionService
from .events import EventService
from .kv_stores import KVStoreService


class WorkerServiceConfig(msgspec.Struct):
    """
    Configuration for the WorkerService.

    Attributes:
        concurrency: Number of concurrent jobs a worker can handle
        heartbeat_interval: Interval for worker heartbeats in seconds
        ttl: Default TTL for worker status in seconds
        max_job_duration: Maximum duration for a job in seconds
        shutdown_timeout: Timeout for worker shutdown in seconds
        workers_bucket_name: Name of the KV bucket for storing worker status
        default_worker_ttl: Default TTL for worker status in seconds
        enable_worker_registration: Whether to enable worker registration
        enable_event_logging: Whether to enable event logging
        auto_create_buckets: Whether to automatically create buckets
    """

    concurrency: int = 1
    heartbeat_interval: float = 30.0
    ttl: float = 120.0
    max_job_duration: float = 3600.0
    shutdown_timeout: float = 10.0
    workers_bucket_name: str = "naq_workers"
    default_worker_ttl: int = 300  # 5 minutes
    enable_worker_registration: bool = True
    enable_event_logging: bool = True
    auto_create_buckets: bool = True

    def as_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "concurrency": self.concurrency,
            "heartbeat_interval": self.heartbeat_interval,
            "ttl": self.ttl,
            "max_job_duration": self.max_job_duration,
            "shutdown_timeout": self.shutdown_timeout,
            "workers_bucket_name": self.workers_bucket_name,
            "default_worker_ttl": self.default_worker_ttl,
            "enable_worker_registration": self.enable_worker_registration,
            "enable_event_logging": self.enable_event_logging,
            "auto_create_buckets": self.auto_create_buckets,
        }


class WorkerService(BaseService):
    """
    Centralized worker management service.

    This service provides functionality for worker registration, status tracking,
    and lifecycle management with integrated event logging.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        *,
        naq_config: Optional[NAQConfig] = None,
        connection_service: Optional[ConnectionService] = None,
        kv_store_service: Optional[KVStoreService] = None,
        event_service: Optional[EventService] = None,
    ) -> None:
        """
        Initialize the worker service.

        Args:
            config: Optional configuration for the service.
            naq_config: Optional NAQ configuration instance. If not provided, uses global config.
            connection_service: Optional ConnectionService dependency.
            kv_store_service: Optional KVStoreService dependency.
            event_service: Optional EventService dependency.
        """
        super().__init__(config)
        # Store the NAQConfig instance
        self._naq_config = naq_config if naq_config is not None else get_config()
        # Extract worker-specific configuration
        self._worker_config = self._extract_worker_config()
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service
        self._workers: Dict[str, Worker] = {}

    def _extract_worker_config(self) -> WorkerServiceConfig:
        """
        Extract worker-specific configuration from the NAQ config.

        Returns:
            WorkerServiceConfig instance with worker parameters.
        """
        # Start with default config
        worker_config = WorkerServiceConfig()

        # Override with NAQ config workers settings if provided
        if self._naq_config and self._naq_config.workers:
            workers_config = self._naq_config.workers
            
            # Map fields from NAQ config to worker config
            if hasattr(workers_config, 'concurrency') and workers_config.concurrency is not None:
                worker_config.concurrency = int(workers_config.concurrency)
                
            if hasattr(workers_config, 'heartbeat_interval') and workers_config.heartbeat_interval is not None:
                worker_config.heartbeat_interval = float(workers_config.heartbeat_interval)
                
            if hasattr(workers_config, 'ttl') and workers_config.ttl is not None:
                worker_config.ttl = float(workers_config.ttl)
                
            if hasattr(workers_config, 'max_job_duration') and workers_config.max_job_duration is not None:
                worker_config.max_job_duration = float(workers_config.max_job_duration)
                
            if hasattr(workers_config, 'shutdown_timeout') and workers_config.shutdown_timeout is not None:
                worker_config.shutdown_timeout = float(workers_config.shutdown_timeout)

        # Override with service config if provided (for backward compatibility)
        if self._config and hasattr(self._config, 'custom_settings') and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "workers_bucket_name" in custom_settings:
                worker_config.workers_bucket_name = custom_settings[
                    "workers_bucket_name"
                ]

            if "default_worker_ttl" in custom_settings:
                worker_config.default_worker_ttl = custom_settings["default_worker_ttl"]

            if "enable_worker_registration" in custom_settings:
                worker_config.enable_worker_registration = custom_settings[
                    "enable_worker_registration"
                ]

            if "enable_event_logging" in custom_settings:
                worker_config.enable_event_logging = custom_settings[
                    "enable_event_logging"
                ]

            if "auto_create_buckets" in custom_settings:
                worker_config.auto_create_buckets = custom_settings[
                    "auto_create_buckets"
                ]

            if "heartbeat_interval" in custom_settings:
                worker_config.heartbeat_interval = custom_settings["heartbeat_interval"]

        return worker_config

    async def _do_initialize(self) -> None:
        """
        Initialize the worker service.

        This method validates the configuration and ensures the required
        services are available.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing WorkerService")

            # Validate configuration
            if self._worker_config.default_worker_ttl <= 0:
                raise ServiceInitializationError("default_worker_ttl must be positive")

            if self._worker_config.heartbeat_interval <= 0:
                raise ServiceInitializationError("heartbeat_interval must be positive")

            # Ensure connection service is available if other services are not provided
            if self._kv_store_service is None and self._connection_service is None:
                raise ServiceInitializationError(
                    "ConnectionService or KVStoreService is required"
                )

            # Ensure connection service is initialized if provided
            if (
                self._connection_service is not None
                and not self._connection_service.is_initialized
            ):
                await self._connection_service.initialize()

            # Create KV store service if not provided
            if self._kv_store_service is None:
                from .kv_stores import KVStoreService, KVStoreServiceConfig

                kv_config = KVStoreServiceConfig(
                    auto_create_buckets=self._worker_config.auto_create_buckets
                )
                self._kv_store_service = KVStoreService(
                    config=ServiceConfig(custom_settings=kv_config.as_dict()),
                    connection_service=self._connection_service,
                )
                await self._kv_store_service.initialize()

            # Create event service if not provided
            if self._event_service is None and self._worker_config.enable_event_logging:
                from .events import EventService, EventServiceConfig

                event_config = EventServiceConfig(
                    enable_event_logging=self._worker_config.enable_event_logging,
                    auto_create_bucket=self._worker_config.auto_create_buckets,
                )
                self._event_service = EventService(
                    config=ServiceConfig(custom_settings=event_config.as_dict()),
                    connection_service=self._connection_service,
                    kv_store_service=self._kv_store_service,
                )
                await self._event_service.initialize()

            self._logger.info("WorkerService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize WorkerService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up worker service resources.

        This method cleans up the services that were created by this service.
        """
        try:
            self._logger.info("Cleaning up WorkerService")

            # Note: We don't clean up externally provided services
            # Only clean up if we created the services
            if self._event_service is not None and self._connection_service is not None:
                await self._event_service.cleanup()

            if (
                self._kv_store_service is not None
                and self._connection_service is not None
            ):
                await self._kv_store_service.cleanup()

            self._logger.info("WorkerService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup WorkerService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def register_worker(self, worker: Worker) -> None:
        """
        Register a worker with the service.

        Args:
            worker: The Worker instance to register.

        Raises:
            NaqException: If registration fails.
        """
        if not self._worker_config.enable_worker_registration:
            return

        try:
            # Store worker status
            worker_key = f"worker:{worker.worker_id}:status"
            worker_status = {
                "worker_id": worker.worker_id,
                "status": worker.status.value,
                "queues": worker.queues,
                "current_job_id": worker.current_job_id,
                "last_heartbeat_utc": time.time(),
                "worker_name": worker.worker_name,
                "concurrency": worker.concurrency,
            }

            await self._kv_store_service.put(
                self._worker_config.workers_bucket_name,
                worker_key,
                worker_status,
                ttl=self._worker_config.default_worker_ttl,
                serialize=True,
            )

            # Track worker locally
            self._workers[worker.worker_id] = worker

            # Log worker registered event
            if self._event_service and self._worker_config.enable_event_logging:
                registered_event = WorkerEvent.registered(
                    worker_id=worker.worker_id, queue_names=worker.queues
                )
                await self._event_service.log_worker_event(registered_event)

            self._logger.info(f"Registered worker {worker.worker_id}")

        except Exception as e:
            error_msg = f"Failed to register worker {worker.worker_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def update_worker_status(self, worker_id: str, status: WORKER_STATUS) -> None:
        """
        Update the status of a worker.

        Args:
            worker_id: ID of the worker to update.
            status: New status for the worker.

        Raises:
            NaqException: If updating status fails.
        """
        try:
            worker_key = f"worker:{worker_id}:status"

            # Get current worker status
            try:
                current_status = await self._kv_store_service.get(
                    self._worker_config.workers_bucket_name,
                    worker_key,
                    deserialize=True,
                )
                if not isinstance(current_status, dict):
                    current_status = {}
            except NaqException:
                current_status = {}

            # Update status
            current_status["status"] = status.value
            current_status["last_heartbeat_utc"] = time.time()

            await self._kv_store_service.put(
                self._worker_config.workers_bucket_name,
                worker_key,
                current_status,
                ttl=self._worker_config.default_worker_ttl,
                serialize=True,
            )

            # Log worker status change event
            if self._event_service and self._worker_config.enable_event_logging:
                from ..models.events import WorkerEvent

                status_event = WorkerEvent(
                    worker_id=worker_id,
                    event_type=WorkerEventType.STATUS_CHANGED,
                    queue_names=current_status.get("queues", []),
                    timestamp=time.time(),
                )
                await self._event_service.log_worker_event(status_event)

            self._logger.debug(f"Updated worker {worker_id} status to {status.value}")

        except Exception as e:
            error_msg = f"Failed to update worker {worker_id} status: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def update_worker_heartbeat(self, worker_id: str) -> None:
        """
        Update the heartbeat timestamp for a worker.

        Args:
            worker_id: ID of the worker to update.

        Raises:
            NaqException: If updating heartbeat fails.
        """
        try:
            worker_key = f"worker:{worker_id}:status"

            # Get current worker status
            try:
                current_status = await self._kv_store_service.get(
                    self._worker_config.workers_bucket_name,
                    worker_key,
                    deserialize=True,
                )
                if not isinstance(current_status, dict):
                    current_status = {}
            except NaqException:
                current_status = {}

            # Update heartbeat
            current_status["last_heartbeat_utc"] = time.time()

            await self._kv_store_service.put(
                self._worker_config.workers_bucket_name,
                worker_key,
                current_status,
                ttl=self._worker_config.default_worker_ttl,
                serialize=True,
            )

            self._logger.debug(f"Updated worker {worker_id} heartbeat")

        except Exception as e:
            error_msg = f"Failed to update worker {worker_id} heartbeat: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def list_workers(self) -> List[Dict[str, Any]]:
        """
        List all registered workers.

        Returns:
            List of worker status dictionaries.

        Raises:
            NaqException: If listing workers fails.
        """
        try:
            # Get all worker keys
            kv = await self._kv_store_service.get_kv_store(
                self._worker_config.workers_bucket_name
            )
            keys = await kv.keys()

            workers = []
            for key_bytes in keys:
                key = (
                    key_bytes.decode("utf-8")
                    if isinstance(key_bytes, bytes)
                    else key_bytes
                )

                # Only process worker status keys
                if key.endswith(":status"):
                    try:
                        entry = await kv.get(key_bytes)
                        if entry is not None:
                            worker_status = msgspec.json.decode(entry.value)
                            workers.append(worker_status)
                    except Exception as e:
                        self._logger.warning(
                            f"Error reading worker status for key {key}: {e}"
                        )
                        continue

            return workers

        except Exception as e:
            error_msg = f"Failed to list workers: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def get_worker_status(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a specific worker.

        Args:
            worker_id: ID of the worker to retrieve.

        Returns:
            Worker status dictionary if found, None otherwise.

        Raises:
            NaqException: If retrieving worker status fails.
        """
        try:
            worker_key = f"worker:{worker_id}:status"

            try:
                worker_status = await self._kv_store_service.get(
                    self._worker_config.workers_bucket_name,
                    worker_key,
                    deserialize=True,
                )

                if isinstance(worker_status, dict):
                    return worker_status
                return None

            except NaqException:
                # Worker not found
                return None

        except Exception as e:
            error_msg = f"Failed to get worker status for {worker_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def unregister_worker(self, worker_id: str) -> bool:
        """
        Unregister a worker.

        Args:
            worker_id: ID of the worker to unregister.

        Returns:
            True if worker was unregistered, False if not found.

        Raises:
            NaqException: If unregistering worker fails.
        """
        try:
            worker_key = f"worker:{worker_id}:status"

            # Delete worker status
            deleted = await self._kv_store_service.delete(
                self._worker_config.workers_bucket_name, worker_key
            )

            if deleted:
                # Remove from local tracking
                self._workers.pop(worker_id, None)

                # Log worker unregistered event
                if self._event_service and self._worker_config.enable_event_logging:
                    from ..models.events import WorkerEvent

                    unregistered_event = WorkerEvent.unregistered(worker_id=worker_id)
                    await self._event_service.log_worker_event(unregistered_event)

                self._logger.info(f"Unregistered worker {worker_id}")

            return deleted

        except Exception as e:
            error_msg = f"Failed to unregister worker {worker_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    @property
    def worker_config(self) -> WorkerServiceConfig:
        """Get the worker service configuration."""
        return self._worker_config

    @property
    def is_worker_registration_enabled(self) -> bool:
        """Check if worker registration is enabled."""
        return self._worker_config.enable_worker_registration

    @property
    def is_event_logging_enabled(self) -> bool:
        """Check if event logging is enabled."""
        return self._worker_config.enable_event_logging
