"""Worker status management module.

This module provides functionality for managing worker status reporting, heartbeats,
and monitoring. It is responsible for tracking worker health and availability
through periodic status updates stored in a NATS Key-Value store.
"""

import asyncio
import os
import socket
import time
from typing import Any, Dict, List, Optional


from ..exceptions import NaqException
from ..models.enums import WORKER_STATUS
from ..services import KVStoreService, ServiceManager, ConnectionService
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    WORKER_KV_NAME,
)
from ..utils.logging import StructuredLogger
from ..utils.error_handling import ErrorHandler
from ..utils.decorators import timing, retry


class WorkerStatusManager:
    """
    Manages worker status reporting, heartbeats, and monitoring.

    This class is responsible for tracking worker health and availability through
    periodic status updates stored in a NATS Key-Value store. It provides methods
    for updating worker status, starting/stopping heartbeat loops, and listing
    active workers.
    """

    def __init__(self, worker, service_manager: Optional[ServiceManager] = None):
        """Initialize the worker status manager.

        Args:
            worker: The worker instance this status manager belongs to.
            service_manager: Optional ServiceManager for accessing services.
        """
        self.worker = worker
        self._service_manager = service_manager
        self._current_status = WORKER_STATUS.STARTING
        self._kv_store_service: Optional[KVStoreService] = None
        self._heartbeat_task = None
        self.logger = StructuredLogger(__name__)
        self.error_handler = ErrorHandler(self.logger)

    @timing
    @retry(max_attempts=3, delay=1.0, backoff="exponential")
    async def _get_kv_store_service(self) -> Optional[KVStoreService]:
        """Initialize and return the KVStoreService for worker statuses using the service layer."""
        if self._kv_store_service is None:
            try:
                if self._service_manager:
                    # Get KVStoreService from ServiceManager
                    self._kv_store_service = await self._service_manager.get_service(
                        "kv_store", KVStoreService
                    )
                else:
                    self.logger.error("ServiceManager not available, cannot get KVStoreService")
                    return None
            except Exception as e:
                self.error_handler.handle_error(
                    e,
                    {"operation": "initialize_kv_store"}
                )
                self._kv_store_service = None
        return self._kv_store_service

    @timing
    async def update_status(
        self, status: WORKER_STATUS | str, job_id: Optional[str] = None
    ) -> None:
        """Updates the worker's status in the KV store.

        Args:
            status: The new status of the worker.
            job_id: Optional job ID if the worker is busy with a specific job.
        """
        if isinstance(status, str):
            # Convert string to enum value (case-insensitive)
            try:
                status_value = status.lower()
                self._current_status = WORKER_STATUS(status_value)
            except ValueError:
                # If string conversion fails, default to IDLE
                self._current_status = WORKER_STATUS.IDLE
                self.logger.warning(
                    "Invalid status string '{status}', defaulting to IDLE",
                    status=status
                )
        else:
            self._current_status = status

        kv_store_service = await self._get_kv_store_service()
        if not kv_store_service:
            return

        payload = {
            "worker_id": self.worker.worker_id,
            "status": self._current_status.value,
            "timestamp": time.time(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
        }
        if job_id:
            payload["job_id"] = str(job_id)

        try:
            await kv_store_service.put(WORKER_KV_NAME, self.worker.worker_id, payload)
        except Exception as e:
            self.error_handler.handle_error(
                e,
                {"operation": "update_worker_status", "worker_id": self.worker.worker_id}
            )

    @timing
    async def _heartbeat(self) -> None:
        """Sends periodic heartbeat updates."""
        while True:
            await self.update_status(self._current_status)
            await asyncio.sleep(DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS)

    @timing
    async def start_heartbeat_loop(self) -> None:
        """Start the heartbeat loop."""
        if not self._heartbeat_task:
            self._heartbeat_task = asyncio.create_task(self._heartbeat())
            await self.update_status(WORKER_STATUS.IDLE)

    @timing
    async def stop_heartbeat_loop(self) -> None:
        """Stop the heartbeat loop."""
        if not self._heartbeat_task or self._heartbeat_task.done():
            return

        # Update status before canceling task to ensure it's captured
        try:
            await self.update_status(WORKER_STATUS.STOPPING)
        except Exception as e:
            self.error_handler.handle_error(
                e,
                {"operation": "update_status_shutdown"}
            )

        # Cancel and wait for task with proper exception handling
        self._heartbeat_task.cancel()
        try:
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        except Exception as e:
            self.error_handler.handle_error(
                e,
                {"operation": "heartbeat_task_shutdown"}
            )

    @timing
    async def unregister_worker(self) -> None:
        """Delete the worker's status entry from the KV store."""
        kv_store_service = await self._get_kv_store_service()
        if not kv_store_service:
            self.logger.warning(
                "Worker status KV store not available. Cannot unregister worker {worker_id}",
                worker_id=self.worker.worker_id
            )
            return

        try:
            await kv_store_service.delete(WORKER_KV_NAME, self.worker.worker_id)
            self.logger.info(
                "Unregistered worker {worker_id}",
                worker_id=self.worker.worker_id
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                {"operation": "unregister_worker", "worker_id": self.worker.worker_id}
            )

    @staticmethod
    @timing
    @retry(max_attempts=3, delay=1.0, backoff="exponential")
    async def list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lists active workers by querying the worker status KV store using the service layer.

        Args:
            nats_url: NATS server URL (if not using default).

        Returns:
            A list of dictionaries, each containing information about a worker.

        Raises:
            NaqException: For errors.
        """
        workers = []
        logger = StructuredLogger(__name__)
        error_handler = ErrorHandler(logger)
        service_manager = None

        try:
            # Use ServiceManager and KVStoreService
            from ..services import ServiceManager, ServiceConfig, KVStoreService
            
            # Create config with the provided URL
            config = ServiceConfig(nats_url=nats_url or DEFAULT_NATS_URL)
            service_manager = ServiceManager(config)
            
            # Register and initialize services
            try:
                await service_manager.register_service(
                    "connection", ConnectionService, config, initialize=True
                )
                kv_store_service = await service_manager.register_service(
                    "kv_store", KVStoreService, config, initialize=True
                )
            except Exception as e:
                error_handler.handle_error(
                    e, context={"operation": "register_services"}
                )
                raise NaqException(f"Error initializing services: {e}") from e

            # Get all keys from the KV store
            try:
                # Get all keys from the KV store
                # Note: KVStoreService doesn't have a direct keys method, so we need to use the underlying KeyValue store
                kv = await kv_store_service.get_kv_store(WORKER_KV_NAME)
                keys = await kv.keys()
                for key in keys:
                    try:
                        entry = await kv_store_service.get(WORKER_KV_NAME, key, deserialize=True)
                        if entry:
                            workers.append(entry)
                    except Exception as e:
                        key_str = key.decode() if isinstance(key, bytes) else str(key)
                        error_handler.handle_error(
                            e,
                            {"operation": "read_worker_data", "key_str": key_str}
                        )
            except Exception as e:
                # Only return empty list for KV store access issues
                # For other errors, raise NaqException
                if "not accessible" in str(e).lower() or "not found" in str(e).lower():
                    logger.warning(
                        "Worker status KV store '{kv_name}' not accessible: {error}",
                        kv_name=WORKER_KV_NAME,
                        error=str(e)
                    )
                    return []
                else:
                    raise NaqException(f"Error accessing worker status KV store: {e}") from e

            return workers

        except Exception as e:
            # Only return empty list for KV store access issues
            # For other errors, raise NaqException
            if "not accessible" in str(e).lower() or "not found" in str(e).lower():
                logger.warning(
                    "Worker status KV store '{kv_name}' not accessible: {error}",
                    kv_name=WORKER_KV_NAME,
                    error=str(e)
                )
                return []
            else:
                raise NaqException(f"Error listing workers: {e}") from e
        finally:
            # Clean up the service manager
            try:
                if service_manager is not None:
                    await service_manager.cleanup_all()
            except Exception as e:
                logger.warning("Error cleaning up service manager: {error}", error=str(e))
