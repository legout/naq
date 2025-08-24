"""Worker monitoring module.

This module provides functionality for monitoring workers.
"""

from typing import Any, Dict, List, Optional

import cloudpickle

from ..services import KVStoreService, ServiceConfig, ServiceManager
from ..settings import DEFAULT_NATS_URL, WORKER_KV_NAME
from ..utils import run_async_from_sync
from ..utils.decorators import timing
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger


class WorkerMonitor:
    """Provides methods for monitoring workers using the service layer."""

    def __init__(
        self,
        service_manager: Optional[ServiceManager] = None,
        nats_url: Optional[str] = None,
    ):
        """Initialize the worker monitor.

        Args:
            service_manager: Optional ServiceManager for accessing services.
            nats_url: NATS server URL. If not provided, uses default.
        """
        self._service_manager = service_manager
        self._nats_url = nats_url or DEFAULT_NATS_URL
        self._logger = StructuredLogger("WorkerMonitor")
        self._error_handler = ErrorHandler("WorkerMonitor")

    @timing()
    async def _get_kv_store_service(self) -> KVStoreService:
        """Get or create the KVStoreService for worker status operations."""
        if self._service_manager:
            # Get KVStoreService from ServiceManager
            if not self._service_manager.has_service("kv_store"):
                # Register KVStoreService if not already registered
                config = ServiceConfig(nats_url=self._nats_url)
                try:
                    await self._service_manager.register_service(
                        "kv_store", KVStoreService, config, initialize=True
                    )
                except Exception as e:
                    self._error_handler.handle_error(
                        e, context={"operation": "register_kv_store_service"}
                    )
            return await self._service_manager.get_service("kv_store", KVStoreService)
        else:
            # Create a temporary KVStoreService for backward compatibility
            from ..services import ConnectionService

            config = ServiceConfig(nats_url=self._nats_url)
            service_manager = ServiceManager(config)

            # Register and initialize services
            try:
                await service_manager.register_service(
                    "connection", ConnectionService, config, initialize=True
                )
            except Exception as e:
                self._error_handler.handle_error(
                    e, context={"operation": "register_connection_service"}
                )

            try:
                kv_store_service = await service_manager.register_service(
                    "kv_store", KVStoreService, config, initialize=True
                )
            except Exception as e:
                self._error_handler.handle_error(
                    e, context={"operation": "register_kv_store_service"}
                )
                raise

            return kv_store_service

    @timing()
    async def list_workers(
        self, nats_url: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store.

        Args:
            nats_url: NATS server URL. If not provided, uses the URL from
                initialization.

        Returns:
            A list of dictionaries, each containing information about a worker.
        """
        workers = []
        url_to_use = nats_url or self._nats_url

        try:
            # Use the new context manager for KV store access
            from ..connection.context_managers import nats_kv_store
            from ..services.config import create_global_config

            # Create config with the provided URL
            config = create_global_config()
            config.nats_url = url_to_use

            # Use the KV store context manager
            async with nats_kv_store(WORKER_KV_NAME, config) as kv:
                # Get all keys
                keys = await kv.keys()
                for key_bytes in keys:
                    try:
                        entry = await kv.get(key_bytes)
                        if entry:
                            worker_data = cloudpickle.loads(entry.value)
                            workers.append(worker_data)
                    except Exception as e:
                        self._error_handler.handle_error(
                            e,
                            context={
                                "operation": "read_worker_data",
                                "key": key_bytes.decode()
                                if isinstance(key_bytes, bytes)
                                else str(key_bytes),
                            },
                        )

            return workers

        except Exception as e:
            # Only return empty list for KV store access issues
            # For other errors, raise NaqException
            if "not accessible" in str(e).lower() or "not found" in str(e).lower():
                self._logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}",
                    kv_store_name=WORKER_KV_NAME,
                )
                return []
            else:
                naq_exception = wrap_naq_exception(e, context="Error listing workers")
                raise naq_exception from e

    def list_workers_sync(self, nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers.

        Args:
            nats_url: NATS server URL. If not provided, uses the URL from
                initialization.

        Returns:
            A list of dictionaries, each containing information about a worker.
        """
        return run_async_from_sync(self.list_workers, nats_url=nats_url)


# Global functions for backward compatibility and public API
def list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists active workers by querying the worker status KV store.

    Args:
        nats_url: NATS server URL. If not provided, uses the default.

    Returns:
        A list of dictionaries, each containing information about a worker.
    """
    monitor = WorkerMonitor(nats_url=nats_url)
    return run_async_from_sync(monitor.list_workers, nats_url=nats_url)


def list_workers_sync(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Synchronous version of list_workers.

    Args:
        nats_url: NATS server URL. If not provided, uses the default.

    Returns:
        A list of dictionaries, each containing information about a worker.
    """
    monitor = WorkerMonitor(nats_url=nats_url)
    return monitor.list_workers_sync(nats_url=nats_url)
