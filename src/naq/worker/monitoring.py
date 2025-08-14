"""Worker monitoring module.

This module provides functionality for monitoring workers.
"""

from typing import Any, Dict, List, Optional

import cloudpickle
from loguru import logger

from ..exceptions import NaqException
from ..settings import DEFAULT_NATS_URL, WORKER_KV_NAME
from ..services import ServiceManager, KVStoreService, ServiceConfig
from ..utils import run_async_from_sync


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

    async def _get_kv_store_service(self) -> KVStoreService:
        """Get or create the KVStoreService for worker status operations."""
        if self._service_manager:
            # Get KVStoreService from ServiceManager
            if not self._service_manager.has_service("kv_store"):
                # Register KVStoreService if not already registered
                config = ServiceConfig(nats_url=self._nats_url)
                await self._service_manager.register_service(
                    "kv_store", KVStoreService, config, initialize=True
                )
            return await self._service_manager.get_service("kv_store", KVStoreService)
        else:
            # Create a temporary KVStoreService for backward compatibility
            from ..services import ConnectionService

            config = ServiceConfig(nats_url=self._nats_url)
            service_manager = ServiceManager(config)

            # Register and initialize services
            await service_manager.register_service(
                "connection", ConnectionService, config, initialize=True
            )
            kv_store_service = await service_manager.register_service(
                "kv_store", KVStoreService, config, initialize=True
            )

            return kv_store_service

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
        kv_store_service = None
        url_to_use = nats_url or self._nats_url

        try:
            # Get KVStoreService
            if url_to_use != self._nats_url:
                # If different URL provided, create a temporary service
                from ..services import ConnectionService

                config = ServiceConfig(nats_url=url_to_use)
                temp_service_manager = ServiceManager(config)

                await temp_service_manager.register_service(
                    "connection", ConnectionService, config, initialize=True
                )
                kv_store_service = await temp_service_manager.register_service(
                    "kv_store", KVStoreService, config, initialize=True
                )
            else:
                kv_store_service = await self._get_kv_store_service()

            # Get the KV store
            kv = await kv_store_service.get_kv_store(WORKER_KV_NAME)

            # Get all keys
            keys = await kv.keys()
            for key_bytes in keys:
                try:
                    entry = await kv.get(key_bytes)
                    if entry:
                        worker_data = cloudpickle.loads(entry.value)
                        workers.append(worker_data)
                except Exception as e:
                    logger.error(
                        f"Error reading worker data for key '{key_bytes.decode()}': {e}"
                    )

            return workers

        except Exception as e:
            # Only return empty list for KV store access issues
            # For other errors, raise NaqException
            if "not accessible" in str(e).lower() or "not found" in str(e).lower():
                logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}"
                )
                return []
            else:
                raise NaqException(f"Error listing workers: {e}") from e
        finally:
            # Clean up temporary service if it was created
            if (
                kv_store_service
                and url_to_use != self._nats_url
                and hasattr(kv_store_service, "cleanup")
            ):
                try:
                    await kv_store_service.cleanup()
                except Exception:
                    pass

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
