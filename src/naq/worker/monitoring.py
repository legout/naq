"""Worker monitoring module.

This module provides functionality for monitoring workers.
"""

from typing import Any, Dict, List, Optional

import cloudpickle

from ..config import get_config
from ..nats_client import NatsClient
from ..settings import WORKER_KV_NAME
from ..utils import run_async_from_sync
from ..utils.decorators import timing
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger


class WorkerMonitor:
    """Provides methods for monitoring workers using the NatsClient."""

    def __init__(
        self,
        nats_client: Optional[NatsClient] = None,
        nats_url: Optional[str] = None,
    ):
        """Initialize the worker monitor.

        Args:
            nats_client: Optional NatsClient for accessing NATS.
            nats_url: NATS server URL. If not provided, uses default.
        """
        self._nats_client = nats_client
        self._nats_url = nats_url
        self._logger = StructuredLogger("WorkerMonitor")
        self._error_handler = ErrorHandler("WorkerMonitor")

    @timing()
    async def _get_nats_client(self) -> NatsClient:
        """Get or create the NatsClient for worker status operations."""
        if self._nats_client is None:
            try:
                config = get_config()
                if self._nats_url:
                    config.nats_url = self._nats_url
                self._nats_client = NatsClient(config)
                await self._nats_client.connect()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to get NATS client")
                self._error_handler.handle_error(wrapped_error)
                raise
        return self._nats_client

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
        
        try:
            nats_client = await self._get_nats_client()
            
            # Get JetStream for KV operations
            js = await nats_client.get_jetstream()
            
            # Get the KV store
            try:
                kv = await js.key_value(WORKER_KV_NAME)
            except Exception as e:
                self._logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}",
                    kv_store_name=WORKER_KV_NAME,
                )
                return []
            
            # Get all keys
            try:
                keys = await kv.keys()
                for key in keys:
                    try:
                        entry = await kv.get(key)
                        if entry:
                            worker_data = cloudpickle.loads(entry.value)
                            workers.append(worker_data)
                    except Exception as e:
                        self._error_handler.handle_error(
                            e,
                            context={
                                "operation": "read_worker_data",
                                "key": key.decode() if isinstance(key, bytes) else str(key),
                            },
                        )
            except Exception as e:
                self._logger.warning(
                    f"Error getting keys from KV store '{WORKER_KV_NAME}': {e}",
                    kv_store_name=WORKER_KV_NAME,
                )
                return []

            return workers

        except Exception as e:
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
    return run_async_from_sync(monitor.list_workers)


def list_workers_sync(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Synchronous version of list_workers.

    Args:
        nats_url: NATS server URL. If not provided, uses the default.

    Returns:
        A list of dictionaries, each containing information about a worker.
    """
    monitor = WorkerMonitor(nats_url=nats_url)
    return monitor.list_workers_sync()
