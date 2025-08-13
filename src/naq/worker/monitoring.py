"""Worker monitoring module.

This module provides functionality for monitoring workers.
"""

from typing import Any, Dict, List

from ..settings import DEFAULT_NATS_URL
from ..utils import run_async_from_sync
from .status import WorkerStatusManager


class WorkerMonitor:
    """Provides static methods for monitoring workers."""

    @staticmethod
    async def list_workers(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        return await WorkerStatusManager.list_workers(nats_url)

    @staticmethod
    def list_workers_sync(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        return run_async_from_sync(WorkerMonitor.list_workers, nats_url=nats_url)