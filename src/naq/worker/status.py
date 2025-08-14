# src/naq/worker/status.py
"""
Worker status management module.

This module provides the WorkerStatusManager class which is responsible for:
- Reporting worker status and heartbeats
- Managing worker registration and unregistration
- Listing active workers
"""

import asyncio
import os
import socket
import time
from typing import Any, Dict, List, Optional

import cloudpickle
import nats
from loguru import logger
from nats.js.kv import KeyValue

from ..connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
)
from ..exceptions import NaqConnectionError, NaqException
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    WORKER_KV_NAME,
    WORKER_STATUS,
)


class WorkerStatusManager:
    """
    Manages worker status reporting, heartbeats, and monitoring.
    """

    def __init__(self, worker):
        """Initialize the worker status manager."""
        self.worker = worker
        self._current_status = WORKER_STATUS.STARTING
        self._kv_store = None
        self._heartbeat_task = None

    async def _get_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return the NATS Key-Value store for worker statuses."""
        if self._kv_store is None:
            if not self.worker._js:
                logger.error("JetStream context not available")
                return None
            try:
                self._kv_store = await self.worker._js.key_value(bucket=WORKER_KV_NAME)
            except nats.js.errors.BucketNotFoundError:
                try:
                    self._kv_store = await self.worker._js.create_key_value(
                        bucket=WORKER_KV_NAME,
                        ttl=self.worker._worker_ttl
                        if self.worker._worker_ttl > 0
                        else 0,
                        description="Stores naq worker status and heartbeats",
                    )
                except Exception as e:
                    logger.error(f"Failed to create worker status KV store: {e}")
                    self._kv_store = None
            except Exception as e:
                logger.error(f"Failed to get worker status KV store: {e}")
                self._kv_store = None
        return self._kv_store

    async def update_status(
        self, status: WORKER_STATUS | str, job_id: Optional[str] = None
    ) -> None:
        """Updates the worker's status in the KV store."""
        self._current_status = (
            status if isinstance(status, WORKER_STATUS) else WORKER_STATUS(status)
        )
        kv_store = await self._get_kv_store()
        if not kv_store:
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
            await kv_store.put(self.worker.worker_id, cloudpickle.dumps(payload))
        except Exception as e:
            logger.error(f"Failed to update worker status: {e}")

    async def _heartbeat(self) -> None:
        """Sends periodic heartbeat updates."""
        while True:
            await self.update_status(self._current_status)
            await asyncio.sleep(DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS)

    async def start_heartbeat_loop(self) -> None:
        """Start the heartbeat loop."""
        if not self._heartbeat_task:
            self._heartbeat_task = asyncio.create_task(self._heartbeat())
            await self.update_status(WORKER_STATUS.IDLE)

    async def stop_heartbeat_loop(self) -> None:
        """Stop the heartbeat loop."""
        if not self._heartbeat_task or self._heartbeat_task.done():
            return

        # Update status before canceling task to ensure it's captured
        try:
            await self.update_status(WORKER_STATUS.STOPPING)
        except Exception as e:
            logger.error(f"Error updating status during shutdown: {e}")

        # Cancel and wait for task with proper exception handling
        self._heartbeat_task.cancel()
        try:
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error during heartbeat task shutdown: {e}")

    async def unregister_worker(self) -> None:
        """Delete the worker's status entry from the KV store."""
        kv_store = await self._get_kv_store()
        if not kv_store:
            logger.warning(
                f"Worker status KV store not available. Cannot unregister worker {self.worker.worker_id}"
            )
            return

        try:
            await kv_store.delete(self.worker.worker_id)
            logger.info(f"Unregistered worker {self.worker.worker_id}")
        except Exception as e:
            logger.error(f"Failed to unregister worker {self.worker.worker_id}: {e}")

    @staticmethod
    async def list_workers(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """
        Lists active workers by querying the worker status KV store.

        Args:
            nats_url: NATS server URL (if not using default).

        Returns:
            A list of dictionaries, each containing information about a worker.

        Raises:
            NaqConnectionError: If connection fails.
            NaqException: For other errors.
        """
        from ..connection import (
            close_nats_connection,
            get_jetstream_context,
            get_nats_connection,
        )
        from ..exceptions import NaqConnectionError, NaqException
        from ..settings import WORKER_KV_NAME

        workers = []
        nc = None
        kv = None
        try:
            nc = await get_nats_connection(url=nats_url)
            js = await get_jetstream_context(nc=nc)
            try:
                kv = await js.key_value(bucket=WORKER_KV_NAME)
            except Exception as e:
                logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}"
                )
                return []  # Return empty list if store doesn't exist

            keys = await kv.keys()
            for key_bytes in keys:
                try:
                    entry = await kv.get(key_bytes)
                    if entry:
                        worker_data = cloudpickle.loads(entry.value)
                        workers.append(worker_data)
                except nats.js.errors.KeyNotFoundError:
                    continue  # Key might have expired between keys() and get()
                except Exception as e:
                    logger.error(
                        f"Error reading worker data for key '{key_bytes.decode()}': {e}"
                    )

            return workers

        except NaqConnectionError:
            raise
        except Exception as e:
            raise NaqException(f"Error listing workers: {e}") from e
        finally:
            if nc:
                await close_nats_connection()