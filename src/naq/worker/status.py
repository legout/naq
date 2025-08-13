# src/naq/worker/status.py
"""
Worker status management for NAQ.

This module contains the WorkerStatusManager class for managing worker status
reporting, heartbeats, and monitoring.
"""

import asyncio
import os
import socket
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import cloudpickle
from loguru import logger
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from ..exceptions import NaqConnectionError, NaqException
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    WORKER_KV_NAME,
    WORKER_STATUS,
)

if TYPE_CHECKING:
    from .core import Worker


class WorkerStatusManager:
    """
    Manages worker status reporting, heartbeats, and monitoring.
    """

    def __init__(self, worker: "Worker"):
        """Initialize the worker status manager."""
        self.worker = worker
        self._current_status = WORKER_STATUS.STARTING
        self._kv_store = None
        self._heartbeat_task = None

    async def _get_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return the NATS Key-Value store for worker statuses using service."""
        if self._kv_store is None:
            try:
                from ..services.kv_stores import KVStoreService
                kv_service = await self.worker._service_manager.get_service(KVStoreService)
                self._kv_store = await kv_service.get_kv_store(
                    WORKER_KV_NAME,
                    ttl=self.worker._worker_ttl if self.worker._worker_ttl > 0 else None,
                    description="Stores naq worker status and heartbeats",
                    create_if_not_exists=True
                )
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
            
            # Log worker status event using service
            try:
                from ..services.events import EventService
                event_service = await self.worker._service_manager.get_service(EventService)
                hostname = socket.gethostname()
                pid = os.getpid()
                
                if self._current_status == WORKER_STATUS.STARTING:
                    await event_service.log_worker_started(
                        worker_id=self.worker.worker_id,
                        hostname=hostname,
                        pid=pid,
                        queue_names=self.worker.queue_names,
                        concurrency_limit=self.worker._concurrency,
                    )
                elif self._current_status == WORKER_STATUS.IDLE:
                    await event_service.log_worker_idle(
                        worker_id=self.worker.worker_id,
                        hostname=hostname,
                        pid=pid,
                    )
                elif self._current_status == WORKER_STATUS.BUSY:
                    await event_service.log_worker_busy(
                        worker_id=self.worker.worker_id,
                        hostname=hostname,
                        pid=pid,
                        current_job_id=job_id or "unknown",
                    )
                elif self._current_status == WORKER_STATUS.STOPPING:
                    await event_service.log_worker_stopped(
                        worker_id=self.worker.worker_id,
                        hostname=hostname,
                        pid=pid,
                    )
            except Exception as event_e:
                logger.warning(f"Failed to log worker status event: {event_e}")
                
        except Exception as e:
            logger.error(f"Failed to update worker status: {e}")

    async def _heartbeat(self) -> None:
        """Sends periodic heartbeat updates."""
        while True:
            await self.update_status(self._current_status)
            
            # Log heartbeat event periodically (every 10th heartbeat to reduce noise)
            if hasattr(self, '_heartbeat_count'):
                self._heartbeat_count += 1
            else:
                self._heartbeat_count = 1
                
            if self._heartbeat_count % 10 == 0:
                try:
                    from ..services.events import EventService
                    event_service = await self.worker._service_manager.get_service(EventService)
                    hostname = socket.gethostname()
                    pid = os.getpid()
                    active_jobs = self.worker._concurrency - self.worker._semaphore._value
                    await event_service.log_worker_heartbeat(
                        worker_id=self.worker.worker_id,
                        hostname=hostname,
                        pid=pid,
                        active_jobs=active_jobs,
                        concurrency_limit=self.worker._concurrency,
                    )
                except Exception as e:
                    logger.debug(f"Failed to log worker heartbeat event: {e}")
                    
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
        from ..services import ServiceManager
        from ..services.kv_stores import KVStoreService

        workers = []
        service_manager = None
        try:
            service_manager = ServiceManager({'nats_url': nats_url})
            kv_service = await service_manager.get_service(KVStoreService)
            
            # Get worker keys
            try:
                keys = await kv_service.keys(WORKER_KV_NAME)
            except Exception as e:
                logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible: {e}"
                )
                return []  # Return empty list if store doesn't exist

            for key in keys:
                try:
                    entry = await kv_service.get(WORKER_KV_NAME, key, deserialize=False)
                    if entry:
                        worker_data = cloudpickle.loads(entry)
                        workers.append(worker_data)
                except KeyNotFoundError:
                    continue  # Key might have expired between keys() and get()
                except Exception as e:
                    logger.error(f"Error reading worker data for key '{key}': {e}")

            return workers

        except NaqConnectionError:
            raise
        except Exception as e:
            raise NaqException(f"Error listing workers: {e}") from e
        finally:
            if service_manager:
                await service_manager.cleanup_all()