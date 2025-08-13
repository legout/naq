"""
Worker Status Management

This module provides functionality for tracking and managing worker status,
including current state, processing jobs, and performance metrics.
"""

import time
from typing import Optional, Dict, Set
from dataclasses import dataclass, field
from datetime import datetime

from ..settings import WORKER_STATUS as WorkerStatusEnum


@dataclass
class WorkerStatus:
    """
    Tracks the current status and metrics of a worker.
    
    This class provides methods to update and query worker status,
    including tracking currently processing jobs and performance metrics.
    """
    
    status: WorkerStatusEnum = WorkerStatusEnum.IDLE
    started_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    current_job_id: Optional[str] = None
    processing_jobs: Set[str] = field(default_factory=set)
    processed_jobs: int = 0
    failed_jobs: int = 0
    total_processing_time: float = 0.0
    job_start_times: Dict[str, float] = field(default_factory=dict)
    
    def set_status(self, status: WorkerStatusEnum) -> None:
        """
        Set the worker status.
        
        Args:
            status: The new status to set
        """
        self.status = status
        
        if status == WorkerStatusEnum.IDLE and self.started_at is None:
            self.started_at = datetime.now()
            
        self.last_heartbeat = datetime.now()
        
    def set_processing_job(self, job_id: str) -> None:
        """
        Mark a job as being processed.
        
        Args:
            job_id: ID of the job being processed
        """
        self.processing_jobs.add(job_id)
        self.current_job_id = job_id
        self.job_start_times[job_id] = time.time()
        
    def clear_processing_job(self, job_id: Optional[str] = None) -> None:
        """
        Clear a job from being processed.
        
        Args:
            job_id: ID of the job to clear. If None, uses current_job_id
        """
        if job_id is None:
            job_id = self.current_job_id
            
        if job_id and job_id in self.processing_jobs:
            self.processing_jobs.remove(job_id)
            
            # Update processing time
            if job_id in self.job_start_times:
                start_time = self.job_start_times.pop(job_id)
                processing_time = time.time() - start_time
                self.total_processing_time += processing_time
                
        self.current_job_id = None
        
    def mark_job_completed(self) -> None:
        """Mark the current job as completed."""
        if self.current_job_id:
            self.processed_jobs += 1
            self.clear_processing_job()
            
    def mark_job_failed(self) -> None:
        """Mark the current job as failed."""
        if self.current_job_id:
            self.failed_jobs += 1
            self.clear_processing_job()
            
    def get_uptime(self) -> Optional[float]:
        """
        Get the worker uptime in seconds.
        
        Returns:
            Uptime in seconds, or None if not started
        """
        if self.started_at is None:
            return None
            
        return (datetime.now() - self.started_at).total_seconds()
        
    def get_average_processing_time(self) -> float:
        """
        Get the average processing time per job.
        
        Returns:
            Average processing time in seconds
        """
        if self.processed_jobs == 0:
            return 0.0
            
        return self.total_processing_time / self.processed_jobs
        
    def get_job_completion_rate(self) -> float:
        """
        Get the job completion rate (completed / total processed).
        
        Returns:
            Completion rate as a percentage (0.0 to 1.0)
        """
        total_jobs = self.processed_jobs + self.failed_jobs
        if total_jobs == 0:
            return 0.0
            
        return self.processed_jobs / total_jobs
        
    def to_dict(self) -> Dict:
        """
        Convert the status to a dictionary.
        
        Returns:
            Dictionary representation of the status
        """
        return {
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "current_job_id": self.current_job_id,
            "processing_jobs": list(self.processing_jobs),
            "processed_jobs": self.processed_jobs,
            "failed_jobs": self.failed_jobs,
            "total_processing_time": self.total_processing_time,
            "average_processing_time": self.get_average_processing_time(),
            "job_completion_rate": self.get_job_completion_rate(),
            "uptime": self.get_uptime()
        }


import asyncio
import os
import socket
import time
from typing import Any, Dict, List, Optional

import cloudpickle
from loguru import logger
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from ..connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
    nats_kv_store,
    Config,
)
from ..exceptions import NaqConnectionError, NaqException
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
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
            except BucketNotFoundError:
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
        from ..exceptions import NaqConnectionError, NaqException
        from ..settings import WORKER_KV_NAME
        from nats.js.errors import BucketNotFoundError

        workers = []
        try:
            config = Config(servers=[nats_url])
            try:
                async with nats_kv_store(WORKER_KV_NAME, config) as kv:
                    keys = await kv.keys()
                    for key_bytes in keys:
                        try:
                            entry = await kv.get(key_bytes)
                            if entry and entry.value is not None:
                                worker_data = cloudpickle.loads(entry.value)
                                workers.append(worker_data)
                        except KeyNotFoundError:
                            continue  # Key might have expired between keys() and get()
                        except Exception as e:
                            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else str(key_bytes)
                            logger.error(
                                f"Error reading worker data for key '{key_str}': {e}"
                            )
            except BucketNotFoundError:
                logger.warning(
                    f"Worker status KV store '{WORKER_KV_NAME}' not accessible"
                )
                return []  # Return empty list if store doesn't exist

            return workers

        except NaqConnectionError:
            raise
        except Exception as e:
            raise NaqException(f"Error listing workers: {e}") from e