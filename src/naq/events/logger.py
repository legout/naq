# src/naq/events/logger.py
import asyncio
import time
from typing import Any, Dict, List, Optional

import anyio
from loguru import logger

from ..models import JobEvent, JobEventType, WorkerEvent, WorkerEventType
from ..settings import DEFAULT_NATS_URL
from ..utils import run_async_from_sync
from .storage import BaseEventStorage, NATSJobEventStorage


class AsyncJobEventLogger:
    """
    High-performance, non-blocking job event logger.
    
    This logger buffers events in memory and flushes them to the storage
    backend periodically or when the buffer reaches a certain size.
    """

    def __init__(
        self,
        storage: Optional[BaseEventStorage] = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_buffer_size: int = 10000,
        nats_url: str = DEFAULT_NATS_URL
    ):
        """
        Initialize the event logger.
        
        Args:
            storage: Storage backend instance. If None, creates NATSJobEventStorage.
            batch_size: Number of events to trigger a flush.
            flush_interval: Time interval (seconds) between automatic flushes.
            max_buffer_size: Maximum buffer size before dropping old events.
            nats_url: NATS URL for default storage backend.
        """
        self.storage = storage or NATSJobEventStorage(nats_url=nats_url)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        
        # Internal state
        self._buffer: List[JobEvent] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_flush_time = time.time()

    async def start(self) -> None:
        """Start the logger and background flush loop."""
        if self._running:
            return
            
        self._running = True
        self._last_flush_time = time.time()
        
        # Start the background flush loop
        self._flush_task = asyncio.create_task(self._flush_loop())
        
        logger.debug(
            f"Started AsyncJobEventLogger (batch_size={self.batch_size}, "
            f"flush_interval={self.flush_interval}s)"
        )

    async def stop(self) -> None:
        """Stop the logger and flush any remaining events."""
        if not self._running:
            return
            
        self._running = False
        
        # Cancel the flush task
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            
        # Final flush of remaining events
        await self._flush_events()
        
        # Close storage connection
        await self.storage.close()
        
        logger.debug("Stopped AsyncJobEventLogger")

    async def log_event(self, event: JobEvent) -> None:
        """
        Log a job event (non-blocking).
        
        Args:
            event: The JobEvent to log.
        """
        if not self._running:
            logger.warning("Event logger not running, dropping event")
            return
            
        async with self._buffer_lock:
            # Add event to buffer
            self._buffer.append(event)
            
            # Enforce max buffer size by dropping oldest events
            if len(self._buffer) > self.max_buffer_size:
                dropped = len(self._buffer) - self.max_buffer_size
                self._buffer = self._buffer[-self.max_buffer_size:]
                logger.warning(f"Buffer overflow: dropped {dropped} old events")
            
            # Check if we should trigger a flush
            should_flush = len(self._buffer) >= self.batch_size
            
        # Trigger immediate flush if buffer is full (outside the lock)
        if should_flush:
            asyncio.create_task(self._flush_events())

    async def _flush_loop(self) -> None:
        """Background loop for periodic flushing."""
        while self._running:
            try:
                # Wait for the flush interval
                await asyncio.sleep(self.flush_interval)
                
                # Check if it's time to flush
                current_time = time.time()
                if current_time - self._last_flush_time >= self.flush_interval:
                    await self._flush_events()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in flush loop: {e}")
                # Continue running despite errors

    async def _flush_events(self) -> None:
        """Flush buffered events to storage."""
        if not self._buffer:
            return
            
        # Get a copy of the buffer and clear it
        async with self._buffer_lock:
            events_to_flush = self._buffer.copy()
            self._buffer.clear()
            
        if not events_to_flush:
            return
            
        # Store events (with retry logic)
        failed_events = []
        for event in events_to_flush:
            try:
                await self.storage.store_event(event)
            except Exception as e:
                logger.error(f"Failed to store event {event.event_type} for job {event.job_id}: {e}")
                failed_events.append(event)
                
        # Re-add failed events to buffer for retry
        if failed_events:
            async with self._buffer_lock:
                self._buffer.extend(failed_events)
                
            logger.warning(f"Re-queued {len(failed_events)} failed events for retry")
        
        self._last_flush_time = time.time()
        
        if events_to_flush:
            success_count = len(events_to_flush) - len(failed_events)
            logger.debug(f"Flushed {success_count}/{len(events_to_flush)} events to storage")

    # Convenience methods for common event types
    
    async def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job enqueued event."""
        event = JobEvent.enqueued(
            job_id=job_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job started event."""
        event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job completed event."""
        event = JobEvent.completed(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            duration_ms=duration_ms,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        error_type: str,
        error_message: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job failed event."""
        event = JobEvent.failed(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        retry_count: int,
        retry_delay: float,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job retry scheduled event."""
        event = JobEvent.retry_scheduled(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            retry_count=retry_count,
            retry_delay=retry_delay,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp: float,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job scheduled event."""
        event = JobEvent.scheduled(
            job_id=job_id,
            queue_name=queue_name,
            scheduled_timestamp=scheduled_timestamp,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_schedule_triggered(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule triggered event."""
        event = JobEvent.schedule_triggered(
            job_id=job_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    # Worker event logging methods

    async def log_worker_event(self, event: WorkerEvent) -> None:
        """
        Log a worker event (uses the same storage backend).
        
        Args:
            event: The WorkerEvent to log.
        """
        # Convert WorkerEvent to a generic event format for storage
        # We'll reuse the JobEvent structure but with worker-specific data
        job_event = JobEvent(
            job_id=f"worker:{event.worker_id}",  # Use worker ID as job ID
            event_type=JobEventType(event.event_type.value),  # Convert event type
            timestamp=event.timestamp,
            worker_id=event.worker_id,
            queue_name=",".join(event.queue_names) if event.queue_names else None,
            message=event.message,
            details={
                "event_category": "worker",
                "hostname": event.hostname,
                "pid": event.pid,
                "queue_names": event.queue_names,
                "current_job_id": event.current_job_id,
                "active_jobs": event.active_jobs,
                "concurrency_limit": event.concurrency_limit,
                "error_type": event.error_type,
                "error_message": event.error_message,
                **(event.details or {})
            },
            error_type=event.error_type,
            error_message=event.error_message,
        )
        await self.log_event(job_event)

    async def log_worker_started(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        queue_names: List[str],
        concurrency_limit: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker started event."""
        event = WorkerEvent.worker_started(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            queue_names=queue_names,
            concurrency_limit=concurrency_limit,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    async def log_worker_stopped(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker stopped event."""
        event = WorkerEvent.worker_stopped(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    async def log_worker_idle(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker idle event."""
        event = WorkerEvent.worker_idle(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    async def log_worker_busy(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        current_job_id: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker busy event."""
        event = WorkerEvent.worker_busy(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            current_job_id=current_job_id,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    async def log_worker_heartbeat(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        active_jobs: int,
        concurrency_limit: int,
        current_job_id: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker heartbeat event."""
        event = WorkerEvent.worker_heartbeat(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            active_jobs=active_jobs,
            concurrency_limit=concurrency_limit,
            current_job_id=current_job_id,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    async def log_worker_error(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        error_type: str,
        error_message: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a worker error event."""
        event = WorkerEvent.worker_error(
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
            error_type=error_type,
            error_message=error_message,
            message=message,
            **kwargs
        )
        await self.log_worker_event(event)

    # Schedule management event logging methods

    async def log_schedule_paused(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule paused event."""
        event = JobEvent.schedule_paused(
            job_id=job_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_schedule_resumed(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule resumed event."""
        event = JobEvent.schedule_resumed(
            job_id=job_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_schedule_cancelled(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule cancelled event."""
        event = JobEvent.schedule_cancelled(
            job_id=job_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_schedule_modified(
        self,
        job_id: str,
        queue_name: str,
        modifications: Dict[str, Any],
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule modified event."""
        event = JobEvent.schedule_modified(
            job_id=job_id,
            queue_name=queue_name,
            modifications=modifications,
            message=message,
            **kwargs
        )
        await self.log_event(event)

    async def log_scheduler_error(
        self,
        error_type: str,
        error_message: str,
        schedule_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a scheduler error event."""
        event = JobEvent.scheduler_error(
            error_type=error_type,
            error_message=error_message,
            schedule_id=schedule_id,
            queue_name=queue_name,
            message=message,
            **kwargs
        )
        await self.log_event(event)


class JobEventLogger:
    """
    Synchronous wrapper for AsyncJobEventLogger.
    
    This provides a synchronous interface that follows the existing
    pattern in the naq library for sync wrappers.
    """

    def __init__(
        self,
        storage: Optional[BaseEventStorage] = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_buffer_size: int = 10000,
        nats_url: str = DEFAULT_NATS_URL
    ):
        """
        Initialize the synchronous event logger.
        
        Args:
            storage: Storage backend instance. If None, creates NATSJobEventStorage.
            batch_size: Number of events to trigger a flush.
            flush_interval: Time interval (seconds) between automatic flushes.
            max_buffer_size: Maximum buffer size before dropping old events.
            nats_url: NATS URL for default storage backend.
        """
        self._async_logger = AsyncJobEventLogger(
            storage=storage,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_buffer_size=max_buffer_size,
            nats_url=nats_url
        )

    def start(self) -> None:
        """Start the logger and background flush loop."""
        run_async_from_sync(self._async_logger.start)

    def stop(self) -> None:
        """Stop the logger and flush any remaining events."""
        run_async_from_sync(self._async_logger.stop)

    def log_event(self, event: JobEvent) -> None:
        """Log a job event."""
        # Use anyio.from_thread.run for better performance in sync contexts
        try:
            anyio.from_thread.run(self._async_logger.log_event, event)
        except RuntimeError:
            # Fall back to run_async_from_sync if not in async context
            run_async_from_sync(self._async_logger.log_event, event)

    def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job enqueued event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_enqueued,
                job_id, queue_name, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_enqueued,
                job_id, queue_name, message, **kwargs
            )

    def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job started event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_started,
                job_id, worker_id, queue_name, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_started,
                job_id, worker_id, queue_name, message, **kwargs
            )

    def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job completed event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_completed,
                job_id, worker_id, queue_name, duration_ms, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_completed,
                job_id, worker_id, queue_name, duration_ms, message, **kwargs
            )

    def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        error_type: str,
        error_message: str,
        duration_ms: int,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job failed event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_failed,
                job_id, worker_id, queue_name, error_type, error_message,
                duration_ms, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_failed,
                job_id, worker_id, queue_name, error_type, error_message,
                duration_ms, message, **kwargs
            )

    def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        retry_count: int,
        retry_delay: float,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job retry scheduled event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_retry_scheduled,
                job_id, worker_id, queue_name, retry_count, retry_delay,
                message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_retry_scheduled,
                job_id, worker_id, queue_name, retry_count, retry_delay,
                message, **kwargs
            )

    def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp: float,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a job scheduled event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_job_scheduled,
                job_id, queue_name, scheduled_timestamp, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_job_scheduled,
                job_id, queue_name, scheduled_timestamp, message, **kwargs
            )

    def log_schedule_triggered(
        self,
        job_id: str,
        queue_name: str,
        message: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log a schedule triggered event."""
        try:
            anyio.from_thread.run(
                self._async_logger.log_schedule_triggered,
                job_id, queue_name, message, **kwargs
            )
        except RuntimeError:
            run_async_from_sync(
                self._async_logger.log_schedule_triggered,
                job_id, queue_name, message, **kwargs
            )