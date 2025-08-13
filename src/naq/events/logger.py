# src/naq/events/logger.py
import asyncio
import time
from typing import Any, Dict, List, Optional

from loguru import logger

from .storage import BaseEventStorage
from ..models import JobEvent, JobEventType
from ..utils import run_async_from_sync
from ..utils.nats_helpers import (
    batch_publish,
    batch_publish_sync,
)


class AsyncJobEventLogger:
    """
    High-performance, non-blocking event logger that buffers events in memory
    and flushes them to the NATS storage backend periodically.
    """

    def __init__(
        self,
        storage: BaseEventStorage,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_buffer_size: int = 10000,
    ):
        """
        Initialize the event logger.
        
        Args:
            storage: Event storage backend instance (e.g., NATSJobEventStorage)
            batch_size: Number of events to accumulate before triggering a flush
            flush_interval: Interval in seconds between automatic flushes
            max_buffer_size: Maximum number of events to buffer before dropping oldest
        """
        self.storage = storage
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        
        # Event buffer and synchronization
        self._buffer: List[JobEvent] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self._stats = {
            "events_logged": 0,
            "events_flushed": 0,
            "flush_attempts": 0,
            "flush_failures": 0,
        }

    async def log_event(self, event: JobEvent) -> None:
        """
        Log an event by adding it to the internal buffer.
        
        This method is fast and non-blocking. If the buffer size exceeds
        batch_size, it triggers an asynchronous flush.
        
        Args:
            event: JobEvent to log
        """
        async with self._lock:
            self._buffer.append(event)
            self._stats["events_logged"] += 1
            
            # Check if we need to trigger a flush
            if len(self._buffer) >= self.batch_size:
                # Don't await here to keep it non-blocking
                if self._flush_task is None or self._flush_task.done():
                    asyncio.create_task(self._flush_events())

    async def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job enqueued event."""
        event = JobEvent.enqueued(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job started event."""
        event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job completed event."""
        event = JobEvent.completed(
            job_id=job_id,
            worker_id=worker_id,
            duration_ms=duration_ms,
            queue_name=queue_name,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job failed event."""
        event = JobEvent.failed(
            job_id=job_id,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            queue_name=queue_name,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        delay_seconds: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job retry scheduled event."""
        event = JobEvent.retry_scheduled(
            job_id=job_id,
            worker_id=worker_id,
            delay_seconds=delay_seconds,
            queue_name=queue_name,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp_utc: float,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job scheduled event."""
        event = JobEvent.scheduled(
            job_id=job_id,
            queue_name=queue_name,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_schedule_triggered(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job schedule triggered event."""
        event = JobEvent.schedule_triggered(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
            details=details,
        )
        await self.log_event(event)

    async def log_job_cancelled(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
    ) -> None:
        """Log a job cancelled event."""
        event = JobEvent.cancelled(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )
        await self.log_event(event)

    async def log_job_status_changed(
        self,
        job_id: str,
        queue_name: str,
        old_status: str,
        new_status: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
    ) -> None:
        """Log a job status changed event."""
        event = JobEvent.status_changed(
            job_id=job_id,
            queue_name=queue_name,
            old_status=old_status,
            new_status=new_status,
            worker_id=worker_id,
            details=details,
            nats_subject=nats_subject,
            nats_sequence=nats_sequence,
        )
        await self.log_event(event)

    async def _flush_loop(self) -> None:
        """
        Background task that runs in a loop and flushes events periodically.
        
        This method wakes up every flush_interval seconds and calls _flush_events.
        """
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_events()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in flush loop: {e}", exc_info=True)

    async def _flush_events(self) -> None:
        """
        Flush events from the buffer to the storage backend.
        
        This method acquires the lock, copies the current buffer, and clears it.
        It then uses batch_publish to send all events at once with retry logic.
        """
        async with self._lock:
            if not self._buffer:
                return
                
            # Copy current buffer and clear it
            events_to_flush = self._buffer.copy()
            self._buffer.clear()
            
        if not events_to_flush:
            return
            
        self._stats["flush_attempts"] += 1
        logger.debug(f"Flushing {len(events_to_flush)} events to storage")
        
        try:
            # Use batch_publish for more efficient event publishing
            successful_events, failed_events = await batch_publish(
                storage=self.storage,
                events=events_to_flush,
                max_retries=3,
            )
            
            # Update statistics
            self._stats["events_flushed"] += len(successful_events)
            self._stats["flush_failures"] += len(failed_events)
            
            # If there were failed events, add them back to the buffer for next attempt
            if failed_events:
                async with self._lock:
                    # Add failed events back to the front of the buffer
                    self._buffer = failed_events + self._buffer
                    
            logger.debug(
                f"Flush completed: {len(successful_events)} successful, "
                f"{len(failed_events)} failed, {len(self._buffer)} remaining"
            )
        except Exception as e:
            logger.error(f"Failed to flush events: {e}")
            # Add all events back to the buffer on failure
            async with self._lock:
                self._buffer = events_to_flush + self._buffer
            self._stats["flush_failures"] += len(events_to_flush)

    async def start(self) -> None:
        """Start the event logger and background flush task."""
        if self._running:
            logger.warning("Event logger is already running")
            return
            
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("Event logger started")

    async def stop(self) -> None:
        """Stop the event logger and flush any remaining events."""
        if not self._running:
            logger.warning("Event logger is not running")
            return
            
        self._running = False
        
        # Cancel the flush task
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining events
        if self._buffer:
            logger.info(f"Flushing remaining {len(self._buffer)} events before shutdown...")
            await self._flush_events()
            
        logger.info("Event logger stopped")

    async def flush(self) -> None:
        """Manually trigger a flush of all buffered events."""
        await self._flush_events()

    def get_stats(self) -> Dict[str, Any]:
        """Get logger statistics."""
        return self._stats.copy()

    def get_buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)


class JobEventLogger:
    """
    Synchronous wrapper for AsyncJobEventLogger.
    
    This class follows the existing _sync pattern in the naq library,
    providing a synchronous interface that uses run_async_from_sync
    to call the underlying async methods.
    """

    def __init__(
        self,
        storage: BaseEventStorage,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_buffer_size: int = 10000,
    ):
        """
        Initialize the synchronous event logger.
        
        Args:
            storage: Event storage backend instance
            batch_size: Number of events to accumulate before triggering a flush
            flush_interval: Interval in seconds between automatic flushes
            max_buffer_size: Maximum number of events to buffer
        """
        self._async_logger = AsyncJobEventLogger(
            storage=storage,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_buffer_size=max_buffer_size,
        )

    def log_event(self, event: JobEvent) -> None:
        """Log an event synchronously."""
        def _async_log():
            return self._async_logger.log_event(event)
        
        return run_async_from_sync(_async_log)

    def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job enqueued event synchronously."""
        def _async_log():
            return self._async_logger.log_job_enqueued(
                job_id=job_id,
                queue_name=queue_name,
                worker_id=worker_id,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job started event synchronously."""
        def _async_log():
            return self._async_logger.log_job_started(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job completed event synchronously."""
        def _async_log():
            return self._async_logger.log_job_completed(
                job_id=job_id,
                worker_id=worker_id,
                duration_ms=duration_ms,
                queue_name=queue_name,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job failed event synchronously."""
        def _async_log():
            return self._async_logger.log_job_failed(
                job_id=job_id,
                worker_id=worker_id,
                error_type=error_type,
                error_message=error_message,
                duration_ms=duration_ms,
                queue_name=queue_name,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        delay_seconds: float,
        queue_name: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job retry scheduled event synchronously."""
        def _async_log():
            return self._async_logger.log_job_retry_scheduled(
                job_id=job_id,
                worker_id=worker_id,
                delay_seconds=delay_seconds,
                queue_name=queue_name,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp_utc: float,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job scheduled event synchronously."""
        def _async_log():
            return self._async_logger.log_job_scheduled(
                job_id=job_id,
                queue_name=queue_name,
                scheduled_timestamp_utc=scheduled_timestamp_utc,
                worker_id=worker_id,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_schedule_triggered(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a job schedule triggered event synchronously."""
        def _async_log():
            return self._async_logger.log_job_schedule_triggered(
                job_id=job_id,
                queue_name=queue_name,
                worker_id=worker_id,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
                details=details,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_cancelled(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
    ) -> None:
        """Log a job cancelled event synchronously."""
        def _async_log():
            return self._async_logger.log_job_cancelled(
                job_id=job_id,
                queue_name=queue_name,
                worker_id=worker_id,
                details=details,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
            )
        
        return run_async_from_sync(_async_log)

    def log_job_status_changed(
        self,
        job_id: str,
        queue_name: str,
        old_status: str,
        new_status: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        nats_subject: Optional[str] = None,
        nats_sequence: Optional[int] = None,
    ) -> None:
        """Log a job status changed event synchronously."""
        def _async_log():
            return self._async_logger.log_job_status_changed(
                job_id=job_id,
                queue_name=queue_name,
                old_status=old_status,
                new_status=new_status,
                worker_id=worker_id,
                details=details,
                nats_subject=nats_subject,
                nats_sequence=nats_sequence,
            )
        
        return run_async_from_sync(_async_log)

    def start(self) -> None:
        """Start the event logger synchronously."""
        def _async_start():
            return self._async_logger.start()
        
        return run_async_from_sync(_async_start)

    def stop(self) -> None:
        """Stop the event logger synchronously."""
        def _async_stop():
            return self._async_logger.stop()
        
        return run_async_from_sync(_async_stop)

    def flush(self) -> None:
        """Manually trigger a flush synchronously."""
        def _async_flush():
            return self._async_logger.flush()
        
        return run_async_from_sync(_async_flush)
    
    def flush_batch(self) -> None:
        """Manually trigger a batch flush synchronously using batch_publish_sync."""
        async with self._async_logger._lock:
            if not self._async_logger._buffer:
                return
                
            # Copy current buffer and clear it
            events_to_flush = self._async_logger._buffer.copy()
            self._async_logger._buffer.clear()
            
        if not events_to_flush:
            return
            
        self._async_logger._stats["flush_attempts"] += 1
        logger.debug(f"Flushing {len(events_to_flush)} events to storage (sync batch)")
        
        try:
            # Use batch_publish_sync for more efficient event publishing
            successful_events, failed_events = batch_publish_sync(
                storage=self._async_logger.storage,
                events=events_to_flush,
                max_retries=3,
            )
            
            # Update statistics
            self._async_logger._stats["events_flushed"] += len(successful_events)
            self._async_logger._stats["flush_failures"] += len(failed_events)
            
            # If there were failed events, add them back to the buffer for next attempt
            if failed_events:
                async with self._async_logger._lock:
                    # Add failed events back to the front of the buffer
                    self._async_logger._buffer = failed_events + self._async_logger._buffer
                    
            logger.debug(
                f"Flush completed: {len(successful_events)} successful, "
                f"{len(failed_events)} failed, {len(self._async_logger._buffer)} remaining"
            )
        except Exception as e:
            logger.error(f"Failed to flush events: {e}")
            # Add all events back to the buffer on failure
            async with self._async_logger._lock:
                self._async_logger._buffer = events_to_flush + self._async_logger._buffer
            self._async_logger._stats["flush_failures"] += len(events_to_flush)

    def get_stats(self) -> Dict[str, Any]:
        """Get logger statistics."""
        return self._async_logger.get_stats()

    def get_buffer_size(self) -> int:
        """Get current buffer size."""
        return self._async_logger.get_buffer_size()