# src/naq/services/events.py
"""
Event Service for NAQ.

This service centralizes event logging, processing, and monitoring,
providing a unified interface for job and worker event management.
"""

from typing import Any, AsyncIterator, Dict, List, Optional

from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .streams import StreamService
from ..events.logger import AsyncJobEventLogger
from ..models.events import JobEvent, WorkerEvent
from ..exceptions import NaqException


class EventService(BaseService):
    """
    Event logging and processing service.
    
    This service provides centralized event management for jobs, workers,
    and system events with high-performance logging and querying capabilities.
    """

    def __init__(
        self, 
        config: Dict[str, Any],
        connection_service: ConnectionService,
        stream_service: StreamService
    ):
        """
        Initialize the event service.
        
        Args:
            config: Service configuration
            connection_service: Connection service for NATS operations
            stream_service: Stream service for event stream management
        """
        super().__init__(config)
        self.connection_service = connection_service
        self.stream_service = stream_service
        self._event_logger: Optional[AsyncJobEventLogger] = None
        
        # Get event configuration from events section
        events_config = self.events_config
        self._batch_size = events_config.batch_size
        self._flush_interval = events_config.flush_interval
        self._max_buffer_size = events_config.max_buffer_size
        self._enabled = events_config.enabled

    async def _do_initialize(self) -> None:
        """Initialize the event service."""
        if not self._enabled:
            logger.debug("Event logging is disabled")
            return
            
        # Get storage URL for events
        events_config = self.events_config
        storage_url = events_config.get_storage_url(self.connection_service.default_nats_url)
        
        # Initialize the event logger
        self._event_logger = AsyncJobEventLogger(
            batch_size=self._batch_size,
            flush_interval=self._flush_interval,
            max_buffer_size=self._max_buffer_size,
            nats_url=storage_url
        )
        await self._event_logger.start()
        logger.debug(f"Event service initialized (enabled={self._enabled}, batch_size={self._batch_size}, flush_interval={self._flush_interval})")

    async def _do_cleanup(self) -> None:
        """Clean up event service resources."""
        if self._event_logger:
            await self._event_logger.stop()
            self._event_logger = None

    # Job Event Methods
    async def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log job enqueued event."""
        if self._enabled and self._event_logger:
            await self._event_logger.log_job_enqueued(
                job_id=job_id,
                queue_name=queue_name,
                worker_id=worker_id,
                **kwargs
            )

    async def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        **kwargs
    ) -> None:
        """Log job started event."""
        if self._event_logger:
            await self._event_logger.log_job_started(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                **kwargs
            )

    async def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        duration_ms: int,
        **kwargs
    ) -> None:
        """Log job completed event."""
        if self._event_logger:
            await self._event_logger.log_job_completed(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                duration_ms=duration_ms,
                **kwargs
            )

    async def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        error_type: str,
        error_message: str,
        duration_ms: int,
        **kwargs
    ) -> None:
        """Log job failed event."""
        if self._event_logger:
            await self._event_logger.log_job_failed(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                error_type=error_type,
                error_message=error_message,
                duration_ms=duration_ms,
                **kwargs
            )

    async def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        queue_name: str,
        retry_count: int,
        retry_delay: float,
        **kwargs
    ) -> None:
        """Log job retry scheduled event."""
        if self._event_logger:
            await self._event_logger.log_job_retry_scheduled(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                retry_count=retry_count,
                retry_delay=retry_delay,
                **kwargs
            )

    async def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp: float,
        **kwargs
    ) -> None:
        """Log job scheduled event."""
        if self._event_logger:
            await self._event_logger.log_job_scheduled(
                job_id=job_id,
                queue_name=queue_name,
                scheduled_timestamp=scheduled_timestamp,
                **kwargs
            )

    # Worker Event Methods
    async def log_worker_started(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        queue_names: List[str],
        concurrency_limit: int,
        **kwargs
    ) -> None:
        """Log worker started event."""
        if self._event_logger:
            await self._event_logger.log_worker_started(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                queue_names=queue_names,
                concurrency_limit=concurrency_limit,
                **kwargs
            )

    async def log_worker_stopped(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        **kwargs
    ) -> None:
        """Log worker stopped event."""
        if self._event_logger:
            await self._event_logger.log_worker_stopped(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                **kwargs
            )

    async def log_worker_idle(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        **kwargs
    ) -> None:
        """Log worker idle event."""
        if self._event_logger:
            await self._event_logger.log_worker_idle(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                **kwargs
            )

    async def log_worker_busy(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        current_job_id: str,
        **kwargs
    ) -> None:
        """Log worker busy event."""
        if self._event_logger:
            await self._event_logger.log_worker_busy(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                current_job_id=current_job_id,
                **kwargs
            )

    async def log_worker_heartbeat(
        self,
        worker_id: str,
        hostname: str,
        pid: int,
        active_jobs: int,
        concurrency_limit: int,
        **kwargs
    ) -> None:
        """Log worker heartbeat event."""
        if self._event_logger:
            await self._event_logger.log_worker_heartbeat(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                active_jobs=active_jobs,
                concurrency_limit=concurrency_limit,
                **kwargs
            )

    # Schedule Event Methods
    async def log_schedule_created(
        self,
        job_id: str,
        queue_name: str,
        schedule_expression: str,
        **kwargs
    ) -> None:
        """Log schedule created event."""
        if self._event_logger:
            await self._event_logger.log_schedule_created(
                job_id=job_id,
                queue_name=queue_name,
                schedule_expression=schedule_expression,
                **kwargs
            )

    async def log_schedule_triggered(
        self,
        job_id: str,
        queue_name: str,
        trigger_timestamp: float,
        **kwargs
    ) -> None:
        """Log schedule triggered event."""
        if self._event_logger:
            await self._event_logger.log_schedule_triggered(
                job_id=job_id,
                queue_name=queue_name,
                trigger_timestamp=trigger_timestamp,
                **kwargs
            )

    async def log_schedule_cancelled(
        self,
        job_id: str,
        queue_name: str,
        **kwargs
    ) -> None:
        """Log schedule cancelled event."""
        if self._event_logger:
            await self._event_logger.log_schedule_cancelled(
                job_id=job_id,
                queue_name=queue_name,
                **kwargs
            )

    async def log_schedule_paused(
        self,
        job_id: str,
        queue_name: str,
        **kwargs
    ) -> None:
        """Log schedule paused event."""
        if self._event_logger:
            await self._event_logger.log_schedule_paused(
                job_id=job_id,
                queue_name=queue_name,
                **kwargs
            )

    async def log_schedule_resumed(
        self,
        job_id: str,
        queue_name: str,
        **kwargs
    ) -> None:
        """Log schedule resumed event."""
        if self._event_logger:
            await self._event_logger.log_schedule_resumed(
                job_id=job_id,
                queue_name=queue_name,
                **kwargs
            )

    async def log_schedule_modified(
        self,
        job_id: str,
        queue_name: str,
        modifications: Dict[str, Any],
        **kwargs
    ) -> None:
        """Log schedule modified event."""
        if self._event_logger:
            await self._event_logger.log_schedule_modified(
                job_id=job_id,
                queue_name=queue_name,
                modifications=modifications,
                **kwargs
            )

    async def log_scheduler_error(
        self,
        error_type: str,
        error_message: str,
        schedule_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log scheduler error event."""
        if self._event_logger:
            await self._event_logger.log_scheduler_error(
                error_type=error_type,
                error_message=error_message,
                schedule_id=schedule_id,
                queue_name=queue_name,
                **kwargs
            )

    # General Event Methods
    async def log_event(self, event: JobEvent) -> None:
        """
        Log a custom job event.
        
        Args:
            event: The job event to log
        """
        if self._event_logger:
            await self._event_logger.log_event(event)

    async def log_worker_event(self, event: WorkerEvent) -> None:
        """
        Log a custom worker event.
        
        Args:
            event: The worker event to log
        """
        if self._event_logger:
            await self._event_logger.log_worker_event(event)

    # Event Querying Methods
    async def get_job_events(
        self,
        job_id: str,
        *,
        limit: Optional[int] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> List[JobEvent]:
        """
        Get events for a specific job.
        
        Args:
            job_id: Job ID to get events for
            limit: Maximum number of events to return
            start_time: Start time filter (timestamp)
            end_time: End time filter (timestamp)
            
        Returns:
            List of job events
        """
        if not self._event_logger:
            return []
            
        try:
            return await self._event_logger.get_job_events(
                job_id=job_id,
                limit=limit,
                start_time=start_time,
                end_time=end_time
            )
        except Exception as e:
            logger.error(f"Error getting events for job {job_id}: {e}")
            return []

    async def get_worker_events(
        self,
        worker_id: str,
        *,
        limit: Optional[int] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> List[WorkerEvent]:
        """
        Get events for a specific worker.
        
        Args:
            worker_id: Worker ID to get events for
            limit: Maximum number of events to return
            start_time: Start time filter (timestamp)
            end_time: End time filter (timestamp)
            
        Returns:
            List of worker events
        """
        if not self._event_logger:
            return []
            
        try:
            return await self._event_logger.get_worker_events(
                worker_id=worker_id,
                limit=limit,
                start_time=start_time,
                end_time=end_time
            )
        except Exception as e:
            logger.error(f"Error getting events for worker {worker_id}: {e}")
            return []

    async def get_queue_events(
        self,
        queue_name: str,
        *,
        limit: Optional[int] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None
    ) -> List[JobEvent]:
        """
        Get events for a specific queue.
        
        Args:
            queue_name: Queue name to get events for
            limit: Maximum number of events to return
            start_time: Start time filter (timestamp)
            end_time: End time filter (timestamp)
            
        Returns:
            List of job events for the queue
        """
        if not self._event_logger:
            return []
            
        try:
            return await self._event_logger.get_queue_events(
                queue_name=queue_name,
                limit=limit,
                start_time=start_time,
                end_time=end_time
            )
        except Exception as e:
            logger.error(f"Error getting events for queue {queue_name}: {e}")
            return []

    async def stream_events(
        self,
        *,
        job_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        event_types: Optional[List[str]] = None
    ) -> AsyncIterator[JobEvent]:
        """
        Stream events with optional filtering.
        
        Args:
            job_id: Filter by job ID
            worker_id: Filter by worker ID
            queue_name: Filter by queue name
            event_types: Filter by event types
            
        Yields:
            Job events matching the filters
        """
        if not self._event_logger:
            return
            
        try:
            async for event in self._event_logger.stream_events(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                event_types=event_types
            ):
                yield event
        except Exception as e:
            logger.error(f"Error streaming events: {e}")

    async def flush_events(self) -> None:
        """Force flush any buffered events."""
        if self._event_logger:
            await self._event_logger.flush()

    @property
    def is_logging_enabled(self) -> bool:
        """Check if event logging is enabled and working."""
        return self._enabled and self._event_logger is not None and self._event_logger.is_running

    async def get_event_stats(self) -> Dict[str, Any]:
        """
        Get event logging statistics.
        
        Returns:
            Dictionary with event logging statistics
        """
        if not self._event_logger:
            return {
                "logging_enabled": False,
                "buffered_events": 0,
                "total_events_logged": 0,
                "last_flush_time": None
            }
            
        try:
            return await self._event_logger.get_stats()
        except Exception as e:
            logger.error(f"Error getting event stats: {e}")
            return {
                "logging_enabled": True,
                "error": str(e)
            }