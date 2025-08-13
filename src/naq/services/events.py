"""
NAQ Event Service Module

This module provides the EventService class that centralizes event logging,
processing, and monitoring for the NAQ job queue system.
"""

import time
from typing import AsyncIterator, Dict, List, Optional, Any

from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .streams import StreamService
from ..models import JobEvent, JobEventType
from ..events.storage import NATSJobEventStorage
from ..events.logger import AsyncJobEventLogger
from ..exceptions import NaqConnectionError


class EventService(BaseService):
    """
    Centralized event logging, processing, and monitoring service.
    
    This service provides a unified interface for all event-related operations
    in the NAQ system, integrating with ConnectionService for NATS operations
    and StreamService for stream operations.
    """

    def __init__(
        self, 
        config: Dict[str, Any],
        connection_service: ConnectionService,
        stream_service: StreamService
    ):
        """
        Initialize the EventService.
        
        Args:
            config: Configuration dictionary
            connection_service: ConnectionService for NATS operations
            stream_service: StreamService for stream operations
        """
        super().__init__(config)
        self._connection_service = connection_service
        self._stream_service = stream_service
        self._storage: Optional[NATSJobEventStorage] = None
        self._logger: Optional[AsyncJobEventLogger] = None
        self._event_config = config.get('events', {})
        
    async def _do_initialize(self) -> None:
        """Initialize the event service and its dependencies."""
        try:
            # Extract event configuration
            nats_url = self._event_config.get('nats_url', 'nats://localhost:4222')
            stream_name = self._event_config.get('stream_name', 'NAQ_JOB_EVENTS')
            subject_prefix = self._event_config.get('subject_prefix', 'naq.jobs.events')
            batch_size = self._event_config.get('batch_size', 100)
            flush_interval = self._event_config.get('flush_interval', 5.0)
            max_buffer_size = self._event_config.get('max_buffer_size', 10000)
            
            # Initialize NATS event storage
            self._storage = NATSJobEventStorage(
                nats_url=nats_url,
                stream_name=stream_name,
                subject_prefix=subject_prefix,
            )
            
            # Initialize async event logger
            self._logger = AsyncJobEventLogger(
                storage=self._storage,
                batch_size=batch_size,
                flush_interval=flush_interval,
                max_buffer_size=max_buffer_size,
            )
            
            # Ensure the event stream exists
            await self._stream_service.ensure_stream(
                name=stream_name,
                subjects=[f"{subject_prefix}.*.*.*"],
            )
            
            # Start the event logger
            await self._logger.start()
            
            logger.info("EventService initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize EventService: {e}")
            raise NaqConnectionError(f"EventService initialization failed: {e}") from e

    async def cleanup(self) -> None:
        """Cleanup event service resources."""
        try:
            if self._logger:
                await self._logger.stop()
                self._logger = None
                
            if self._storage:
                await self._storage.close()
                self._storage = None
                
            await super().cleanup()
            logger.info("EventService cleaned up successfully")
            
        except Exception as e:
            logger.error(f"Error during EventService cleanup: {e}")
            # Continue cleanup even if there are errors

    async def log_event(self, event: JobEvent) -> None:
        """
        Log a job event.
        
        Args:
            event: JobEvent to log
            
        Raises:
            NaqConnectionError: If logging fails due to connection issues
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        try:
            await self._logger.log_event(event)
            logger.debug(f"Logged event {event.event_type.value} for job {event.job_id}")
        except Exception as e:
            logger.error(f"Failed to log event {event.event_type.value} for job {event.job_id}: {e}")
            raise NaqConnectionError(f"Failed to log event: {e}") from e

    async def log_job_started(
        self, 
        job_id: str, 
        worker_id: str, 
        queue: str
    ) -> None:
        """
        Log a job started event (convenience method).
        
        Args:
            job_id: ID of the job that started
            worker_id: ID of the worker that started the job
            queue: Name of the queue the job was taken from
            
        Raises:
            NaqConnectionError: If logging fails due to connection issues
        """
        event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue,
        )
        await self.log_event(event)

    async def stream_events(
        self, 
        filters: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[JobEvent]:
        """
        Stream events with optional filtering.
        
        Args:
            filters: Optional dictionary of filters to apply.
                     Supported filters:
                     - job_id: Filter by specific job ID
                     - event_type: Filter by event type
                     - context: Filter by context (e.g., 'worker', 'queue')
                     
        Yields:
            JobEvent: Events matching the filters
            
        Raises:
            NaqConnectionError: If streaming fails due to connection issues
        """
        if not self._storage:
            raise NaqConnectionError("EventService not initialized")
            
        try:
            # Extract filter parameters
            job_id = filters.get('job_id', '*') if filters else '*'
            event_type = filters.get('event_type') if filters else None
            context = filters.get('context') if filters else None
            
            # Convert string event type to enum if provided
            if isinstance(event_type, str):
                try:
                    event_type = JobEventType(event_type)
                except ValueError:
                    logger.warning(f"Invalid event type filter: {event_type}")
                    event_type = None
            
            # Stream events from storage
            async for event in self._storage.stream_events(
                job_id=job_id,
                context=context,
                event_type=event_type
            ):
                yield event
                
        except Exception as e:
            logger.error(f"Failed to stream events: {e}")
            raise NaqConnectionError(f"Failed to stream events: {e}") from e

    async def get_event_history(self, job_id: str) -> List[JobEvent]:
        """
        Get event history for a specific job.
        
        Args:
            job_id: ID of the job to get history for
            
        Returns:
            List[JobEvent]: List of events for the job, ordered by timestamp
            
        Raises:
            NaqConnectionError: If retrieving history fails due to connection issues
        """
        if not self._storage:
            raise NaqConnectionError("EventService not initialized")
            
        try:
            events = await self._storage.get_events(job_id)
            logger.debug(f"Retrieved {len(events)} events for job {job_id}")
            return events
            
        except Exception as e:
            logger.error(f"Failed to get event history for job {job_id}: {e}")
            raise NaqConnectionError(f"Failed to get event history: {e}") from e

    async def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job enqueued event.
        
        Args:
            job_id: ID of the job that was enqueued
            queue_name: Name of the queue the job was added to
            worker_id: Optional ID of the worker that enqueued the job
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_enqueued(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details,
        )

    async def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job completed event.
        
        Args:
            job_id: ID of the job that completed
            worker_id: ID of the worker that completed the job
            duration_ms: Duration of the job execution in milliseconds
            queue_name: Optional name of the queue the job was taken from
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_completed(
            job_id=job_id,
            worker_id=worker_id,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details=details,
        )

    async def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        error_type: str,
        error_message: str,
        duration_ms: float,
        queue_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job failed event.
        
        Args:
            job_id: ID of the job that failed
            worker_id: ID of the worker that was processing the job
            error_type: Type of error that occurred
            error_message: Error message describing what went wrong
            duration_ms: Duration of the job execution before failure in milliseconds
            queue_name: Optional name of the queue the job was taken from
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_failed(
            job_id=job_id,
            worker_id=worker_id,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            queue_name=queue_name,
            details=details,
        )

    async def log_job_retry_scheduled(
        self,
        job_id: str,
        worker_id: str,
        delay_seconds: float,
        queue_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job retry scheduled event.
        
        Args:
            job_id: ID of the job that was scheduled for retry
            worker_id: ID of the worker that scheduled the retry
            delay_seconds: Delay before the retry attempt in seconds
            queue_name: Optional name of the queue the job will be retried on
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_retry_scheduled(
            job_id=job_id,
            worker_id=worker_id,
            delay_seconds=delay_seconds,
            queue_name=queue_name,
            details=details,
        )

    async def log_job_scheduled(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp_utc: float,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job scheduled event.
        
        Args:
            job_id: ID of the job that was scheduled
            queue_name: Name of the queue the job was scheduled for
            scheduled_timestamp_utc: UTC timestamp when the job is scheduled to run
            worker_id: Optional ID of the worker that scheduled the job
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_scheduled(
            job_id=job_id,
            queue_name=queue_name,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            worker_id=worker_id,
            details=details,
        )

    async def log_job_cancelled(
        self,
        job_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a job cancelled event.
        
        Args:
            job_id: ID of the job that was cancelled
            queue_name: Name of the queue the job was cancelled from
            worker_id: Optional ID of the worker that cancelled the job
            details: Optional additional details about the event
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        await self._logger.log_job_cancelled(
            job_id=job_id,
            queue_name=queue_name,
            worker_id=worker_id,
            details=details,
        )

    async def flush_events(self) -> None:
        """
        Manually trigger a flush of all buffered events.
        
        Raises:
            NaqConnectionError: If flushing fails due to connection issues
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        try:
            await self._logger.flush()
            logger.debug("Manually flushed event buffer")
        except Exception as e:
            logger.error(f"Failed to flush events: {e}")
            raise NaqConnectionError(f"Failed to flush events: {e}") from e

    def get_logger_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the event logger.
        
        Returns:
            Dict[str, Any]: Dictionary containing logger statistics
            
        Raises:
            NaqConnectionError: If the service is not initialized
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        return self._logger.get_stats()

    def get_buffer_size(self) -> int:
        """
        Get the current size of the event buffer.
        
        Returns:
            int: Number of events currently in the buffer
            
        Raises:
            NaqConnectionError: If the service is not initialized
        """
        if not self._logger:
            raise NaqConnectionError("EventService not initialized")
            
        return self._logger.get_buffer_size()