"""Event Service for NAQ.

This module provides a centralized service for event logging, processing,
and monitoring with high-performance logging and streaming capabilities.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

from naq.exceptions import NaqException
from naq.services.base import BaseService


class EventService(BaseService):
    """Centralized service for event logging and processing.
    
    This service provides high-performance event logging, streaming,
    and history queries for job events.
    """

    def __init__(self, connection_service: "ConnectionService", stream_service: "StreamService") -> None:
        """Initialize the EventService.
        
        Args:
            connection_service: The connection service for NATS operations
            stream_service: The stream service for event streaming
        """
        super().__init__()
        self._connection_service = connection_service
        self._stream_service = stream_service

    async def _do_initialize(self) -> None:
        """Initialize the event service."""
        # Services are initialized through dependencies
        pass

    async def cleanup(self) -> None:
        """Clean up the event service resources."""
        # No specific cleanup needed
        await super().cleanup()

    async def log_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Log an event.
        
        Args:
            event_type: The type of event
            data: The event data
            
        Raises:
            NAQError: If there's an error logging the event
        """
        try:
            # Add timestamp to event data
            event_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": event_type,
                **data
            }
            
            # Get or create the events stream
            stream = await self._stream_service.ensure_stream("NAQ_EVENTS")
            
            # Publish the event to the stream
            async with self._connection_service.connection_scope() as nc:
                js = nc.jetstream()
                await js.publish("NAQ_EVENTS", event_data)
        except Exception as e:
            raise NaqException(f"Failed to log event '{event_type}': {e}") from e

    async def log_job_started(self, job_id: str, function_name: str) -> None:
        """Log a job started event.
        
        Args:
            job_id: The ID of the job
            function_name: The name of the function being executed
            
        Raises:
            NAQError: If there's an error logging the event
        """
        await self.log_event(
            "job_started",
            {
                "job_id": job_id,
                "function_name": function_name
            }
        )

    async def stream_events(self, event_type: Optional[str] = None) -> None:
        """Stream events in real-time.
        
        Args:
            event_type: Optional filter for specific event types
            
        Raises:
            NAQError: If there's an error streaming events
        """
        try:
            async with self._connection_service.connection_scope() as nc:
                js = nc.jetstream()
                
                # Subscribe to events
                if event_type:
                    subject = f"NAQ_EVENTS.{event_type}"
                else:
                    subject = "NAQ_EVENTS.*"
                    
                subscription = await js.subscribe(subject)
                
                # Process events (this would typically be done in a separate task)
                # For now, we'll just acknowledge them
                async for msg in subscription:
                    await msg.ack()
        except Exception as e:
            raise NaqException(f"Failed to stream events: {e}") from e

    async def get_event_history(
        self, 
        event_type: Optional[str] = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get event history.
        
        Args:
            event_type: Optional filter for specific event types
            limit: Maximum number of events to retrieve
            
        Returns:
            List of events
            
        Raises:
            NAQError: If there's an error retrieving event history
        """
        try:
            events = []
            
            # Get the events stream
            stream = await self._stream_service.ensure_stream("NAQ_EVENTS")
            
            # Retrieve events from the stream
            # This is a simplified implementation - in practice, you would
            # use stream consumers or direct stream access
            async with self._connection_service.connection_scope() as nc:
                js = nc.jetstream()
                
                # This is a placeholder implementation
                # In a real implementation, you would retrieve actual events
                pass
                
            return events
        except Exception as e:
            raise NaqException(f"Failed to get event history: {e}") from e


# Import at the end to avoid circular imports
from naq.services.connection import ConnectionService
from naq.services.streams import StreamService