# src/naq/events/processor.py
import asyncio
from typing import Callable, Dict, List, Optional

from loguru import logger

from ..models import JobEvent, JobEventType
from ..settings import DEFAULT_NATS_URL
from .storage import BaseEventStorage, NATSJobEventStorage


class AsyncJobEventProcessor:
    """
    Real-time event processor that subscribes to the NATS event stream
    and dispatches events to registered handlers.
    
    This component allows users to build reactive, event-driven logic
    based on job lifecycles.
    """

    def __init__(
        self,
        storage: Optional[BaseEventStorage] = None,
        nats_url: str = DEFAULT_NATS_URL
    ):
        """
        Initialize the event processor.
        
        Args:
            storage: Storage backend instance. If None, creates NATSJobEventStorage.
            nats_url: NATS URL for default storage backend.
        """
        self.storage = storage or NATSJobEventStorage(nats_url=nats_url)
        
        # Event handlers mapping: event_type -> list of handlers
        self._event_handlers: Dict[JobEventType, List[Callable]] = {}
        
        # Global handlers that receive all events
        self._global_handlers: List[Callable] = []
        
        # Internal state
        self._processing_task: Optional[asyncio.Task] = None
        self._running = False

    def add_handler(self, event_type: JobEventType, handler: Callable) -> None:
        """
        Register a handler for a specific event type.
        
        Args:
            event_type: The JobEventType to listen for.
            handler: The callable to execute when the event occurs.
                    Should accept a JobEvent parameter.
        """
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        
        self._event_handlers[event_type].append(handler)
        logger.debug(f"Registered handler for event type: {event_type}")

    def add_global_handler(self, handler: Callable) -> None:
        """
        Register a global handler that receives all events.
        
        Args:
            handler: The callable to execute for any event.
                    Should accept a JobEvent parameter.
        """
        self._global_handlers.append(handler)
        logger.debug("Registered global event handler")

    def remove_handler(self, event_type: JobEventType, handler: Callable) -> bool:
        """
        Remove a specific handler for an event type.
        
        Args:
            event_type: The JobEventType the handler is registered for.
            handler: The handler to remove.
            
        Returns:
            True if handler was found and removed, False otherwise.
        """
        if event_type in self._event_handlers:
            try:
                self._event_handlers[event_type].remove(handler)
                logger.debug(f"Removed handler for event type: {event_type}")
                return True
            except ValueError:
                pass
        return False

    def remove_global_handler(self, handler: Callable) -> bool:
        """
        Remove a global handler.
        
        Args:
            handler: The global handler to remove.
            
        Returns:
            True if handler was found and removed, False otherwise.
        """
        try:
            self._global_handlers.remove(handler)
            logger.debug("Removed global event handler")
            return True
        except ValueError:
            return False

    async def start(self) -> None:
        """Start the event processor and begin listening for events."""
        if self._running:
            return
            
        self._running = True
        
        # Start the background processing task
        self._processing_task = asyncio.create_task(self._process_events())
        
        logger.info("Started AsyncJobEventProcessor")

    async def stop(self) -> None:
        """Stop the event processor."""
        if not self._running:
            return
            
        self._running = False
        
        # Cancel the processing task
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
            
        # Close storage connection
        await self.storage.close()
        
        logger.info("Stopped AsyncJobEventProcessor")

    async def _process_events(self) -> None:
        """Background event processing loop."""
        try:
            async for event in self.storage.stream_events():
                if not self._running:
                    break
                    
                # Process the event
                await self._handle_event(event)
                
        except asyncio.CancelledError:
            logger.debug("Event processing task cancelled")
        except Exception as e:
            logger.error(f"Error in event processing loop: {e}")
        finally:
            logger.debug("Event processing loop finished")

    async def _handle_event(self, event: JobEvent) -> None:
        """
        Handle a single event by dispatching it to registered handlers.
        
        Args:
            event: The JobEvent to handle.
        """
        try:
            # Collect all handlers for this event
            handlers = []
            
            # Add specific event type handlers
            if event.event_type in self._event_handlers:
                handlers.extend(self._event_handlers[event.event_type])
            
            # Add global handlers
            handlers.extend(self._global_handlers)
            
            if not handlers:
                return  # No handlers registered
            
            # Execute all handlers concurrently
            tasks = []
            for handler in handlers:
                task = asyncio.create_task(self._execute_handler(handler, event))
                tasks.append(task)
            
            # Wait for all handlers to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Error handling event {event.event_type} for job {event.job_id}: {e}")

    async def _execute_handler(self, handler: Callable, event: JobEvent) -> None:
        """
        Execute a single handler for an event.
        
        Args:
            handler: The handler function to execute.
            event: The JobEvent to pass to the handler.
        """
        try:
            # Support both sync and async handlers
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                # Run sync handler in thread pool to avoid blocking
                await asyncio.to_thread(handler, event)
                
        except Exception as e:
            logger.error(f"Error executing event handler: {e}")

    def get_handler_count(self, event_type: Optional[JobEventType] = None) -> int:
        """
        Get the number of registered handlers.
        
        Args:
            event_type: If specified, returns count for that event type only.
                       If None, returns total count including global handlers.
                       
        Returns:
            Number of registered handlers.
        """
        if event_type is not None:
            return len(self._event_handlers.get(event_type, []))
        
        # Return total count
        total = len(self._global_handlers)
        for handlers in self._event_handlers.values():
            total += len(handlers)
        return total

    # Convenience methods for common event types

    def on_job_enqueued(self, handler: Callable) -> None:
        """Register a handler for job enqueued events."""
        self.add_handler(JobEventType.ENQUEUED, handler)

    def on_job_started(self, handler: Callable) -> None:
        """Register a handler for job started events."""
        self.add_handler(JobEventType.STARTED, handler)

    def on_job_completed(self, handler: Callable) -> None:
        """Register a handler for job completed events."""
        self.add_handler(JobEventType.COMPLETED, handler)

    def on_job_failed(self, handler: Callable) -> None:
        """Register a handler for job failed events."""
        self.add_handler(JobEventType.FAILED, handler)

    def on_job_retry_scheduled(self, handler: Callable) -> None:
        """Register a handler for job retry scheduled events."""
        self.add_handler(JobEventType.RETRY_SCHEDULED, handler)

    def on_job_scheduled(self, handler: Callable) -> None:
        """Register a handler for job scheduled events."""
        self.add_handler(JobEventType.SCHEDULED, handler)

    def on_schedule_triggered(self, handler: Callable) -> None:
        """Register a handler for schedule triggered events."""
        self.add_handler(JobEventType.SCHEDULE_TRIGGERED, handler)

    def on_all_events(self, handler: Callable) -> None:
        """Register a handler for all event types."""
        self.add_global_handler(handler)

    async def get_job_events(self, job_id: str) -> List[JobEvent]:
        """
        Get all events for a specific job.
        
        Args:
            job_id: The job ID to get events for.
            
        Returns:
            List of JobEvent objects for the specified job.
        """
        return await self.storage.get_events(job_id)

    async def stream_job_events(
        self,
        job_id: Optional[str] = None,
        event_type: Optional[JobEventType] = None,
        queue_name: Optional[str] = None,
        worker_id: Optional[str] = None
    ):
        """
        Stream events with optional filtering.
        
        Args:
            job_id: Optional job ID filter.
            event_type: Optional event type filter.
            queue_name: Optional queue name filter.
            worker_id: Optional worker ID filter.
            
        Yields:
            JobEvent objects as they are received.
        """
        async for event in self.storage.stream_events(
            job_id=job_id,
            event_type=event_type,
            queue_name=queue_name,
            worker_id=worker_id
        ):
            yield event