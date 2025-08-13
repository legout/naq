# src/naq/events/processor.py
import asyncio
from typing import Callable, Dict, List, Optional

import msgspec

from .storage import NATSJobEventStorage
from ..models import JobEvent, JobEventType
from ..utils.nats_helpers import (
    jetstream_subscription,
    nats_subscription,
)


class AsyncJobEventProcessor:
    """
    Real-time event processor that subscribes to NATS event stream and dispatches events to registered handlers.
    
    This component allows users of the `naq` library to build reactive, event-driven logic based on job lifecycles.
    """

    def __init__(self, storage: NATSJobEventStorage):
        """
        Initialize the event processor.
        
        Args:
            storage: NATSJobEventStorage instance for accessing event streams
        """
        self.storage = storage
        self._event_handlers: Dict[JobEventType, List[Callable]] = {}
        self._global_handlers: List[Callable] = []
        self._processing_task: Optional[asyncio.Task] = None
        self._running = False

    def add_handler(self, event_type: JobEventType, handler: Callable[[JobEvent], None]) -> None:
        """
        Register a handler function for a specific event type.
        
        Args:
            event_type: The type of event to handle
            handler: Callable that takes a JobEvent and returns None
        """
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    def add_global_handler(self, handler: Callable[[JobEvent], None]) -> None:
        """
        Register a handler function that will be called for any event.
        
        Args:
            handler: Callable that takes a JobEvent and returns None
        """
        self._global_handlers.append(handler)

    async def _process_events(self) -> None:
        """
        Background processing loop that consumes events from NATS and dispatches them to handlers.
        
        This method runs as a background task and continuously processes incoming events.
        """
        try:
            # Use jetstream_subscription helper for better error handling and retry logic
            async with jetstream_subscription(
                js=self.storage._js,
                stream_name=self.storage.stream_name,
                subject=f"{self.storage.subject_prefix}.>",
                consumer_name="event-processor",
                deliver_policy="new",
            ) as subscription:
                async for msg in subscription.messages:
                    try:
                        event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                        await self._dispatch_event(event)
                        await msg.ack()
                    except Exception as e:
                        # Log error but continue processing
                        print(f"Error processing event: {e}")
                        await msg.nak()
        except asyncio.CancelledError:
            # Task was cancelled, exit gracefully
            pass
        except Exception as e:
            # Log error but continue processing
            print(f"Error in event processing loop: {e}")

    async def _dispatch_event(self, event: JobEvent) -> None:
        """
        Dispatch an event to all registered handlers.
        
        Args:
            event: The JobEvent to dispatch
        """
        # Collect all handler coroutines
        handler_coroutines = []

        # Add specific event type handlers
        if event.event_type in self._event_handlers:
            for handler in self._event_handlers[event.event_type]:
                if asyncio.iscoroutinefunction(handler):
                    handler_coroutines.append(handler(event))
                else:
                    # For sync handlers, run them in a thread pool to avoid blocking
                    handler_coroutines.append(asyncio.to_thread(handler, event))

        # Add global handlers
        for handler in self._global_handlers:
            if asyncio.iscoroutinefunction(handler):
                handler_coroutines.append(handler(event))
            else:
                handler_coroutines.append(asyncio.to_thread(handler, event))

        # Execute all handlers concurrently
        if handler_coroutines:
            await asyncio.gather(*handler_coroutines, return_exceptions=True)

    async def start(self) -> None:
        """
        Start the event processing loop.
        
        This creates a background task that will process events as they arrive.
        """
        if self._running:
            return
            
        self._running = True
        self._processing_task = asyncio.create_task(self._process_events())

    async def stop(self) -> None:
        """
        Stop the event processing loop.
        
        This cancels the background task and waits for it to complete.
        """
        if not self._running:
            return
            
        self._running = False
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
            self._processing_task = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()