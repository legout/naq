# Subtask 05.07: Implement EventService

## Overview
Implement the EventService to centralize event logging, processing, and monitoring across the codebase.

## Objectives
- Centralize all event logging operations through EventService
- Provide high-performance event logging capabilities
- Implement event streaming and filtering
- Add event history queries and analytics support
- Support batch event processing
- Integrate with ConnectionService and StreamService
- Ensure consistent event formatting and storage

## Implementation Steps

### 1. Implement EventService Class
Create the EventService that inherits from BaseService:

```python
import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, AsyncIterator, Union
from datetime import datetime
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from .connection import ConnectionService
from .streams import StreamService
from naq.exceptions import EventError

class EventService(BaseService):
    """Event logging and processing service."""
    
    def __init__(self, config: Dict[str, Any], 
                 connection_service: ConnectionService,
                 stream_service: StreamService):
        super().__init__(config)
        self.connection_service = connection_service
        self.stream_service = stream_service
        self._logger = logging.getLogger(__name__)
        self._event_buffer: List[Dict[str, Any]] = []
        self._buffer_size = config.get("event_buffer_size", 100)
        self._flush_interval = config.get("event_flush_interval", 5.0)
        self._flush_task: Optional[asyncio.Task] = None
        
    async def _do_initialize(self) -> None:
        """Initialize event service."""
        self._logger.info("Initializing EventService")
        
        # Ensure event stream exists
        await self.stream_service.ensure_stream(
            name="events",
            subjects=["events.>"],
            retention="limits",
            max_msgs=1000000
        )
        
        # Start buffer flush task
        self._flush_task = asyncio.create_task(self._buffer_flush_loop())
        
    async def _do_cleanup(self) -> None:
        """Cleanup event service."""
        self._logger.info("Cleaning up EventService")
        
        # Cancel flush task
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
                
        # Flush remaining events
        await self._flush_buffer()
        
    async def _buffer_flush_loop(self) -> None:
        """Background task to flush event buffer periodically."""
        while True:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Error in event flush loop: {e}")
                
    async def _flush_buffer(self) -> None:
        """Flush buffered events to stream."""
        if not self._event_buffer:
            return
            
        try:
            js = await self.connection_service.get_jetstream()
            
            # Publish buffered events
            for event in self._event_buffer:
                subject = f"events.{event.get('type', 'unknown')}"
                payload = json.dumps(event).encode()
                await js.publish(subject, payload)
                
            self._logger.debug(f"Flushed {len(self._event_buffer)} events")
            self._event_buffer.clear()
            
        except Exception as e:
            self._logger.error(f"Failed to flush event buffer: {e}")
            
    async def log_event(self, event: Union[Dict[str, Any], str]) -> None:
        """Log job event.
        
        Args:
            event: Event data as dict or string
        """
        try:
            # Convert string event to dict if needed
            if isinstance(event, str):
                event = {'message': event, 'type': 'log'}
                
            # Add timestamp if not present
            if 'timestamp' not in event:
                event['timestamp'] = datetime.utcnow().isoformat()
                
            # Add service metadata
            event['service'] = 'naq'
            event['service_version'] = self.config.get('version', '1.0.0')
            
            # Buffer the event
            self._event_buffer.append(event)
            
            # Flush buffer if full
            if len(self._event_buffer) >= self._buffer_size:
                await self._flush_buffer()
                
        except Exception as e:
            raise ServiceError(f"Failed to log event: {e}")
            
    async def log_job_started(self, job_id: str, worker_id: str, queue: str) -> None:
        """Convenience method for job started event."""
        await self.log_event({
            'type': 'job_started',
            'job_id': job_id,
            'worker_id': worker_id,
            'queue': queue,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def log_job_completed(self, job_id: str, duration: float, success: bool) -> None:
        """Convenience method for job completed event."""
        await self.log_event({
            'type': 'job_completed',
            'job_id': job_id,
            'duration': duration,
            'success': success,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def log_job_failed(self, job_id: str, error: str, duration: float) -> None:
        """Convenience method for job failed event."""
        await self.log_event({
            'type': 'job_failed',
            'job_id': job_id,
            'error': error,
            'duration': duration,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def log_worker_started(self, worker_id: str, queue: str) -> None:
        """Convenience method for worker started event."""
        await self.log_event({
            'type': 'worker_started',
            'worker_id': worker_id,
            'queue': queue,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def log_worker_stopped(self, worker_id: str, queue: str) -> None:
        """Convenience method for worker stopped event."""
        await self.log_event({
            'type': 'worker_stopped',
            'worker_id': worker_id,
            'queue': queue,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def stream_events(self, filters: Optional[Dict[str, Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        """Stream events with optional filtering.
        
        Args:
            filters: Dictionary of field-value pairs to filter by
            
        Yields:
            Event dictionaries
        """
        try:
            js = await self.connection_service.get_jetstream()
            
            # Create consumer for events stream
            consumer_name = f"events_consumer_{asyncio.get_event_loop().time()}"
            consumer = await js.pull_subscribe("events.>", consumer_name)
            
            while True:
                try:
                    # Get messages with timeout
                    msgs = await consumer.fetch(1, timeout=1.0)
                    
                    for msg in msgs:
                        try:
                            # Parse event payload
                            event = json.loads(msg.data.decode())
                            
                            # Apply filters if provided
                            if filters:
                                if not self._matches_filters(event, filters):
                                    await consumer.ack(msg)
                                    continue
                                    
                            # Add message metadata
                            event['_message_id'] = msg.seq
                            event['_timestamp'] = datetime.utcnow().isoformat()
                            
                            yield event
                            
                            # Acknowledge message
                            await consumer.ack(msg)
                            
                        except Exception as e:
                            self._logger.error(f"Error processing event: {e}")
                            await consumer.ack(msg)
                            
                except asyncio.TimeoutError:
                    # No messages available, continue loop
                    continue
                    
        except Exception as e:
            raise ServiceError(f"Failed to stream events: {e}")
            
    def _matches_filters(self, event: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if event matches filter criteria."""
        for field, expected_value in filters.items():
            event_value = event.get(field)
            
            if event_value is None:
                return False
                
            # Handle different comparison types
            if isinstance(expected_value, dict):
                # Handle range queries, regex, etc.
                if 'min' in expected_value and event_value < expected_value['min']:
                    return False
                if 'max' in expected_value and event_value > expected_value['max']:
                    return False
                if 'regex' in expected_value:
                    import re
                    if not re.search(expected_value['regex'], str(event_value)):
                        return False
            else:
                # Simple equality check
                if event_value != expected_value:
                    return False
                    
        return True
        
    async def get_event_history(self, job_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get event history for job.
        
        Args:
            job_id: Job ID to get history for
            limit: Maximum number of events to return
            
        Returns:
            List of event dictionaries
        """
        try:
            js = await self.connection_service.get_jetstream()
            
            # Get stream info
            stream_info = await js.stream_info("events")
            
            # Search for job-related events
            events = []
            subject_filter = f"events.*"
            
            # Use stream message iterator to find job events
            for msg in await js.stream_messages("events", subject_filter):
                try:
                    event = json.loads(msg.data.decode())
                    
                    if event.get('job_id') == job_id:
                        events.append(event)
                        
                        if len(events) >= limit:
                            break
                            
                except Exception as e:
                    self._logger.error(f"Error processing event from stream: {e}")
                    
            return sorted(events, key=lambda x: x.get('timestamp', ''))
            
        except Exception as e:
            raise ServiceError(f"Failed to get event history for job {job_id}: {e}")
            
    async def get_event_stats(self, start_time: Optional[str] = None, 
                            end_time: Optional[str] = None) -> Dict[str, Any]:
        """Get event statistics.
        
        Args:
            start_time: Start time for statistics (ISO format)
            end_time: End time for statistics (ISO format)
            
        Returns:
            Event statistics dictionary
        """
        try:
            js = await self.connection_service.get_jetstream()
            
            # Get stream info
            stream_info = await js.stream_info("events")
            
            stats = {
                'total_events': stream_info.state.messages,
                'stream_size': stream_info.state.bytes,
                'subjects': stream_info.config.subjects,
                'start_time': start_time,
                'end_time': end_time
            }
            
            # Get event type distribution
            event_types = {}
            subject_filter = "events.*"
            
            for msg in await js.stream_messages("events", subject_filter):
                try:
                    event = json.loads(msg.data.decode())
                    event_type = event.get('type', 'unknown')
                    event_types[event_type] = event_types.get(event_type, 0) + 1
                except Exception:
                    pass
                    
            stats['event_types'] = event_types
            
            return stats
            
        except Exception as e:
            raise ServiceError(f"Failed to get event stats: {e}")
            
    async def batch_log_events(self, events: List[Dict[str, Any]]) -> None:
        """Log multiple events in batch.
        
        Args:
            events: List of event dictionaries
        """
        try:
            # Add timestamp to all events
            timestamp = datetime.utcnow().isoformat()
            for event in events:
                if 'timestamp' not in event:
                    event['timestamp'] = timestamp
                    
            # Add service metadata
            for event in events:
                event['service'] = 'naq'
                event['service_version'] = self.config.get('version', '1.0.0')
                
            # Buffer all events
            self._event_buffer.extend(events)
            
            # Flush buffer if full
            if len(self._event_buffer) >= self._buffer_size:
                await self._flush_buffer()
                
        except Exception as e:
            raise ServiceError(f"Failed to batch log events: {e}")
            
    async def cleanup_old_events(self, max_age: int = 86400) -> None:
        """Clean up old events from stream.
        
        Args:
            max_age: Maximum age in seconds to keep events
        """
        try:
            await self.stream_service.purge_stream("events")
            self._logger.info(f"Cleaned up events older than {max_age}s")
            
        except Exception as e:
            self._logger.error(f"Failed to cleanup old events: {e}")
```

### 2. Update Service Registration
Register the EventService in the service system:

```python
# In __init__.py or base.py
from .events import EventService

# Register the service
ServiceManager.register_service("events", EventService)
```

### 3. Add Event Indexing
Implement event indexing for better performance:

```python
async def create_event_index(self, index_name: str, fields: List[str]) -> None:
    """Create event index for faster queries.
    
    Args:
        index_name: Index name
        fields: List of fields to index
    """
    try:
        # Store index configuration in KV store
        await self.kv_service.put(
            bucket="event_indexes",
            key=index_name,
            value={'fields': fields, 'created_at': datetime.utcnow().isoformat()}
        )
        
        self._logger.info(f"Created event index {index_name} for fields {fields}")
        
    except Exception as e:
        raise ServiceError(f"Failed to create event index {index_name}: {e}")
```

### 4. Add Event Analytics
Implement basic event analytics:

```python
async def get_event_analytics(self, event_type: str, 
                            start_time: Optional[str] = None,
                            end_time: Optional[str] = None) -> Dict[str, Any]:
    """Get analytics for specific event type.
    
    Args:
        event_type: Event type to analyze
        start_time: Start time for analysis
        end_time: End time for analysis
        
    Returns:
        Analytics data
    """
    try:
        events = []
        
        # Get events of specified type
        async for event in self.stream_events({'type': event_type}):
            if start_time and event.get('timestamp') < start_time:
                continue
            if end_time and event.get('timestamp') > end_time:
                continue
                
            events.append(event)
            
        # Calculate analytics
        analytics = {
            'event_type': event_type,
            'total_count': len(events),
            'success_rate': sum(1 for e in events if e.get('success', False)) / len(events) if events else 0,
            'average_duration': sum(e.get('duration', 0) for e in events) / len(events) if events else 0,
            'time_range': {
                'start': start_time,
                'end': end_time
            }
        }
        
        return analytics
        
    except Exception as e:
        raise ServiceError(f"Failed to get event analytics: {e}")
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_event_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        
        # Log job started
        await js.publish("job_events", json.dumps({
            'type': 'job_started',
            'job_id': job_id,
            'worker_id': worker_id
        }).encode())
        
        # Log job completed
        await js.publish("job_events", json.dumps({
            'type': 'job_completed',
            'job_id': job_id,
            'duration': duration
        }).encode())
        
        # Stream events
        async for msg in js.subscribe("job_events"):
            event = json.loads(msg.data.decode())
            # Process event
            
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_event_pattern():
    async with ServiceManager(config) as services:
        event_service = await services.get_service(EventService)
        
        # Log events with convenience methods
        await event_service.log_job_started(job_id, worker_id, queue)
        await event_service.log_job_completed(job_id, duration, True)
        await event_service.log_job_failed(job_id, "error message", duration)
        
        # Stream events with filtering
        async for event in event_service.stream_events({'type': 'job_completed'}):
            print(f"Job completed: {event['job_id']}")
            
        # Get event history
        history = await event_service.get_event_history(job_id)
        
        # Get event statistics
        stats = await event_service.get_event_stats()
        
        # Batch log events
        events = [
            {'type': 'custom_event', 'data': 'value1'},
            {'type': 'custom_event', 'data': 'value2'}
        ]
        await event_service.batch_log_events(events)
```

## Success Criteria
- [ ] EventService implemented with high-performance event logging
- [ ] Event streaming and filtering functional
- [ ] Event history queries working
- [ ] Batch event processing implemented
- [ ] Event analytics capabilities added
- [ ] Integration with ConnectionService and StreamService successful
- [ ] Buffer management and flushing working correctly
- [ ] All existing event functionality preserved

## Dependencies
- **Depends on**: Subtasks 05.03-05.05 (Core services)
- **Prepares for**: Subtask 05.08 (Implement SchedulerService)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 1.5 hours
- **Total**: 4.5-5.5 hours