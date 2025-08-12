# NAQ: NATS-based Job Event Logging System - Complete Implementation Guide

## Table of Contents
1. [Overview & NATS Architecture](#overview--nats-architecture)
2. [Job Event Model](#job-event-model)
3. [NATS Subject Design](#nats-subject-design)
4. [NATS Job Event Storage](#nats-job-event-storage)
5. [Job Event Logger](#job-event-logger)
6. [Job Event Processor](#job-event-processor)
7. [Integration Patterns](#integration-patterns)
8. [Performance Optimization](#performance-optimization)
9. [Configuration & Setup](#configuration--setup)
10. [Implementation Examples](#implementation-examples)

## Overview & NATS Architecture

NAQ (NATS Async Queue) is a high-performance job processing system built on NATS messaging. The event logging system leverages NATS JetStream for persistent, scalable job lifecycle monitoring.

### Why NATS for Job Event Logging?

1. **Native Streaming**: JetStream provides durable, ordered message streaming
2. **Subject-based Routing**: Natural filtering by job ID, event type, worker, queue
3. **Real-time Pub/Sub**: Immediate event delivery to interested consumers
4. **Horizontal Scaling**: Multi-server clusters with automatic failover
5. **Message Ordering**: Guarantees event ordering within job streams
6. **Persistence Options**: Memory, file, and replicated storage
7. **Consumer Groups**: Load balancing and fault tolerance

### Architecture Comparison: SQLite vs NATS

| Feature | SQLite | NATS JetStream |
|---------|--------|----------------|
| **Scalability** | Single process | Distributed cluster |
| **Real-time** | Polling required | Native pub/sub |
| **Persistence** | File/Memory | File/Memory/Replicated |
| **Ordering** | Query-based | Stream-guaranteed |
| **Filtering** | SQL WHERE | Subject wildcards |
| **Streaming** | Application-level | Built-in streaming |
| **Fault Tolerance** | Single point of failure | Multi-node redundancy |

### NATS JetStream Configuration

```yaml
# NATS Server with JetStream enabled
jetstream {
  store_dir: "/var/lib/nats/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}

# Stream configuration for job events
stream {
  name: "NAQ_JOB_EVENTS"
  subjects: ["naq.jobs.events.>"]
  storage: file
  retention: limits
  max_age: 168h        # 7 days
  max_msgs: 10000000   # 10M messages
  max_bytes: 10GB
  max_msg_size: 1MB
  duplicate_window: 2m
}
```

## Job Event Model

### JobEvent Structure

Updated from TaskEvent to JobEvent with NAQ branding:

```python
from enum import Enum
import time
from typing import Any, Dict, Optional
import msgspec

class JobEventType(str, Enum):
    """Job lifecycle event types."""
    
    # Job lifecycle
    ENQUEUED = "enqueued"
    DEQUEUED = "dequeued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    
    # Retry events
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_STARTED = "retry_started"
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    
    # Dependency events
    DEPENDENCY_SATISFIED = "dependency_satisfied"
    DEPENDENCY_FAILED = "dependency_failed"
    WAITING_FOR_DEPENDENCIES = "waiting_for_dependencies"
    
    # Schedule events
    SCHEDULED = "scheduled"
    SCHEDULE_TRIGGERED = "schedule_triggered"
    SCHEDULE_PAUSED = "schedule_paused"
    SCHEDULE_RESUMED = "schedule_resumed"
    SCHEDULE_DEACTIVATED = "schedule_deactivated"
    
    # Worker events
    WORKER_ASSIGNED = "worker_assigned"
    WORKER_RELEASED = "worker_released"
    
    # Queue events
    QUEUE_PRIORITY_CHANGED = "queue_priority_changed"
    QUEUE_MOVED = "queue_moved"
    
    # TTL events
    TTL_EXPIRED = "ttl_expired"
    TTL_EXTENDED = "ttl_extended"


class JobEvent(msgspec.Struct):
    """Job lifecycle event for comprehensive logging and monitoring."""
    
    # Core identification
    job_id: str
    event_type: JobEventType
    timestamp: float = msgspec.field(default_factory=time.time)
    
    # Context information
    worker_id: Optional[str] = None
    worker_type: Optional[str] = None
    queue_name: Optional[str] = None
    
    # Event-specific data
    message: Optional[str] = None
    details: Dict[str, Any] = msgspec.field(default_factory=dict)
    
    # Error information (for failure events)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    
    # Performance metrics
    duration_ms: Optional[float] = None
    memory_mb: Optional[float] = None
    cpu_time_ms: Optional[float] = None
    
    # Metadata
    tags: Dict[str, str] = msgspec.field(default_factory=dict)
    metadata: Dict[str, Any] = msgspec.field(default_factory=dict)
    
    # NATS-specific fields
    nats_subject: Optional[str] = None
    nats_sequence: Optional[int] = None
    
    @classmethod
    def enqueued(
        cls,
        job_id: str,
        queue_name: str,
        priority: int = 0,
        **kwargs
    ) -> "JobEvent":
        """Create job enqueued event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.ENQUEUED,
            queue_name=queue_name,
            message=f"Job enqueued to '{queue_name}' with priority {priority}",
            details={"priority": priority},
            **kwargs
        )
    
    @classmethod
    def started(
        cls,
        job_id: str,
        worker_id: str,
        worker_type: str,
        **kwargs
    ) -> "JobEvent":
        """Create job started event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.STARTED,
            worker_id=worker_id,
            worker_type=worker_type,
            message=f"Job started on {worker_type} worker {worker_id}",
            **kwargs
        )
    
    @classmethod
    def completed(
        cls,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        **kwargs
    ) -> "JobEvent":
        """Create job completed event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.COMPLETED,
            worker_id=worker_id,
            duration_ms=duration_ms,
            message=f"Job completed successfully in {duration_ms:.2f}ms",
            **kwargs
        )
    
    @classmethod
    def failed(
        cls,
        job_id: str,
        worker_id: str,
        error: Exception,
        duration_ms: float,
        retry_count: int = 0,
        **kwargs
    ) -> "JobEvent":
        """Create job failed event."""
        return cls(
            job_id=job_id,
            event_type=JobEventType.FAILED,
            worker_id=worker_id,
            duration_ms=duration_ms,
            error_type=type(error).__name__,
            error_message=str(error),
            message=f"Job failed after {duration_ms:.2f}ms (retry {retry_count})",
            details={"retry_count": retry_count},
            **kwargs
        )
    
    def to_nats_subject(self) -> str:
        """Generate NATS subject for this event."""
        base = f"naq.jobs.events.{self.job_id}"
        
        if self.worker_id:
            return f"{base}.worker.{self.worker_id}.{self.event_type.value}"
        elif self.queue_name:
            return f"{base}.queue.{self.queue_name}.{self.event_type.value}"
        else:
            return f"{base}.{self.event_type.value}"
```

## NATS Subject Design

### Subject Hierarchy

```
naq.jobs.events.{job_id}.{event_type}                    # Basic job event
naq.jobs.events.{job_id}.lifecycle.{event_type}          # Lifecycle events
naq.jobs.events.{job_id}.worker.{worker_id}.{event_type} # Worker-specific events
naq.jobs.events.{job_id}.queue.{queue_name}.{event_type} # Queue-specific events
naq.jobs.events.{job_id}.retry.{retry_count}.{event_type} # Retry events
naq.jobs.events.{job_id}.schedule.{schedule_name}.{event_type} # Schedule events

# Global event aggregation
naq.jobs.events.global.{event_type}                      # All events of type
naq.jobs.events.global.worker.{worker_id}.{event_type}   # Worker aggregation
naq.jobs.events.global.queue.{queue_name}.{event_type}   # Queue aggregation
```

### Subject Examples

```
# Job lifecycle events
naq.jobs.events.job-123.enqueued
naq.jobs.events.job-123.started
naq.jobs.events.job-123.completed
naq.jobs.events.job-123.failed

# Worker-specific events
naq.jobs.events.job-123.worker.worker-456.assigned
naq.jobs.events.job-123.worker.worker-456.started
naq.jobs.events.job-123.worker.worker-456.released

# Queue-specific events
naq.jobs.events.job-123.queue.high-priority.enqueued
naq.jobs.events.job-123.queue.high-priority.dequeued

# Global aggregation
naq.jobs.events.global.failed                  # All job failures
naq.jobs.events.global.worker.worker-456.>     # All events from worker-456
naq.jobs.events.global.queue.high-priority.>   # All high-priority queue events
```

### Subscription Patterns

```python
# Subscribe to specific job events
"naq.jobs.events.job-123.>"

# Subscribe to all job failures
"naq.jobs.events.*.failed"

# Subscribe to all worker events
"naq.jobs.events.*.worker.*.>"

# Subscribe to specific queue events
"naq.jobs.events.*.queue.high-priority.>"

# Subscribe to all events (monitoring)
"naq.jobs.events.>"
```

## NATS Job Event Storage

### NATSJobEventStorage Implementation

```python
import asyncio
import json
from typing import Any, AsyncIterator, Dict, List, Optional
from datetime import datetime, timedelta

import nats
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, ConsumerConfig, DeliverPolicy
import msgspec

from .base import BaseJobEventStorage
from ..models.event import JobEvent, JobEventType
from ..config.loader import LoggingConfig


class NATSJobEventStorage(BaseJobEventStorage):
    """NATS JetStream-based job event storage."""
    
    def __init__(
        self,
        nats_url: str = "nats://localhost:4222",
        stream_name: str = "NAQ_JOB_EVENTS",
        subject_prefix: str = "naq.jobs.events",
        connection_options: Optional[Dict[str, Any]] = None,
        stream_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize NATS job event storage.
        
        Args:
            nats_url: NATS server URL
            stream_name: JetStream stream name
            subject_prefix: Subject prefix for all events
            connection_options: NATS connection options
            stream_config: JetStream stream configuration
        """
        self.nats_url = nats_url
        self.stream_name = stream_name
        self.subject_prefix = subject_prefix
        self.connection_options = connection_options or {}
        self.stream_config = stream_config or self._default_stream_config()
        
        self._nc: Optional[nats.NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._logger = LoggingConfig.get_logger("storage.nats.events")
        
        # Consumer tracking for cleanup
        self._consumers: List[str] = []
        self._subscriptions: List[nats.js.JetStreamSubscription] = []
    
    def _default_stream_config(self) -> Dict[str, Any]:
        """Default JetStream configuration."""
        return {
            "subjects": [f"{self.subject_prefix}.>"],
            "storage": "file",
            "retention": "limits",
            "max_age": 7 * 24 * 60 * 60 * 1000000000,  # 7 days in nanoseconds
            "max_msgs": 10_000_000,
            "max_bytes": 10 * 1024 * 1024 * 1024,  # 10GB
            "max_msg_size": 1024 * 1024,  # 1MB
            "duplicate_window": 2 * 60 * 1000000000,  # 2 minutes in nanoseconds
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._disconnect()
    
    async def _connect(self) -> None:
        """Connect to NATS and setup JetStream."""
        if self._nc and not self._nc.is_closed:
            return
        
        try:
            # Connect to NATS
            self._nc = await nats.connect(self.nats_url, **self.connection_options)
            self._js = self._nc.jetstream()
            
            # Create or update stream
            await self._setup_stream()
            
            self._logger.info(f"Connected to NATS at {self.nats_url}")
            
        except Exception as e:
            self._logger.error(f"Failed to connect to NATS: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """Disconnect from NATS."""
        try:
            # Clean up subscriptions
            for subscription in self._subscriptions:
                try:
                    await subscription.unsubscribe()
                except:
                    pass
            self._subscriptions.clear()
            
            # Close NATS connection
            if self._nc and not self._nc.is_closed:
                await self._nc.close()
                self._nc = None
                self._js = None
            
            self._logger.info("Disconnected from NATS")
            
        except Exception as e:
            self._logger.error(f"Error disconnecting from NATS: {e}")
    
    async def _setup_stream(self) -> None:
        """Create or update JetStream stream."""
        try:
            # Check if stream exists
            try:
                stream_info = await self._js.stream_info(self.stream_name)
                self._logger.debug(f"Stream {self.stream_name} exists with {stream_info.state.messages} messages")
            except nats.js.errors.NotFoundError:
                # Create new stream
                stream_config = StreamConfig(
                    name=self.stream_name,
                    **self.stream_config
                )
                
                await self._js.add_stream(stream_config)
                self._logger.info(f"Created JetStream stream: {self.stream_name}")
                
        except Exception as e:
            self._logger.error(f"Failed to setup stream: {e}")
            raise
    
    async def store_event(self, event: JobEvent) -> None:
        """Store a job event."""
        if not self._js:
            raise RuntimeError("Not connected to NATS")
        
        # Generate subject for the event
        subject = self._generate_subject(event)
        
        # Serialize event
        data = msgspec.msgpack.encode(event)
        
        # Add NATS metadata to event
        event.nats_subject = subject
        
        try:
            # Publish to JetStream
            ack = await self._js.publish(subject, data)
            event.nats_sequence = ack.seq
            
            self._logger.debug(f"Stored event {event.event_type} for job {event.job_id} (seq: {ack.seq})")
            
        except Exception as e:
            self._logger.error(f"Failed to store event: {e}")
            raise
    
    def _generate_subject(self, event: JobEvent) -> str:
        """Generate NATS subject for event."""
        base = f"{self.subject_prefix}.{event.job_id}"
        
        # Add context-specific routing
        if event.worker_id:
            return f"{base}.worker.{event.worker_id}.{event.event_type.value}"
        elif event.queue_name:
            return f"{base}.queue.{event.queue_name}.{event.event_type.value}"
        else:
            return f"{base}.{event.event_type.value}"
    
    async def get_events(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[JobEvent]:
        """Get events for a specific job."""
        if not self._js:
            raise RuntimeError("Not connected to NATS")
        
        subject_filter = f"{self.subject_prefix}.{job_id}.>"
        events = []
        
        try:
            # Create ephemeral consumer
            consumer_config = ConsumerConfig(
                filter_subject=subject_filter,
                deliver_policy=DeliverPolicy.ALL,
                max_deliver=1,
                ack_policy="none"
            )
            
            consumer_info = await self._js.add_consumer(
                self.stream_name, 
                consumer_config
            )
            consumer_name = consumer_info.name
            self._consumers.append(consumer_name)
            
            # Fetch messages
            messages_fetched = 0
            skip_count = 0
            
            async for msg in self._js.subscribe(
                subject_filter, 
                stream=self.stream_name,
                config=consumer_config
            ):
                # Handle offset
                if skip_count < offset:
                    skip_count += 1
                    continue
                
                # Decode event
                try:
                    event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                    event.nats_subject = msg.subject
                    event.nats_sequence = msg.metadata.sequence.stream
                    events.append(event)
                    messages_fetched += 1
                    
                    # Check limit
                    if limit and messages_fetched >= limit:
                        break
                        
                except Exception as e:
                    self._logger.warning(f"Failed to decode event: {e}")
                    continue
            
            # Clean up consumer
            try:
                await self._js.delete_consumer(self.stream_name, consumer_name)
            except:
                pass
            
        except Exception as e:
            self._logger.error(f"Failed to get events for job {job_id}: {e}")
            raise
        
        return events
    
    async def get_events_by_type(
        self,
        event_type: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[JobEvent]:
        """Get events by type."""
        if not self._js:
            raise RuntimeError("Not connected to NATS")
        
        subject_filter = f"{self.subject_prefix}.*.{event_type}"
        events = []
        
        try:
            consumer_config = ConsumerConfig(
                filter_subject=subject_filter,
                deliver_policy=DeliverPolicy.ALL,
                max_deliver=1,
                ack_policy="none"
            )
            
            consumer_info = await self._js.add_consumer(
                self.stream_name,
                consumer_config
            )
            consumer_name = consumer_info.name
            self._consumers.append(consumer_name)
            
            messages_fetched = 0
            skip_count = 0
            
            async for msg in self._js.subscribe(
                subject_filter,
                stream=self.stream_name,
                config=consumer_config
            ):
                if skip_count < offset:
                    skip_count += 1
                    continue
                
                try:
                    event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                    event.nats_subject = msg.subject
                    event.nats_sequence = msg.metadata.sequence.stream
                    events.append(event)
                    messages_fetched += 1
                    
                    if limit and messages_fetched >= limit:
                        break
                        
                except Exception as e:
                    self._logger.warning(f"Failed to decode event: {e}")
                    continue
            
            # Clean up consumer
            try:
                await self._js.delete_consumer(self.stream_name, consumer_name)
            except:
                pass
                
        except Exception as e:
            self._logger.error(f"Failed to get events by type {event_type}: {e}")
            raise
        
        return events
    
    async def list_events(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[JobEvent]:
        """List events with optional filtering."""
        if not self._js:
            raise RuntimeError("Not connected to NATS")
        
        # Build subject filter from filters
        subject_filter = self._build_subject_filter(filters)
        events = []
        
        try:
            consumer_config = ConsumerConfig(
                filter_subject=subject_filter,
                deliver_policy=DeliverPolicy.ALL,
                max_deliver=1,
                ack_policy="none"
            )
            
            consumer_info = await self._js.add_consumer(
                self.stream_name,
                consumer_config
            )
            consumer_name = consumer_info.name
            self._consumers.append(consumer_name)
            
            messages_fetched = 0
            skip_count = 0
            
            async for msg in self._js.subscribe(
                subject_filter,
                stream=self.stream_name,
                config=consumer_config
            ):
                if skip_count < offset:
                    skip_count += 1
                    continue
                
                try:
                    event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                    event.nats_subject = msg.subject
                    event.nats_sequence = msg.metadata.sequence.stream
                    
                    # Apply additional filtering
                    if self._passes_filters(event, filters):
                        events.append(event)
                        messages_fetched += 1
                        
                        if limit and messages_fetched >= limit:
                            break
                            
                except Exception as e:
                    self._logger.warning(f"Failed to decode event: {e}")
                    continue
            
            # Clean up consumer
            try:
                await self._js.delete_consumer(self.stream_name, consumer_name)
            except:
                pass
                
        except Exception as e:
            self._logger.error(f"Failed to list events: {e}")
            raise
        
        return events
    
    def _build_subject_filter(self, filters: Optional[Dict[str, Any]]) -> str:
        """Build NATS subject filter from query filters."""
        if not filters:
            return f"{self.subject_prefix}.>"
        
        # Build subject based on available filters
        parts = [self.subject_prefix]
        
        if 'job_id' in filters:
            parts.append(filters['job_id'])
        else:
            parts.append('*')
        
        if 'worker_id' in filters:
            parts.extend(['worker', filters['worker_id']])
        elif 'queue_name' in filters:
            parts.extend(['queue', filters['queue_name']])
        
        if 'event_type' in filters:
            if isinstance(filters['event_type'], list):
                # For multiple event types, use broader filter and filter in code
                parts.append('>')
            else:
                parts.append(filters['event_type'])
        else:
            parts.append('>')
        
        return '.'.join(parts)
    
    def _passes_filters(self, event: JobEvent, filters: Optional[Dict[str, Any]]) -> bool:
        """Check if event passes additional filters."""
        if not filters:
            return True
        
        # Check event type filter for lists
        if 'event_type' in filters:
            event_type_filter = filters['event_type']
            if isinstance(event_type_filter, list):
                if event.event_type.value not in event_type_filter:
                    return False
        
        # Check worker_id filter
        if 'worker_id' in filters:
            if event.worker_id != filters['worker_id']:
                return False
        
        return True
    
    async def count_events(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count events with optional filtering."""
        # For NATS, we need to iterate through messages to count
        # This is not as efficient as SQL COUNT, but necessary for NATS
        events = await self.list_events(filters=filters)
        return len(events)
    
    async def delete_events(self, job_id: str) -> int:
        """Delete all events for a job (not supported in JetStream)."""
        # JetStream doesn't support selective message deletion
        # This would require stream management or TTL-based expiration
        self._logger.warning("Event deletion not supported in NATS JetStream")
        return 0
    
    async def cleanup_expired_events(self) -> int:
        """Remove expired events (handled by JetStream configuration)."""
        # JetStream handles TTL automatically based on max_age configuration
        return 0
    
    async def stream_events(
        self,
        job_id: Optional[str] = None,
        event_types: Optional[List[str]] = None
    ) -> AsyncIterator[JobEvent]:
        """Stream events in real-time."""
        if not self._js:
            raise RuntimeError("Not connected to NATS")
        
        # Build subject filter
        if job_id:
            subject_filter = f"{self.subject_prefix}.{job_id}.>"
        else:
            subject_filter = f"{self.subject_prefix}.>"
        
        try:
            # Create push consumer for real-time streaming
            consumer_config = ConsumerConfig(
                filter_subject=subject_filter,
                deliver_policy=DeliverPolicy.NEW,  # Only new messages
                ack_policy="none"
            )
            
            subscription = await self._js.subscribe(
                subject_filter,
                stream=self.stream_name,
                config=consumer_config
            )
            self._subscriptions.append(subscription)
            
            async for msg in subscription.messages:
                try:
                    event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                    event.nats_subject = msg.subject
                    event.nats_sequence = msg.metadata.sequence.stream
                    
                    # Apply event type filter
                    if event_types and event.event_type.value not in event_types:
                        continue
                    
                    yield event
                    
                except Exception as e:
                    self._logger.warning(f"Failed to decode streamed event: {e}")
                    continue
                    
        except Exception as e:
            self._logger.error(f"Error streaming events: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        if not self._nc or self._nc.is_closed:
            return {"status": "disconnected"}
        
        try:
            # Check NATS connection
            await self._nc.flush(timeout=5.0)
            
            # Check JetStream and get stream info
            stream_info = await self._js.stream_info(self.stream_name)
            
            return {
                "status": "healthy",
                "nats_url": self.nats_url,
                "stream_name": self.stream_name,
                "subjects": list(stream_info.config.subjects),
                "messages": stream_info.state.messages,
                "bytes": stream_info.state.bytes,
                "first_seq": stream_info.state.first_seq,
                "last_seq": stream_info.state.last_seq,
                "consumers": len(stream_info.state.consumer_count)
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
```

## Job Event Logger

### AsyncJobEventLogger

Updated from AsyncEventLogger with NATS optimizations:

```python
import asyncio
import time
from typing import List, Optional

import anyio
import msgspec

from ..config.loader import LoggingConfig
from ..models.event import JobEvent, JobEventType
from .nats_storage import NATSJobEventStorage


class AsyncJobEventLogger:
    """Async job event logger optimized for NATS."""
    
    def __init__(
        self,
        storage: NATSJobEventStorage,
        enabled: bool = True,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_buffer_size: int = 10000,
        nats_batch_publish: bool = True
    ):
        """
        Initialize async job event logger.
        
        Args:
            storage: NATS job event storage backend
            enabled: Whether event logging is enabled
            batch_size: Number of events to batch before flushing
            flush_interval: Maximum time between flushes in seconds
            max_buffer_size: Maximum events in buffer before forced flush
            nats_batch_publish: Use NATS batch publishing for efficiency
        """
        self.storage = storage
        self.enabled = enabled
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self.nats_batch_publish = nats_batch_publish
        
        self._buffer: List[JobEvent] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._logger = LoggingConfig.get_logger("events.job_logger")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
    
    async def start(self) -> None:
        """Start the job event logger and background flush task."""
        if not self.enabled:
            return
        
        self._stop_event.clear()
        self._flush_task = asyncio.create_task(self._flush_loop())
        self._logger.debug("Job event logger started")
    
    async def stop(self) -> None:
        """Stop the job event logger and flush remaining events."""
        if not self.enabled:
            return
        
        self._stop_event.set()
        
        if self._flush_task:
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining events
        await self._flush_events()
        self._logger.debug("Job event logger stopped")
    
    async def log_event(self, event: JobEvent) -> None:
        """Log a single event."""
        if not self.enabled:
            return
        
        async with self._lock:
            self._buffer.append(event)
            
            # Force flush if buffer is getting too large
            if len(self._buffer) >= self.max_buffer_size:
                await self._flush_events()
            elif len(self._buffer) >= self.batch_size:
                # Schedule immediate flush for batch size
                asyncio.create_task(self._flush_events())
    
    async def log_job_enqueued(
        self,
        job_id: str,
        queue_name: str,
        priority: int = 0,
        **kwargs
    ) -> None:
        """Log job enqueued event."""
        event = JobEvent.enqueued(job_id, queue_name, priority, **kwargs)
        await self.log_event(event)
    
    async def log_job_started(
        self,
        job_id: str,
        worker_id: str,
        worker_type: str,
        **kwargs
    ) -> None:
        """Log job started event."""
        event = JobEvent.started(job_id, worker_id, worker_type, **kwargs)
        await self.log_event(event)
    
    async def log_job_completed(
        self,
        job_id: str,
        worker_id: str,
        duration_ms: float,
        **kwargs
    ) -> None:
        """Log job completed event."""
        event = JobEvent.completed(job_id, worker_id, duration_ms, **kwargs)
        await self.log_event(event)
    
    async def log_job_failed(
        self,
        job_id: str,
        worker_id: str,
        error: Exception,
        duration_ms: float,
        retry_count: int = 0,
        **kwargs
    ) -> None:
        """Log job failed event."""
        event = JobEvent.failed(job_id, worker_id, error, duration_ms, retry_count, **kwargs)
        await self.log_event(event)
    
    async def _flush_loop(self) -> None:
        """Background task to periodically flush events."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.flush_interval
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                # Timeout reached, flush events
                await self._flush_events()
    
    async def _flush_events(self) -> None:
        """Flush buffered events to NATS storage."""
        async with self._lock:
            if not self._buffer:
                return
            
            events_to_flush = self._buffer.copy()
            self._buffer.clear()
        
        try:
            if self.nats_batch_publish and len(events_to_flush) > 1:
                # Use NATS batch publishing
                await self._batch_publish_events(events_to_flush)
            else:
                # Individual publishing
                for event in events_to_flush:
                    await self.storage.store_event(event)
            
            self._logger.debug(f"Flushed {len(events_to_flush)} job events")
            
        except Exception as e:
            self._logger.error(f"Failed to flush job events: {e}")
            # Re-add events to buffer for retry
            async with self._lock:
                self._buffer.extend(events_to_flush)
    
    async def _batch_publish_events(self, events: List[JobEvent]) -> None:
        """Publish events in batch to NATS."""
        # Group events by subject for better batching
        subject_groups = {}
        
        for event in events:
            subject = self.storage._generate_subject(event)
            if subject not in subject_groups:
                subject_groups[subject] = []
            subject_groups[subject].append(event)
        
        # Publish each subject group
        for subject, subject_events in subject_groups.items():
            for event in subject_events:
                # Individual publish is still needed as NATS doesn't have true batch publish
                # But grouping helps with connection efficiency
                await self.storage.store_event(event)
```

## Job Event Processor

### AsyncJobEventProcessor

Updated for NATS streaming capabilities:

```python
import asyncio
from typing import Any, Callable, Dict, List, Optional

import anyio

from ..config.loader import LoggingConfig
from ..models.event import JobEvent, JobEventType
from .nats_storage import NATSJobEventStorage


JobEventHandler = Callable[[JobEvent], None]
AsyncJobEventHandler = Callable[[JobEvent], None]


class AsyncJobEventProcessor:
    """Async job event processor for real-time event handling with NATS."""
    
    def __init__(
        self,
        storage: NATSJobEventStorage,
        enabled: bool = True,
        consumer_name: Optional[str] = None
    ):
        """
        Initialize async job event processor.
        
        Args:
            storage: NATS job event storage for streaming events
            enabled: Whether event processing is enabled
            consumer_name: Optional consumer name for NATS consumer group
        """
        self.storage = storage
        self.enabled = enabled
        self.consumer_name = consumer_name or f"job_processor_{id(self)}"
        
        self._handlers: Dict[JobEventType, List[AsyncJobEventHandler]] = {}
        self._global_handlers: List[AsyncJobEventHandler] = []
        self._processing_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._logger = LoggingConfig.get_logger("events.job_processor")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
    
    async def start(self) -> None:
        """Start the job event processor."""
        if not self.enabled:
            return
        
        self._stop_event.clear()
        self._processing_task = asyncio.create_task(self._process_events())
        self._logger.debug("Job event processor started")
    
    async def stop(self) -> None:
        """Stop the job event processor."""
        if not self.enabled:
            return
        
        self._stop_event.set()
        
        if self._processing_task:
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        self._logger.debug("Job event processor stopped")
    
    def add_handler(
        self,
        event_type: JobEventType,
        handler: AsyncJobEventHandler
    ) -> None:
        """Add an event handler for a specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        self._logger.debug(f"Added handler for {event_type}")
    
    def add_global_handler(self, handler: AsyncJobEventHandler) -> None:
        """Add a global event handler that receives all events."""
        self._global_handlers.append(handler)
        self._logger.debug("Added global job event handler")
    
    async def process_event(self, event: JobEvent) -> None:
        """Process a single event through all relevant handlers."""
        if not self.enabled:
            return
        
        handlers_to_call = []
        
        # Add specific handlers for this event type
        if event.event_type in self._handlers:
            handlers_to_call.extend(self._handlers[event.event_type])
        
        # Add global handlers
        handlers_to_call.extend(self._global_handlers)
        
        # Call all handlers
        for handler in handlers_to_call:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                self._logger.error(f"Error in job event handler: {e}")
    
    async def monitor_job(
        self,
        job_id: str,
        handler: AsyncJobEventHandler,
        event_types: Optional[List[JobEventType]] = None
    ) -> None:
        """Monitor events for a specific job."""
        async def job_handler(event: JobEvent) -> None:
            if event.job_id == job_id:
                if event_types is None or event.event_type in event_types:
                    await handler(event)
        
        self.add_global_handler(job_handler)
    
    async def wait_for_job_event(
        self,
        job_id: str,
        event_type: JobEventType,
        timeout: Optional[float] = None
    ) -> Optional[JobEvent]:
        """Wait for a specific job event to occur."""
        event_received = asyncio.Event()
        received_event = None
        
        async def wait_handler(event: JobEvent) -> None:
            nonlocal received_event
            if event.job_id == job_id and event.event_type == event_type:
                received_event = event
                event_received.set()
        
        self.add_global_handler(wait_handler)
        
        try:
            await asyncio.wait_for(event_received.wait(), timeout=timeout)
            return received_event
        except asyncio.TimeoutError:
            return None
        finally:
            self.remove_global_handler(wait_handler)
    
    async def _process_events(self) -> None:
        """Background task to process events from NATS stream."""
        while not self._stop_event.is_set():
            try:
                # Stream events from NATS
                async for event in self.storage.stream_events():
                    if self._stop_event.is_set():
                        break
                    
                    await self.process_event(event)
                
            except Exception as e:
                self._logger.error(f"Error processing job events: {e}")
                await asyncio.sleep(1.0)  # Brief pause on error
```

## Configuration & Setup

### NAQ Configuration

Updated environment variables and settings:

```python
# settings.py
LOG_LEVEL = "INFO"
DISABLE_LOGGING = False

# Job queue configuration
JOB_QUEUE_TYPE = "nats"
JOB_QUEUE_URL = "nats://localhost:4222"
RESULT_STORAGE_TYPE = "nats"
RESULT_STORAGE_URL = "nats://localhost:4222"
EVENT_STORAGE_TYPE = "nats"
EVENT_STORAGE_URL = "nats://localhost:4222"

# NATS-specific settings
NATS_STREAM_NAME = "NAQ_JOB_EVENTS"
NATS_SUBJECT_PREFIX = "naq.jobs.events"
NATS_CONNECTION_TIMEOUT = 10.0
NATS_MAX_RECONNECT_ATTEMPTS = 10

# Worker configuration
DEFAULT_WORKER = "async"
MAX_WORKERS = 10
THREAD_WORKERS = 4
PROCESS_WORKERS = 2

# Job configuration
JOB_TIMEOUT = 300  # 5 minutes
JOB_TTL = 3600    # 1 hour
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5.0
RESULT_TTL = 1800 # 30 minutes
```

```python
# env.py
import os
from .settings import *

# Override with NAQ_ prefixed environment variables
log_level = os.environ.get("NAQ_LOG_LEVEL", LOG_LEVEL)
disable_logging = os.environ.get("NAQ_DISABLE_LOGGING", str(DISABLE_LOGGING)).lower() in ("1", "true")

job_queue_type = os.environ.get("NAQ_JOB_QUEUE_TYPE", JOB_QUEUE_TYPE)
job_queue_url = os.environ.get("NAQ_JOB_QUEUE_URL", JOB_QUEUE_URL)
event_storage_url = os.environ.get("NAQ_EVENT_STORAGE_URL", EVENT_STORAGE_URL)

nats_stream_name = os.environ.get("NAQ_NATS_STREAM_NAME", NATS_STREAM_NAME)
nats_subject_prefix = os.environ.get("NAQ_NATS_SUBJECT_PREFIX", NATS_SUBJECT_PREFIX)
nats_connection_timeout = float(os.environ.get("NAQ_NATS_CONNECTION_TIMEOUT", NATS_CONNECTION_TIMEOUT))

default_worker = os.environ.get("NAQ_DEFAULT_WORKER", DEFAULT_WORKER)
job_ttl = int(os.environ.get("NAQ_JOB_TTL", JOB_TTL))
```

### NATS Server Configuration

```yaml
# nats-server.conf
port: 4222
http_port: 8222

jetstream {
  store_dir: "/var/lib/nats/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}

# Clustering (optional)
cluster {
  name: "naq-cluster"
  listen: 0.0.0.0:6222
  routes: [
    "nats://node1:6222"
    "nats://node2:6222"
    "nats://node3:6222"
  ]
}

# Logging
log_file: "/var/log/nats/nats-server.log"
logtime: true
debug: false
trace: false

# Authorization (optional)
authorization {
  users: [
    {user: "naq", password: "naq_password", permissions: {
      publish: "naq.>"
      subscribe: "naq.>"
    }}
  ]
}
```

## Implementation Examples

### Complete Usage Example

```python
import asyncio
from naq.events import AsyncJobEventLogger, AsyncJobEventProcessor
from naq.storage import NATSJobEventStorage
from naq.models import JobEvent, JobEventType

async def main():
    # Create NATS storage
    storage = NATSJobEventStorage(
        nats_url="nats://localhost:4222",
        stream_name="NAQ_JOB_EVENTS",
        subject_prefix="naq.jobs.events"
    )
    
    # Create logger
    logger = AsyncJobEventLogger(storage)
    
    # Create processor
    processor = AsyncJobEventProcessor(storage)
    
    # Add event handlers
    async def handle_job_failure(event: JobEvent):
        print(f"Job {event.job_id} failed: {event.error_message}")
        # Send alert, update metrics, etc.
    
    async def handle_job_completion(event: JobEvent):
        print(f"Job {event.job_id} completed in {event.duration_ms}ms")
    
    processor.add_handler(JobEventType.FAILED, handle_job_failure)
    processor.add_handler(JobEventType.COMPLETED, handle_job_completion)
    
    # Start components
    async with storage, logger, processor:
        # Log some events
        await logger.log_job_enqueued("job-123", "high-priority", priority=1)
        await logger.log_job_started("job-123", "worker-456", "async")
        
        # Simulate job completion
        await asyncio.sleep(1.0)
        await logger.log_job_completed("job-123", "worker-456", 1250.5)
        
        # Wait for processing
        await asyncio.sleep(2.0)

if __name__ == "__main__":
    asyncio.run(main())
```

### Real-time Job Monitoring

```python
async def monitor_job_execution(job_id: str):
    """Monitor a specific job's execution in real-time."""
    storage = NATSJobEventStorage()
    processor = AsyncJobEventProcessor(storage)
    
    events_received = []
    job_completed = asyncio.Event()
    
    async def job_monitor(event: JobEvent):
        events_received.append(event)
        print(f"Job {job_id}: {event.event_type} at {event.timestamp}")
        
        if event.event_type in (JobEventType.COMPLETED, JobEventType.FAILED):
            job_completed.set()
    
    # Monitor only events for this job
    await processor.monitor_job(job_id, job_monitor)
    
    async with storage, processor:
        # Wait for job completion
        try:
            await asyncio.wait_for(job_completed.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            print(f"Job {job_id} monitoring timed out")
        
        return events_received
```

### Event Analytics Dashboard

```python
async def job_analytics_dashboard():
    """Real-time job analytics using NATS events."""
    storage = NATSJobEventStorage()
    processor = AsyncJobEventProcessor(storage)
    
    # Metrics tracking
    metrics = {
        "jobs_started": 0,
        "jobs_completed": 0,
        "jobs_failed": 0,
        "total_duration_ms": 0.0,
        "worker_usage": {},
        "queue_usage": {}
    }
    
    async def update_metrics(event: JobEvent):
        if event.event_type == JobEventType.STARTED:
            metrics["jobs_started"] += 1
            
            # Track worker usage
            if event.worker_id:
                if event.worker_id not in metrics["worker_usage"]:
                    metrics["worker_usage"][event.worker_id] = 0
                metrics["worker_usage"][event.worker_id] += 1
        
        elif event.event_type == JobEventType.COMPLETED:
            metrics["jobs_completed"] += 1
            if event.duration_ms:
                metrics["total_duration_ms"] += event.duration_ms
        
        elif event.event_type == JobEventType.FAILED:
            metrics["jobs_failed"] += 1
        
        elif event.event_type == JobEventType.ENQUEUED:
            # Track queue usage
            if event.queue_name:
                if event.queue_name not in metrics["queue_usage"]:
                    metrics["queue_usage"][event.queue_name] = 0
                metrics["queue_usage"][event.queue_name] += 1
        
        # Print updated metrics
        print(f"\n--- Job Metrics ---")
        print(f"Started: {metrics['jobs_started']}")
        print(f"Completed: {metrics['jobs_completed']}")
        print(f"Failed: {metrics['jobs_failed']}")
        if metrics['jobs_completed'] > 0:
            avg_duration = metrics['total_duration_ms'] / metrics['jobs_completed']
            print(f"Average Duration: {avg_duration:.2f}ms")
        print(f"Worker Usage: {metrics['worker_usage']}")
        print(f"Queue Usage: {metrics['queue_usage']}")
    
    processor.add_global_handler(update_metrics)
    
    async with storage, processor:
        # Run dashboard
        try:
            await asyncio.sleep(3600)  # Run for 1 hour
        except KeyboardInterrupt:
            print("Dashboard stopped")
        
        return metrics
```

This comprehensive NATS-based job event logging system provides:

1. **High Performance**: NATS native streaming with efficient message routing
2. **Real-time Capabilities**: Instant event delivery via pub/sub
3. **Scalability**: Horizontal scaling with NATS clustering
4. **Durability**: JetStream persistence with configurable retention
5. **Flexible Filtering**: Subject-based routing for efficient event filtering
6. **Fault Tolerance**: NATS built-in reliability and reconnection
7. **Cloud Native**: Perfect for containerized and microservice architectures

The system maintains the async-first, sync-wrapped pattern while leveraging NATS's unique capabilities for distributed job event logging and monitoring.