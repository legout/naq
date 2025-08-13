# src/naq/events/storage.py
import asyncio
import time
from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, List, Optional

import msgspec
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, RetentionPolicy, StorageType

from ..connection import ConnectionManager, get_jetstream_context, nats_connection, Config
from ..exceptions import NaqConnectionError
from ..models import JobEvent, JobEventType


class BaseEventStorage(ABC):
    """Abstract base class for event storage backends."""

    @abstractmethod
    async def store_event(self, event: JobEvent) -> None:
        """Store a job event."""
        pass

    @abstractmethod
    async def get_events(self, job_id: str) -> List[JobEvent]:
        """Retrieve all events for a specific job ID."""
        pass

    @abstractmethod
    async def stream_events(
        self, 
        job_id: str, 
        context: Optional[str] = None,
        event_type: Optional[JobEventType] = None
    ) -> AsyncIterator[JobEvent]:
        """Stream events for a specific job ID in real-time."""
        pass


class NATSJobEventStorage(BaseEventStorage):
    """
    NATS JetStream-based event storage implementation.
    
    This class provides durable, time-ordered storage for job events
    using NATS JetStream streams and consumers.
    """

    def __init__(
        self,
        nats_url: str = "nats://localhost:4222",
        stream_name: str = "NAQ_JOB_EVENTS",
        subject_prefix: str = "naq.jobs.events",
        max_age: int = 60 * 60 * 24,  # 24 hours
        max_msgs: int = 1000000,
        storage_type: StorageType = StorageType.FILE,
        retention_policy: RetentionPolicy = RetentionPolicy.LIMITS,
    ):
        """
        Initialize NATS event storage.
        
        Args:
            nats_url: NATS server URL
            stream_name: Name of the JetStream stream
            subject_prefix: Prefix for NATS subjects
            max_age: Maximum age of messages in seconds
            max_msgs: Maximum number of messages to retain
            storage_type: Storage type (FILE or MEMORY)
            retention_policy: Retention policy
        """
        self.nats_url = nats_url
        self.stream_name = stream_name
        self.subject_prefix = subject_prefix
        self.max_age = max_age
        self.max_msgs = max_msgs
        self.storage_type = storage_type
        self.retention_policy = retention_policy
        
        self._connection_manager = ConnectionManager()
        self._nc: Optional[NATSClient] = None
        self._js: Optional[JetStreamContext] = None
        self._connected = False

    async def _connect(self) -> None:
        """Establish connection to NATS and JetStream."""
        if self._connected:
            return
            
        try:
            # Create a config with the specific NATS URL
            config = Config()
            config.nats.servers = [self.nats_url]
            
            # Create a connection using the new connection approach
            # We need to create a connection that persists beyond the context manager
            import nats
            self._nc = await nats.connect(
                servers=config.nats.servers,
                name=config.nats.client_name,
                max_reconnect_attempts=config.nats.max_reconnect_attempts,
                reconnect_time_wait=config.nats.reconnect_time_wait,
            )
            self._js = self._nc.jetstream()
            await self._setup_stream()
            self._connected = True
        except Exception as e:
            raise NaqConnectionError(f"Failed to connect to NATS: {e}") from e

    async def _disconnect(self) -> None:
        """Close NATS connection."""
        if not self._connected:
            return
            
        try:
            if self._nc:
                await self._nc.close()
            self._connected = False
            self._nc = None
            self._js = None
        except Exception as e:
            raise NaqConnectionError(f"Failed to disconnect from NATS: {e}") from e

    async def _setup_stream(self) -> None:
        """Create JetStream stream if it doesn't exist."""
        if not self._js:
            raise NaqConnectionError("JetStream context not available")
            
        try:
            # Check if stream exists
            await self._js.stream_info(self.stream_name)
        except nats.errors.NotFoundError:
            # Create stream if it doesn't exist
            subjects = [f"{self.subject_prefix}.*.*.*"]
            await self._js.add_stream(
                name=self.stream_name,
                subjects=subjects,
                storage=self.storage_type,
                retention=self.retention_policy,
                max_age=self.max_age,
                max_msgs=self.max_msgs,
            )
        except Exception as e:
            raise NaqConnectionError(f"Failed to setup stream '{self.stream_name}': {e}") from e

    def _generate_subject(self, event: JobEvent, context: Optional[str] = None) -> str:
        """
        Generate NATS subject for an event.
        
        Args:
            event: Job event
            context: Optional context (e.g., 'worker', 'queue')
            
        Returns:
            str: NATS subject
        """
        parts = [self.subject_prefix, event.job_id]
        
        if context:
            parts.append(context)
        else:
            # Try to infer context from event properties
            if event.worker_id:
                parts.append("worker")
            elif event.queue_name:
                parts.append("queue")
            else:
                parts.append("general")
        
        parts.append(event.event_type.value)
        return ".".join(parts)

    async def store_event(self, event: JobEvent) -> None:
        """
        Store a job event in JetStream.
        
        Args:
            event: Job event to store
        """
        await self._connect()
        
        if not self._js:
            raise NaqConnectionError("JetStream context not available")
            
        # Generate subject for the event
        subject = self._generate_subject(event)
        event.nats_subject = subject
        
        # Serialize event using msgspec
        data = msgspec.msgpack.encode(event)
        
        try:
            # Publish to JetStream
            ack = await self._js.publish(subject, data)
            if hasattr(ack, 'seq'):
                event.nats_sequence = ack.seq
        except Exception as e:
            raise NaqConnectionError(f"Failed to store event: {e}") from e

    async def get_events(self, job_id: str) -> List[JobEvent]:
        """
        Retrieve all events for a specific job ID.
        
        Args:
            job_id: Job ID to retrieve events for
            
        Returns:
            List[JobEvent]: List of events for the job
        """
        await self._connect()
        
        if not self._js:
            raise NaqConnectionError("JetStream context not available")
            
        # Create subject filter for the job
        subject_filter = f"{self.subject_prefix}.{job_id}.>"
        
        try:
            # Create ephemeral consumer for pulling messages
            consumer_name = f"ephemeral-{job_id}-{int(time.time())}"
            consumer_config = ConsumerConfig(
                durable_name=consumer_name,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.NONE,  # Don't ack for ephemeral consumer
                max_deliver=1,
            )
            
            # Create consumer
            consumer = await self._js.add_consumer(
                self.stream_name,
                config=consumer_config,
                durable_name=consumer_name,
            )
            
            # Pull all messages
            events = []
            while True:
                try:
                    batch = await consumer.fetch_messages(batch=100, timeout=1)
                    if not batch:
                        break
                        
                    for msg in batch:
                        try:
                            event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                            events.append(event)
                        except Exception:
                            # Skip malformed messages
                            continue
                except asyncio.TimeoutError:
                    break
                    
            return sorted(events, key=lambda e: e.timestamp)
            
        except Exception as e:
            raise NaqConnectionError(f"Failed to get events for job {job_id}: {e}") from e

    async def stream_events(
        self, 
        job_id: str, 
        context: Optional[str] = None,
        event_type: Optional[JobEventType] = None
    ) -> AsyncIterator[JobEvent]:
        """
        Stream events for a specific job ID in real-time.
        
        Args:
            job_id: Job ID to stream events for
            context: Optional context filter
            event_type: Optional event type filter
            
        Yields:
            JobEvent: Events as they arrive
        """
        await self._connect()
        
        if not self._js:
            raise NaqConnectionError("JetStream context not available")
            
        # Build subject filter
        parts = [self.subject_prefix, job_id]
        if context:
            parts.append(context)
        parts.append(">")  # Wildcard for event type
        subject_filter = ".".join(parts)
        
        try:
            # Create push consumer
            consumer_name = f"stream-{job_id}-{int(time.time())}"
            consumer_config = ConsumerConfig(
                durable_name=consumer_name,
                deliver_policy=DeliverPolicy.NEW,  # Only new messages
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_deliver=3,
                replay_policy=nats.js.api.ReplayPolicy.INSTANT,
            )
            
            # Create consumer
            consumer = await self._js.add_consumer(
                self.stream_name,
                config=consumer_config,
                durable_name=consumer_name,
            )
            
            # Subscribe to messages
            async def message_handler(msg):
                try:
                    event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                    
                    # Apply filters
                    if event_type and event.event_type != event_type:
                        await msg.ack()
                        return
                        
                    if context and event.worker_id and not event.worker_id.startswith(context):
                        await msg.ack()
                        return
                        
                    yield event
                    await msg.ack()
                except Exception:
                    # Acknowledge even on error to avoid blocking
                    await msg.ack()
                    
            # Create iterator
            async for msg in consumer.consume_messages(subject=subject_filter):
                async for event in message_handler(msg):
                    yield event
                    
        except Exception as e:
            raise NaqConnectionError(f"Failed to stream events for job {job_id}: {e}") from e

    async def __aenter__(self):
        """Async context manager entry."""
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._disconnect()

    async def close(self) -> None:
        """Close the connection."""
        await self._disconnect()