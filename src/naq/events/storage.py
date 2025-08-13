# src/naq/events/storage.py
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional

import msgspec
from loguru import logger
from nats import NATS
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig, StorageType

from ..services import ServiceManager
from ..services.connection import ConnectionService
from ..services.streams import StreamService
from ..models import JobEvent, JobEventType
from ..settings import DEFAULT_NATS_URL


class BaseEventStorage(ABC):
    """
    Abstract base class for job event storage backends.
    
    This interface allows for different storage implementations while
    maintaining a consistent API for the event logging system.
    """

    @abstractmethod
    async def store_event(self, event: JobEvent) -> None:
        """
        Store a job event.
        
        Args:
            event: The JobEvent to store.
        """
        pass

    @abstractmethod
    async def get_events(self, job_id: str) -> List[JobEvent]:
        """
        Retrieve all events for a specific job.
        
        Args:
            job_id: The job ID to get events for.
            
        Returns:
            List of JobEvent objects for the specified job.
        """
        pass

    @abstractmethod
    async def stream_events(
        self,
        job_id: Optional[str] = None,
        event_type: Optional[JobEventType] = None,
        queue_name: Optional[str] = None,
        worker_id: Optional[str] = None
    ) -> AsyncIterator[JobEvent]:
        """
        Stream events in real-time.
        
        Args:
            job_id: Optional job ID filter.
            event_type: Optional event type filter.
            queue_name: Optional queue name filter.
            worker_id: Optional worker ID filter.
            
        Yields:
            JobEvent objects as they are received.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """
        Close the storage connection and cleanup resources.
        """
        pass


class NATSJobEventStorage(BaseEventStorage):
    """
    NATS JetStream-based storage backend for job events.
    
    This implementation uses NATS JetStream to provide durable, ordered
    streaming of job events with subject-based filtering capabilities.
    """

    def __init__(
        self,
        config: Optional[Dict] = None,
        nats_url: str = DEFAULT_NATS_URL,  # Legacy parameter
        stream_name: str = "NAQ_JOB_EVENTS",
        subject_prefix: str = "naq.jobs.events"
    ):
        """
        Initialize the NATS event storage.
        
        Args:
            config: Configuration dictionary, preferred over legacy parameters
            nats_url: NATS server URL (legacy parameter).
            stream_name: JetStream stream name for events.
            subject_prefix: Base subject prefix for event routing.
        """
        # Handle configuration - prefer passed config over legacy parameters
        if config is not None:
            self._config = config
            self.nats_url = config.get('nats_url', nats_url)
        else:
            # Fallback to legacy parameters for backward compatibility
            self._config = {'nats_url': nats_url}
            self.nats_url = nats_url
            
        self.stream_name = stream_name
        self.subject_prefix = subject_prefix
        
        # Service management
        self._service_manager: Optional[ServiceManager] = None
        self._connected = False

    async def _connect(self) -> None:
        """Establish NATS connection and setup JetStream."""
        if self._connected:
            return

        try:
            # Initialize service manager
            self._service_manager = ServiceManager(self._config)
            await self._service_manager.initialize_all()
            
            # Setup the stream using StreamService
            stream_service = await self._service_manager.get_service(StreamService)
            await stream_service.ensure_stream(
                name=self.stream_name,
                subjects=[f"{self.subject_prefix}.>"],
                storage_type="FILE",
                max_age=7 * 24 * 60 * 60,  # 7 days retention
                max_msgs=1_000_000,  # Max 1M messages
                max_bytes=1024 * 1024 * 1024,  # 1GB max size
                duplicate_window=60,  # 1 minute dedup window
            )
            
            self._connected = True
            logger.debug(f"Connected to NATS event storage: {self.nats_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to NATS event storage: {e}")
            raise


    def _generate_subject(self, event: JobEvent) -> str:
        """
        Generate a NATS subject for the event.
        
        Subject hierarchy: naq.jobs.events.{job_id}.{context}.{event_type}
        Examples:
        - naq.jobs.events.job-123.worker.worker-abc.started
        - naq.jobs.events.job-456.queue.high-priority.enqueued
        
        Args:
            event: The JobEvent to generate a subject for.
            
        Returns:
            The NATS subject string.
        """
        parts = [self.subject_prefix, event.job_id]
        
        # Add context information
        if event.worker_id:
            parts.extend(["worker", event.worker_id])
        elif event.queue_name:
            parts.extend(["queue", event.queue_name])
        else:
            parts.append("system")
        
        # Add event type
        parts.append(event.event_type.value)
        
        return ".".join(parts)

    async def store_event(self, event: JobEvent) -> None:
        """
        Store a job event in the NATS stream.
        
        Args:
            event: The JobEvent to store.
        """
        await self._connect()
        
        # Generate subject and serialize event
        subject = self._generate_subject(event)
        event_data = msgspec.msgpack.encode(event)
        
        try:
            connection_service = await self._service_manager.get_service(ConnectionService)
            async with connection_service.jetstream_scope() as js:
                # Update event with NATS metadata
                event.nats_subject = subject
                
                # Publish to JetStream
                ack = await js.publish(subject, event_data)
                
                # Update event with sequence number
                event.nats_sequence = ack.seq
                
                logger.debug(f"Stored event {event.event_type} for job {event.job_id} (seq: {ack.seq})")
            
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            raise

    async def get_events(self, job_id: str) -> List[JobEvent]:
        """
        Retrieve all events for a specific job.
        
        Args:
            job_id: The job ID to get events for.
            
        Returns:
            List of JobEvent objects for the specified job, ordered by timestamp.
        """
        await self._connect()
        
        events = []
        subject_filter = f"{self.subject_prefix}.{job_id}.>"
        
        try:
            connection_service = await self._service_manager.get_service(ConnectionService)
            async with connection_service.jetstream_scope() as js:
                # Create an ephemeral pull consumer
                consumer_config = ConsumerConfig(
                    deliver_policy=DeliverPolicy.ALL,
                    filter_subject=subject_filter,
                )
                
                consumer = await js.create_consumer(
                    self.stream_name,
                    config=consumer_config
                )
                
                # Fetch all available messages
                batch_size = 100
                while True:
                    msgs = await consumer.fetch(batch_size, timeout=1.0)
                    if not msgs:
                        break
                        
                    for msg in msgs:
                        try:
                            event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                            events.append(event)
                            await msg.ack()
                            
                        except Exception as e:
                            logger.error(f"Failed to decode event message: {e}")
                            await msg.ack()  # Ack to avoid redelivery
                            continue
                    
                    if len(msgs) < batch_size:
                        break
                        
                # Clean up the ephemeral consumer
                await consumer.delete()
                
            # Sort events by timestamp
            events.sort(key=lambda e: e.timestamp)
            
            logger.debug(f"Retrieved {len(events)} events for job {job_id}")
            return events
            
        except Exception as e:
            logger.error(f"Failed to retrieve events for job {job_id}: {e}")
            raise

    async def stream_events(
        self,
        job_id: Optional[str] = None,
        event_type: Optional[JobEventType] = None,
        queue_name: Optional[str] = None,
        worker_id: Optional[str] = None
    ) -> AsyncIterator[JobEvent]:
        """
        Stream events in real-time with optional filtering.
        
        Args:
            job_id: Optional job ID filter.
            event_type: Optional event type filter.
            queue_name: Optional queue name filter.
            worker_id: Optional worker ID filter.
            
        Yields:
            JobEvent objects as they are received.
        """
        await self._connect()
        
        # Build subject filter
        subject_parts = [self.subject_prefix]
        
        if job_id:
            subject_parts.append(job_id)
            
            if worker_id:
                subject_parts.extend(["worker", worker_id])
            elif queue_name:
                subject_parts.extend(["queue", queue_name])
            else:
                subject_parts.append("*")
                
            if event_type:
                subject_parts.append(event_type.value)
            else:
                subject_parts.append("*")
        else:
            subject_parts.append(">")  # Match all
            
        subject_filter = ".".join(subject_parts)
        
        try:
            connection_service = await self._service_manager.get_service(ConnectionService)
            async with connection_service.jetstream_scope() as js:
                # Create a push consumer for real-time streaming
                consumer_config = ConsumerConfig(
                    deliver_policy=DeliverPolicy.NEW,
                    filter_subject=subject_filter,
                )
                
                consumer = await js.create_consumer(
                    self.stream_name,
                    config=consumer_config
                )
                
                logger.debug(f"Streaming events with filter: {subject_filter}")
                
                async for msg in consumer.messages():
                    try:
                        event = msgspec.msgpack.decode(msg.data, type=JobEvent)
                        
                        # Additional filtering if needed
                        if event_type and event.event_type != event_type:
                            await msg.ack()
                            continue
                        if queue_name and event.queue_name != queue_name:
                            await msg.ack()
                            continue
                        if worker_id and event.worker_id != worker_id:
                            await msg.ack()
                            continue
                        
                        await msg.ack()
                        yield event
                        
                    except Exception as e:
                        logger.error(f"Failed to decode streamed event: {e}")
                        await msg.ack()
                        continue
                        
        except Exception as e:
            logger.error(f"Failed to stream events: {e}")
            raise

    async def close(self) -> None:
        """Close the storage connection and cleanup resources."""
        if self._connected:
            logger.debug("Closing NATS event storage connection")
            self._connected = False
            if self._service_manager:
                await self._service_manager.cleanup_all()
                self._service_manager = None