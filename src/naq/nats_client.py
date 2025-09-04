"""
Unified NATS Client for NAQ

This module provides a centralized NATS client that handles connections,
JetStream contexts, and core operations. It replaces the service layer
with a simpler, more direct approach.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional, Union

import nats
from nats.aio.client import Client as NATSClient, Subscription
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from nats.js.api import ConsumerConfig, StreamConfig

from .config import get_config
from .exceptions import NaqConnectionError, NaqException
from .config.types import NatsConfig
from .utils.logging import StructuredLogger
from .utils.decorators import retry
from .utils.validation import validate_parameter


class NatsClientConfig:
    """
    Configuration for the NatsClient.
    
    This class provides a structured way to configure NATS client parameters
    with support for environment variable overrides and default values.
    """
    
    def __init__(
        self,
        nats_url: Optional[str] = None,
        max_reconnect_attempts: int = 5,
        reconnect_time_wait: float = 2.0,
        connection_timeout: float = 30.0,
        ping_interval: float = 30.0,
        max_outstanding_pings: int = 3,
        client_name: str = "naq_client",
        **kwargs: Any,
    
    ):
        """Initialize NATS client configuration."""
        # Get global config for defaults
        config = get_config()
        
        # Set NATS URL
        self.nats_url = nats_url or (
            config.nats.servers[0] if config.nats.servers else "nats://localhost:4222"
        )
        
        # Set connection parameters
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_time_wait = reconnect_time_wait
        self.connection_timeout = connection_timeout
        self.ping_interval = ping_interval
        self.max_outstanding_pings = max_outstanding_pings
        self.client_name = client_name
        
        # Additional connection options
        self.connection_options = kwargs


class NatsClient:
    """
    A unified client for interacting with NATS and JetStream.

    This class provides a high-level interface for all NATS operations, replacing
    the previous service layer approach. It supports both synchronous and
    asynchronous operations for publishing, subscribing, and managing JetStream
    streams and consumers.

    Key features:
    - Unified sync/async API
    - JetStream stream and consumer management
    - Connection management with automatic reconnection
    - Integration with the new configuration system

    Examples:
        >>> # Create a client with default settings
        >>> client = NatsClient()
        >>> 
        >>> # Connect to NATS
        >>> await client.connect()
        >>> 
        >>> # Create a client with custom configuration
        >>> config = NatsClientConfig(servers=["nats://localhost:4222"])
        >>> client = NatsClient(config=config)
        >>> 
        >>> # Use in a context manager
        >>> async with NatsClient() as client:
        ...     await client.publish("subject", b"message")
    """
    
    def __init__(self, config: Optional[NatsClientConfig] = None) -> None:
        """
        Initialize the NATS client.
        
        Args:
            config: Optional configuration for the client.
        """
        self._config = config or NatsClientConfig()
        self._nc: Optional[NATSClient] = None
        self._js: Optional[JetStreamContext] = None
        self._logger = StructuredLogger("naq.nats_client")
        self._connection_lock = asyncio.Lock()
        self._is_connected = False
        
    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to NATS."""
        return self._is_connected and self._nc is not None and self._nc.is_connected
    
    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
    async def connect(self) -> None:
        """
        Connect to NATS server.
        
        Raises:
            NaqConnectionError: If connection fails.
        """
        async with self._connection_lock:
            if self.is_connected:
                return
                
            try:
                self._logger.info(
                    "Connecting to NATS",
                    url=self._config.nats_url,
                    client_name=self._config.client_name,
                )
                
                # Connect to NATS
                self._nc = await nats.connect(
                    servers=[self._config.nats_url],
                    name=self._config.client_name,
                    max_reconnect_attempts=self._config.max_reconnect_attempts,
                    reconnect_time_wait=self._config.reconnect_time_wait,
                    connect_timeout=self._config.connection_timeout,
                    ping_interval=self._config.ping_interval,
                    max_outstanding_pings=self._config.max_outstanding_pings,
                    **self._config.connection_options,
                )
                
                # Get JetStream context
                self._js = self._nc.jetstream()
                
                self._is_connected = True
                self._logger.info("Connected to NATS successfully")
                
            except Exception as e:
                self._is_connected = False
                self._nc = None
                self._js = None
                error_msg = f"Failed to connect to NATS: {e}"
                self._logger.error(error_msg)
                raise NaqConnectionError(error_msg) from e
    
    async def disconnect(self) -> None:
        """Disconnect from NATS server."""
        async with self._connection_lock:
            if not self.is_connected:
                return
                
            try:
                self._logger.info("Disconnecting from NATS")
                
                if self._nc:
                    await self._nc.close()
                
                self._nc = None
                self._js = None
                self._is_connected = False
                
                self._logger.info("Disconnected from NATS successfully")
                
            except Exception as e:
                error_msg = f"Error disconnecting from NATS: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    @asynccontextmanager
    async def connection(self) -> AsyncIterator[NATSClient]:
        """
        Context manager for NATS connection.
        
        Yields:
            NATSClient: The NATS client connection.
        """
        if not self.is_connected:
            await self.connect()
            
        try:
            yield self._nc
        except Exception as e:
            self._logger.error("Error in NATS connection context", error=str(e))
            raise
    
    @asynccontextmanager
    async def jetstream(self) -> AsyncIterator[JetStreamContext]:
        """
        Context manager for JetStream context.
        
        Yields:
            JetStreamContext: The JetStream context.
        """
        if not self.is_connected:
            await self.connect()
            
        try:
            yield self._js
        except Exception as e:
            self._logger.error("Error in JetStream context", error=str(e))
            raise
    
    async def ensure_stream(
        self,
        stream_name: str,
        subjects: list[str],
        **kwargs: Any,
    ) -> None:
        """
        Ensure a JetStream stream exists.
        
        Args:
            stream_name: Name of the stream.
            subjects: List of subjects for the stream.
            **kwargs: Additional stream configuration options.
        """
        validate_parameter(stream_name, "stream_name", str)
        validate_parameter(subjects, "subjects", list)
        
        async with self.jetstream() as js:
            try:
                # Check if stream exists
                await js.stream_info(stream_name)
                self._logger.debug("Stream already exists", stream_name=stream_name)
            except Exception:
                # Stream doesn't exist, create it
                stream_config = StreamConfig(
                    name=stream_name,
                    subjects=subjects,
                    **kwargs,
                )
                
                self._logger.info(
                    "Creating JetStream stream",
                    stream_name=stream_name,
                    subjects=subjects,
                )
                
                await js.add_stream(stream_config)
                self._logger.info("Stream created successfully", stream_name=stream_name)
    
    async def publish(
        self,
        subject: str,
        payload: bytes,
        **kwargs: Any,
    ) -> str:
        """
        Publish a message to a subject.
        
        Args:
            subject: NATS subject to publish to.
            payload: Message payload as bytes.
            **kwargs: Additional publish options.
            
        Returns:
            str: The message ID.
        """
        validate_parameter(subject, "subject", str)
        validate_parameter(payload, "payload", bytes)
        
        async with self.connection() as nc:
            try:
                self._logger.debug(
                    "Publishing message",
                    subject=subject,
                    payload_size=len(payload),
                )
                
                msg_id = await nc.publish(subject, payload, **kwargs)
                
                self._logger.debug(
                    "Message published successfully",
                    subject=subject,
                    message_id=msg_id,
                )
                
                return msg_id
                
            except Exception as e:
                error_msg = f"Failed to publish message to {subject}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def jetstream_publish(
        self,
        subject: str,
        payload: bytes,
        **kwargs: Any,
    ) -> str:
        """
        Publish a message to JetStream.
        
        Args:
            subject: NATS subject to publish to.
            payload: Message payload as bytes.
            **kwargs: Additional publish options.
            
        Returns:
            str: The message ID.
        """
        validate_parameter(subject, "subject", str)
        validate_parameter(payload, "payload", bytes)
        
        async with self.jetstream() as js:
            try:
                self._logger.debug(
                    "Publishing JetStream message",
                    subject=subject,
                    payload_size=len(payload),
                )
                
                ack = await js.publish(subject, payload, **kwargs)
                
                self._logger.debug(
                    "JetStream message published successfully",
                    subject=subject,
                    message_id=ack.seq,
                )
                
                return str(ack.seq)
                
            except Exception as e:
                error_msg = f"Failed to publish JetStream message to {subject}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def subscribe(
        self,
        subject: str,
        queue_group: Optional[str] = None,
        **kwargs: Any,
    ) -> Subscription:
        """
        Subscribe to a subject.
        
        Args:
            subject: NATS subject to subscribe to.
            queue_group: Optional queue group name.
            **kwargs: Additional subscription options.
            
        Returns:
            nats.js.api.Subscription: The subscription.
        """
        validate_parameter(subject, "subject", str)
        
        async with self.connection() as nc:
            try:
                self._logger.debug(
                    "Creating subscription",
                    subject=subject,
                    queue_group=queue_group,
                )
                
                if queue_group:
                    subscription = await nc.subscribe(
                        subject, queue=queue_group, **kwargs
                    )
                else:
                    subscription = await nc.subscribe(subject, **kwargs)
                
                self._logger.debug(
                    "Subscription created successfully",
                    subject=subject,
                    queue_group=queue_group,
                )
                
                return subscription
                
            except Exception as e:
                error_msg = f"Failed to subscribe to {subject}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def pull_subscribe(
        self,
        subject: str,
        durable_name: str,
        **kwargs: Any,
    ) -> JetStreamContext.PullSubscription:
        """
        Create a pull subscription.
        
        Args:
            subject: NATS subject to subscribe to.
            durable_name: Durable consumer name.
            **kwargs: Additional subscription options.
            
        Returns:
            nats.js.api.PullSubscription: The pull subscription.
        """
        validate_parameter(subject, "subject", str)
        validate_parameter(durable_name, "durable_name", str)
        
        async with self.jetstream() as js:
            try:
                self._logger.debug(
                    "Creating pull subscription",
                    subject=subject,
                    durable_name=durable_name,
                )
                
                subscription = await js.pull_subscribe(
                    subject, durable=durable_name, **kwargs
                )
                
                self._logger.debug(
                    "Pull subscription created successfully",
                    subject=subject,
                    durable_name=durable_name,
                )
                
                return subscription
                
            except Exception as e:
                error_msg = f"Failed to create pull subscription for {subject}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def fetch_messages(
        self,
        subscription: JetStreamContext.PullSubscription,
        batch_size: int = 1,
        timeout: float = 1.0,
    ) -> list[nats.aio.msg.Msg]:
        """
        Fetch messages from a pull subscription.
        
        Args:
            subscription: The pull subscription.
            batch_size: Number of messages to fetch.
            timeout: Fetch timeout in seconds.
            
        Returns:
            list[nats.aio.msg.Msg]: List of messages.
        """
        validate_parameter(subscription, "subscription", JetStreamContext.PullSubscription)
        validate_parameter(batch_size, "batch_size", int, min_value=1)
        validate_parameter(timeout, "timeout", (int, float), min_value=0.1)
        
        try:
            self._logger.debug(
                "Fetching messages",
                batch_size=batch_size,
                timeout=timeout,
            )
            
            messages = await subscription.fetch(batch=batch_size, timeout=timeout)
            
            self._logger.debug(
                "Messages fetched successfully",
                count=len(messages),
            )
            
            return messages
            
        except asyncio.TimeoutError:
            # Timeout is expected when no messages are available
            return []
        except Exception as e:
            error_msg = f"Failed to fetch messages: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e
    
    async def purge_stream(
        self,
        stream_name: str,
        subject: Optional[str] = None,
    ) -> None:
        """
        Purge messages from a stream.
        
        Args:
            stream_name: Name of the stream.
            subject: Optional subject to filter messages.
        """
        validate_parameter(stream_name, "stream_name", str)
        
        async with self.jetstream() as js:
            try:
                self._logger.info(
                    "Purging stream",
                    stream_name=stream_name,
                    subject=subject,
                )
                
                if subject:
                    await js.purge_stream(stream_name, subject=subject)
                else:
                    await js.purge_stream(stream_name)
                
                self._logger.info(
                    "Stream purged successfully",
                    stream_name=stream_name,
                    subject=subject,
                )
                
            except Exception as e:
                error_msg = f"Failed to purge stream {stream_name}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def get_kv(self, bucket_name: str) -> KeyValue:
        """
        Get a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
            
        Returns:
            KeyValue: The KV bucket.
        """
        validate_parameter(bucket_name, "bucket_name", str)
        
        async with self.jetstream() as js:
            try:
                self._logger.debug(
                    "Getting KV bucket",
                    bucket_name=bucket_name,
                )
                
                kv = await js.key_value(bucket_name)
                
                self._logger.debug(
                    "KV bucket retrieved successfully",
                    bucket_name=bucket_name,
                )
                
                return kv
                
            except Exception as e:
                error_msg = f"Failed to get KV bucket {bucket_name}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def get_kv_store(self, bucket_name: str) -> KeyValue:
        """
        Get a Key-Value store bucket (alias for get_kv for backward compatibility).
        
        Args:
            bucket_name: Name of the KV bucket.
            
        Returns:
            KeyValue: The KV bucket.
        """
        return await self.get_kv(bucket_name)
    
    async def create_kv(
        self,
        bucket_name: str,
        **kwargs: Any,
    ) -> KeyValue:
        """
        Create a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
            **kwargs: Additional bucket configuration options.
            
        Returns:
            KeyValue: The KV bucket.
        """
        validate_parameter(bucket_name, "bucket_name", str)
        
        async with self.jetstream() as js:
            try:
                self._logger.info(
                    "Creating KV bucket",
                    bucket_name=bucket_name,
                )
                
                kv = await js.create_key_value(bucket=bucket_name, **kwargs)
                
                self._logger.info(
                    "KV bucket created successfully",
                    bucket_name=bucket_name,
                )
                
                return kv
                
            except Exception as e:
                error_msg = f"Failed to create KV bucket {bucket_name}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def delete_kv(self, bucket_name: str) -> None:
        """
        Delete a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
        """
        validate_parameter(bucket_name, "bucket_name", str)
        
        async with self.jetstream() as js:
            try:
                self._logger.info(
                    "Deleting KV bucket",
                    bucket_name=bucket_name,
                )
                
                await js.delete_key_value(bucket_name)
                
                self._logger.info(
                    "KV bucket deleted successfully",
                    bucket_name=bucket_name,
                )
                
            except Exception as e:
                error_msg = f"Failed to delete KV bucket {bucket_name}: {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e
    
    async def trigger_due_jobs(self) -> tuple[int, int]:
        """
        Trigger processing of due scheduled jobs.
        
        This method checks for scheduled jobs that are due to be processed
        and enqueues them for execution.
        
        Returns:
            tuple[int, int]: A tuple of (processed_count, error_count)
                where processed_count is the number of jobs processed
                and error_count is the number of errors encountered
        """
        from .schemas import SCHEDULED_JOBS_KV_NAME
        from .scheduler import LockData
        import msgspec
        import time
        
        processed_count = 0
        error_count = 0
        
        try:
            self._logger.debug("Checking for due scheduled jobs")
            
            # Get the scheduled jobs KV store
            kv = await self.get_kv(SCHEDULED_JOBS_KV_NAME)
            
            # Get all keys in the KV store
            keys = await kv.keys()
            
            if not keys:
                self._logger.debug("No scheduled jobs found")
                return (0, 0)
            
            current_time = time.time()
            
            # Check each job to see if it's due
            for key in keys:
                try:
                    entry = await kv.get(key)
                    if entry is None:
                        continue
                    
                    # Decode the job data
                    try:
                        job_data = msgspec.msgpack.decode(entry.value)
                    except Exception as e:
                        self._logger.warning("Failed to decode job data for key {}: {}", key, e)
                        error_count += 1
                        continue
                    
                    # Check if job is due
                    if job_data.get("scheduled_time", 0) <= current_time:
                        # Job is due, enqueue it
                        job_subject = job_data.get("subject")
                        job_payload = job_data.get("payload", b"")
                        
                        if job_subject:
                            try:
                                await self.jetstream_publish(job_subject, job_payload)
                                processed_count += 1
                                
                                # Remove the scheduled job
                                await kv.delete(key)
                                
                                self._logger.debug(
                                    "Processed scheduled job {}",
                                    key,
                                    subject=job_subject
                                )
                            except Exception as e:
                                self._logger.error("Failed to enqueue scheduled job {}: {}", key, e)
                                error_count += 1
                except Exception as e:
                    self._logger.error("Error processing scheduled job {}: {}", key, e)
                    error_count += 1
            
            self._logger.info(
                "Scheduled job processing complete",
                processed=processed_count,
                errors=error_count
            )
            
            return (processed_count, error_count)
            
        except Exception as e:
            error_msg = f"Error processing scheduled jobs: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e
    
    async def __aenter__(self) -> "NatsClient":
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()
    
    def __repr__(self) -> str:
        """String representation of the client."""
        return f"NatsClient(url={self._config.nats_url}, connected={self.is_connected})"