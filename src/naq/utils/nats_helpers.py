"""
NATS-specific utilities for NAQ.

This module provides comprehensive NATS utilities extracted from various modules
and new helpers as specified in Task 08. It includes connection management,
stream creation, KV store operations, subject building, message handling,
and NATS-specific operation patterns.

The module follows the NAQ pattern of async-first implementation with sync
wrappers where appropriate, and integrates seamlessly with existing utils.
"""

import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import (
    Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union,
    TypeVar, Awaitable, AsyncContextManager, ContextManager
)
from datetime import datetime
import uuid
import msgspec

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import (
    ConsumerConfig, DeliverPolicy, RetentionPolicy, StorageType, 
    StreamConfig, StreamInfo, KeyValueConfig, KeyValueStatus
)
from nats.errors import NotFoundError, TimeoutError, NoRespondersError

from loguru import logger

from ..exceptions import NaqConnectionError, NaqException
from .async_helpers import run_async, run_sync
from .retry import RetryConfig, retry_async, retry
from .error_handling import create_error_context, ErrorCategory
from .context_managers import managed_resource, managed_resource_sync

# Type variables for generic functions
T = TypeVar('T')
R = TypeVar('R')


# =============================================================================
# Connection Utilities (Extracted from src/naq/connection/utils.py)
# =============================================================================

@dataclass
class ConnectionMetrics:
    """Connection usage metrics."""
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0


class ConnectionMonitor:
    """Monitor connection usage and performance."""
    
    def __init__(self):
        self.metrics = ConnectionMetrics()
        self._connection_times: List[float] = []
    
    def record_connection_start(self):
        """Record connection start."""
        self.metrics.total_connections += 1
        self.metrics.active_connections += 1
    
    def record_connection_end(self, duration: float):
        """Record connection end."""
        self.metrics.active_connections -= 1
        self._connection_times.append(duration)
        self.metrics.average_connection_time = sum(self._connection_times) / len(self._connection_times)
    
    def record_connection_failure(self):
        """Record connection failure."""
        self.metrics.failed_connections += 1


# Global connection monitor
connection_monitor = ConnectionMonitor()


async def test_nats_connection(nats_url: str = "nats://localhost:4222", timeout: float = 5.0) -> bool:
    """
    Test NATS connection health.
    
    Args:
        nats_url: NATS server URL
        timeout: Connection timeout in seconds
        
    Returns:
        bool: True if connection is healthy, False otherwise
        
    Example:
        ```python
        if await test_nats_connection("nats://localhost:4222"):
            print("NATS connection is healthy")
        ```
    """
    try:
        conn = await nats.connect(
            servers=[nats_url],
            connect_timeout=timeout,
            ping_interval=timeout/2,
            max_outstanding_pings=2
        )
        try:
            # Simple ping test
            await conn.flush(timeout=timeout)
            return True
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"NATS connection test failed: {e}")
        return False


def test_nats_connection_sync(nats_url: str = "nats://localhost:4222", timeout: float = 5.0) -> bool:
    """
    Synchronous version of test_nats_connection.
    
    Args:
        nats_url: NATS server URL
        timeout: Connection timeout in seconds
        
    Returns:
        bool: True if connection is healthy, False otherwise
    """
    return run_async(test_nats_connection, nats_url, timeout)


async def wait_for_nats_connection(
    nats_url: str = "nats://localhost:4222", 
    timeout: int = 30, 
    check_interval: float = 1.0
) -> bool:
    """
    Wait for NATS connection to be available.
    
    Args:
        nats_url: NATS server URL
        timeout: Maximum time to wait in seconds
        check_interval: Interval between connection checks in seconds
        
    Returns:
        bool: True if connection became available, False if timeout reached
        
    Example:
        ```python
        if await wait_for_nats_connection("nats://localhost:4222", timeout=60):
            print("NATS connection is now available")
        ```
    """
    start_time = asyncio.get_event_loop().time()
    
    while (asyncio.get_event_loop().time() - start_time) < timeout:
        if await test_nats_connection(nats_url):
            return True
        await asyncio.sleep(check_interval)
    
    return False


def wait_for_nats_connection_sync(
    nats_url: str = "nats://localhost:4222", 
    timeout: int = 30, 
    check_interval: float = 1.0
) -> bool:
    """
    Synchronous version of wait_for_nats_connection.
    
    Args:
        nats_url: NATS server URL
        timeout: Maximum time to wait in seconds
        check_interval: Interval between connection checks in seconds
        
    Returns:
        bool: True if connection became available, False if timeout reached
    """
    return run_async(wait_for_nats_connection, nats_url, timeout, check_interval)


# =============================================================================
# Stream Creation Utilities (New implementations as specified in Task 08)
# =============================================================================

@dataclass
class StreamConfigHelper:
    """Helper class for stream configuration with error handling."""
    
    name: str
    subjects: List[str]
    storage_type: StorageType = StorageType.FILE
    retention_policy: RetentionPolicy = RetentionPolicy.LIMITS
    max_age: int = 60 * 60 * 24  # 24 hours
    max_msgs: int = 1000000
    max_bytes: int = 1024 * 1024 * 1024  # 1GB
    replicas: int = 1
    duplicate_window: int = 120  # 2 minutes
    
    def to_stream_config(self) -> StreamConfig:
        """Convert to NATS StreamConfig."""
        return StreamConfig(
            name=self.name,
            subjects=self.subjects,
            storage=self.storage_type,
            retention=self.retention_policy,
            max_age=self.max_age,
            max_msgs=self.max_msgs,
            max_bytes=self.max_bytes,
            num_replicas=self.replicas,
            duplicate_window=self.duplicate_window
        )


async def create_stream_with_retry(
    js: JetStreamContext,
    stream_config: Union[StreamConfig, StreamConfigHelper],
    retry_config: Optional[RetryConfig] = None
) -> StreamInfo:
    """
    Create a JetStream stream with retry logic and error handling.
    
    Args:
        js: JetStream context
        stream_config: Stream configuration
        retry_config: Optional retry configuration
        
    Returns:
        StreamInfo: Information about the created stream
        
    Raises:
        NaqException: If stream creation fails after all retries
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            config = StreamConfigHelper(
                name="MY_STREAM",
                subjects=["my.subject.*"]
            )
            stream_info = await create_stream_with_retry(js, config)
        ```
    """
    if isinstance(stream_config, StreamConfigHelper):
        stream_config = stream_config.to_stream_config()
    
    if retry_config is None:
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            retryable_exceptions=(NoRespondersError, TimeoutError, ConnectionError)
        )
    
    async def _create_stream():
        try:
            # Check if stream already exists
            await js.stream_info(stream_config.name)
            logger.info(f"Stream '{stream_config.name}' already exists")
            return await js.stream_info(stream_config.name)
        except NotFoundError:
            # Stream doesn't exist, create it
            logger.info(f"Creating stream '{stream_config.name}'")
            return await js.add_stream(stream_config)
    
    try:
        return await retry_async(_create_stream, config=retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="create_stream",
            exception=e,
            stream_name=stream_config.name,
            subjects=stream_config.subjects
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to create stream '{stream_config.name}': {e}") from e


def create_stream_with_retry_sync(
    js: JetStreamContext,
    stream_config: Union[StreamConfig, StreamConfigHelper],
    retry_config: Optional[RetryConfig] = None
) -> StreamInfo:
    """
    Synchronous version of create_stream_with_retry.
    
    Args:
        js: JetStream context
        stream_config: Stream configuration
        retry_config: Optional retry configuration
        
    Returns:
        StreamInfo: Information about the created stream
    """
    return run_async(create_stream_with_retry, js, stream_config, retry_config)


async def ensure_stream_exists(
    js: JetStreamContext,
    stream_name: str,
    subjects: Optional[List[str]] = None,
    **kwargs: Any
) -> StreamInfo:
    """
    Ensure a stream exists, creating it if necessary.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        subjects: List of subjects for the stream
        **kwargs: Additional stream configuration parameters
        
    Returns:
        StreamInfo: Information about the stream
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            stream_info = await ensure_stream_exists(
                js, 
                "MY_STREAM",
                subjects=["my.subject.*"],
                max_age=3600
            )
        ```
    """
    try:
        # Check if stream exists
        return await js.stream_info(stream_name)
    except NotFoundError:
        # Create stream with default configuration
        config = StreamConfigHelper(
            name=stream_name,
            subjects=subjects or [f"{stream_name}.>"],
            **kwargs
        )
        return await create_stream_with_retry(js, config)


def ensure_stream_exists_sync(
    js: JetStreamContext,
    stream_name: str,
    subjects: Optional[List[str]] = None,
    **kwargs: Any
) -> StreamInfo:
    """
    Synchronous version of ensure_stream_exists.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        subjects: List of subjects for the stream
        **kwargs: Additional stream configuration parameters
        
    Returns:
        StreamInfo: Information about the stream
    """
    return run_async(ensure_stream_exists, js, stream_name, subjects, **kwargs)


# =============================================================================
# KV Store Operations with Retries (New implementations as specified in Task 08)
# =============================================================================

@dataclass
class KVOperationConfig:
    """Configuration for KV store operations."""
    
    bucket_name: str
    retry_config: Optional[RetryConfig] = None
    timeout: float = 5.0
    
    def __post_init__(self):
        if self.retry_config is None:
            self.retry_config = RetryConfig(
                max_attempts=3,
                base_delay=0.5,
                max_delay=5.0,
                retryable_exceptions=(TimeoutError, NoRespondersError, ConnectionError)
            )


async def kv_get_with_retry(
    js: JetStreamContext,
    key: str,
    config: Union[KVOperationConfig, str]
) -> Optional[bytes]:
    """
    Get a value from NATS KeyValue store with retry logic.
    
    Args:
        js: JetStream context
        key: Key to retrieve
        config: KV operation configuration or bucket name string
        
    Returns:
        Optional[bytes]: The value if found, None otherwise
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            # Using config object
            config = KVOperationConfig(bucket_name="MY_BUCKET")
            value = await kv_get_with_retry(js, "my_key", config)
            
            # Using bucket name string
            value = await kv_get_with_retry(js, "my_key", "MY_BUCKET")
        ```
    """
    if isinstance(config, str):
        config = KVOperationConfig(bucket_name=config)
    
    async def _get_value():
        try:
            kv = await js.key_value(config.bucket_name)
            entry = await kv.get(key)
            return entry.value if entry else None
        except NotFoundError:
            return None
    
    try:
        return await retry_async(_get_value, config=config.retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="kv_get",
            exception=e,
            bucket_name=config.bucket_name,
            key=key
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to get key '{key}' from bucket '{config.bucket_name}': {e}") from e


def kv_get_with_retry_sync(
    js: JetStreamContext,
    key: str,
    config: Union[KVOperationConfig, str]
) -> Optional[bytes]:
    """
    Synchronous version of kv_get_with_retry.
    
    Args:
        js: JetStream context
        key: Key to retrieve
        config: KV operation configuration or bucket name string
        
    Returns:
        Optional[bytes]: The value if found, None otherwise
    """
    return run_async(kv_get_with_retry, js, key, config)


async def kv_put_with_retry(
    js: JetStreamContext,
    key: str,
    value: bytes,
    config: Union[KVOperationConfig, str]
) -> None:
    """
    Put a value into NATS KeyValue store with retry logic.
    
    Args:
        js: JetStream context
        key: Key to set
        value: Value to store
        config: KV operation configuration or bucket name string
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            config = KVOperationConfig(bucket_name="MY_BUCKET")
            await kv_put_with_retry(js, "my_key", b"my_value", config)
        ```
    """
    if isinstance(config, str):
        config = KVOperationConfig(bucket_name=config)
    
    async def _put_value():
        kv = await js.key_value(config.bucket_name)
        await kv.put(key, value)
    
    try:
        await retry_async(_put_value, config=config.retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="kv_put",
            exception=e,
            bucket_name=config.bucket_name,
            key=key,
            value_size=len(value)
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to put key '{key}' into bucket '{config.bucket_name}': {e}") from e


def kv_put_with_retry_sync(
    js: JetStreamContext,
    key: str,
    value: bytes,
    config: Union[KVOperationConfig, str]
) -> None:
    """
    Synchronous version of kv_put_with_retry.
    
    Args:
        js: JetStream context
        key: Key to set
        value: Value to store
        config: KV operation configuration or bucket name string
    """
    return run_async(kv_put_with_retry, js, key, value, config)


async def kv_delete_with_retry(
    js: JetStreamContext,
    key: str,
    config: Union[KVOperationConfig, str]
) -> bool:
    """
    Delete a key from NATS KeyValue store with retry logic.
    
    Args:
        js: JetStream context
        key: Key to delete
        config: KV operation configuration or bucket name string
        
    Returns:
        bool: True if key was deleted, False if it didn't exist
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            config = KVOperationConfig(bucket_name="MY_BUCKET")
            deleted = await kv_delete_with_retry(js, "my_key", config)
        ```
    """
    if isinstance(config, str):
        config = KVOperationConfig(bucket_name=config)
    
    async def _delete_key():
        try:
            kv = await js.key_value(config.bucket_name)
            await kv.delete(key)
            return True
        except NotFoundError:
            return False
    
    try:
        return await retry_async(_delete_key, config=config.retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="kv_delete",
            exception=e,
            bucket_name=config.bucket_name,
            key=key
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to delete key '{key}' from bucket '{config.bucket_name}': {e}") from e


def kv_delete_with_retry_sync(
    js: JetStreamContext,
    key: str,
    config: Union[KVOperationConfig, str]
) -> bool:
    """
    Synchronous version of kv_delete_with_retry.
    
    Args:
        js: JetStream context
        key: Key to delete
        config: KV operation configuration or bucket name string
        
    Returns:
        bool: True if key was deleted, False if it didn't exist
    """
    return run_async(kv_delete_with_retry, js, key, config)


async def create_kv_bucket_with_retry(
    js: JetStreamContext,
    bucket_name: str,
    retry_config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> KeyValueStatus:
    """
    Create a NATS KeyValue bucket with retry logic.
    
    Args:
        js: JetStream context
        bucket_name: Name of the bucket to create
        retry_config: Optional retry configuration
        **kwargs: Additional bucket configuration parameters
        
    Returns:
        KeyValueStatus: Status of the created bucket
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            status = await create_kv_bucket_with_retry(
                js, 
                "MY_BUCKET",
                max_value_size=1024*1024,
                history=5
            )
        ```
    """
    if retry_config is None:
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            retryable_exceptions=(TimeoutError, NoRespondersError, ConnectionError)
        )
    
    async def _create_bucket():
        try:
            # Check if bucket already exists
            kv = await js.key_value(bucket_name)
            return await kv.status()
        except NotFoundError:
            # Bucket doesn't exist, create it
            config = KeyValueConfig(bucket=bucket_name, **kwargs)
            kv = await js.create_key_value(config)
            return await kv.status()
    
    try:
        return await retry_async(_create_bucket, config=retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="create_kv_bucket",
            exception=e,
            bucket_name=bucket_name,
            **kwargs
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to create KV bucket '{bucket_name}': {e}") from e


def create_kv_bucket_with_retry_sync(
    js: JetStreamContext,
    bucket_name: str,
    retry_config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> KeyValueStatus:
    """
    Synchronous version of create_kv_bucket_with_retry.
    
    Args:
        js: JetStream context
        bucket_name: Name of the bucket to create
        retry_config: Optional retry configuration
        **kwargs: Additional bucket configuration parameters
        
    Returns:
        KeyValueStatus: Status of the created bucket
    """
    return run_async(create_kv_bucket_with_retry, js, bucket_name, retry_config, **kwargs)


# =============================================================================
# Subject Building and Parsing Utilities (New implementations as specified in Task 08)
# =============================================================================

@dataclass
class SubjectParts:
    """Parsed subject parts with validation."""
    prefix: str
    domain: str
    entity_type: str
    entity_id: str
    action: Optional[str] = None
    version: Optional[str] = None
    
    def validate(self) -> bool:
        """Validate subject parts."""
        if not all([self.prefix, self.domain, self.entity_type, self.entity_id]):
            return False
        if any("." in part for part in [self.domain, self.entity_type, self.entity_id]):
            return False
        return True
    
    def to_subject(self) -> str:
        """Convert back to subject string."""
        parts = [self.prefix, self.domain, self.entity_type, self.entity_id]
        if self.action:
            parts.append(self.action)
        if self.version:
            parts.append(self.version)
        return ".".join(parts)


def build_subject(
    prefix: str,
    domain: str,
    entity_type: str,
    entity_id: str,
    action: Optional[str] = None,
    version: Optional[str] = None
) -> str:
    """
    Build a NATS subject string from components.
    
    Args:
        prefix: Subject prefix (e.g., "naq")
        domain: Domain (e.g., "jobs", "events")
        entity_type: Type of entity (e.g., "job", "worker")
        entity_id: Entity identifier
        action: Optional action (e.g., "created", "updated")
        version: Optional version
        
    Returns:
        str: Formatted subject string
        
    Example:
        ```python
        subject = build_subject(
            prefix="naq",
            domain="jobs",
            entity_type="job",
            entity_id="123",
            action="enqueued"
        )
        # Returns: "naq.jobs.job.123.enqueued"
        ```
    """
    parts = SubjectParts(prefix, domain, entity_type, entity_id, action, version)
    if not parts.validate():
        raise ValueError(f"Invalid subject parts: {parts}")
    return parts.to_subject()


def parse_subject(subject: str) -> SubjectParts:
    """
    Parse a NATS subject string into components.
    
    Args:
        subject: NATS subject string to parse
        
    Returns:
        SubjectParts: Parsed subject components
        
    Raises:
        ValueError: If subject format is invalid
        
    Example:
        ```python
        parts = parse_subject("naq.jobs.job.123.enqueued")
        print(parts.domain)  # "jobs"
        print(parts.entity_id)  # "123"
        print(parts.action)  # "enqueued"
        ```
    """
    parts = subject.split(".")
    if len(parts) < 4:
        raise ValueError(f"Subject must have at least 4 parts: {subject}")
    
    subject_parts = SubjectParts(
        prefix=parts[0],
        domain=parts[1],
        entity_type=parts[2],
        entity_id=parts[3]
    )
    
    if len(parts) > 4:
        subject_parts.action = parts[4]
    if len(parts) > 5:
        subject_parts.version = parts[5]
    
    if not subject_parts.validate():
        raise ValueError(f"Invalid subject parts: {subject_parts}")
    
    return subject_parts


def create_subject_wildcard(
    prefix: str,
    domain: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    action: Optional[str] = None
) -> str:
    """
    Create a subject pattern with wildcards for subscription.
    
    Args:
        prefix: Subject prefix
        domain: Domain
        entity_type: Optional entity type (or wildcard)
        entity_id: Optional entity ID (or wildcard)
        action: Optional action (or wildcard)
        
    Returns:
        str: Subject pattern with wildcards
        
    Example:
        ```python
        # Match all job events
        pattern = create_subject_wildcard("naq", "jobs", "job", "*", "enqueued")
        # Returns: "naq.jobs.job.*.enqueued"
        
        # Match all events in jobs domain
        pattern = create_subject_wildcard("naq", "jobs")
        # Returns: "naq.jobs.>"
        ```
    """
    parts = [prefix, domain]
    
    if entity_type is not None:
        parts.append(entity_type)
        if entity_id is not None:
            parts.append(entity_id)
            if action is not None:
                parts.append(action)
        else:
            # Use multi-level wildcard for remaining parts
            parts.append(">")
    else:
        # Use multi-level wildcard for remaining parts
        parts.append(">")
    
    return ".".join(parts)


def matches_subject_pattern(subject: str, pattern: str) -> bool:
    """
    Check if a subject matches a pattern with wildcards.
    
    Args:
        subject: Subject to check
        pattern: Pattern with wildcards (* and >)
        
    Returns:
        bool: True if subject matches pattern
        
    Example:
        ```python
        matches = matches_subject_pattern(
            "naq.jobs.job.123.enqueued",
            "naq.jobs.job.*.enqueued"
        )
        # Returns: True
        ```
    """
    subject_parts = subject.split(".")
    pattern_parts = pattern.split(".")
    
    if len(pattern_parts) > len(subject_parts):
        return False
    
    for i, pattern_part in enumerate(pattern_parts):
        if pattern_part == "*":
            # Single-level wildcard
            continue
        elif pattern_part == ">":
            # Multi-level wildcard - matches everything remaining
            return True
        elif pattern_part != subject_parts[i]:
            return False
    
    # If we get here, all parts matched exactly
    return len(pattern_parts) == len(subject_parts)


# =============================================================================
# Message Handling Utilities (New implementations as specified in Task 08)
# =============================================================================

@dataclass
class MessageHandlerConfig:
    """Configuration for message handlers."""
    
    queue_group: Optional[str] = None
    durable_name: Optional[str] = None
    deliver_policy: DeliverPolicy = DeliverPolicy.ALL
    ack_policy: str = "explicit"
    max_deliver: int = 3
    replay_policy: str = "instant"
    max_waiting: int = 512
    max_ack_pending: int = 1024


async def create_consumer_with_retry(
    js: JetStreamContext,
    stream_name: str,
    subject_filter: str,
    config: MessageHandlerConfig,
    retry_config: Optional[RetryConfig] = None
) -> str:
    """
    Create a JetStream consumer with retry logic.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        subject_filter: Subject filter for the consumer
        config: Message handler configuration
        retry_config: Optional retry configuration
        
    Returns:
        str: Name of the created consumer
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            config = MessageHandlerConfig(
                queue_group="my_queue",
                durable_name="my_consumer"
            )
            consumer_name = await create_consumer_with_retry(
                js, "MY_STREAM", "my.subject", config
            )
        ```
    """
    if retry_config is None:
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            retryable_exceptions=(TimeoutError, NoRespondersError, ConnectionError)
        )
    
    consumer_config = ConsumerConfig(
        durable_name=config.durable_name,
        deliver_policy=config.deliver_policy,
        ack_policy=config.ack_policy,
        max_deliver=config.max_deliver,
        replay_policy=config.replay_policy,
        max_waiting=config.max_waiting,
        max_ack_pending=config.max_ack_pending
    )
    
    async def _create_consumer():
        consumer = await js.add_consumer(
            stream_name,
            config=consumer_config,
            filter_subject=subject_filter,
            durable_name=config.durable_name
        )
        return consumer.name
    
    try:
        return await retry_async(_create_consumer, config=retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="create_consumer",
            exception=e,
            stream_name=stream_name,
            subject_filter=subject_filter,
            queue_group=config.queue_group
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to create consumer for stream '{stream_name}': {e}") from e


def create_consumer_with_retry_sync(
    js: JetStreamContext,
    stream_name: str,
    subject_filter: str,
    config: MessageHandlerConfig,
    retry_config: Optional[RetryConfig] = None
) -> str:
    """
    Synchronous version of create_consumer_with_retry.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        subject_filter: Subject filter for the consumer
        config: Message handler configuration
        retry_config: Optional retry configuration
        
    Returns:
        str: Name of the created consumer
    """
    return run_async(create_consumer_with_retry, js, stream_name, subject_filter, config, retry_config)


async def publish_message_with_retry(
    conn: NATSClient,
    subject: str,
    data: bytes,
    headers: Optional[Dict[str, str]] = None,
    retry_config: Optional[RetryConfig] = None
) -> None:
    """
    Publish a message to NATS with retry logic.
    
    Args:
        conn: NATS connection
        subject: Subject to publish to
        data: Message data
        headers: Optional message headers
        retry_config: Optional retry configuration
        
    Example:
        ```python
        async with nats_connection() as conn:
            await publish_message_with_retry(
                conn,
                "my.subject",
                b"message data",
                headers={"message-type": "update"}
            )
        ```
    """
    if retry_config is None:
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=0.5,
            max_delay=5.0,
            retryable_exceptions=(TimeoutError, NoRespondersError, ConnectionError)
        )
    
    async def _publish():
        await conn.publish(subject, data, headers=headers)
    
    try:
        await retry_async(_publish, config=retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="publish_message",
            exception=e,
            subject=subject,
            data_size=len(data),
            headers=headers
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to publish message to '{subject}': {e}") from e


def publish_message_with_retry_sync(
    conn: NATSClient,
    subject: str,
    data: bytes,
    headers: Optional[Dict[str, str]] = None,
    retry_config: Optional[RetryConfig] = None
) -> None:
    """
    Synchronous version of publish_message_with_retry.
    
    Args:
        conn: NATS connection
        subject: Subject to publish to
        data: Message data
        headers: Optional message headers
        retry_config: Optional retry configuration
    """
    return run_async(publish_message_with_retry, conn, subject, data, headers, retry_config)


async def request_with_retry(
    conn: NATSClient,
    subject: str,
    data: bytes,
    timeout: float = 5.0,
    retry_config: Optional[RetryConfig] = None
) -> bytes:
    """
    Make a NATS request with retry logic.
    
    Args:
        conn: NATS connection
        subject: Subject to request
        data: Request data
        timeout: Request timeout
        retry_config: Optional retry configuration
        
    Returns:
        bytes: Response data
        
    Example:
        ```python
        async with nats_connection() as conn:
            response = await request_with_retry(
                conn,
                "my.service",
                b"request data",
                timeout=10.0
            )
        ```
    """
    if retry_config is None:
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=0.5,
            max_delay=5.0,
            retryable_exceptions=(TimeoutError, NoRespondersError, ConnectionError)
        )
    
    async def _request():
        response = await conn.request(subject, data, timeout=timeout)
        return response.data
    
    try:
        return await retry_async(_request, config=retry_config)
    except Exception as e:
        error_context = create_error_context(
            operation="request",
            exception=e,
            subject=subject,
            data_size=len(data),
            timeout=timeout
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to make request to '{subject}': {e}") from e


def request_with_retry_sync(
    conn: NATSClient,
    subject: str,
    data: bytes,
    timeout: float = 5.0,
    retry_config: Optional[RetryConfig] = None
) -> bytes:
    """
    Synchronous version of request_with_retry.
    
    Args:
        conn: NATS connection
        subject: Subject to request
        data: Request data
        timeout: Request timeout
        retry_config: Optional retry configuration
        
    Returns:
        bytes: Response data
    """
    return run_async(request_with_retry, conn, subject, data, timeout, retry_config)


# =============================================================================
# NATS-specific Operation Patterns (New implementations as specified in Task 08)
# =============================================================================

@asynccontextmanager
async def nats_subscription(
    conn: NATSClient,
    subject: str,
    queue_group: Optional[str] = None,
    **kwargs: Any
) -> AsyncIterator[nats.js.Subscription]:
    """
    Context manager for NATS subscriptions with proper cleanup.
    
    Args:
        conn: NATS connection
        subject: Subject to subscribe to
        queue_group: Optional queue group name
        **kwargs: Additional subscription parameters
        
    Yields:
        nats.js.Subscription: The subscription object
        
    Example:
        ```python
        async with nats_connection() as conn:
            async with nats_subscription(conn, "my.subject", queue_group="workers") as sub:
                async for msg in sub.messages:
                    print(f"Received: {msg.data}")
        ```
    """
    sub = None
    try:
        sub = await conn.subscribe(subject, queue=queue_group, **kwargs)
        yield sub
    except Exception as e:
        logger.error(f"Error creating NATS subscription: {e}")
        raise
    finally:
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception as e:
                logger.warning(f"Error unsubscribing from NATS: {e}")


@asynccontextmanager
async def jetstream_subscription(
    js: JetStreamContext,
    stream_name: str,
    consumer_name: str,
    **kwargs: Any
) -> AsyncIterator[nats.js.Consumer]:
    """
    Context manager for JetStream consumer subscriptions with proper cleanup.
    
    Args:
        js: JetStream context
        stream_name: Name of the stream
        consumer_name: Name of the consumer
        **kwargs: Additional subscription parameters
        
    Yields:
        nats.js.Consumer: The consumer object
        
    Example:
        ```python
        async with nats_jetstream() as (conn, js):
            async with jetstream_subscription(js, "MY_STREAM", "MY_CONSUMER") as consumer:
                async for msg in consumer.messages:
                    print(f"Received: {msg.data}")
                    await msg.ack()
        ```
    """
    consumer = None
    try:
        consumer = await js.consumer(stream_name, consumer_name)
        yield consumer
    except Exception as e:
        logger.error(f"Error creating JetStream consumer: {e}")
        raise
    finally:
        # Note: JetStream consumers don't need explicit cleanup like subscriptions
        pass


@asynccontextmanager
async def nats_request_context(
    conn: NATSClient,
    subject: str,
    timeout: float = 5.0
) -> AsyncIterator[Callable[[bytes], Awaitable[bytes]]]:
    """
    Context manager for making NATS requests within a specific timeout context.
    
    Args:
        conn: NATS connection
        subject: Subject for requests
        timeout: Request timeout
        
    Yields:
        Callable: Function to make requests
        
    Example:
        ```python
        async with nats_connection() as conn:
            async with nats_request_context(conn, "my.service", timeout=10.0) as request:
                response = await request(b"request data")
        ```
    """
    async def make_request(data: bytes) -> bytes:
        response = await conn.request(subject, data, timeout=timeout)
        return response.data
    
    try:
        yield make_request
    except Exception as e:
        logger.error(f"Error in NATS request context: {e}")
        raise


@contextmanager
def nats_request_context_sync(
    conn: NATSClient,
    subject: str,
    timeout: float = 5.0
) -> Iterator[Callable[[bytes], bytes]]:
    """
    Synchronous context manager for making NATS requests.
    
    Args:
        conn: NATS connection
        subject: Subject for requests
        timeout: Request timeout
        
    Yields:
        Callable: Function to make requests
        
    Example:
        ```python
        with nats_connection_sync() as conn:
            with nats_request_context_sync(conn, "my.service", timeout=10.0) as request:
                response = request(b"request data")
        ```
    """
    def make_request(data: bytes) -> bytes:
        return run_async(request_with_retry_sync, conn, subject, data, timeout)
    
    try:
        yield make_request
    except Exception as e:
        logger.error(f"Error in NATS request context: {e}")
        raise


async def batch_publish(
    conn: NATSClient,
    messages: List[Tuple[str, bytes, Optional[Dict[str, str]]]],
    batch_size: int = 100,
    delay_between_batches: float = 0.1
) -> List[Exception]:
    """
    Publish multiple messages in batches with error handling.
    
    Args:
        conn: NATS connection
        messages: List of (subject, data, headers) tuples
        batch_size: Number of messages per batch
        delay_between_batches: Delay between batches in seconds
        
    Returns:
        List[Exception]: List of exceptions that occurred
        
    Example:
        ```python
        messages = [
            ("subject1", b"data1", {"type": "update"}),
            ("subject2", b"data2", {"type": "create"}),
        ]
        errors = await batch_publish(conn, messages, batch_size=50)
        if errors:
            print(f"Failed to publish {len(errors)} messages")
        ```
    """
    errors = []
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        
        # Publish batch concurrently
        tasks = []
        for subject, data, headers in batch:
            task = asyncio.create_task(
                publish_message_with_retry(conn, subject, data, headers)
            )
            tasks.append(task)
        
        # Wait for all tasks in batch to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect errors
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
        
        # Delay between batches (except for the last batch)
        if i + batch_size < len(messages):
            await asyncio.sleep(delay_between_batches)
    
    return errors


def batch_publish_sync(
    conn: NATSClient,
    messages: List[Tuple[str, bytes, Optional[Dict[str, str]]]],
    batch_size: int = 100,
    delay_between_batches: float = 0.1
) -> List[Exception]:
    """
    Synchronous version of batch_publish.
    
    Args:
        conn: NATS connection
        messages: List of (subject, data, headers) tuples
        batch_size: Number of messages per batch
        delay_between_batches: Delay between batches in seconds
        
    Returns:
        List[Exception]: List of exceptions that occurred
    """
    return run_async(batch_publish, conn, messages, batch_size, delay_between_batches)


# =============================================================================
# NATS Connection Context Managers (Consolidated from connection module)
# =============================================================================

@asynccontextmanager
async def nats_connection_context(
    servers: Optional[List[str]] = None,
    client_name: Optional[str] = None,
    max_reconnect_attempts: int = 5,
    reconnect_time_wait: float = 2.0,
    **kwargs: Any
) -> AsyncIterator[NATSClient]:
    """
    Context manager for NATS connections with comprehensive error handling.
    
    Args:
        servers: List of NATS server URLs
        client_name: Client name for connection
        max_reconnect_attempts: Maximum reconnection attempts
        reconnect_time_wait: Time to wait between reconnection attempts
        **kwargs: Additional connection parameters
        
    Yields:
        NATSClient: The NATS connection object
        
    Example:
        ```python
        async with nats_connection_context(
            servers=["nats://localhost:4222"],
            client_name="my-app"
        ) as conn:
            await conn.publish("my.subject", b"hello")
        ```
    """
    if servers is None:
        servers = ["nats://localhost:4222"]
    if client_name is None:
        client_name = "naq-client"
    
    conn = None
    start_time = time.time()
    
    try:
        connection_monitor.record_connection_start()
        
        conn = await nats.connect(
            servers=servers,
            name=client_name,
            max_reconnect_attempts=max_reconnect_attempts,
            reconnect_time_wait=reconnect_time_wait,
            **kwargs
        )
        
        yield conn
        
    except Exception as e:
        connection_monitor.record_connection_failure()
        error_context = create_error_context(
            operation="nats_connection",
            exception=e,
            servers=servers,
            client_name=client_name
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqConnectionError(f"Failed to establish NATS connection: {e}") from e
        
    finally:
        if conn is not None:
            try:
                await conn.close()
                duration = time.time() - start_time
                connection_monitor.record_connection_end(duration)
            except Exception as e:
                logger.warning(f"Error closing NATS connection: {e}")


@asynccontextmanager
async def jetstream_context_from_connection(
    conn: NATSClient
) -> AsyncIterator[JetStreamContext]:
    """
    Context manager for JetStream contexts from existing connection.
    
    Args:
        conn: Existing NATS connection
        
    Yields:
        JetStreamContext: The JetStream context object
        
    Example:
        ```python
        async with nats_connection_context() as conn:
            async with jetstream_context_from_connection(conn) as js:
                stream_info = await js.stream_info("MY_STREAM")
        ```
    """
    try:
        js = conn.jetstream()
        yield js
    except Exception as e:
        error_context = create_error_context(
            operation="jetstream_context",
            exception=e
        )
        error_context.category = ErrorCategory.CONNECTION
        raise NaqException(f"Failed to create JetStream context: {e}") from e


@asynccontextmanager
async def nats_jetstream_context(
    servers: Optional[List[str]] = None,
    **kwargs: Any
) -> AsyncIterator[Tuple[NATSClient, JetStreamContext]]:
    """
    Combined context manager for NATS connection and JetStream.
    
    Args:
        servers: List of NATS server URLs
        **kwargs: Additional connection parameters
        
    Yields:
        Tuple[NATSClient, JetStreamContext]: The connection and JetStream context
        
    Example:
        ```python
        async with nats_jetstream_context(servers=["nats://localhost:4222"]) as (conn, js):
            await js.add_stream(name="MY_STREAM", subjects=["my.subject.*"])
            await conn.publish("my.subject.test", b"hello")
        ```
    """
    async with nats_connection_context(servers, **kwargs) as conn:
        async with jetstream_context_from_connection(conn) as js:
            yield conn, js


# =============================================================================
# Synchronous Wrappers for Context Managers
# =============================================================================

@contextmanager
def nats_connection_context_sync(
    servers: Optional[List[str]] = None,
    **kwargs: Any
) -> Iterator[NATSClient]:
    """
    Synchronous context manager for NATS connections.
    
    Args:
        servers: List of NATS server URLs
        **kwargs: Additional connection parameters
        
    Yields:
        NATSClient: The NATS connection object
    """
    async def _get_connection():
        async with nats_connection_context(servers, **kwargs) as conn:
            return conn
    
    conn = run_async(_get_connection)
    try:
        yield conn
    finally:
        run_async(conn.close)


@contextmanager
def jetstream_context_sync(
    conn: NATSClient
) -> Iterator[JetStreamContext]:
    """
    Synchronous context manager for JetStream contexts.
    
    Args:
        conn: NATS connection
        
    Yields:
        JetStreamContext: The JetStream context object
    """
    async def _get_jetstream():
        async with jetstream_context_from_connection(conn) as js:
            return js
    
    js = run_async(_get_jetstream)
    try:
        yield js
    finally:
        # No cleanup needed for JetStream context
        pass


@contextmanager
def nats_jetstream_context_sync(
    servers: Optional[List[str]] = None,
    **kwargs: Any
) -> Iterator[Tuple[NATSClient, JetStreamContext]]:
    """
    Synchronous combined context manager for NATS connection and JetStream.
    
    Args:
        servers: List of NATS server URLs
        **kwargs: Additional connection parameters
        
    Yields:
        Tuple[NATSClient, JetStreamContext]: The connection and JetStream context
    """
    async def _get_both():
        async with nats_jetstream_context(servers, **kwargs) as (conn, js):
            return conn, js
    
    conn, js = run_async(_get_both)
    try:
        yield conn, js
    finally:
        run_async(conn.close)


# =============================================================================
# Utility Functions
# =============================================================================

def get_connection_metrics() -> ConnectionMetrics:
    """
    Get current connection metrics.
    
    Returns:
        ConnectionMetrics: Copy of current metrics
    """
    return ConnectionMetrics(
        total_connections=connection_monitor.metrics.total_connections,
        active_connections=connection_monitor.metrics.active_connections,
        failed_connections=connection_monitor.metrics.failed_connections,
        average_connection_time=connection_monitor.metrics.average_connection_time
    )


def reset_connection_metrics() -> None:
    """Reset connection metrics."""
    connection_monitor.metrics = ConnectionMetrics()
    connection_monitor._connection_times.clear()


async def is_jetstream_enabled(conn: NATSClient) -> bool:
    """
    Check if JetStream is enabled on the NATS server.
    
    Args:
        conn: NATS connection
        
    Returns:
        bool: True if JetStream is enabled, False otherwise
        
    Example:
        ```python
        async with nats_connection_context() as conn:
            if await is_jetstream_enabled(conn):
                print("JetStream is available")
        ```
    """
    try:
        js = conn.jetstream()
        await js.account_info()
        return True
    except Exception:
        return False


def is_jetstream_enabled_sync(conn: NATSClient) -> bool:
    """
    Synchronous version of is_jetstream_enabled.
    
    Args:
        conn: NATS connection
        
    Returns:
        bool: True if JetStream is enabled, False otherwise
    """
    return run_async(is_jetstream_enabled, conn)


# =============================================================================
# Backward Compatibility Aliases
# =============================================================================

# Aliases for connection utilities from connection/utils.py
test_nats_connection_alias = test_nats_connection
wait_for_nats_connection_alias = wait_for_nats_connection
ConnectionMetricsAlias = ConnectionMetrics
ConnectionMonitorAlias = ConnectionMonitor
connection_monitor_alias = connection_monitor

# Aliases for context managers from connection/context_managers.py
nats_connection_alias = nats_connection_context
jetstream_context_alias = jetstream_context_from_connection
nats_jetstream_alias = nats_jetstream_context
nats_kv_store_alias = nats_kv_store  # This would need to be implemented