"""Connection context managers for NATS and JetStream operations."""

from contextlib import asynccontextmanager
from typing import Tuple, Optional, AsyncGenerator
import asyncio
import time
from loguru import logger

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .settings import DEFAULT_NATS_URL
from ..exceptions import NaqConnectionError


@asynccontextmanager
async def nats_connection(
    url: str = DEFAULT_NATS_URL, 
    client_name: Optional[str] = None,
    max_reconnect_attempts: Optional[int] = None,
    reconnect_time_wait: Optional[float] = None,
    connection_timeout: Optional[float] = None,
    **kwargs
) -> AsyncGenerator[NATSClient, None]:
    """
    Context manager for NATS connections with automatic cleanup.
    
    Args:
        url: NATS server URL
        client_name: Client name for identification
        max_reconnect_attempts: Maximum reconnection attempts
        reconnect_time_wait: Time between reconnection attempts
        connection_timeout: Connection timeout
        **kwargs: Additional connection parameters
        
    Yields:
        Connected NATS client
        
    Raises:
        NaqConnectionError: If connection fails
    """
    start_time = time.time()
    conn = None
    
    try:
        # Build connection options
        connect_options = {
            'servers': [url] if not url.startswith(('nats://', 'tls://')) else [url],
            'name': client_name or "naq_client",
        }
        
        if max_reconnect_attempts is not None:
            connect_options['max_reconnect_attempts'] = max_reconnect_attempts
        if reconnect_time_wait is not None:
            connect_options['reconnect_time_wait'] = reconnect_time_wait
        if connection_timeout is not None:
            connect_options['connection_timeout'] = connection_timeout
            
        # Add any additional options
        connect_options.update(kwargs)
        
        conn = await nats.connect(**connect_options)
        logger.info(f"NATS connection established to {url}")
        yield conn
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"NATS connection failed after {duration:.2f}s: {e}")
        raise NaqConnectionError(f"Failed to connect to NATS at {url}: {e}") from e
        
    finally:
        if conn and conn.is_connected:
            await conn.close()
            duration = time.time() - start_time
            logger.info(f"NATS connection to {url} closed after {duration:.2f}s")


@asynccontextmanager
async def jetstream_context(conn: NATSClient) -> AsyncGenerator[JetStreamContext, None]:
    """
    Context manager for JetStream contexts with automatic cleanup.
    
    Args:
        conn: Connected NATS client
        
    Yields:
        JetStream context
        
    Raises:
        NaqConnectionError: If JetStream context creation fails
    """
    try:
        js = conn.jetstream()
        yield js
        
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise NaqConnectionError(f"Failed to create JetStream context: {e}") from e


@asynccontextmanager
async def nats_jetstream(
    url: str = DEFAULT_NATS_URL,
    client_name: Optional[str] = None,
    max_reconnect_attempts: Optional[int] = None,
    reconnect_time_wait: Optional[float] = None,
    connection_timeout: Optional[float] = None,
    **kwargs
) -> AsyncGenerator[Tuple[NATSClient, JetStreamContext], None]:
    """
    Combined context manager for NATS connection and JetStream.
    
    Args:
        url: NATS server URL
        client_name: Client name for identification
        max_reconnect_attempts: Maximum reconnection attempts
        reconnect_time_wait: Time between reconnection attempts
        connection_timeout: Connection timeout
        **kwargs: Additional connection parameters
        
    Yields:
        Tuple of (NATS client, JetStream context)
    """
    async with nats_connection(
        url=url,
        client_name=client_name,
        max_reconnect_attempts=max_reconnect_attempts,
        reconnect_time_wait=reconnect_time_wait,
        connection_timeout=connection_timeout,
        **kwargs
    ) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js


@asynccontextmanager
async def nats_kv_store(
    bucket_name: str,
    url: str = DEFAULT_NATS_URL,
    client_name: Optional[str] = None,
    max_reconnect_attempts: Optional[int] = None,
    reconnect_time_wait: Optional[float] = None,
    connection_timeout: Optional[float] = None,
    **kwargs
) -> AsyncGenerator[nats.js.api.KeyValue, None]:
    """
    Context manager for NATS KeyValue operations.
    
    Args:
        bucket_name: Name of the KV bucket
        url: NATS server URL
        client_name: Client name for identification
        max_reconnect_attempts: Maximum reconnection attempts
        reconnect_time_wait: Time between reconnection attempts
        connection_timeout: Connection timeout
        **kwargs: Additional connection parameters
        
    Yields:
        KeyValue store client
    """
    async with nats_jetstream(
        url=url,
        client_name=client_name,
        max_reconnect_attempts=max_reconnect_attempts,
        reconnect_time_wait=reconnect_time_wait,
        connection_timeout=connection_timeout,
        **kwargs
    ) as (conn, js):
        try:
            kv = await js.key_value(bucket_name)
            yield kv
            
        except Exception as e:
            logger.error(f"KV store error for bucket {bucket_name}: {e}")
            raise NaqConnectionError(f"Failed to access KV bucket {bucket_name}: {e}") from e


@asynccontextmanager
async def legacy_nats_connection(url: str = DEFAULT_NATS_URL) -> AsyncGenerator[NATSClient, None]:
    """
    Legacy context manager for backward compatibility with existing connection patterns.
    
    This uses the existing connection manager but wraps it in a context manager interface.
    
    Args:
        url: NATS server URL
        
    Yields:
        Connected NATS client
    """
    conn = None
    try:
        conn = await get_nats_connection(url)
        yield conn
    finally:
        if conn:
            await close_nats_connection(url)


@asynccontextmanager
async def legacy_jetstream_context(conn: NATSClient) -> AsyncGenerator[JetStreamContext, None]:
    """
    Legacy context manager for backward compatibility with existing JetStream patterns.
    
    Args:
        conn: Connected NATS client
        
    Yields:
        JetStream context
    """
    try:
        js = await get_jetstream_context(conn)
        yield js
    finally:
        # No explicit cleanup needed for JetStream context
        pass


@asynccontextmanager
async def legacy_nats_jetstream(url: str = DEFAULT_NATS_URL) -> AsyncGenerator[Tuple[NATSClient, JetStreamContext], None]:
    """
    Legacy combined context manager for backward compatibility.
    
    Args:
        url: NATS server URL
        
    Yields:
        Tuple of (NATS client, JetStream context)
    """
    async with legacy_nats_connection(url) as conn:
        async with legacy_jetstream_context(conn) as js:
            yield conn, js