# src/naq/connection/context_managers.py
"""
Standalone context managers for NATS connections.

These context managers provide a clean, resource-safe way to work with NATS
connections without requiring the full service layer infrastructure.
"""

from contextlib import asynccontextmanager
from typing import Optional, Tuple
import time

import nats
from nats.js import JetStreamContext
from loguru import logger

from ..settings import DEFAULT_NATS_URL
from ..exceptions import NaqConnectionError


@asynccontextmanager
async def nats_connection(config: Optional[dict] = None):
    """
    Context manager for NATS connections.
    
    Provides automatic connection establishment, error handling, and cleanup.
    
    Args:
        config: Optional configuration dict. If None, uses default config
        
    Yields:
        Connected NATS client
        
    Usage:
        async with nats_connection() as conn:
            await conn.publish("subject", b"data")
    """
    if config is None:
        # Use basic default config
        config = {'nats_url': DEFAULT_NATS_URL}
    
    nats_url = config.get('nats_url', DEFAULT_NATS_URL)
    client_name = config.get('client_name', 'naq-client')
    
    # Connection parameters from config
    connect_params = {
        'servers': [nats_url] if isinstance(nats_url, str) else nats_url,
        'name': client_name,
    }
    
    # Add optional connection parameters if present in config
    if 'max_reconnect_attempts' in config:
        connect_params['max_reconnect_attempts'] = config['max_reconnect_attempts']
    if 'reconnect_time_wait' in config:
        connect_params['reconnect_time_wait'] = config['reconnect_time_wait']
    if 'connection_timeout' in config:
        connect_params['connect_timeout'] = config['connection_timeout']
    if 'drain_timeout' in config:
        connect_params['drain_timeout'] = config['drain_timeout']
    if 'flush_timeout' in config:
        connect_params['flush_timeout'] = config['flush_timeout']
    if 'ping_interval' in config:
        connect_params['ping_interval'] = config['ping_interval']
    if 'max_outstanding_pings' in config:
        connect_params['max_outstanding_pings'] = config['max_outstanding_pings']
    
    conn = None
    start_time = time.time()
    
    try:
        logger.debug(f"Establishing NATS connection to {nats_url}")
        conn = await nats.connect(**connect_params)
        
        connection_time = time.time() - start_time
        logger.debug(f"NATS connection established in {connection_time:.3f}s")
        
        yield conn
        
    except Exception as e:
        logger.error(f"NATS connection error: {e}")
        raise NaqConnectionError(f"Failed to connect to NATS at {nats_url}: {e}") from e
        
    finally:
        if conn and not conn.is_closed:
            try:
                await conn.close()
                logger.debug("NATS connection closed")
            except Exception as e:
                logger.warning(f"Error closing NATS connection: {e}")


@asynccontextmanager  
async def jetstream_context(conn: nats.aio.client.Client):
    """
    Context manager for JetStream contexts from an existing connection.
    
    Args:
        conn: An established NATS connection
        
    Yields:
        JetStream context
        
    Usage:
        async with nats_connection() as conn:
            async with jetstream_context(conn) as js:
                await js.publish("subject", b"data")
    """
    try:
        logger.debug("Creating JetStream context")
        js = conn.jetstream()
        yield js
        
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise NaqConnectionError(f"Failed to create JetStream context: {e}") from e


@asynccontextmanager
async def nats_jetstream(config: Optional[dict] = None) -> Tuple[nats.aio.client.Client, JetStreamContext]:
    """
    Combined context manager for NATS connection and JetStream.
    
    Provides both connection and JetStream context in a single context manager
    for convenience when both are needed.
    
    Args:
        config: Optional configuration dict. If None, uses default config
        
    Yields:
        Tuple of (connection, jetstream_context)
        
    Usage:
        async with nats_jetstream() as (conn, js):
            await conn.publish("subject", b"data")
            await js.add_stream(name="test_stream")
    """
    async with nats_connection(config) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js


@asynccontextmanager
async def nats_kv_store(bucket_name: str, config: Optional[dict] = None):
    """
    Context manager for NATS KeyValue operations.
    
    Automatically handles connection establishment and KV store access.
    
    Args:
        bucket_name: Name of the KV bucket to access
        config: Optional configuration dict. If None, uses default config
        
    Yields:
        KeyValue store instance
        
    Usage:
        async with nats_kv_store("my_bucket") as kv:
            await kv.put("key", b"value")
    """
    async with nats_jetstream(config) as (conn, js):
        try:
            logger.debug(f"Accessing KV store: {bucket_name}")
            kv = await js.key_value(bucket_name)
            yield kv
            
        except Exception as e:
            logger.error(f"KV store error for bucket {bucket_name}: {e}")
            raise NaqConnectionError(f"Failed to access KV store '{bucket_name}': {e}") from e


@asynccontextmanager
async def nats_object_store(bucket_name: str, config: Optional[dict] = None):
    """
    Context manager for NATS Object Store operations.
    
    Args:
        bucket_name: Name of the object store bucket to access
        config: Optional configuration dict. If None, uses default config
        
    Yields:
        Object store instance
        
    Usage:
        async with nats_object_store("my_bucket") as store:
            await store.put("filename", b"data")
    """
    async with nats_jetstream(config) as (conn, js):
        try:
            logger.debug(f"Accessing object store: {bucket_name}")
            store = await js.object_store(bucket_name)
            yield store
            
        except Exception as e:
            logger.error(f"Object store error for bucket {bucket_name}: {e}")
            raise NaqConnectionError(f"Failed to access object store '{bucket_name}': {e}") from e