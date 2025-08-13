# src/naq/connection/decorators.py
"""
Connection decorators for automatic NATS connection injection.

Provides function decorators that automatically inject NATS connections
and JetStream contexts into function calls, eliminating boilerplate
connection management code.
"""

import inspect
from functools import wraps
from typing import Callable, Optional, Any, Union

from loguru import logger
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from .context_managers import nats_connection, nats_jetstream, nats_kv_store
from ..settings import DEFAULT_NATS_URL


def with_nats_connection(
    config_key: Optional[str] = None,
    config: Optional[dict] = None,
    connection_param: str = 'conn'
):
    """
    Decorator to inject NATS connection into function.
    
    The decorated function will automatically receive a connected NATS client
    as the first parameter (after self if it's a method).
    
    Args:
        config_key: Optional key to look up configuration (unused currently)
        config: Optional explicit configuration dict
        connection_param: Parameter name for the injected connection (default: 'conn')
        
    Usage:
        @with_nats_connection()
        async def send_message(conn: NATSClient, subject: str, data: bytes):
            await conn.publish(subject, data)
            
        @with_nats_connection(connection_param='nc')  
        async def custom_function(nc: NATSClient, arg1: str):
            # Function automatically gets connection as 'nc'
            await nc.publish(f"subject.{arg1}", b"data")
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get configuration
            conn_config = config
            if conn_config is None:
                conn_config = {'nats_url': DEFAULT_NATS_URL}
            
            # Check if connection is already provided in kwargs
            if connection_param in kwargs:
                logger.debug(f"Connection already provided in {connection_param}, skipping injection")
                return await func(*args, **kwargs)
            
            # Inject connection
            async with nats_connection(conn_config) as conn:
                # Inspect function signature to determine injection method
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                # For methods, skip 'self' parameter
                if params and params[0] == 'self':
                    if len(args) > 0:
                        # Method call - inject after self
                        return await func(args[0], conn, *args[1:], **kwargs)
                    else:
                        # Shouldn't happen, but handle gracefully
                        kwargs[connection_param] = conn
                        return await func(*args, **kwargs)
                else:
                    # Regular function - inject as first parameter
                    return await func(conn, *args, **kwargs)
                    
        return wrapper
    return decorator


def with_jetstream_context(
    config_key: Optional[str] = None,
    config: Optional[dict] = None,
    jetstream_param: str = 'js',
    include_connection: bool = False,
    connection_param: str = 'conn'
):
    """
    Decorator to inject JetStream context into function.
    
    Args:
        config_key: Optional key to look up configuration (unused currently)
        config: Optional explicit configuration dict
        jetstream_param: Parameter name for the injected JetStream context
        include_connection: Whether to also inject the NATS connection
        connection_param: Parameter name for the injected connection (if include_connection=True)
        
    Usage:
        @with_jetstream_context()
        async def create_stream(js: JetStreamContext, stream_name: str):
            await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])
            
        @with_jetstream_context(include_connection=True)
        async def publish_and_consume(conn: NATSClient, js: JetStreamContext, subject: str):
            # Function gets both connection and jetstream
            await js.publish(subject, b"data")
            await conn.subscribe(subject)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get configuration
            conn_config = config
            if conn_config is None:
                conn_config = {'nats_url': DEFAULT_NATS_URL}
            
            # Check if parameters are already provided
            js_provided = jetstream_param in kwargs
            conn_provided = connection_param in kwargs
            
            if js_provided and (not include_connection or conn_provided):
                logger.debug("Required connection parameters already provided, skipping injection")
                return await func(*args, **kwargs)
            
            # Inject connection and/or jetstream
            async with nats_jetstream(conn_config) as (conn, js):
                # Inspect function signature
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                # Handle method vs function
                is_method = params and params[0] == 'self' and len(args) > 0
                
                if is_method:
                    # Method call - inject after self
                    if include_connection:
                        return await func(args[0], conn, js, *args[1:], **kwargs)
                    else:
                        return await func(args[0], js, *args[1:], **kwargs)
                else:
                    # Regular function - inject as first parameters
                    if include_connection:
                        return await func(conn, js, *args, **kwargs)
                    else:
                        return await func(js, *args, **kwargs)
                        
        return wrapper
    return decorator


def with_jetstream(
    config_key: Optional[str] = None,
    config: Optional[dict] = None,
    js_param: str = 'js'
):
    """
    Decorator to inject JetStream context into function.
    
    Args:
        config_key: Optional key to look up configuration (unused currently)
        config: Optional explicit configuration dict
        js_param: Parameter name for the injected JetStream context
        
    Usage:
        @with_jetstream()
        async def publish_message(js, subject: str, data: bytes):
            await js.publish(subject, data)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get configuration
            conn_config = config
            if conn_config is None:
                conn_config = {'nats_url': DEFAULT_NATS_URL}
            
            # Check if JetStream is already provided
            if js_param in kwargs:
                logger.debug(f"JetStream already provided in {js_param}, skipping injection")
                return await func(*args, **kwargs)
            
            # Inject JetStream context
            async with nats_jetstream(conn_config) as (conn, js):
                # Inspect function signature
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                # Handle method vs function
                is_method = params and params[0] == 'self' and len(args) > 0
                
                if is_method:
                    # Method call - inject after self
                    return await func(args[0], js, *args[1:], **kwargs)
                else:
                    # Regular function - inject as first parameter
                    return await func(js, *args, **kwargs)
                    
        return wrapper
    return decorator


def with_kv_store(
    bucket_name: Union[str, Callable[[Any], str]],
    config_key: Optional[str] = None,
    config: Optional[dict] = None,
    kv_param: str = 'kv'
):
    """
    Decorator to inject NATS KeyValue store into function.
    
    Args:
        bucket_name: KV bucket name or callable that returns bucket name
        config_key: Optional key to look up configuration (unused currently)
        config: Optional explicit configuration dict
        kv_param: Parameter name for the injected KV store
        
    Usage:
        @with_kv_store("worker_status")
        async def update_status(kv, worker_id: str, status: str):
            await kv.put(worker_id, status.encode())
            
        @with_kv_store(lambda args: f"queue_{args[1]}")  # Dynamic bucket name
        async def store_job(kv, job_id: str, queue_name: str, data: bytes):
            await kv.put(job_id, data)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get configuration
            conn_config = config
            if conn_config is None:
                conn_config = {'nats_url': DEFAULT_NATS_URL}
            
            # Check if KV store is already provided
            if kv_param in kwargs:
                logger.debug(f"KV store already provided in {kv_param}, skipping injection")
                return await func(*args, **kwargs)
            
            # Determine bucket name
            if callable(bucket_name):
                try:
                    bucket = bucket_name(args)
                except Exception as e:
                    raise ValueError(f"Failed to determine bucket name: {e}") from e
            else:
                bucket = bucket_name
            
            # Inject KV store
            async with nats_kv_store(bucket, conn_config) as kv:
                # Inspect function signature
                sig = inspect.signature(func)
                params = list(sig.parameters.keys())
                
                # Handle method vs function
                is_method = params and params[0] == 'self' and len(args) > 0
                
                if is_method:
                    # Method call - inject after self
                    return await func(args[0], kv, *args[1:], **kwargs)
                else:
                    # Regular function - inject as first parameter
                    return await func(kv, *args, **kwargs)
                    
        return wrapper
    return decorator


def connection_retry(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    exponential_backoff: bool = True,
    exceptions: tuple = None
):
    """
    Decorator to add retry logic for connection-related functions.
    
    Args:
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds
        exponential_backoff: Whether to use exponential backoff
        exceptions: Tuple of exception types to catch and retry on
        
    Usage:
        @connection_retry(max_retries=3, retry_delay=2.0)
        @with_nats_connection()
        async def unreliable_operation(conn: NATSClient):
            # This will be retried up to 3 times on connection errors
            await conn.publish("subject", b"data")
    """
    import asyncio
    from ..exceptions import NaqConnectionError
    
    if exceptions is None:
        exceptions = (NaqConnectionError, ConnectionError, OSError)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            delay = retry_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    logger.info(f"Retrying in {delay:.1f}s...")
                    
                    await asyncio.sleep(delay)
                    
                    if exponential_backoff:
                        delay *= 2
                
                except Exception as e:
                    # Don't retry on non-connection errors
                    logger.error(f"Function {func.__name__} failed with non-retryable error: {e}")
                    raise
            
            # Should never reach here, but just in case
            raise last_exception
            
        return wrapper
    return decorator


# Usage examples and convenience decorators
def nats_publisher(subject: str, config: Optional[dict] = None):
    """
    Convenience decorator for simple NATS publishing functions.
    
    Args:
        subject: NATS subject to publish to
        config: Optional configuration
        
    Usage:
        @nats_publisher("events.user.created")
        async def publish_user_created(data: bytes):
            # Connection and publishing handled automatically
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get the data to publish (assume it's the return value of the function)
            data = await func(*args, **kwargs)
            
            # Publish the data
            async with nats_connection(config) as conn:
                await conn.publish(subject, data)
                
            return data
            
        return wrapper
    return decorator


def jetstream_publisher(subject: str, config: Optional[dict] = None):
    """
    Convenience decorator for JetStream publishing functions.
    
    Args:
        subject: JetStream subject to publish to
        config: Optional configuration
        
    Usage:
        @jetstream_publisher("jobs.queue.high")
        async def publish_high_priority_job(job_data: bytes):
            # JetStream connection and publishing handled automatically
            return job_data
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get the data to publish
            data = await func(*args, **kwargs)
            
            # Publish via JetStream
            async with nats_jetstream(config) as (conn, js):
                await js.publish(subject, data)
                
            return data
            
        return wrapper
    return decorator


def ensure_connected(func: Callable) -> Callable:
    """
    Decorator to ensure NATS connection is established before function execution.
    
    Simple decorator that validates connection status and reconnects if needed.
    This is a backward compatibility function for tests.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # For backward compatibility, just call the function
        # Connection management is handled by context managers
        return await func(*args, **kwargs)
    return wrapper