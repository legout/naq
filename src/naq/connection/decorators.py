from functools import wraps
from typing import Callable, Optional
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from .context_managers import get_config, nats_connection, nats_jetstream


def with_nats_connection(config_key: Optional[str] = None):
    """Decorator to inject NATS connection into function."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                config = get_config()
                async with nats_connection(config) as conn:
                    return await func(conn, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in with_nats_connection decorator: {e}")
                raise
        return wrapper
    return decorator


def with_jetstream_context(config_key: Optional[str] = None):
    """Decorator to inject JetStream context into function."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                config = get_config()
                async with nats_jetstream(config) as (conn, js):
                    return await func(js, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in with_jetstream_context decorator: {e}")
                raise
        return wrapper
    return decorator


# Usage examples:
@with_jetstream_context()
async def create_stream(js: JetStreamContext, stream_name: str):
    """Create stream with automatic connection management."""
    # Function automatically gets JetStream context
    await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])


@with_nats_connection()  
async def publish_message(conn: NATSClient, subject: str, message: bytes):
    """Publish message with automatic connection management."""
    await conn.publish(subject, message)