from contextlib import asynccontextmanager
from typing import Tuple, Optional
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from ..settings import DEFAULT_NATS_URL
from ..utils import setup_logging


# Define a simple Config class for now - this would be replaced with a proper configuration system
class Config:
    def __init__(self, servers=None, client_name=None, max_reconnect_attempts=None, reconnect_time_wait=None):
        self.nats = NATSConfig(servers, client_name, max_reconnect_attempts, reconnect_time_wait)


class NATSConfig:
    def __init__(self, servers=None, client_name=None, max_reconnect_attempts=None, reconnect_time_wait=None):
        self.servers = servers or [DEFAULT_NATS_URL]
        self.client_name = client_name or "naq-client"
        self.max_reconnect_attempts = max_reconnect_attempts or 5
        self.reconnect_time_wait = reconnect_time_wait or 2.0


def get_config():
    """Get the current configuration."""
    return Config()


@asynccontextmanager
async def nats_connection(config: Optional[Config] = None):
    """Context manager for NATS connections."""
    config = config or get_config()
    
    conn = await nats.connect(
        servers=config.nats.servers,
        name=config.nats.client_name,
        max_reconnect_attempts=config.nats.max_reconnect_attempts,
        reconnect_time_wait=config.nats.reconnect_time_wait,
        # ... other config parameters
    )
    
    try:
        yield conn
    except Exception as e:
        logger.error(f"NATS connection error: {e}")
        raise
    finally:
        await conn.close()


@asynccontextmanager
async def jetstream_context(conn: NATSClient):
    """Context manager for JetStream contexts."""
    try:
        js = conn.jetstream()
        yield js
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise


@asynccontextmanager
async def nats_jetstream(config: Optional[Config] = None):
    """Combined context manager for NATS connection and JetStream."""
    async with nats_connection(config) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js


@asynccontextmanager
async def nats_kv_store(bucket_name: str, config: Optional[Config] = None):
    """Context manager for NATS KeyValue operations.""" 
    async with nats_jetstream(config) as (conn, js):
        try:
            kv = await js.key_value(bucket_name)
            yield kv
        except Exception as e:
            logger.error(f"KV store error for bucket {bucket_name}: {e}")
            raise