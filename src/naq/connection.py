# src/naq/connection.py
from typing import Dict, Optional
from loguru import logger
import asyncio
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from .exceptions import ConnectionError as NaqConnectionError
from .settings import DEFAULT_NATS_URL
from .utils import setup_logging

setup_logging()  # Setup logging for the module

class ConnectionManager:
    """
    Manages NATS connections with a connection pool approach.
    This allows multiple connections to different NATS servers
    or with different configurations.
    """
    
    def __init__(self):
        self._connections: Dict[str, NATSClient] = {}
        self._js_contexts: Dict[str, JetStreamContext] = {}
        self._lock = asyncio.Lock()
    
    async def get_connection(self, url: str = DEFAULT_NATS_URL) -> NATSClient:
        """
        Gets a NATS client connection from the pool or creates a new one.
        
        Args:
            url: NATS server URL
            
        Returns:
            A connected NATS client
            
        Raises:
            NaqConnectionError: If connection fails
        """
        async with self._lock:
            # Check if we have a valid connection for this URL
            if url in self._connections and self._connections[url].is_connected:
                return self._connections[url]
            
            # Create a new connection
            try:
                nc = await nats.connect(url, name="naq_client")
                logger.info(f"NATS connection established to {url}")
                self._connections[url] = nc
                return nc
            except Exception as e:
                # Clean up any partial connection
                if url in self._connections:
                    del self._connections[url]
                raise NaqConnectionError(f"Failed to connect to NATS at {url}: {e}") from e
    
    async def get_jetstream(self, url: str = DEFAULT_NATS_URL) -> JetStreamContext:
        """
        Gets a JetStream context for a specific connection.
        
        Args:
            url: NATS server URL
            
        Returns:
            A JetStream context
            
        Raises:
            NaqConnectionError: If getting JetStream context fails
        """
        async with self._lock:
            if url in self._js_contexts:
                return self._js_contexts[url]
            
            # Get the connection first
            nc = await self.get_connection(url)
            
            # Create JetStream context
            try:
                js = nc.jetstream()
                self._js_contexts[url] = js
                logger.info(f"JetStream context obtained for {url}")
                return js
            except Exception as e:
                raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e
    
    async def close_connection(self, url: str = DEFAULT_NATS_URL) -> None:
        """
        Closes a specific NATS connection.
        
        Args:
            url: NATS server URL to close
        """
        async with self._lock:
            if url in self._connections and self._connections[url].is_connected:
                await self._connections[url].close()
                logger.info(f"NATS connection to {url} closed")
                
                # Clean up our references
                del self._connections[url]
                if url in self._js_contexts:
                    del self._js_contexts[url]
    
    async def close_all(self) -> None:
        """Closes all NATS connections in the pool."""
        async with self._lock:
            for url, nc in list(self._connections.items()):
                if nc.is_connected:
                    await nc.close()
                    logger.info(f"NATS connection to {url} closed")
            
            # Clear all references
            self._connections.clear()
            self._js_contexts.clear()

# Create a singleton instance
_manager = ConnectionManager()

# Provide compatibility with existing code
async def get_nats_connection(url: str = DEFAULT_NATS_URL) -> NATSClient:
    """Gets a NATS client connection, reusing if possible."""
    if url is None:
        url = DEFAULT_NATS_URL
    return await _manager.get_connection(url)

async def get_jetstream_context(nc: Optional[NATSClient] = None) -> JetStreamContext:
    """Gets a JetStream context from a NATS connection."""
    if nc is not None:
        # If a connection is provided directly, use it
        try:
            return nc.jetstream()
        except Exception as e:
            raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e
    
    # Otherwise use the connection manager
    return await _manager.get_jetstream()

async def close_nats_connection(url: str = DEFAULT_NATS_URL):
    """Closes a specific NATS connection."""
    await _manager.close_connection(url)

async def close_all_connections():
    """Closes all NATS connections managed by the connection pool."""
    await _manager.close_all()

async def ensure_stream(
    js: Optional[JetStreamContext] = None,
    stream_name: str = "naq_jobs",  # Default stream name
    subjects: Optional[list[str]] = None,
) -> None:
    """Ensures a JetStream stream exists."""
    if js is None:
        js = await get_jetstream_context()

    if subjects is None:
        subjects = [f"{stream_name}.*"]  # Default subject pattern

    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        logger.info(f"Stream '{stream_name}' already exists.")
    except nats.js.errors.NotFoundError:
        # Create stream if it doesn't exist
        logger.info(f"Stream '{stream_name}' not found, creating...")
        await js.add_stream(
            name=stream_name,
            subjects=subjects,
            # Configure retention, storage, etc. as needed
            # Default is LimitsPolicy (limits based) with MemoryStorage
            # For persistence like RQ, FileStorage is needed.
            storage=nats.js.api.StorageType.FILE,  # Use File storage
            retention=nats.js.api.RetentionPolicy.WORK_QUEUE,  # Consume then delete
        )
        logger.info(f"Stream '{stream_name}' created with subjects {subjects}.")
    except Exception as e:
        raise NaqConnectionError(f"Failed to ensure stream '{stream_name}': {e}") from e
