# src/naq/services/connection.py
from typing import Dict, Optional, Any, AsyncGenerator
from contextlib import asynccontextmanager
from loguru import logger
import asyncio
import threading
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from .base import BaseService
from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL
from ..connection import get_nats_connection, get_jetstream_context, close_nats_connection, close_all_connections


class ConnectionService(BaseService):
    """
    Centralized NATS connection management service.
    
    This service provides connection pooling, reuse, and lifecycle management
    for all NATS connections in the NAQ system, eliminating the need for
    duplicated connection patterns throughout the codebase.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._connections: Dict[str, NATSClient] = {}
        self._js_contexts: Dict[str, JetStreamContext] = {}
        self._lock = asyncio.Lock()
        self._tls = threading.local()
        self._connection_configs: Dict[str, Dict[str, Any]] = {}
        self._reconnect_attempts: Dict[str, int] = {}
        self._max_reconnect_attempts = 5
        self._reconnect_delay = 1.0
        
    async def _do_initialize(self) -> None:
        """Initialize the connection service."""
        # Extract connection configuration from the config
        nats_config = self.config.get('nats', {})
        
        # Set default connection parameters
        self._default_url = nats_config.get('url', DEFAULT_NATS_URL)
        self._max_reconnect_attempts = nats_config.get('max_reconnect_attempts', 5)
        self._reconnect_delay = nats_config.get('reconnect_delay', 1.0)
        self._connection_timeout = nats_config.get('connection_timeout', 10.0)
        self._ping_interval = nats_config.get('ping_interval', 60)
        self._max_outstanding_pings = nats_config.get('max_outstanding_pings', 2)
        
        # Initialize connection configurations
        self._connection_configs[self._default_url] = {
            'url': self._default_url,
            'name': 'naq_connection_service',
            'reconnect_time_wait': self._reconnect_delay,
            'max_reconnect_attempts': self._max_reconnect_attempts,
            'timeout': self._connection_timeout,
            'ping_interval': self._ping_interval,
            'max_outstanding_pings': self._max_outstanding_pings,
        }
        
        logger.info("ConnectionService initialized with default configuration")
        
    async def cleanup(self) -> None:
        """Cleanup all connections and resources."""
        await super().cleanup()
        await self.close_all_connections()
        logger.info("ConnectionService cleaned up")
        
    async def get_connection(self, url: Optional[str] = None) -> NATSClient:
        """
        Get a pooled NATS connection.
        
        Args:
            url: NATS server URL. If None, uses the default URL.
            
        Returns:
            A connected NATS client
            
        Raises:
            NaqConnectionError: If connection fails after retries
        """
        target_url = url if url is not None else self._default_url
            
        # Check if we already have a connection
        async with self._lock:
            if target_url in self._connections and self._connections[target_url].is_connected:
                logger.debug(f"Reusing existing connection to {target_url}")
                return self._connections[target_url]
                
        # Create a new connection with retry logic
        return await self._create_connection_with_retry(target_url)
        
    async def _create_connection_with_retry(self, url: str) -> NATSClient:
        """Create a new connection with retry logic."""
        attempt = 0
        last_error = None
        
        while attempt < self._max_reconnect_attempts:
            try:
                attempt += 1
                logger.info(f"Attempting to connect to NATS at {url} (attempt {attempt}/{self._max_reconnect_attempts})")
                
                # Get connection configuration
                config = self._connection_configs.get(url, self._connection_configs[self._default_url])
                config['url'] = url  # Ensure URL is set correctly
                
                # Create the connection
                nc = await nats.connect(**config)
                
                async with self._lock:
                    self._connections[url] = nc
                    self._reconnect_attempts[url] = 0  # Reset reconnect counter
                    
                logger.info(f"Successfully connected to NATS at {url}")
                return nc
                
            except Exception as e:
                last_error = e
                logger.warning(f"Connection attempt {attempt} failed for {url}: {e}")
                
                if attempt < self._max_reconnect_attempts:
                    # Exponential backoff
                    delay = self._reconnect_delay * (2 ** (attempt - 1))
                    logger.info(f"Waiting {delay}s before retry...")
                    await asyncio.sleep(delay)
                else:
                    break
                    
        # All attempts failed
        error_msg = f"Failed to connect to NATS at {url} after {self._max_reconnect_attempts} attempts"
        if last_error:
            error_msg += f": {last_error}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from last_error
        
    async def get_jetstream(self, url: Optional[str] = None) -> JetStreamContext:
        """
        Get a JetStream context for a specific connection.
        
        Args:
            url: NATS server URL. If None, uses the default URL.
            
        Returns:
            A JetStream context
            
        Raises:
            NaqConnectionError: If getting JetStream context fails
        """
        target_url = url if url is not None else self._default_url
            
        # Check if we already have a JetStream context
        async with self._lock:
            if target_url in self._js_contexts:
                return self._js_contexts[target_url]
                
        # Get the connection first
        nc = await self.get_connection(target_url)
        
        # Create JetStream context
        try:
            js = nc.jetstream()
            
            async with self._lock:
                self._js_contexts[target_url] = js
                
            logger.info(f"JetStream context obtained for {target_url}")
            return js
            
        except Exception as e:
            error_msg = f"Failed to get JetStream context for {target_url}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    @asynccontextmanager
    async def connection_scope(self, url: Optional[str] = None) -> AsyncGenerator[NATSClient, None]:
        """
        Context manager for connection operations.
        
        Provides a safe way to use connections with automatic cleanup
        and error handling.
        
        Args:
            url: NATS server URL. If None, uses the default URL.
            
        Yields:
            A NATS client connection
            
        Example:
            async with connection_service.connection_scope() as nc:
                # Use the connection
                await nc.publish("subject", b"data")
        """
        target_url = url if url is not None else self._default_url
            
        nc = None
        try:
            nc = await self.get_connection(target_url)
            yield nc
        except Exception as e:
            logger.error(f"Error in connection scope for {target_url}: {e}")
            raise
        finally:
            # Note: We don't close the connection here as it's managed by the pool
            # The connection will be closed when the service is cleaned up
            pass
            
    async def close_connection(self, url: Optional[str] = None) -> None:
        """
        Close a specific NATS connection.
        
        Args:
            url: NATS server URL to close. If None, uses the default URL.
        """
        target_url = url if url is not None else self._default_url
            
        async with self._lock:
            if target_url in self._connections:
                nc = self._connections[target_url]
                if nc.is_connected:
                    try:
                        await nc.close()
                        logger.info(f"NATS connection to {target_url} closed")
                    except Exception as e:
                        logger.error(f"Error closing connection to {target_url}: {e}")
                        
                # Clean up references
                del self._connections[target_url]
                self._js_contexts.pop(target_url, None)
                self._reconnect_attempts.pop(target_url, None)
                
    async def close_all_connections(self) -> None:
        """Close all NATS connections managed by the service."""
        async with self._lock:
            for url, nc in list(self._connections.items()):
                if nc.is_connected:
                    try:
                        await nc.close()
                        logger.info(f"NATS connection to {url} closed")
                    except Exception as e:
                        logger.error(f"Error closing connection to {url}: {e}")
                        
            # Clean up all references
            self._connections.clear()
            self._js_contexts.clear()
            self._reconnect_attempts.clear()
            
    async def add_connection_config(self, url: str, config: Dict[str, Any]) -> None:
        """
        Add or update connection configuration for a specific URL.
        
        Args:
            url: NATS server URL
            config: Connection configuration parameters
        """
        # Merge with default configuration
        default_config = self._connection_configs.get(self._default_url, {}).copy()
        default_config.update(config)
        default_config['url'] = url
        
        async with self._lock:
            self._connection_configs[url] = default_config
            
        logger.info(f"Connection configuration updated for {url}")
        
    async def check_connection_health(self, url: Optional[str] = None) -> bool:
        """
        Check if a connection is healthy.
        
        Args:
            url: NATS server URL to check. If None, uses the default URL.
            
        Returns:
            True if the connection is healthy, False otherwise
        """
        target_url = url if url is not None else self._default_url
            
        async with self._lock:
            if target_url not in self._connections:
                return False
                
            nc = self._connections[target_url]
            if not nc.is_connected:
                return False
                
        try:
            # Try to ping the server - use a simple request instead
            await nc.request("$SYS.REQ.SERVER.PING", b"", timeout=1.0)
            return True
        except Exception as e:
            logger.warning(f"Connection health check failed for {target_url}: {e}")
            return False
            
    async def reconnect(self, url: Optional[str] = None) -> NATSClient:
        """
        Force reconnection to a NATS server.
        
        Args:
            url: NATS server URL to reconnect to. If None, uses the default URL.
            
        Returns:
            A new NATS client connection
            
        Raises:
            NaqConnectionError: If reconnection fails
        """
        target_url = url if url is not None else self._default_url
            
        # Close existing connection if any
        await self.close_connection(target_url)
        
        # Create new connection
        return await self._create_connection_with_retry(target_url)
        
    def get_connection_status(self, url: Optional[str] = None) -> Dict[str, Any]:
        """
        Get status information about connections.
        
        Args:
            url: NATS server URL to get status for. If None, returns status for all connections.
            
        Returns:
            Dictionary with connection status information
        """
        if url is None:
            # Return status for all connections
            status = {}
            for conn_url, nc in self._connections.items():
                status[conn_url] = {
                    'connected': nc.is_connected,
                    'reconnect_attempts': self._reconnect_attempts.get(conn_url, 0),
                    'has_jetstream': conn_url in self._js_contexts
                }
            return status
        else:
            # Return status for specific connection
            if url in self._connections:
                nc = self._connections[url]
                return {
                    'connected': nc.is_connected,
                    'reconnect_attempts': self._reconnect_attempts.get(url, 0),
                    'has_jetstream': url in self._js_contexts
                }
            else:
                return {
                    'connected': False,
                    'reconnect_attempts': 0,
                    'has_jetstream': False,
                    'error': 'No connection found'
                }