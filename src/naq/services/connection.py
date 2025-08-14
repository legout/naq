"""NATS Connection Service for NAQ.

This module provides a centralized service for managing NATS connections,
including connection pooling, lifecycle management, and JetStream context.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import nats
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from ..exceptions import NaqConnectionError
from .base import BaseService

logger = logging.getLogger(__name__)


class ConnectionService(BaseService):
    """Service for managing NATS connections and JetStream contexts."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the connection service.
        
        Args:
            config: Configuration dictionary containing NATS connection parameters
        """
        super().__init__(config)
        self._connections: Dict[str, NATS] = {}
        self._jetstream_contexts: Dict[str, JetStreamContext] = {}
        self._connection_locks: Dict[str, asyncio.Lock] = {}
    
    async def _do_initialize(self) -> None:
        """Initialize the connection service.
        
        This method sets up the connection pool and configuration.
        """
        # Initialize connection locks for thread safety
        self._connection_locks = {}
        logger.debug("ConnectionService initialized")
    
    async def _do_cleanup(self) -> None:
        """Clean up all connections in the pool."""
        for connection_key, connection in self._connections.items():
            try:
                await connection.close()
                logger.debug(f"Closed connection: {connection_key}")
            except Exception as e:
                logger.error(f"Error closing connection {connection_key}: {e}")
        
        self._connections.clear()
        self._jetstream_contexts.clear()
        self._connection_locks.clear()
        logger.debug("ConnectionService cleaned up")
    
    async def get_connection(self, connection_key: str = "default") -> NATS:
        """Get a NATS connection, creating one if it doesn't exist.
        
        Args:
            connection_key: Key to identify the connection configuration
            
        Returns:
            NATS connection instance
            
        Raises:
            NaqConnectionError: If connection fails
        """
        if connection_key not in self._connection_locks:
            self._connection_locks[connection_key] = asyncio.Lock()
        
        async with self._connection_locks[connection_key]:
            if connection_key not in self._connections:
                try:
                    # Get connection configuration
                    connection_config = self.config.get(connection_key, {})
                    if not connection_config:
                        # Use default configuration
                        connection_config = self.config.get("default", {})
                    
                    # Create connection
                    connection = await nats.connect(**connection_config)
                    self._connections[connection_key] = connection
                    logger.debug(f"Created new connection: {connection_key}")
                except Exception as e:
                    raise NaqConnectionError(f"Failed to create NATS connection: {e}") from e
            
            return self._connections[connection_key]
    
    async def get_jetstream(self, connection_key: str = "default") -> JetStreamContext:
        """Get a JetStream context for a connection.
        
        Args:
            connection_key: Key to identify the connection
            
        Returns:
            JetStream context instance
            
        Raises:
            NaqConnectionError: If JetStream context cannot be created
        """
        if connection_key not in self._jetstream_contexts:
            try:
                connection = await self.get_connection(connection_key)
                jetstream = connection.jetstream()
                self._jetstream_contexts[connection_key] = jetstream
                logger.debug(f"Created JetStream context: {connection_key}")
            except Exception as e:
                raise NaqConnectionError(f"Failed to create JetStream context: {e}") from e
        
        return self._jetstream_contexts[connection_key]
    
    @asynccontextmanager
    async def connection_scope(self, connection_key: str = "default"):
        """Async context manager for safely using a NATS connection.
        
        Args:
            connection_key: Key to identify the connection configuration
            
        Yields:
            NATS connection instance
            
        Example:
            async with connection_service.connection_scope() as nc:
                # Use the connection
                await nc.publish("subject", b"data")
        """
        connection = await self.get_connection(connection_key)
        try:
            yield connection
        except Exception as e:
            logger.error(f"Error in connection scope {connection_key}: {e}")
            raise