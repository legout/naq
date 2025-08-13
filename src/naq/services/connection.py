# src/naq/services/connection.py
"""
NATS Connection Service for NAQ.

This service centralizes all NATS connection management, eliminating the 72+
instances of repeated connection patterns throughout the codebase.
"""

from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from .base import BaseService
from ..connection import ConnectionManager
from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL


class ConnectionService(BaseService):
    """
    Centralized NATS connection management service.
    
    This service provides connection pooling, lifecycle management, and
    context managers for safe resource handling. It replaces all direct
    usage of get_nats_connection and related functions.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connection service.
        
        Args:
            config: Service configuration including NATS URL and connection options
        """
        super().__init__(config)
        self._manager: Optional[ConnectionManager] = None
        self._default_nats_url = config.get('nats_url', DEFAULT_NATS_URL)

    async def _do_initialize(self) -> None:
        """Initialize the connection manager."""
        self._manager = ConnectionManager()
        logger.debug("Connection service initialized")

    async def _do_cleanup(self) -> None:
        """Clean up all connections."""
        if self._manager:
            await self._manager.close_all()
            self._manager = None

    async def get_connection(
        self, 
        url: Optional[str] = None, 
        *, 
        prefer_thread_local: bool = False
    ) -> NATSClient:
        """
        Get a pooled NATS connection.
        
        Args:
            url: NATS server URL (defaults to configured URL)
            prefer_thread_local: Use thread-local connection for sync helpers
            
        Returns:
            Connected NATS client
            
        Raises:
            NaqConnectionError: If connection fails
            ServiceNotInitializedError: If service not initialized
        """
        if not self._manager:
            from .base import ServiceNotInitializedError
            raise ServiceNotInitializedError("ConnectionService not initialized")
            
        if url is None:
            url = self._default_nats_url
            
        return await self._manager.get_connection(
            url, 
            prefer_thread_local=prefer_thread_local
        )

    async def get_jetstream(
        self, 
        url: Optional[str] = None, 
        *, 
        prefer_thread_local: bool = False
    ) -> JetStreamContext:
        """
        Get a JetStream context.
        
        Args:
            url: NATS server URL (defaults to configured URL)
            prefer_thread_local: Use thread-local context for sync helpers
            
        Returns:
            JetStream context
            
        Raises:
            NaqConnectionError: If getting JetStream context fails
            ServiceNotInitializedError: If service not initialized
        """
        if not self._manager:
            from .base import ServiceNotInitializedError
            raise ServiceNotInitializedError("ConnectionService not initialized")
            
        if url is None:
            url = self._default_nats_url
            
        return await self._manager.get_jetstream(
            url,
            prefer_thread_local=prefer_thread_local
        )

    async def close_connection(
        self, 
        url: Optional[str] = None, 
        *, 
        thread_local: bool = False
    ) -> None:
        """
        Close a specific connection.
        
        Args:
            url: NATS server URL to close (defaults to configured URL)
            thread_local: Close thread-local connection
        """
        if not self._manager:
            return
            
        if url is None:
            url = self._default_nats_url
            
        await self._manager.close_connection(url, thread_local=thread_local)

    @asynccontextmanager
    async def connection_scope(
        self, 
        url: Optional[str] = None, 
        *, 
        prefer_thread_local: bool = False
    ):
        """
        Context manager for connection operations.
        
        Args:
            url: NATS server URL (defaults to configured URL)
            prefer_thread_local: Use thread-local connection
            
        Yields:
            Connected NATS client
            
        Usage:
            async with conn_service.connection_scope() as nc:
                # Use connection
                await nc.publish("subject", b"data")
        """
        connection = await self.get_connection(url, prefer_thread_local=prefer_thread_local)
        try:
            yield connection
        finally:
            # Connection is returned to pool automatically
            # No explicit cleanup needed due to pooling
            pass

    @asynccontextmanager
    async def jetstream_scope(
        self, 
        url: Optional[str] = None, 
        *, 
        prefer_thread_local: bool = False
    ):
        """
        Context manager for JetStream operations.
        
        Args:
            url: NATS server URL (defaults to configured URL)  
            prefer_thread_local: Use thread-local context
            
        Yields:
            JetStream context
            
        Usage:
            async with conn_service.jetstream_scope() as js:
                # Use JetStream
                await js.publish("subject", b"data")
        """
        jetstream = await self.get_jetstream(url, prefer_thread_local=prefer_thread_local)
        try:
            yield jetstream
        finally:
            # JetStream context cleanup handled by connection pooling
            pass

    @asynccontextmanager
    async def connection_and_jetstream_scope(
        self, 
        url: Optional[str] = None, 
        *, 
        prefer_thread_local: bool = False
    ):
        """
        Context manager for both connection and JetStream operations.
        
        Args:
            url: NATS server URL (defaults to configured URL)
            prefer_thread_local: Use thread-local resources
            
        Yields:
            Tuple of (connection, jetstream_context)
            
        Usage:
            async with conn_service.connection_and_jetstream_scope() as (nc, js):
                # Use both connection and JetStream
                await nc.publish("subject", b"data")
                await js.add_stream(name="test_stream")
        """
        connection = await self.get_connection(url, prefer_thread_local=prefer_thread_local)
        jetstream = await self.get_jetstream(url, prefer_thread_local=prefer_thread_local)
        try:
            yield connection, jetstream
        finally:
            # Resources returned to pool automatically
            pass

    @property
    def default_nats_url(self) -> str:
        """Get the default NATS URL for this service."""
        return self._default_nats_url

    def update_default_nats_url(self, url: str) -> None:
        """
        Update the default NATS URL.
        
        Args:
            url: New default NATS URL
        """
        self._default_nats_url = url
        logger.debug(f"Default NATS URL updated to: {url}")


# Compatibility functions for gradual migration
async def get_connection_service(config: Optional[Dict[str, Any]] = None) -> ConnectionService:
    """
    Get a connection service instance.
    
    This is a convenience function for creating a connection service
    without using the full service manager.
    
    Args:
        config: Optional configuration override
        
    Returns:
        Initialized connection service
    """
    if config is None:
        config = {'nats_url': DEFAULT_NATS_URL}
        
    service = ConnectionService(config)
    await service.initialize()
    return service