"""JetStream Stream Service for NAQ.

This module provides a centralized service for managing JetStream streams,
including creation, configuration, and lifecycle management.
"""

import logging
from typing import Any, Dict, Optional

from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError

from ..exceptions import NaqException
from .base import BaseService
from .connection import ConnectionService

logger = logging.getLogger(__name__)


class StreamService(BaseService):
    """Service for managing JetStream streams."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the stream service.
        
        Args:
            config: Configuration dictionary for stream management
        """
        super().__init__(config)
        self._connection_service: Optional[ConnectionService] = None
        self._streams: Dict[str, Any] = {}  # Store stream references
    
    async def _do_initialize(self) -> None:
        """Initialize the stream service.
        
        This method sets up the connection service dependency.
        """
        # The connection service will be injected during service registration
        if "connection_service" in self.config:
            self._connection_service = self.config["connection_service"]
        
        logger.debug("StreamService initialized")
    
    async def _do_cleanup(self) -> None:
        """Clean up stream service resources."""
        self._streams.clear()
        logger.debug("StreamService cleaned up")
    
    async def ensure_stream(
        self, 
        stream_name: str, 
        stream_config: StreamConfig,
        connection_key: str = "default"
    ) -> Any:
        """Ensure a JetStream stream exists with the specified configuration.
        
        Args:
            stream_name: Name of the stream to ensure
            stream_config: Configuration for the stream
            connection_key: Key to identify the connection configuration
            
        Returns:
            Stream info object
            
        Raises:
            NaqException: If stream cannot be created or accessed
        """
        try:
            if self._connection_service is None:
                raise NaqException("Connection service not available")
            
            # Get JetStream context
            jetstream = await self._connection_service.get_jetstream(connection_key)
            
            # Try to get existing stream info
            try:
                stream_info = await jetstream.stream_info(stream_name)
                logger.debug(f"Stream {stream_name} already exists")
                self._streams[stream_name] = stream_info
                return stream_info
            except NotFoundError:
                # Stream doesn't exist, create it
                stream_info = await jetstream.add_stream(stream_config)
                self._streams[stream_name] = stream_info
                logger.debug(f"Created stream {stream_name}")
                return stream_info
        except Exception as e:
            raise NaqException(f"Failed to ensure stream {stream_name}: {e}") from e
    
    async def get_stream_info(self, stream_name: str, connection_key: str = "default") -> Any:
        """Get information about a JetStream stream.
        
        Args:
            stream_name: Name of the stream to get info for
            connection_key: Key to identify the connection configuration
            
        Returns:
            Stream info object
            
        Raises:
            NaqException: If stream info cannot be retrieved
        """
        try:
            if self._connection_service is None:
                raise NaqException("Connection service not available")
            
            # Check if we have cached stream info
            if stream_name in self._streams:
                return self._streams[stream_name]
            
            # Get JetStream context and stream info
            jetstream = await self._connection_service.get_jetstream(connection_key)
            stream_info = await jetstream.stream_info(stream_name)
            self._streams[stream_name] = stream_info
            return stream_info
        except Exception as e:
            raise NaqException(f"Failed to get stream info for {stream_name}: {e}") from e
    
    async def delete_stream(self, stream_name: str, connection_key: str = "default") -> bool:
        """Delete a JetStream stream.
        
        Args:
            stream_name: Name of the stream to delete
            connection_key: Key to identify the connection configuration
            
        Returns:
            True if stream was deleted, False if it didn't exist
            
        Raises:
            NaqException: If stream deletion fails
        """
        try:
            if self._connection_service is None:
                raise NaqException("Connection service not available")
            
            # Get JetStream context
            jetstream = await self._connection_service.get_jetstream(connection_key)
            
            # Delete the stream
            await jetstream.delete_stream(stream_name)
            
            # Remove from cache
            if stream_name in self._streams:
                del self._streams[stream_name]
            
            logger.debug(f"Deleted stream {stream_name}")
            return True
        except NotFoundError:
            logger.debug(f"Stream {stream_name} not found for deletion")
            return False
        except Exception as e:
            raise NaqException(f"Failed to delete stream {stream_name}: {e}") from e
    
    async def purge_stream(self, stream_name: str, connection_key: str = "default") -> bool:
        """Purge all messages from a JetStream stream.
        
        Args:
            stream_name: Name of the stream to purge
            connection_key: Key to identify the connection configuration
            
        Returns:
            True if stream was purged
            
        Raises:
            NaqException: If stream purge fails
        """
        try:
            if self._connection_service is None:
                raise NaqException("Connection service not available")
            
            # Get JetStream context
            jetstream = await self._connection_service.get_jetstream(connection_key)
            
            # Purge the stream
            await jetstream.purge_stream(stream_name)
            
            logger.debug(f"Purged stream {stream_name}")
            return True
        except Exception as e:
            raise NaqException(f"Failed to purge stream {stream_name}: {e}") from e