# src/naq/services/streams.py
"""
JetStream Stream Service for NAQ.

This service centralizes all stream creation, management, and operations,
replacing the 9+ instances of ensure_stream and related stream operations.
"""

from typing import Any, Dict, List, Optional

import nats
from loguru import logger
from nats.js.api import RetentionPolicy, StorageType, StreamConfig, StreamInfo

from .base import BaseService
from .connection import ConnectionService
from ..exceptions import NaqConnectionError


class StreamService(BaseService):
    """
    JetStream stream management service.
    
    This service provides centralized stream creation, configuration,
    management, and operations for all NAQ components.
    """

    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        """
        Initialize the stream service.
        
        Args:
            config: Service configuration
            connection_service: Connection service for NATS operations
        """
        super().__init__(config)
        self.connection_service = connection_service
        self._stream_cache: Dict[str, StreamInfo] = {}

    async def _do_initialize(self) -> None:
        """Initialize the stream service."""
        logger.debug("Stream service initialized")

    async def _do_cleanup(self) -> None:
        """Clean up stream service resources."""
        self._stream_cache.clear()

    async def ensure_stream(
        self, 
        name: str, 
        subjects: List[str], 
        *,
        storage: StorageType = StorageType.FILE,
        retention: RetentionPolicy = RetentionPolicy.WORK_QUEUE,
        max_age: Optional[int] = None,
        max_bytes: Optional[int] = None,
        max_msgs: Optional[int] = None,
        replicas: int = 1,
        **kwargs
    ) -> StreamInfo:
        """
        Ensure a stream exists with the specified configuration.
        
        Args:
            name: Stream name
            subjects: List of subjects for the stream
            storage: Storage type (FILE or MEMORY)
            retention: Retention policy
            max_age: Maximum age in seconds
            max_bytes: Maximum bytes
            max_msgs: Maximum messages
            replicas: Number of replicas
            **kwargs: Additional stream configuration options
            
        Returns:
            Stream information
            
        Raises:
            NaqConnectionError: If stream operations fail
        """
        async with self.connection_service.jetstream_scope() as js:
            try:
                # Check if stream already exists
                stream_info = await js.stream_info(name)
                logger.debug(f"Stream '{name}' already exists")
                
                # Cache the stream info
                self._stream_cache[name] = stream_info
                return stream_info
                
            except nats.js.errors.NotFoundError:
                # Stream doesn't exist, create it
                logger.info(f"Creating stream '{name}' with subjects {subjects}")
                
                # Build stream configuration
                config = StreamConfig(
                    name=name,
                    subjects=subjects,
                    storage=storage,
                    retention=retention,
                    replicas=replicas,
                )
                
                # Set optional parameters if provided
                if max_age is not None:
                    config.max_age = max_age
                if max_bytes is not None:
                    config.max_bytes = max_bytes
                if max_msgs is not None:
                    config.max_msgs = max_msgs
                
                # Apply any additional configuration
                for key, value in kwargs.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
                
                try:
                    stream_info = await js.add_stream(config)
                    logger.info(f"Stream '{name}' created successfully")
                    
                    # Cache the stream info
                    self._stream_cache[name] = stream_info
                    return stream_info
                    
                except Exception as e:
                    raise NaqConnectionError(
                        f"Failed to create stream '{name}': {e}"
                    ) from e
                    
            except Exception as e:
                raise NaqConnectionError(
                    f"Failed to ensure stream '{name}': {e}"
                ) from e

    async def get_stream_info(self, name: str, *, refresh_cache: bool = False) -> Optional[StreamInfo]:
        """
        Get stream information.
        
        Args:
            name: Stream name
            refresh_cache: Force refresh from server instead of using cache
            
        Returns:
            Stream information or None if not found
        """
        if not refresh_cache and name in self._stream_cache:
            return self._stream_cache[name]
            
        async with self.connection_service.jetstream_scope() as js:
            try:
                stream_info = await js.stream_info(name)
                self._stream_cache[name] = stream_info
                return stream_info
            except nats.js.errors.NotFoundError:
                # Remove from cache if it was there
                self._stream_cache.pop(name, None)
                return None
            except Exception as e:
                logger.error(f"Error getting stream info for '{name}': {e}")
                return None

    async def delete_stream(self, name: str) -> bool:
        """
        Delete a stream.
        
        Args:
            name: Stream name to delete
            
        Returns:
            True if stream was deleted, False if not found
            
        Raises:
            NaqConnectionError: If deletion fails
        """
        async with self.connection_service.jetstream_scope() as js:
            try:
                await js.delete_stream(name)
                logger.info(f"Stream '{name}' deleted")
                
                # Remove from cache
                self._stream_cache.pop(name, None)
                return True
                
            except nats.js.errors.NotFoundError:
                logger.warning(f"Stream '{name}' not found for deletion")
                return False
                
            except Exception as e:
                raise NaqConnectionError(
                    f"Failed to delete stream '{name}': {e}"
                ) from e

    async def purge_stream(
        self, 
        name: str, 
        *, 
        subject: Optional[str] = None,
        sequence: Optional[int] = None,
        keep: Optional[int] = None
    ) -> int:
        """
        Purge messages from a stream.
        
        Args:
            name: Stream name
            subject: Optional subject filter
            sequence: Purge up to sequence number
            keep: Number of messages to keep
            
        Returns:
            Number of messages purged
            
        Raises:
            NaqConnectionError: If purge fails
        """
        async with self.connection_service.jetstream_scope() as js:
            try:
                # Build purge request
                purge_req = {}
                if subject:
                    purge_req['filter'] = subject
                if sequence:
                    purge_req['seq'] = sequence
                if keep:
                    purge_req['keep'] = keep
                
                if purge_req:
                    result = await js.purge_stream(name, **purge_req)
                else:
                    result = await js.purge_stream(name)
                
                purged_count = getattr(result, 'purged', 0)
                logger.info(f"Purged {purged_count} messages from stream '{name}'")
                return purged_count
                
            except nats.js.errors.NotFoundError:
                logger.warning(f"Stream '{name}' not found for purging")
                return 0
                
            except Exception as e:
                raise NaqConnectionError(
                    f"Failed to purge stream '{name}': {e}"
                ) from e

    async def list_streams(self) -> List[str]:
        """
        List all streams.
        
        Returns:
            List of stream names
        """
        async with self.connection_service.jetstream_scope() as js:
            try:
                streams = await js.streams_info()
                return [stream.config.name for stream in streams]
            except Exception as e:
                logger.error(f"Error listing streams: {e}")
                return []

    async def stream_exists(self, name: str) -> bool:
        """
        Check if a stream exists.
        
        Args:
            name: Stream name
            
        Returns:
            True if stream exists, False otherwise
        """
        info = await self.get_stream_info(name)
        return info is not None

    async def get_stream_message_count(self, name: str) -> int:
        """
        Get the number of messages in a stream.
        
        Args:
            name: Stream name
            
        Returns:
            Number of messages in the stream
        """
        info = await self.get_stream_info(name, refresh_cache=True)
        if info and info.state:
            return info.state.messages
        return 0

    async def get_stream_subjects(self, name: str) -> List[str]:
        """
        Get the subjects for a stream.
        
        Args:
            name: Stream name
            
        Returns:
            List of subjects for the stream
        """
        info = await self.get_stream_info(name)
        if info and info.config:
            return info.config.subjects or []
        return []

    def clear_cache(self) -> None:
        """Clear the stream info cache."""
        self._stream_cache.clear()
        logger.debug("Stream cache cleared")