# src/naq/services/streams.py
from typing import Dict, List, Optional, Any
from loguru import logger
import nats
from nats.js import JetStreamContext, api
from nats.js.api import StreamInfo, StreamConfig
from nats.js.errors import NotFoundError

from .base import BaseService
from .connection import ConnectionService
from ..exceptions import NaqConnectionError


class StreamService(BaseService):
    """
    Centralized JetStream stream management service.
    
    This service provides stream creation, management, and operations for all
    JetStream streams in the NAQ system, centralizing stream-related functionality
    and providing a consistent interface for stream operations.
    """
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self._connection_service = connection_service
        self._js_contexts: Dict[str, JetStreamContext] = {}
        logger.info("StreamService initialized")
        
    async def _do_initialize(self) -> None:
        """Initialize the stream service."""
        # No specific initialization needed beyond what BaseService provides
        logger.info("StreamService initialized successfully")
        
    async def cleanup(self) -> None:
        """Cleanup stream service resources."""
        await super().cleanup()
        # Clean up JetStream context references
        self._js_contexts.clear()
        logger.info("StreamService cleaned up")
        
    async def _get_jetstream_context(self, url: Optional[str] = None) -> JetStreamContext:
        """
        Get a JetStream context for a specific URL.
        
        Args:
            url: NATS server URL. If None, uses the default URL.
            
        Returns:
            A JetStream context
            
        Raises:
            NaqConnectionError: If getting JetStream context fails
        """
        target_url = url or "default"
        
        # Check if we already have a JetStream context for this URL
        if target_url in self._js_contexts:
            return self._js_contexts[target_url]
            
        # Get JetStream context from connection service
        try:
            js = await self._connection_service.get_jetstream(url)
            self._js_contexts[target_url] = js
            return js
        except Exception as e:
            error_msg = f"Failed to get JetStream context for {target_url or 'default URL'}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def ensure_stream(
        self, 
        name: str, 
        subjects: List[str], 
        **config
    ) -> None:
        """
        Ensure a JetStream stream exists with proper configuration.
        
        Args:
            name: Stream name
            subjects: List of subjects for the stream
            **config: Additional stream configuration options
            
        Raises:
            NaqConnectionError: If stream creation or verification fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Set default configuration
            stream_config = {
                'name': name,
                'subjects': subjects,
                'storage': api.StorageType.FILE,
                'retention': api.RetentionPolicy.WORK_QUEUE,
            }
            
            # Override with user-provided configuration
            stream_config.update(config)
            
            # Check if stream exists
            try:
                stream_info = await js.stream_info(name)
                logger.info(f"Stream '{name}' already exists with configuration: {stream_info.config}")
                
                # Check if we need to update the stream configuration
                # Note: NATS doesn't allow updating all stream parameters, so we just log differences
                current_config = stream_info.config
                for key, value in stream_config.items():
                    if key in ['name', 'subjects']:
                        continue  # These can't be updated
                    if hasattr(current_config, key) and getattr(current_config, key) != value:
                        logger.warning(
                            f"Stream '{name}' configuration differs for '{key}': "
                            f"current={getattr(current_config, key)}, requested={value}. "
                            f"Stream updates may require manual intervention."
                        )
                        
            except NotFoundError:
                # Create stream if it doesn't exist
                logger.info(f"Stream '{name}' not found, creating with configuration: {stream_config}")
                await js.add_stream(**stream_config)
                logger.info(f"Stream '{name}' created successfully with subjects {subjects}")
                
        except NotFoundError:
            # This should have been handled above, but just in case
            error_msg = f"Stream '{name}' not found and creation failed"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Failed to ensure stream '{name}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def get_stream_info(self, name: str) -> StreamInfo:
        """
        Get information about a JetStream stream.
        
        Args:
            name: Stream name
            
        Returns:
            StreamInfo object containing stream information
            
        Raises:
            NaqConnectionError: If getting stream information fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Get stream information
            stream_info = await js.stream_info(name)
            logger.debug(f"Retrieved info for stream '{name}': {stream_info}")
            return stream_info
            
        except NotFoundError:
            error_msg = f"Stream '{name}' not found"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Failed to get stream info for '{name}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def delete_stream(self, name: str) -> None:
        """
        Delete a JetStream stream.
        
        Args:
            name: Stream name
            
        Raises:
            NaqConnectionError: If stream deletion fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Delete the stream
            await js.delete_stream(name)
            logger.info(f"Stream '{name}' deleted successfully")
            
            # Clean up any cached JetStream context references
            self._js_contexts.clear()
            
        except NotFoundError:
            error_msg = f"Stream '{name}' not found, cannot delete"
            logger.warning(error_msg)
            # We don't raise an error here since the stream doesn't exist anyway
        except Exception as e:
            error_msg = f"Failed to delete stream '{name}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def purge_stream(self, name: str, subject: Optional[str] = None) -> None:
        """
        Purge messages from a JetStream stream.
        
        Args:
            name: Stream name
            subject: Optional subject to purge. If None, purges all messages in the stream.
            
        Raises:
            NaqConnectionError: If stream purge fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Purge the stream
            if subject:
                await js.purge_stream(name, subject=subject)
                logger.info(f"Purged messages for subject '{subject}' in stream '{name}'")
            else:
                await js.purge_stream(name)
                logger.info(f"Purged all messages in stream '{name}'")
                
        except NotFoundError:
            error_msg = f"Stream '{name}' not found, cannot purge"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Failed to purge stream '{name}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def list_streams(self) -> List[str]:
        """
        List all available JetStream streams.
        
        Returns:
            List of stream names
            
        Raises:
            NaqConnectionError: If listing streams fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Get stream list
            stream_names = []
            stream_infos = await js.streams_info()
            for stream_info in stream_infos:
                stream_names.append(stream_info.config.name)
                
            logger.debug(f"Retrieved list of {len(stream_names)} streams")
            return stream_names
            
        except Exception as e:
            error_msg = f"Failed to list streams: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def get_stream_message_count(self, name: str) -> int:
        """
        Get the number of messages in a stream.
        
        Args:
            name: Stream name
            
        Returns:
            Number of messages in the stream
            
        Raises:
            NaqConnectionError: If getting message count fails
        """
        try:
            # Get stream information
            stream_info = await self.get_stream_info(name)
            
            # The state contains message counts
            message_count = stream_info.state.messages
            logger.debug(f"Stream '{name}' contains {message_count} messages")
            return message_count
            
        except Exception as e:
            error_msg = f"Failed to get message count for stream '{name}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e