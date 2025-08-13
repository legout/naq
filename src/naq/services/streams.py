"""
JetStream Stream Service

This module provides a centralized service for JetStream stream creation, management,
and operations, ensuring consistent configuration and lifecycle management for streams.
"""

from typing import Any, Optional, List

import msgspec
import nats
from nats.js import JetStreamContext
from nats.js.api import StreamInfo
from .base import (
    BaseService,
    ServiceConfig,
    ServiceInitializationError,
    ServiceRuntimeError,
)
from .connection import ConnectionService


class StreamServiceConfig(msgspec.Struct):
    """
    Configuration for the StreamService.

    Attributes:
        default_storage: Default storage type for streams (file or memory)
        default_retention: Default retention policy for streams
        default_replicas: Default number of replicas for stream data
        max_age: Maximum age for messages in streams
        max_msgs: Maximum number of messages in streams
        max_bytes: Maximum size of messages in streams
        auto_create_streams: Whether to automatically create streams when accessed
    """

    default_storage: str = "file"
    default_retention: str = "work_queue"
    default_replicas: int = 1
    max_age: Optional[str] = None
    max_msgs: Optional[int] = None
    max_bytes: Optional[int] = None
    auto_create_streams: bool = True


class StreamService(BaseService):
    """
    Centralized JetStream stream management service.

    This service provides consistent stream creation, management, and operations
    for JetStream streams, leveraging the ConnectionService for NATS connectivity.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        connection_service: Optional[ConnectionService] = None,
    ) -> None:
        """
        Initialize the stream service.

        Args:
            config: Optional configuration for the service.
            connection_service: Optional ConnectionService instance for NATS
                              connectivity. If not provided, one will be created.
        """
        super().__init__(config)
        self._stream_config = self._extract_stream_config()
        self._connection_service = connection_service or ConnectionService(config)
        self._jetstream_context: Optional[JetStreamContext] = None

    def _extract_stream_config(self) -> StreamServiceConfig:
        """
        Extract stream-specific configuration from the service config.

        Returns:
            StreamServiceConfig instance with stream parameters.
        """
        # Start with default config
        stream_config = StreamServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "default_storage" in custom_settings:
                stream_config.default_storage = custom_settings["default_storage"]

            if "default_retention" in custom_settings:
                stream_config.default_retention = custom_settings["default_retention"]

            if "default_replicas" in custom_settings:
                stream_config.default_replicas = custom_settings["default_replicas"]

            if "max_age" in custom_settings:
                stream_config.max_age = custom_settings["max_age"]

            if "max_msgs" in custom_settings:
                stream_config.max_msgs = custom_settings["max_msgs"]

            if "max_bytes" in custom_settings:
                stream_config.max_bytes = custom_settings["max_bytes"]

            if "auto_create_streams" in custom_settings:
                stream_config.auto_create_streams = custom_settings[
                    "auto_create_streams"
                ]

        return stream_config

    async def _do_initialize(self) -> None:
        """
        Initialize the stream service.

        This method sets up the connection service and JetStream context.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing StreamService")

            # Initialize connection service if not already initialized
            if not self._connection_service.is_initialized:
                await self._connection_service.initialize()

            # Get JetStream context
            self._jetstream_context = await self._connection_service.get_jetstream()

            self._logger.info("StreamService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize StreamService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up stream service resources.

        This method cleans up the connection service if it was created by this service.
        """
        try:
            self._logger.info("Cleaning up StreamService")

            # Clear JetStream context reference
            self._jetstream_context = None

            # Note: We don't cleanup the connection service here as it might
            # be shared with other services. The ServiceManager will handle
            # cleanup of all services in the correct order.

            self._logger.info("StreamService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup StreamService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def _get_jetstream(self) -> JetStreamContext:
        """
        Get the JetStream context, ensuring it's available.

        Returns:
            The JetStream context.

        Raises:
            ServiceRuntimeError: If JetStream context is not available.
        """
        if self._jetstream_context is None:
            # Try to get it from connection service
            try:
                self._jetstream_context = await self._connection_service.get_jetstream()
            except Exception as e:
                error_msg = "JetStream context not available"
                self._logger.error(error_msg)
                raise ServiceRuntimeError(error_msg) from e

        return self._jetstream_context

    def _get_storage_type(self, storage: Optional[str] = None) -> Any:
        """
        Get the NATS storage type from string representation.

        Args:
            storage: Storage type string ('file' or 'memory')

        Returns:
            NATS StorageType enum value.

        Raises:
            ServiceRuntimeError: If storage type is invalid.
        """
        if storage is None:
            storage = self._stream_config.default_storage

        storage = storage.lower()
        if storage == "file":
            return nats.js.api.StorageType.FILE
        elif storage == "memory":
            return nats.js.api.StorageType.MEMORY
        else:
            raise ServiceRuntimeError(f"Invalid storage type: {storage}")

    def _get_retention_policy(self, retention: Optional[str] = None) -> Any:
        """
        Get the NATS retention policy from string representation.

        Args:
            retention: Retention policy string ('work_queue', 'limits', or 'interest')

        Returns:
            NATS RetentionPolicy enum value.

        Raises:
            ServiceRuntimeError: If retention policy is invalid.
        """
        if retention is None:
            retention = self._stream_config.default_retention

        retention = retention.lower()
        if retention == "work_queue":
            return nats.js.api.RetentionPolicy.WORK_QUEUE
        elif retention == "limits":
            return nats.js.api.RetentionPolicy.LIMITS
        elif retention == "interest":
            return nats.js.api.RetentionPolicy.INTEREST
        else:
            raise ServiceRuntimeError(f"Invalid retention policy: {retention}")

    async def ensure_stream(
        self,
        stream_name: str,
        subjects: Optional[List[str]] = None,
        storage: Optional[str] = None,
        retention: Optional[str] = None,
        replicas: Optional[int] = None,
        max_age: Optional[str] = None,
        max_msgs: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> StreamInfo:
        """
        Ensure a JetStream stream exists with the specified configuration.

        This method creates the stream if it doesn't exist, or updates it if it does
        exist with different configuration.

        Args:
            stream_name: Name of the stream
            subjects: List of subjects the stream should handle
            storage: Storage type ('file' or 'memory')
            retention: Retention policy ('work_queue', 'limits', or 'interest')
            replicas: Number of replicas for stream data
            max_age: Maximum age for messages in streams (e.g., '24h', '7d')
            max_msgs: Maximum number of messages in streams
            max_bytes: Maximum size of messages in streams

        Returns:
            StreamInfo object with stream details

        Raises:
            ServiceRuntimeError: If stream creation or update fails.
        """
        try:
            js = await self._get_jetstream()

            # Set defaults from config if not provided
            if subjects is None:
                subjects = [f"{stream_name}.*"]
            if storage is None:
                storage = self._stream_config.default_storage
            if retention is None:
                retention = self._stream_config.default_retention
            if replicas is None:
                replicas = self._stream_config.default_replicas
            if max_age is None:
                max_age = self._stream_config.max_age
            if max_msgs is None:
                max_msgs = self._stream_config.max_msgs
            if max_bytes is None:
                max_bytes = self._stream_config.max_bytes

            try:
                # Check if stream exists
                stream_info = await js.stream_info(stream_name)
                self._logger.info(f"Stream '{stream_name}' already exists")

                # Check if we need to update the stream
                # Note: NATS doesn't allow updating all parameters, so we'll just
                # log a warning if there are differences
                current_config = stream_info.config
                if subjects and set(current_config.subjects) != set(subjects):
                    self._logger.warning(
                        f"Stream '{stream_name}' subjects differ. "
                        f"Current: {current_config.subjects}, Requested: {subjects}"
                    )

                return stream_info

            except nats.js.errors.NotFoundError:
                # Create stream if it doesn't exist
                if not self._stream_config.auto_create_streams:
                    raise ServiceRuntimeError(
                        f"Stream '{stream_name}' does not exist and "
                        f"auto_create_streams is False"
                    )

                self._logger.info(f"Stream '{stream_name}' not found, creating...")

                # Build stream configuration
                stream_config = {
                    "name": stream_name,
                    "subjects": subjects,
                    "storage": self._get_storage_type(storage),
                    "retention": self._get_retention_policy(retention),
                    "replicas": replicas,
                }

                # Add optional parameters if provided
                if max_age:
                    stream_config["max_age"] = max_age
                if max_msgs:
                    stream_config["max_msgs"] = max_msgs
                if max_bytes:
                    stream_config["max_bytes"] = max_bytes

                # Create the stream
                stream_info = await js.add_stream(**stream_config)
                self._logger.info(
                    f"Stream '{stream_name}' created with subjects {subjects}"
                )
                return stream_info

        except Exception as e:
            error_msg = f"Failed to ensure stream '{stream_name}': {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def get_stream_info(self, stream_name: str) -> StreamInfo:
        """
        Get information about a JetStream stream.

        Args:
            stream_name: Name of the stream

        Returns:
            StreamInfo object with stream details

        Raises:
            ServiceRuntimeError: If getting stream info fails.
        """
        try:
            js = await self._get_jetstream()

            stream_info = await js.stream_info(stream_name)
            self._logger.debug(f"Retrieved info for stream '{stream_name}'")
            return stream_info

        except nats.js.errors.NotFoundError:
            error_msg = f"Stream '{stream_name}' not found"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Failed to get stream info for '{stream_name}': {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def delete_stream(self, stream_name: str) -> bool:
        """
        Delete a JetStream stream.

        Args:
            stream_name: Name of the stream to delete

        Returns:
            True if the stream was deleted, False if it didn't exist

        Raises:
            ServiceRuntimeError: If stream deletion fails.
        """
        try:
            js = await self._get_jetstream()

            try:
                await js.delete_stream(stream_name)
                self._logger.info(f"Stream '{stream_name}' deleted successfully")
                return True
            except nats.js.errors.NotFoundError:
                self._logger.warning(
                    f"Stream '{stream_name}' not found, nothing to delete"
                )
                return False

        except Exception as e:
            error_msg = f"Failed to delete stream '{stream_name}': {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def purge_stream(self, stream_name: str) -> bool:
        """
        Purge all messages from a JetStream stream.

        Args:
            stream_name: Name of the stream to purge

        Returns:
            True if the stream was purged, False if it didn't exist

        Raises:
            ServiceRuntimeError: If stream purging fails.
        """
        try:
            js = await self._get_jetstream()

            try:
                await js.purge_stream(stream_name)
                self._logger.info(f"Stream '{stream_name}' purged successfully")
                return True
            except nats.js.errors.NotFoundError:
                self._logger.warning(
                    f"Stream '{stream_name}' not found, nothing to purge"
                )
                return False

        except Exception as e:
            error_msg = f"Failed to purge stream '{stream_name}': {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    @property
    def stream_config(self) -> StreamServiceConfig:
        """Get the stream configuration."""
        return self._stream_config

    @property
    def connection_service(self) -> ConnectionService:
        """Get the connection service."""
        return self._connection_service
