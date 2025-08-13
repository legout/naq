"""
NATS Connection Service

This module provides a centralized service for managing NATS connections,
including connection pooling, lifecycle management, and JetStream context.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

import msgspec
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from ..connection import ConnectionManager
from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL
from .base import (
    BaseService,
    ServiceConfig,
    ServiceInitializationError,
    ServiceRuntimeError,
)


class ConnectionServiceConfig(msgspec.Struct):
    """
    Configuration for the ConnectionService.

    Attributes:
        nats_url: URL of the NATS server
        max_reconnect_attempts: Maximum number of reconnection attempts
        reconnect_time_wait: Time to wait between reconnection attempts (seconds)
        connection_timeout: Timeout for establishing a connection (seconds)
        ping_interval: Interval for sending ping commands (seconds)
        max_outstanding_pings: Maximum number of outstanding pings before
            connection is considered stale
        prefer_thread_local: Whether to prefer thread-local connections
    """

    nats_url: Optional[str] = None
    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    connection_timeout: float = 10.0
    ping_interval: float = 30.0
    max_outstanding_pings: int = 3
    prefer_thread_local: bool = False


class ConnectionService(BaseService):
    """
    Centralized NATS connection management service.

    This service provides pooled NATS connections, JetStream contexts,
    and connection lifecycle management with error recovery and reconnection logic.
    """

    def __init__(self, config: Optional[ServiceConfig] = None) -> None:
        """
        Initialize the connection service.

        Args:
            config: Optional configuration for the service.
        """
        super().__init__(config)
        self._connection_config = self._extract_connection_config()
        self._connection_manager = ConnectionManager()
        self._connections: Dict[str, NATSClient] = {}
        self._jetstream_contexts: Dict[str, JetStreamContext] = {}
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}

    def _extract_connection_config(self) -> ConnectionServiceConfig:
        """
        Extract connection-specific configuration from the service config.

        Returns:
            ConnectionServiceConfig instance with connection parameters.
        """
        # Start with default config
        connection_config = ConnectionServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "nats_url" in custom_settings:
                connection_config.nats_url = custom_settings["nats_url"]

            if "max_reconnect_attempts" in custom_settings:
                connection_config.max_reconnect_attempts = custom_settings[
                    "max_reconnect_attempts"
                ]

            if "reconnect_time_wait" in custom_settings:
                connection_config.reconnect_time_wait = custom_settings[
                    "reconnect_time_wait"
                ]

            if "connection_timeout" in custom_settings:
                connection_config.connection_timeout = custom_settings[
                    "connection_timeout"
                ]

            if "ping_interval" in custom_settings:
                connection_config.ping_interval = custom_settings["ping_interval"]

            if "max_outstanding_pings" in custom_settings:
                connection_config.max_outstanding_pings = custom_settings[
                    "max_outstanding_pings"
                ]

            if "prefer_thread_local" in custom_settings:
                connection_config.prefer_thread_local = custom_settings[
                    "prefer_thread_local"
                ]

        # Use service config nats_url if connection config doesn't have it
        if (
            connection_config.nats_url is None
            and self._config
            and self._config.nats_url
        ):
            connection_config.nats_url = self._config.nats_url

        return connection_config

    async def _do_initialize(self) -> None:
        """
        Initialize the connection service.

        This method sets up the initial connection manager and validates
        the configuration.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing ConnectionService")

            # Validate configuration
            if not self._connection_config.nats_url:
                self._connection_config.nats_url = DEFAULT_NATS_URL

            self._logger.info(f"NATS URL: {self._connection_config.nats_url}")
            self._logger.info(
                f"Max reconnect attempts: "
                f"{self._connection_config.max_reconnect_attempts}"
            )
            self._logger.info(
                f"Reconnect time wait: {self._connection_config.reconnect_time_wait}s"
            )

            # Test initial connection
            await self.get_connection()

            self._logger.info("ConnectionService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize ConnectionService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up connection service resources.

        This method closes all active connections and cancels any
        pending reconnection tasks.
        """
        try:
            self._logger.info("Cleaning up ConnectionService")

            # Cancel all reconnection tasks
            for url, task in self._reconnect_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        self._logger.debug(f"Reconnection task for {url} cancelled")

            self._reconnect_tasks.clear()

            # Close all connections
            await self._connection_manager.close_all()

            # Clear our caches
            self._connections.clear()
            self._jetstream_contexts.clear()

            self._logger.info("ConnectionService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup ConnectionService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def get_connection(self, url: Optional[str] = None) -> NATSClient:
        """
        Get a NATS connection from the pool.

        This method returns an existing connection if available, or creates
        a new one with the configured parameters.

        Args:
            url: Optional NATS server URL. If not provided, uses the configured URL.

        Returns:
            A connected NATS client.

        Raises:
            NaqConnectionError: If connection fails.
        """
        if url is None:
            url = self._connection_config.nats_url or DEFAULT_NATS_URL

        # Check if we already have a cached connection
        if url in self._connections and self._connections[url].is_connected:
            return self._connections[url]

        try:
            # Get connection from the underlying connection manager
            nc = await self._connection_manager.get_connection(
                url, prefer_thread_local=self._connection_config.prefer_thread_local
            )

            # Cache the connection
            self._connections[url] = nc

            # Set up connection monitoring for reconnection
            self._monitor_connection(url, nc)

            self._logger.debug(f"Got NATS connection to {url}")
            return nc

        except Exception as e:
            error_msg = f"Failed to get NATS connection to {url}: {e}"
            self._logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def get_jetstream(self, url: Optional[str] = None) -> JetStreamContext:
        """
        Get a JetStream context for a NATS connection.

        This method returns an existing JetStream context if available,
        or creates a new one.

        Args:
            url: Optional NATS server URL. If not provided, uses the configured URL.

        Returns:
            A JetStream context.

        Raises:
            NaqConnectionError: If getting JetStream context fails.
        """
        if url is None:
            url = self._connection_config.nats_url or DEFAULT_NATS_URL

        # Check if we already have a cached JetStream context
        if url in self._jetstream_contexts:
            return self._jetstream_contexts[url]

        try:
            # Get JetStream context from the underlying connection manager
            js = await self._connection_manager.get_jetstream(
                url, prefer_thread_local=self._connection_config.prefer_thread_local
            )

            # Cache the JetStream context
            self._jetstream_contexts[url] = js

            self._logger.debug(f"Got JetStream context for {url}")
            return js

        except Exception as e:
            error_msg = f"Failed to get JetStream context for {url}: {e}"
            self._logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    @asynccontextmanager
    async def connection_scope(
        self, url: Optional[str] = None
    ) -> AsyncIterator[NATSClient]:
        """
        Async context manager for safe connection handling.

        This method provides a connection that is automatically monitored
        and reconnected if necessary. The connection is returned to the pool
        when the context exits.

        Args:
            url: Optional NATS server URL. If not provided, uses the configured URL.

        Yields:
            A connected NATS client.

        Raises:
            NaqConnectionError: If connection fails.
        """
        if url is None:
            url = self._connection_config.nats_url or DEFAULT_NATS_URL

        nc = None
        try:
            # Get a connection
            nc = await self.get_connection(url)
            yield nc

        except Exception as e:
            error_msg = f"Error in connection scope for {url}: {e}"
            self._logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    def _monitor_connection(self, url: str, nc: NATSClient) -> None:
        """
        Monitor a connection and schedule reconnection if needed.

        Args:
            url: NATS server URL.
            nc: NATS client to monitor.
        """
        # Cancel any existing reconnection task for this URL
        if url in self._reconnect_tasks and not self._reconnect_tasks[url].done():
            self._reconnect_tasks[url].cancel()

        # Create a new monitoring task
        self._reconnect_tasks[url] = asyncio.create_task(
            self._connection_monitor_task(url, nc)
        )

    async def _connection_monitor_task(self, url: str, nc: NATSClient) -> None:
        """
        Task to monitor connection and handle reconnection.

        Args:
            url: NATS server URL.
            nc: NATS client to monitor.
        """
        try:
            while True:
                # Check if connection is still active
                if not nc.is_connected:
                    self._logger.warning(
                        f"Connection to {url} lost, attempting to reconnect..."
                    )
                    await self._reconnect(url, nc)
                    break

                # Wait before next check
                await asyncio.sleep(self._connection_config.ping_interval)

        except asyncio.CancelledError:
            # Task was cancelled, exit gracefully
            self._logger.debug(f"Connection monitor task for {url} cancelled")
            raise
        except Exception as e:
            self._logger.error(f"Error in connection monitor for {url}: {e}")

    async def _reconnect(self, url: str, nc: NATSClient) -> None:
        """
        Attempt to reconnect a lost connection.

        Args:
            url: NATS server URL.
            nc: NATS client to reconnect.

        Raises:
            NaqConnectionError: If reconnection fails after all attempts.
        """
        attempt = 0
        last_error = None

        while attempt < self._connection_config.max_reconnect_attempts:
            attempt += 1
            try:
                self._logger.info(
                    f"Reconnection attempt {attempt}/"
                    f"{self._connection_config.max_reconnect_attempts} for {url}"
                )

                # Close the old connection if it's still connected
                if nc.is_connected:
                    await nc.close()

                # Create a new connection
                new_nc = await nats.connect(
                    url,
                    name="naq_client",
                    reconnect_time_wait=self._connection_config.reconnect_time_wait,
                    max_reconnect_attempts=self._connection_config.max_reconnect_attempts,
                    connect_timeout=self._connection_config.connection_timeout,
                    ping_interval=self._connection_config.ping_interval,
                    max_outstanding_pings=self._connection_config.max_outstanding_pings,
                )

                # Update our cache
                self._connections[url] = new_nc

                # Update the connection manager
                await self._connection_manager.get_connection(
                    url, prefer_thread_local=self._connection_config.prefer_thread_local
                )

                self._logger.info(f"Successfully reconnected to {url}")
                return

            except Exception as e:
                last_error = e
                self._logger.warning(f"Reconnection attempt {attempt} failed: {e}")

                # Wait before next attempt
                if attempt < self._connection_config.max_reconnect_attempts:
                    await asyncio.sleep(self._connection_config.reconnect_time_wait)

        # All attempts failed
        error_msg = (
            f"Failed to reconnect to {url} after "
            f"{self._connection_config.max_reconnect_attempts} attempts"
        )
        self._logger.error(error_msg)
        raise NaqConnectionError(error_msg) from last_error

    async def close_connection(self, url: Optional[str] = None) -> None:
        """
        Close a specific NATS connection.

        Args:
            url: Optional NATS server URL. If not provided, uses the configured URL.
        """
        if url is None:
            url = self._connection_config.nats_url or DEFAULT_NATS_URL

        try:
            # Cancel any reconnection task for this URL
            if url in self._reconnect_tasks and not self._reconnect_tasks[url].done():
                self._reconnect_tasks[url].cancel()
                try:
                    await self._reconnect_tasks[url]
                except asyncio.CancelledError:
                    pass
                del self._reconnect_tasks[url]

            # Close the connection
            await self._connection_manager.close_connection(
                url, thread_local=self._connection_config.prefer_thread_local
            )

            # Remove from our caches
            self._connections.pop(url, None)
            self._jetstream_contexts.pop(url, None)

            self._logger.info(f"Closed connection to {url}")

        except Exception as e:
            error_msg = f"Failed to close connection to {url}: {e}"
            self._logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    @property
    def connection_config(self) -> ConnectionServiceConfig:
        """Get the connection configuration."""
        return self._connection_config

    @property
    def active_connections(self) -> Dict[str, bool]:
        """Get the status of all active connections."""
        return {url: nc.is_connected for url, nc in self._connections.items()}
