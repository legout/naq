"""
NATS Connection Service

This module provides a centralized service for managing NATS connections,
including connection pooling, lifecycle management, and JetStream context.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from ..config import get_config
from ..config.types import NAQConfig
from ..connection.manager import ConnectionManager
from ..exceptions import NaqConnectionError
from .base import (
    BaseService,
    ServiceConfig,
    ServiceInitializationError,
    ServiceRuntimeError,
)
from .config import ConnectionServiceConfig as BaseConnectionServiceConfig


class ConnectionServiceConfig(BaseConnectionServiceConfig):
    """
    Configuration for the ConnectionService.

    Extends the base ConnectionServiceConfig with connection-specific settings.
    """

    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    connection_timeout: float = 30.0  # Increased timeout for resilience
    ping_interval: float = 30.0
    max_outstanding_pings: int = 3
    prefer_thread_local: bool = False


class ConnectionService(BaseService):
    """
    Centralized NATS connection management service.

    This service provides pooled NATS connections, JetStream contexts,
    and connection lifecycle management with error recovery and reconnection logic.

    Performance Features:
    - Connection pooling and reuse for efficient resource management
    - Lazy connection initialization (connections created only when needed)
    - Connection caching to minimize connection overhead
    - Automatic reconnection with exponential backoff
    - Connection health monitoring and statistics
    """

    def __init__(self, config: Optional[ServiceConfig] = None, *, naq_config: Optional[NAQConfig] = None) -> None:
        """
        Initialize the connection service.

        Args:
            config: Optional configuration for the service.
            naq_config: Optional NAQ configuration instance. If not provided, uses global config.
        """
        super().__init__(config)
        # Store the NAQConfig instance
        self._naq_config = naq_config if naq_config is not None else get_config()
        # Extract connection-specific configuration
        self._connection_config = self._extract_connection_config()
        self._connection_manager = ConnectionManager()
        self._connections: Dict[str, NATSClient] = {}
        self._jetstream_contexts: Dict[str, JetStreamContext] = {}
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}
        self._connection_stats: Dict[str, Dict[str, Any]] = {}
        self._connection_locks: Dict[str, asyncio.Lock] = {}

    def _extract_connection_config(self) -> ConnectionServiceConfig:
        """
        Extract connection-specific configuration from the NAQ config.

        Returns:
            ConnectionServiceConfig instance with connection parameters.
        """
        # Start with default config from base
        connection_config = ConnectionServiceConfig(
            nats_url=self._config.nats_url if self._config else None,
            log_level=self._config.log_level if self._config else None,
        )

        # Override with NAQ config connection settings if provided
        if self._naq_config and self._naq_config.connection:
            naq_conn_config = self._naq_config.connection
            
            # Map NAQ config to connection service config
            if naq_conn_config.servers:
                connection_config.nats_url = naq_conn_config.servers[0]
            
            if naq_conn_config.max_reconnect_attempts is not None:
                connection_config.max_reconnect_attempts = naq_conn_config.max_reconnect_attempts
            
            if naq_conn_config.reconnect_time_wait is not None:
                connection_config.reconnect_time_wait = naq_conn_config.reconnect_time_wait
            
            if naq_conn_config.connection_timeout is not None:
                connection_config.connection_timeout = naq_conn_config.connection_timeout
            
            if naq_conn_config.drain_timeout is not None:
                # Use drain_timeout as ping_interval if not explicitly set
                connection_config.ping_interval = naq_conn_config.drain_timeout

        # Override with service config custom settings if provided (for backward compatibility)
        if self._config and hasattr(self._config, 'custom_settings') and self._config.custom_settings:
            custom_settings = self._config.custom_settings

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
                # Use the first server from the config, or fallback to default
                if self._naq_config.nats.servers:
                    self._connection_config.nats_url = self._naq_config.nats.servers[0]
                else:
                    self._connection_config.nats_url = "nats://localhost:4222"

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
            self._logger.debug("Attempting to cancel reconnection tasks...")

            # Cancel all reconnection tasks with timeout
            if self._reconnect_tasks:
                cancel_tasks = []
                for url, task in self._reconnect_tasks.items():
                    if not task.done():
                        self._logger.debug(f"Cancelling reconnection task for {url}")
                        task.cancel()
                        cancel_tasks.append(task)

                # Wait for all tasks to be cancelled with timeout
                if cancel_tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*cancel_tasks, return_exceptions=True),
                            timeout=5.0,
                        )
                        self._logger.debug("Reconnection tasks cancelled successfully.")
                    except asyncio.TimeoutError:
                        self._logger.warning(
                            "Timeout while waiting for reconnection tasks to cancel"
                        )
                    except Exception as e:
                        self._logger.warning(
                            f"Error while cancelling reconnection tasks: {e}"
                        )

            self._reconnect_tasks.clear()
            self._logger.debug("Reconnection tasks cleared.")

            # Close all connections
            self._logger.debug(
                "Attempting to close all NATS connections via ConnectionManager..."
            )
            await self._connection_manager.close_all()
            self._logger.debug("All NATS connections closed via ConnectionManager.")

            # Clear our caches
            self._connections.clear()
            self._jetstream_contexts.clear()
            self._logger.debug("Internal connection caches cleared.")

            self._logger.info("ConnectionService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup ConnectionService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def get_connection(self, url: Optional[str] = None) -> NATSClient:
        """
        Get a NATS connection from the pool.

        This method implements performance optimizations:
        - Connection pooling and reuse
        - Lazy connection initialization
        - Connection caching with health checks
        - Minimal overhead for cached connections

        Args:
            url: Optional NATS server URL. If not provided, uses the configured URL.

        Returns:
            A connected NATS client.

        Raises:
            NaqConnectionError: If connection fails.
        """
        if url is None:
            url = self._connection_config.nats_url or (self._naq_config.nats.servers[0] if self._naq_config.nats.servers else "nats://localhost:4222")

        # Initialize connection stats if not exists
        if url not in self._connection_stats:
            self._connection_stats[url] = {
                "created_count": 0,
                "cache_hits": 0,
                "connection_errors": 0,
                "last_used": 0,
            }
        
        # Initialize connection lock if not exists (for thread safety)
        if url not in self._connection_locks:
            self._connection_locks[url] = asyncio.Lock()

        # Check if we already have a cached and healthy connection
        if url in self._connections and self._connections[url].is_connected:
            self._connection_stats[url]["cache_hits"] += 1
            self._connection_stats[url]["last_used"] = asyncio.get_event_loop().time()
            self._logger.debug(f"Connection cache hit for {url}")
            return self._connections[url]

        # Use lock to prevent multiple connection attempts for the same URL
        async with self._connection_locks[url]:
            # Double-check pattern in case another coroutine created the connection while we waited
            if url in self._connections and self._connections[url].is_connected:
                self._connection_stats[url]["cache_hits"] += 1
                self._connection_stats[url]["last_used"] = asyncio.get_event_loop().time()
                self._logger.debug(f"Connection cache hit after lock for {url}")
                return self._connections[url]

            try:
                import time
                start_time = time.perf_counter()
                
                # Get connection from the underlying connection manager
                nc = await self._connection_manager.get_connection(
                    url, prefer_thread_local=self._connection_config.prefer_thread_local
                )

                # Cache the connection
                self._connections[url] = nc
                
                # Update connection stats
                connection_time = time.perf_counter() - start_time
                self._connection_stats[url]["created_count"] += 1
                self._connection_stats[url]["last_used"] = asyncio.get_event_loop().time()
                self._logger.debug(f"Created new NATS connection to {url} in {connection_time:.3f}s")

                # Set up connection monitoring for reconnection
                self._monitor_connection(url, nc)

                return nc

            except Exception as e:
                self._connection_stats[url]["connection_errors"] += 1
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
            url = self._connection_config.nats_url or (self._naq_config.nats.servers[0] if self._naq_config.nats.servers else "nats://localhost:4222")

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
            url = self._connection_config.nats_url or (self._naq_config.nats.servers[0] if self._naq_config.nats.servers else "nats://localhost:4222")

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
            # Check if the connection is still valid before monitoring
            if not nc or not hasattr(nc, "is_connected"):
                self._logger.warning(
                    f"Invalid connection object for {url}, stopping monitor"
                )
                return

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

                # Create a new connection using nats.connect directly
                # since we need a long-lived connection for caching
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
            url = self._connection_config.nats_url or (self._naq_config.nats.servers[0] if self._naq_config.nats.servers else "nats://localhost:4222")

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
