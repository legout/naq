# src/naq/connection/manager.py
from typing import Dict, Optional
from loguru import logger
import asyncio
import threading
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext, api

from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL


class ConnectionManager:
    """
    Manages NATS connections with pooling and optional thread-local reuse.

    - Async paths reuse a single connection per URL within the process.
    - Sync helper paths can reuse a thread-local connection to avoid connect/close per call.
    """

    def __init__(self):
        self._connections: Dict[str, NATSClient] = {}
        self._js_contexts: Dict[str, JetStreamContext] = {}
        self._lock = asyncio.Lock()
        # Thread-local storage for sync helpers
        self._tls = threading.local()

    def _get_tls_maps(self):
        """Initialize thread-local maps lazily."""
        if not hasattr(self._tls, "connections"):
            self._tls.connections = {}
            self._tls.js_contexts = {}
        return self._tls.connections, self._tls.js_contexts

    async def get_connection(
        self, url: str = DEFAULT_NATS_URL, *, prefer_thread_local: bool = False
    ) -> NATSClient:
        """
        Gets a NATS client connection from the pool or creates a new one.

        Args:
            url: NATS server URL
            prefer_thread_local: When True, reuse a thread-local connection (for sync helpers)

        Returns:
            A connected NATS client

        Raises:
            NaqConnectionError: If connection fails
        """
        if url is None:
            url = DEFAULT_NATS_URL

        # Thread-local fast path (no async lock contention) for sync wrappers
        if prefer_thread_local:
            tls_conns, _ = self._get_tls_maps()
            nc = tls_conns.get(url)
            if nc and nc.is_connected:
                logger.debug(
                    f"ConnectionManager: Found existing thread-local connection for {url}"
                )
                return nc

        # First check if we already have a connection without holding the lock
        if not prefer_thread_local:
            if url in self._connections and self._connections[url].is_connected:
                return self._connections[url]

        # For thread-local, check outside the lock first
        if prefer_thread_local:
            tls_conns, _ = self._get_tls_maps()
            nc = tls_conns.get(url)
            if nc and nc.is_connected:
                return nc

        # Create a new connection outside the lock
        try:
            nc = await nats.connect(url, name="naq_client")
            logger.info(f"NATS connection established to {url}")
            
            # Now acquire the lock only to store the connection
            async with self._lock:
                # Double-check pattern to prevent race conditions
                if not prefer_thread_local:
                    # Check again if another connection was created while we were connecting
                    if url in self._connections and self._connections[url].is_connected:
                        # Close our new connection and use the existing one
                        await nc.close()
                        return self._connections[url]
                    self._connections[url] = nc
                else:
                    # For thread-local, check again
                    tls_conns, _ = self._get_tls_maps()
                    existing_nc = tls_conns.get(url)
                    if existing_nc and existing_nc.is_connected:
                        # Close our new connection and use the existing one
                        await nc.close()
                        return existing_nc
                    tls_conns[url] = nc
                return nc
        except Exception as e:
            raise NaqConnectionError(
                f"Failed to connect to NATS at {url}: {e}"
            ) from e

    async def get_jetstream(
        self, url: str = DEFAULT_NATS_URL, *, prefer_thread_local: bool = False
    ) -> JetStreamContext:
        """
        Gets a JetStream context for a specific connection.

        Args:
            url: NATS server URL
            prefer_thread_local: When True, reuse a thread-local JS context (for sync helpers)

        Returns:
            A JetStream context

        Raises:
            NaqConnectionError: If getting JetStream context fails
        """
        if url is None:
            url = DEFAULT_NATS_URL

        if prefer_thread_local:
            _, tls_js = self._get_tls_maps()
            js = tls_js.get(url)
            if js:
                return js

        async with self._lock:
            if not prefer_thread_local:
                if url in self._js_contexts:
                    return self._js_contexts[url]
            else:
                _, tls_js = self._get_tls_maps()
                js = tls_js.get(url)
                if js:
                    return js

            # Get the connection first
            nc = await self.get_connection(url, prefer_thread_local=prefer_thread_local)

            # Create JetStream context
            try:
                js = nc.jetstream()
                if prefer_thread_local:
                    _, tls_js = self._get_tls_maps()
                    tls_js[url] = js
                else:
                    self._js_contexts[url] = js
                logger.info(f"JetStream context obtained for {url}")
                return js
            except Exception as e:
                raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e

    async def close_connection(
        self, url: str = DEFAULT_NATS_URL, *, thread_local: bool = False
    ) -> None:
        """
        Closes a specific NATS connection.

        Args:
            url: NATS server URL to close
            thread_local: When True, closes the thread-local connection if present
        """
        if url is None:
            url = DEFAULT_NATS_URL

        async with self._lock:
            if thread_local:
                tls_conns, tls_js = self._get_tls_maps()
                nc = tls_conns.get(url)
                if nc and nc.is_connected:
                    await nc.close()
                    logger.info(f"[TLS] NATS connection to {url} closed")
                tls_conns.pop(url, None)
                tls_js.pop(url, None)
                return

            if url in self._connections and self._connections[url].is_connected:
                await self._connections[url].close()
                logger.info(f"NATS connection to {url} closed")
                # Clean up our references
                del self._connections[url]
                if url in self._js_contexts:
                    del self._js_contexts[url]

    async def close_all(self) -> None:
        """Closes all NATS connections in the pool (both process and thread-local)."""
        async with self._lock:
            logger.debug(
                "ConnectionManager: Starting close_all process-wide connections."
            )
            # Process-wide connections
            for url, nc in list(self._connections.items()):
                if nc.is_connected:
                    logger.debug(
                        f"ConnectionManager: Attempting to drain and close process-wide NATS connection to {url}"
                    )
                    try:
                        await nc.drain()
                        await nc.close()
                        logger.info(f"NATS connection to {url} closed")
                    except Exception as e:
                        logger.warning(
                            f"ConnectionManager: Flush timeout when draining NATS connection to {url}. Forcing close. Error: {e}"
                        )
                        await nc.close()  # Attempt to force close
                else:
                    logger.debug(
                        f"ConnectionManager: Process-wide NATS connection to {url} already disconnected or not found."
                    )
            self._connections.clear()
            self._js_contexts.clear()
            logger.debug("ConnectionManager: Process-wide connection caches cleared.")

            logger.debug(
                "ConnectionManager: Starting close_all thread-local connections."
            )
            # Thread-local connections for current thread
            tls_conns, tls_js = self._get_tls_maps()
            for url, nc in list(tls_conns.items()):
                if nc.is_connected:
                    logger.debug(
                        f"ConnectionManager: Attempting to drain and close thread-local NATS connection to {url}"
                    )
                    try:
                        await nc.drain()
                        await nc.close()
                        logger.info(f"[TLS] NATS connection to {url} closed")
                    except Exception as e:
                        logger.warning(
                            f"ConnectionManager: [TLS] Flush timeout when draining NATS connection to {url}. Forcing close. Error: {e}"
                        )
                        await nc.close()  # Attempt to force close
                else:
                    logger.debug(
                        f"ConnectionManager: Thread-local NATS connection to {url} already disconnected or not found."
                    )
            tls_conns.clear()
            tls_js.clear()
            logger.debug("ConnectionManager: Thread-local connection caches cleared.")


# Create a singleton instance
_manager = ConnectionManager()


# Provide compatibility with existing code
async def get_nats_connection(
    url: str = DEFAULT_NATS_URL, *, prefer_thread_local: bool = False
) -> NATSClient:
    """
    Gets a NATS client connection, reusing if possible.

    .. deprecated::
        Use the `nats_connection` context manager instead for better resource management.
        This function will be removed in a future version.
    """
    import warnings

    warnings.warn(
        "get_nats_connection is deprecated. Use the nats_connection context manager instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return await _manager.get_connection(url, prefer_thread_local=prefer_thread_local)


async def get_jetstream_context(
    nc: Optional[NATSClient] = None, *, prefer_thread_local: bool = False
) -> JetStreamContext:
    """
    Gets a JetStream context from a NATS connection.

    .. deprecated::
        Use the `jetstream_context` or `nats_jetstream` context managers instead
        for better resource management. This function will be removed in a future version.
    """
    import warnings

    warnings.warn(
        "get_jetstream_context is deprecated. Use the jetstream_context or "
        "nats_jetstream context managers instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if nc is not None:
        # If a connection is provided directly, use it
        try:
            return nc.jetstream()
        except Exception as e:
            raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e

    # Otherwise use the connection manager
    return await _manager.get_jetstream(prefer_thread_local=prefer_thread_local)


async def close_nats_connection(
    url: str = DEFAULT_NATS_URL, *, thread_local: bool = False
):
    """Closes a specific NATS connection."""
    await _manager.close_connection(url, thread_local=thread_local)


async def close_all_connections():
    """Closes all NATS connections managed by the connection pool."""
    await _manager.close_all()


async def ensure_stream(
    js: Optional[JetStreamContext] = None,
    stream_name: str = "naq_jobs",  # Default stream name
    subjects: Optional[list[str]] = None,
) -> None:
    """
    Ensures a JetStream stream exists.

    .. deprecated::
        Use the `nats_jetstream` context manager instead for better resource management.
        This function will be removed in a future version.
    """
    import warnings

    warnings.warn(
        "ensure_stream is deprecated. Use the nats_jetstream context manager instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if js is None:
        js = await get_jetstream_context()

    if subjects is None:
        subjects = [f"{stream_name}.*"]  # Default subject pattern

    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        logger.info(f"Stream '{stream_name}' already exists.")
    except Exception:
        # Create stream if it doesn't exist
        logger.info(f"Stream '{stream_name}' not found, creating...")
        await js.add_stream(
            name=stream_name,
            subjects=subjects,
            storage=api.StorageType.FILE,  # Use File storage
            retention=api.RetentionPolicy.WORK_QUEUE,  # Consume then delete
        )
        logger.info(f"Stream '{stream_name}' created with subjects {subjects}.")
