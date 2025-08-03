# src/naq/connection.py
from typing import Dict, Optional
from loguru import logger
import asyncio
import threading
import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext

from .exceptions import NaqConnectionError
from .settings import DEFAULT_NATS_URL
from .utils import setup_logging


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
                return nc

        async with self._lock:
            # Process-wide pooled connection
            if not prefer_thread_local:
                if url in self._connections and self._connections[url].is_connected:
                    return self._connections[url]

            # If prefer_thread_local, try to re-check TLS map under lock to avoid racing creation
            if prefer_thread_local:
                tls_conns, _ = self._get_tls_maps()
                nc = tls_conns.get(url)
                if nc and nc.is_connected:
                    return nc

            # Create a new connection
            try:
                nc = await nats.connect(url, name="naq_client")
                logger.info(f"NATS connection established to {url}")
                if prefer_thread_local:
                    tls_conns[url] = nc
                else:
                    self._connections[url] = nc
                return nc
            except Exception as e:
                # Clean up any partial connection
                if not prefer_thread_local and url in self._connections:
                    del self._connections[url]
                else:
                    tls_conns, _ = self._get_tls_maps()
                    if url in tls_conns:
                        del tls_conns[url]
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
            # Process-wide connections
            for url, nc in list(self._connections.items()):
                if nc.is_connected:
                    await nc.close()
                    logger.info(f"NATS connection to {url} closed")
            self._connections.clear()
            self._js_contexts.clear()

            # Thread-local connections for current thread
            tls_conns, tls_js = self._get_tls_maps()
            for url, nc in list(tls_conns.items()):
                if nc.is_connected:
                    await nc.close()
                    logger.info(f"[TLS] NATS connection to {url} closed")
            tls_conns.clear()
            tls_js.clear()


# Create a singleton instance
_manager = ConnectionManager()


# Provide compatibility with existing code
async def get_nats_connection(
    url: str = DEFAULT_NATS_URL, *, prefer_thread_local: bool = False
) -> NATSClient:
    """Gets a NATS client connection, reusing if possible."""
    return await _manager.get_connection(url, prefer_thread_local=prefer_thread_local)


async def get_jetstream_context(
    nc: Optional[NATSClient] = None, *, prefer_thread_local: bool = False
) -> JetStreamContext:
    """Gets a JetStream context from a NATS connection."""
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
    """Ensures a JetStream stream exists."""
    if js is None:
        js = await get_jetstream_context()

    if subjects is None:
        subjects = [f"{stream_name}.*"]  # Default subject pattern

    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        logger.info(f"Stream '{stream_name}' already exists.")
    except nats.js.errors.NotFoundError:
        # Create stream if it doesn't exist
        logger.info(f"Stream '{stream_name}' not found, creating...")
        await js.add_stream(
            name=stream_name,
            subjects=subjects,
            storage=nats.js.api.StorageType.FILE,  # Use File storage
            retention=nats.js.api.RetentionPolicy.WORK_QUEUE,  # Consume then delete
        )
        logger.info(f"Stream '{stream_name}' created with subjects {subjects}.")
    except Exception as e:
        raise NaqConnectionError(f"Failed to ensure stream '{stream_name}': {e}") from e
