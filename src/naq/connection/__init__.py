# Import specific items to avoid circular imports
from .context_managers import (
    nats_connection,
    jetstream_context,
    nats_jetstream,
    nats_kv_store,
)
from .utils import (
    ConnectionMetrics,
    ConnectionMonitor,
    test_nats_connection,
    wait_for_nats_connection,
    connection_monitor,
)
from .decorators import (
    with_nats_connection,
    with_jetstream_context,
)

__all__ = [
    "nats_connection",
    "jetstream_context",
    "nats_jetstream",
    "nats_kv_store",
    "ConnectionMetrics",
    "ConnectionMonitor",
    "test_nats_connection",
    "wait_for_nats_connection",
    "connection_monitor",
    "with_nats_connection",
    "with_jetstream_context",
]


async def get_nats_connection(config=None):
    """
    Get a NATS connection.

    This function provides a simple interface for getting a NATS connection,
    primarily intended for testing purposes. It uses the nats_connection
    context manager internally.

    Args:
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Returns:
        NATSClient: An established NATS client connection.

    Raises:
        NaqConnectionError: If connection establishment fails.
    """
    async with nats_connection(config) as nc:
        return nc


async def get_jetstream_context(nc, config=None):
    """
    Get a JetStream context from a NATS connection.

    This function provides a simple interface for getting a JetStream context,
    primarily intended for testing purposes. It uses the jetstream_context
    context manager internally.

    Args:
        nc: An established NATS client connection.
        config: Optional configuration object for the JetStream context.
               If None, the default global configuration will be used.

    Returns:
        JetStreamContext: The JetStream context for interacting with JetStream.

    Raises:
        NaqConnectionError: If JetStream context creation fails.
    """
    async with jetstream_context(nc) as js:
        return js


async def ensure_stream(js, stream_name, subjects=None, config=None):
    """
    Ensure a JetStream stream exists, creating it if necessary.

    This function provides a simple interface for ensuring a JetStream stream
    exists, primarily intended for testing purposes. It checks if the stream
    exists and creates it if it doesn't.

    Args:
        js: An established JetStream context.
        stream_name: The name of the stream to ensure exists.
        subjects: Optional list of subjects for the stream.
        config: Optional configuration for the stream.

    Returns:
        StreamInfo: Information about the stream.

    Raises:
        NaqConnectionError: If stream creation or retrieval fails.
    """
    try:
        # Try to get stream info to see if it exists
        stream_info = await js.stream_info(stream_name)
        return stream_info
    except Exception:
        # Stream doesn't exist, create it
        from nats.aio.client import StreamConfig

        if config is None:
            config = StreamConfig(
                name=stream_name,
                subjects=subjects or [f"{stream_name}.*"],
            )

        await js.add_stream(config)
        return await js.stream_info(stream_name)
