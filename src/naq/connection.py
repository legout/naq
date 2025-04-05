# src/naq/connection.py
import nats
from nats.js import JetStreamContext
from nats.aio.client import Client as NATSClient # Use alias to avoid name clash
from typing import Optional

from .settings import DEFAULT_NATS_URL
from .exceptions import ConnectionError as NaqConnectionError

# Global connection cache (simple approach)
_nc: Optional[NATSClient] = None
_js: Optional[JetStreamContext] = None

async def get_nats_connection(url: str = DEFAULT_NATS_URL) -> NATSClient:
    """Gets a NATS client connection, reusing if possible."""
    global _nc
    if _nc is None or not _nc.is_connected:
        try:
            _nc = await nats.connect(url, name="naq_client")
            print(f"NATS connection established to {url}")
        except Exception as e:
            _nc = None # Reset on failure
            raise NaqConnectionError(f"Failed to connect to NATS at {url}: {e}") from e
    return _nc

async def get_jetstream_context(nc: Optional[NATSClient] = None) -> JetStreamContext:
    """Gets a JetStream context from a NATS connection."""
    global _js
    if nc is None:
        nc = await get_nats_connection() # Ensure connection exists

    # Check if JS context is already created for this connection instance
    # Simple check based on global _js and connection id might not be robust
    # if multiple connections are used, but okay for this prototype.
    if _js is None or _js._nc != nc: # Check if associated connection is the same
        try:
            _js = nc.jetstream()
            print("JetStream context obtained.")
        except Exception as e:
            _js = None # Reset on failure
            raise NaqConnectionError(f"Failed to get JetStream context: {e}") from e
    return _js

async def close_nats_connection():
    """Closes the global NATS connection if it exists."""
    global _nc, _js
    if _nc and _nc.is_connected:
        await _nc.close()
        print("NATS connection closed.")
    _nc = None
    _js = None

async def ensure_stream(
    js: Optional[JetStreamContext] = None,
    stream_name: str = "naq_jobs", # Default stream name
    subjects: Optional[list[str]] = None,
) -> None:
    """Ensures a JetStream stream exists."""
    if js is None:
        js = await get_jetstream_context()

    if subjects is None:
        subjects = [f"{stream_name}.*"] # Default subject pattern

    try:
        # Check if stream exists
        await js.stream_info(stream_name)
        print(f"Stream '{stream_name}' already exists.")
    except nats.js.errors.StreamNotFoundError:
        # Create stream if it doesn't exist
        print(f"Stream '{stream_name}' not found, creating...")
        await js.add_stream(
            name=stream_name,
            subjects=subjects,
            # Configure retention, storage, etc. as needed
            # Default is LimitsPolicy (limits based) with MemoryStorage
            # For persistence like RQ, FileStorage is needed.
            storage=nats.js.api.StorageType.FILE, # Use File storage
            retention=nats.js.api.RetentionPolicy.WORK_QUEUE, # Consume then delete
        )
        print(f"Stream '{stream_name}' created with subjects {subjects}.")
    except Exception as e:
        raise NaqConnectionError(f"Failed to ensure stream '{stream_name}': {e}") from e
