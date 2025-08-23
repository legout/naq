"""NATS helper utilities for NAQ.

This module contains common NATS helper utilities used throughout the NAQ codebase.
"""

from typing import List, Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from loguru import logger

from ..exceptions import NaqConnectionError


def build_subject(*components: str) -> str:
    """
    Build a NATS subject from components.

    Joins the components with dots to create a valid NATS subject.
    Empty components are filtered out to avoid consecutive dots.

    Args:
        *components: Variable number of string components to join

    Returns:
        A valid NATS subject string

    Examples:
        >>> build_subject("orders", "processing")
        'orders.processing'
        >>> build_subject("events", "", "user", "created")
        'events.user.created'
    """
    # Filter out empty components and join with dots
    filtered_components = [comp for comp in components if comp]
    return ".".join(filtered_components)


def parse_subject(subject: str) -> List[str]:
    """
    Parse a NATS subject into its components.

    Splits the subject by dots to extract the individual components.

    Args:
        subject: The NATS subject string to parse

    Returns:
        A list of subject components

    Examples:
        >>> parse_subject("orders.processing")
        ['orders', 'processing']
        >>> parse_subject("events.user.created")
        ['events', 'user', 'created']
    """
    if not subject:
        return []

    # Split by dots and return the list
    return subject.split(".")


async def stream_exists(
    js: Optional[JetStreamContext] = None,
    nc: Optional[NATSClient] = None,
    stream_name: str = "naq_jobs",
) -> bool:
    """
    Check if a JetStream stream exists.

    Args:
        js: Optional JetStream context. If None, one will be created from nc.
        nc: Optional NATS client. If None and js is None, a new connection will be established.
        stream_name: Name of the stream to check

    Returns:
        True if the stream exists, False otherwise

    Raises:
        NaqConnectionError: If there's an error connecting to NATS or JetStream

    Examples:
        >>> # Using existing JetStream context
        >>> exists = await stream_exists(js=js_context, stream_name="my_stream")

        >>> # Using existing NATS client
        >>> exists = await stream_exists(nc=nats_client, stream_name="my_stream")

        >>> # Using new connection
        >>> exists = await stream_exists(stream_name="my_stream")
    """
    try:
        # If JetStream context is provided, use it directly
        if js is not None:
            await js.stream_info(stream_name)
            return True

        # If NATS client is provided but no JetStream context, create one
        if nc is not None:
            js_context = nc.jetstream()
            await js_context.stream_info(stream_name)
            return True

        # If neither is provided, establish a new connection
        from ..connection import get_nats_connection, get_jetstream_context

        nc_new = await get_nats_connection()
        js_context = await get_jetstream_context(nc_new)
        await js_context.stream_info(stream_name)
        return True

    except nats.js.errors.NotFoundError:
        # Stream doesn't exist
        return False
    except Exception as e:
        logger.error(f"Error checking if stream '{stream_name}' exists: {e}")
        raise NaqConnectionError(f"Failed to check stream existence: {e}") from e
