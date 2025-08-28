"""
Context managers for NATS connections in NAQ.

This module provides asynchronous context managers for handling NATS connections
with proper resource management, error handling, and configuration.
"""

import contextlib
from typing import Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.errors import ErrNoServers, ErrTimeout, ErrConnectionClosed
from nats.js.errors import BucketNotFoundError
from loguru import logger

from ..settings import DEFAULT_NATS_URL
from ..services.config import create_global_config, GlobalServiceConfig
from ..exceptions import NaqConnectionError


@contextlib.asynccontextmanager
async def jetstream_context(nc: NATSClient):
    """
    Asynchronous context manager for obtaining a JetStream context from a NATS
    connection.

    This context manager handles the creation of a JetStream context from an existing
    NATS client connection, with proper error handling and logging.

    Args:
        nc: An established NATS client connection.

    Yields:
        JetStreamContext: The JetStream context for interacting with JetStream.

    Raises:
        NaqConnectionError: If JetStream context creation fails.

    Example:
        ```python
        # Using with an existing NATS connection
        async with nats_connection() as nc:
            async with jetstream_context(nc) as js:
                await js.add_stream(name="mystream", subjects=["my.subject"])
                await js.publish("my.subject", b"message")
        ```
    """
    js = None
    try:
        logger.debug("Creating JetStream context")
        js = nc.jetstream()
        logger.debug("JetStream context created successfully")
        yield js
    except Exception as e:
        error_msg = f"Failed to create JetStream context: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e


@contextlib.asynccontextmanager
async def nats_connection(config: Optional[GlobalServiceConfig] = None):
    """
    Asynchronous context manager for establishing and managing NATS connections.

    This context manager handles the complete lifecycle of a NATS connection,
    including establishment, configuration, error logging, and proper closure.
    It ensures that resources are properly cleaned up even if exceptions occur.

    Args:
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Yields:
        NATSClient: An established NATS client connection.

    Raises:
        NaqConnectionError: If connection establishment fails.
        ErrNoServers: If no NATS servers are available.
        ErrTimeout: If connection times out.
        ErrConnectionClosed: If connection is closed unexpectedly.

    Example:
        ```python
        # Using default configuration
        async with nats_connection() as nc:
            await nc.publish("subject", b"message")

        # Using custom configuration
        config = GlobalServiceConfig(nats_url="nats://custom:4222")
        async with nats_connection(config) as nc:
            await nc.publish("subject", b"message")
        ```
    """
    # Get configuration if not provided
    config = config or create_global_config()

    # Extract NATS connection parameters from config
    servers = config.nats_url or DEFAULT_NATS_URL
    custom_settings = config.custom_settings or {}
    
    client_name = custom_settings.get("client_name", "naq_client")
    max_reconnect_attempts = custom_settings.get("max_reconnect_attempts", 5)
    reconnect_time_wait = custom_settings.get("reconnect_time_wait", 2)
    connect_timeout = custom_settings.get("connect_timeout", 5)
    ping_interval = custom_settings.get("ping_interval", 60)
    max_outstanding_pings = custom_settings.get("max_outstanding_pings", 2)

    conn = None
    try:
        logger.info(f"Establishing NATS connection to {servers}")

        # Create NATS connection
        conn = await nats.connect(
            servers=servers,
            name=client_name,
            max_reconnect_attempts=max_reconnect_attempts,
            reconnect_time_wait=reconnect_time_wait,
            connect_timeout=connect_timeout,
            ping_interval=ping_interval,
            max_outstanding_pings=max_outstanding_pings,
            error_cb=nats_error_cb,
            disconnected_cb=nats_disconnected_cb,
            reconnected_cb=nats_reconnected_cb,
            closed_cb=nats_closed_cb,
        )

        logger.info(f"NATS connection established successfully to {servers}")
        yield conn

    except ErrNoServers as e:
        error_msg = f"No NATS servers available at {servers}: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e

    except ErrTimeout as e:
        error_msg = f"NATS connection timeout to {servers}: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e

    except ErrConnectionClosed as e:
        error_msg = f"NATS connection closed unexpectedly: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e

    except Exception as e:
        error_msg = f"Unexpected error establishing NATS connection to {servers}: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e

    finally:
        # Ensure connection is closed properly
        if conn is not None:
            try:
                if conn.is_connected:
                    await conn.close()
                    logger.info("NATS connection closed successfully")
            except Exception as e:
                error_msg = f"Error closing NATS connection: {e}"
                logger.error(error_msg)
                # Don't raise here as we're in finally block


async def nats_error_cb(err):
    logger.error(f"NATS connection error: {err}")


async def nats_disconnected_cb():
    logger.warning("NATS connection disconnected")


async def nats_reconnected_cb():
    logger.info("NATS connection reconnected")


async def nats_closed_cb():
    logger.info("NATS connection closed")


@contextlib.asynccontextmanager
async def nats_jetstream(config: Optional[GlobalServiceConfig] = None):
    """
    Asynchronous context manager that combines NATS connection and JetStream context.

    This context manager provides both a NATS connection and a JetStream context
    in a single async with statement, handling the complete lifecycle of both
    resources with proper error handling and cleanup.

    Args:
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Yields:
        A tuple containing (NATSClient, JetStreamContext) - the established
        NATS connection and JetStream context.

    Raises:
        NaqConnectionError: If connection establishment or JetStream context
            creation fails.
        ErrNoServers: If no NATS servers are available.
        ErrTimeout: If connection times out.
        ErrConnectionClosed: If connection is closed unexpectedly.

    Example:
        ```python
        # Using default configuration
        async with nats_jetstream() as (nc, js):
            await js.add_stream(name="mystream", subjects=["my.subject"])
            await js.publish("my.subject", b"message")

        # Using custom configuration
        config = GlobalServiceConfig(nats_url="nats://custom:4222")
        async with nats_jetstream(config) as (nc, js):
            await js.add_stream(name="mystream", subjects=["my.subject"])
            await js.publish("my.subject", b"message")
        ```
    """
    # Get configuration if not provided
    config = config or create_global_config()

    try:
        logger.debug("Establishing NATS connection and JetStream context")

        # Use the existing nats_connection context manager
        async with nats_connection(config) as conn:
            # Use the existing jetstream_context context manager
            async with jetstream_context(conn) as js:
                logger.debug(
                    "NATS connection and JetStream context established successfully"
                )
                yield conn, js

    except Exception as e:
        error_msg = f"Failed to establish NATS connection and JetStream context: {e}"
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e


@contextlib.asynccontextmanager
async def nats_kv_store(bucket_name: str, config: Optional[GlobalServiceConfig] = None):
    """
    Asynchronous context manager for NATS Key-Value store operations.

    This context manager provides a convenient way to access NATS Key-Value stores
    by handling the complete lifecycle of NATS connection, JetStream context,
    and KV store access with proper error handling and resource cleanup.

    Args:
        bucket_name: Name of the KV bucket to access or create.
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Yields:
        KeyValue: An established Key-Value store instance for the specified bucket.

    Raises:
        NaqConnectionError: If connection establishment, JetStream context creation,
            or KV store access fails.
        ErrNoServers: If no NATS servers are available.
        ErrTimeout: If connection times out.
        ErrConnectionClosed: If connection is closed unexpectedly.
        BucketNotFoundError: If the KV bucket doesn't exist and auto-creation fails.

    Example:
        ```python
        # Using default configuration
        async with nats_kv_store("my_bucket") as kv:
            await kv.put("key", b"value")
            value = await kv.get("key")

        # Using custom configuration
        config = GlobalServiceConfig(nats_url="nats://custom:4222")
        async with nats_kv_store("my_bucket", config) as kv:
            await kv.put("key", b"value")
            value = await kv.get("key")
        ```
    """
    # Get configuration if not provided
    config = config or create_global_config()

    try:
        logger.debug(
            f"Establishing NATS connection and JetStream context for KV store '{bucket_name}'"
        )

        # Use the existing nats_jetstream context manager
        async with nats_jetstream(config) as (nc, js):
            logger.debug(f"Attempting to access KV store '{bucket_name}'")

            kv = None
            try:
                # Try to get existing KV store
                kv = await js.key_value(bucket=bucket_name)
                logger.debug(
                    f"Successfully connected to existing KV store '{bucket_name}'"
                )
            except BucketNotFoundError:
                # Try to create the KV store if it doesn't exist
                logger.info(f"KV store '{bucket_name}' not found, creating...")
                try:
                    kv = await js.create_key_value(
                        bucket=bucket_name,
                        description=f"NAQ KV store for {bucket_name}",
                    )
                    logger.info(f"Successfully created KV store '{bucket_name}'")
                except Exception as create_error:
                    error_msg = (
                        f"Failed to create KV store '{bucket_name}': {create_error}"
                    )
                    logger.error(error_msg)
                    raise NaqConnectionError(error_msg) from create_error
            except Exception as e:
                error_msg = f"Failed to access KV store '{bucket_name}': {e}"
                logger.error(error_msg)
                raise NaqConnectionError(error_msg) from e

            yield kv

    except Exception as e:
        error_msg = (
            f"Failed to establish NATS KV store context for '{bucket_name}': {e}"
        )
        logger.error(error_msg)
        raise NaqConnectionError(error_msg) from e
