"""
Decorators for NATS connections in NAQ.

This module provides asynchronous decorators for handling NATS connections
with automatic lifecycle management and error handling.
"""

import functools
from typing import Any, Awaitable, Callable, Optional, TypeVar


from ..config import NAQConfig
from .context_managers import nats_connection, nats_jetstream

# Type variable for the decorated function
F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def with_nats_connection(
    config: Optional[NAQConfig] = None,
) -> Callable[[F], Callable[..., Awaitable[Any]]]:
    """
    Asynchronous decorator that injects a NATS connection into a decorated function.

    This decorator manages the complete lifecycle of a NATS connection, automatically
    establishing the connection before the decorated function is called and ensuring
    proper cleanup after the function completes, even if an exception occurs.

    The decorator injects the NATS connection as the first argument to the decorated
    function, allowing the function to use the connection for NATS operations without
    needing to manage the connection lifecycle itself.

    Args:
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Returns:
        A decorator function that wraps the original async function.

    Example:
        ```python
        # Using default configuration
        @with_nats_connection()
        async def publish_message(nc: NATSClient, subject: str, message: bytes) -> None:
            await nc.publish(subject, message)
            print(f"Message published to {subject}")

        # Using custom configuration
        config = NAQConfig(nats=NatsConfig(servers=["nats://custom:4222"]))
        @with_nats_connection(config)
        async def publish_with_custom_config(
            nc: NATSClient, subject: str, message: bytes
        ) -> None:
            await nc.publish(subject, message)
            print(f"Message published to {subject} using custom config")

        # Usage
        await publish_message("my.subject", b"Hello, NATS!")
        await publish_with_custom_config("my.subject", b"Hello, custom NATS!")
        ```

    Note:
        The decorated function must accept the NATS connection as its first argument.
        The decorator handles all connection management, including establishment,
        error handling, and cleanup, ensuring that resources are properly released
        even if the decorated function raises an exception.
    """

    def decorator(func: F) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Use the nats_connection context manager to handle the connection lifecycle
            async with nats_connection(config) as nc:
                # Inject the NATS connection as the first argument
                return await func(nc, *args, **kwargs)

        return wrapper

    return decorator


def with_jetstream_context(
    config: Optional[NAQConfig] = None,
) -> Callable[[F], Callable[..., Awaitable[Any]]]:
    """
    Asynchronous decorator that injects a JetStream context into a decorated function.

    This decorator manages the complete lifecycle of a NATS connection and
    JetStream context, automatically establishing both before the decorated function
    is called and ensuring proper cleanup after the function completes, even if
    an exception occurs.

    The decorator injects the JetStream context as the first argument to the decorated
    function, allowing the function to use the context for JetStream operations without
    needing to manage the connection lifecycle itself.

    Args:
        config: Optional configuration object for the NATS connection.
               If None, the default global configuration will be used.

    Returns:
        A decorator function that wraps the original async function.

    Example:
        ```python
        # Using default configuration
        @with_jetstream_context()
        async def create_stream(
            js: JetStreamContext, stream_name: str, subjects: list[str]
        ) -> None:
            await js.add_stream(name=stream_name, subjects=subjects)
            print(f"Stream '{stream_name}' created successfully")

        # Using custom configuration
        config = NAQConfig(nats=NatsConfig(servers=["nats://custom:4222"]))
        @with_jetstream_context(config)
        async def publish_message(
            js: JetStreamContext, subject: str, message: bytes
        ) -> None:
            await js.publish(subject, message)
            print(f"Message published to {subject} using custom config")

        # Usage
        await create_stream("my_stream", ["my.subject"])
        await publish_message("my.subject", b"Hello, JetStream!")
        ```

    Note:
        The decorated function must accept the JetStream context as its first argument.
        The decorator handles all connection management, including establishment,
        error handling, and cleanup, ensuring that resources are properly released
        even if the decorated function raises an exception.
    """

    def decorator(func: F) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Use the nats_jetstream context manager to handle the connection and
            # JetStream lifecycle
            async with nats_jetstream(config) as (_, js):
                # Inject the JetStream context as the first argument
                return await func(js, *args, **kwargs)

        return wrapper

    return decorator
