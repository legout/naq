"""Connection utilities for NATS connection management."""

import asyncio
from typing import List
import msgspec
from loguru import logger

from .context_managers import nats_connection


class ConnectionMetrics(msgspec.Struct):
    """Data class to store and track metrics related to NATS connection usage and
    performance.

    This class provides a structured way to monitor connection statistics including
    total connections established, currently active connections, failed connection
    attempts, and the average time taken to establish connections.

    Attributes:
        total_connections: Total number of connection attempts made
        active_connections: Number of currently active connections
        failed_connections: Number of failed connection attempts
        average_connection_time: Average time in seconds to establish a connection
    """

    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0


class ConnectionMonitor:
    """Monitor and track NATS connection metrics.

    This class provides methods to record connection events and calculate
    connection statistics including total connections, active connections,
    failed connections, and average connection time.

    Attributes:
        metrics: ConnectionMetrics instance storing all connection statistics
        _connection_durations: Internal list storing all connection durations
    """

    def __init__(self) -> None:
        """Initialize the ConnectionMonitor with empty metrics and durations list."""
        self.metrics = ConnectionMetrics()
        self._connection_durations: List[float] = []

    def record_connection_start(self) -> None:
        """Record the start of a new connection.

        Increments both total_connections and active_connections metrics.
        """
        self.metrics.total_connections += 1
        self.metrics.active_connections += 1

    def record_connection_end(self, duration: float) -> None:
        """Record the end of a connection with its duration.

        Decrements active_connections, records the duration, and updates
        the average_connection_time based on all recorded durations.

        Args:
            duration: The duration of the connection in seconds
        """
        self.metrics.active_connections -= 1
        self._connection_durations.append(duration)

        # Calculate average connection time
        if self._connection_durations:
            self.metrics.average_connection_time = sum(
                self._connection_durations
            ) / len(self._connection_durations)

    def record_connection_failure(self) -> None:
        """Record a failed connection attempt.

        Increments the failed_connections metric.
        """
        self.metrics.failed_connections += 1


async def test_nats_connection() -> bool:
    """
    Test the health and connectivity of the NATS server.

    This function uses the nats_connection context manager to establish a connection
    and performs a simple NATS ping/flush operation to verify connectivity.

    Returns:
        bool: True if the connection test is successful, False otherwise.

    Example:
        ```python
        # Test NATS connection
        is_connected = await test_nats_connection()
        if is_connected:
            print("NATS connection is healthy")
        else:
            print("NATS connection test failed")
        ```
    """
    try:
        # Use the nats_connection context manager
        async with nats_connection() as nc:
            # Perform a simple ping/flush operation to verify connectivity
            await nc.flush()
            logger.debug("NATS connection test successful")
            return True
    except Exception as e:
        error_msg = f"NATS connection test failed: {e}"
        logger.error(error_msg)
        return False


async def wait_for_nats_connection(
    timeout: float = 30.0, retry_delay: float = 1.0
) -> bool:
    """
    Wait for NATS connection to become available.

    This function repeatedly calls test_nats_connection until successful or the timeout
    is reached. It includes a delay between retries to avoid overwhelming the system
    with connection attempts.

    Args:
        timeout: Maximum time in seconds to wait for a successful connection.
            Defaults to 30.0.
        retry_delay: Delay in seconds between connection attempts. Defaults to 1.0.

    Returns:
        bool: True if connection becomes available within timeout, False otherwise.

    Example:
        ```python
        # Wait for NATS connection with default timeout and retry delay
        is_connected = await wait_for_nats_connection()
        if is_connected:
            print("NATS connection is available")
        else:
            print("NATS connection not available within timeout")

        # Wait for NATS connection with custom timeout and retry delay
        is_connected = await wait_for_nats_connection(timeout=60.0, retry_delay=2.0)
        ```
    """
    start_time = asyncio.get_event_loop().time()
    logger.debug(
        f"Waiting for NATS connection (timeout: {timeout}s, "
        f"retry_delay: {retry_delay}s)"
    )

    while True:
        # Test the connection
        if await test_nats_connection():
            logger.debug("NATS connection established successfully")
            return True

        # Check if timeout has been reached
        elapsed_time = asyncio.get_event_loop().time() - start_time
        if elapsed_time >= timeout:
            logger.warning(f"NATS connection not established within {timeout}s timeout")
            return False

        # Calculate remaining time to ensure we don't sleep past the timeout
        remaining_time = timeout - elapsed_time
        sleep_time = min(retry_delay, remaining_time)

        logger.debug(f"NATS connection test failed, retrying in {sleep_time}s")
        await asyncio.sleep(sleep_time)


# Global connection monitor instance
connection_monitor = ConnectionMonitor()
