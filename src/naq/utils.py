# src/naq/utils.py
import asyncio
import sys
from typing import Any, Coroutine, TypeVar

from loguru import logger

T = TypeVar("T")


def run_async_from_sync(coro: Coroutine[Any, Any, T]) -> T:
    """
    Runs an async coroutine from a synchronous context.

    Handles event loop management by using asyncio.run() which creates a new
    event loop if one isn't already running. If an event loop is already running,
    it raises an informative error directing the user to use the async interface.

    Args:
        coro: The coroutine to run.

    Returns:
        The result of the coroutine.

    Raises:
        RuntimeError: If called when an asyncio event loop is already running.
    """
    try:
        # asyncio.run() creates a new event loop if one isn't running,
        # runs the coroutine to completion, and then closes the loop.
        return asyncio.run(coro)
    except RuntimeError as e:
        if "cannot call run() while another loop is running" in str(e):
            # This occurs when the sync function is called from within an async context
            # that already has a running event loop.
            raise RuntimeError(
                "Cannot run naq sync function when an asyncio event loop is already running. "
                "Please use the async version of the function (e.g., `await naq.enqueue(...)`)."
            ) from e
        else:
            # Re-raise other RuntimeErrors
            raise


def setup_logging(level: str = "INFO"):
    """Configures logging based on the provided level string using loguru."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=level.upper(),
        # format="{time} - {name} - {level} - {message}",
        colorize=True,
    )
    # Optionally silence overly verbose libraries if needed
    # logging.getLogger("nats").setLevel(logging.WARNING)
