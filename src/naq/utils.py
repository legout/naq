# src/naq/utils.py
import asyncio
import sys
from typing import Any, Callable, Coroutine, TypeVar

import anyio
from loguru import logger

from .settings import LOG_LEVEL, LOG_TO_FILE_ENABLED, LOG_FILE_PATH

T = TypeVar("T")


def run_async_from_sync(
    func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
) -> T:
    """
    Runs an async function from a synchronous context.

    Handles event loop management by using anyio.run() which creates a new
    event loop if one isn't already running. If an event loop is already running,
    it raises an informative error directing the user to use the async interface.

    Args:
        func: The async function to run.
        *args: Positional arguments to pass to the async function.
        **kwargs: Keyword arguments to pass to the async function.

    Returns:
        The result of the async function.

    Raises:
        RuntimeError: If called when an asyncio event loop is already running.
    """
    try:
        # anyio.run() creates a new event loop if one isn't running,
        # runs the coroutine to completion, and then closes the loop.
        return anyio.run(func, *args, **kwargs)
    except RuntimeError as e:
        # Check for the specific error message from anyio when a loop is already running.
        # The exact message might differ from asyncio.
        if (
            "cannot be called from a running event loop" in str(e).lower()
            or "anyio.run() cannot be called from within a running event loop"
            in str(e).lower()
        ):
            raise RuntimeError(
                "Cannot run naq sync function when an event loop is already running. "
                "Please use the async version of the function (e.g., `await naq.enqueue(...)`)."
            ) from e
        else:
            # Re-raise other RuntimeErrors
            raise


def setup_logging(level: str | None = None):
    """Configures logging based on environment variables or provided level string using loguru."""
    logger.remove()  # Remove all existing handlers

    # Determine the effective log level
    # CLI argument takes precedence over environment variable
    effective_level = level.upper() if level else LOG_LEVEL

    # Add stdout handler
    logger.add(
        sys.stdout,
        level=effective_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Add file handler if enabled
    if LOG_TO_FILE_ENABLED:
        logger.add(
            LOG_FILE_PATH,
            level=effective_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="1 week",  # Keep logs for 1 week
            compression="zip",  # Compress rotated logs
        )
    # Optionally silence overly verbose libraries if needed
    # logging.getLogger("nats").setLevel(logging.WARNING)
