# src/naq/utils.py
import asyncio
from typing import Coroutine, TypeVar, Any

T = TypeVar('T')

def run_async_from_sync(coro: Coroutine[Any, Any, T]) -> T:
    """
    Runs an async coroutine from a synchronous context.

    Handles event loop management. If an event loop is already running,
    it schedules the coroutine on it. Otherwise, it creates a new loop.

    Args:
        coro: The coroutine to run.

    Returns:
        The result of the coroutine.
    """
    try:
        # Check if an event loop is already running in this thread
        loop = asyncio.get_running_loop()
        # If yes, we cannot use loop.run_until_complete directly.
        # This scenario is complex (e.g., running sync naq from within an async app).
        # A simple approach might be to run in a separate thread, but that adds complexity.
        # For now, raise an error or document this limitation.
        # Let's try asyncio.run() which handles loop creation/closing,
        # but it cannot be called when another loop is running.
        # A more robust solution might involve checking loop.is_running()
        # and using loop.call_soon_threadsafe if necessary, but that's much harder.

        # Let's stick to the simple case: assume no loop is running or use asyncio.run()
        # which handles the basic case well.
        # If a loop *is* running, asyncio.run() will raise a RuntimeError.
        return asyncio.run(coro)

    except RuntimeError as e:
        if "cannot call run() while another loop is running" in str(e):
            # This is the tricky case. For a library function, it's hard to solve universally.
            # Option 1: Raise a specific error telling the user to use the async version.
            raise RuntimeError(
                "Cannot run naq sync function when an asyncio event loop is already running. "
                "Please use the async version of the function (e.g., `await naq.enqueue(...)`)."
            ) from e
            # Option 2: Try to schedule on the existing loop (complex, might block inappropriately)
            # fut = asyncio.run_coroutine_threadsafe(coro, loop)
            # return fut.result() # This blocks the sync thread until the coro completes in the async thread
        else:
            # Re-raise other RuntimeErrors
            raise

