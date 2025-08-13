"""Synchronous interface module for the worker.

This module provides synchronous interface methods for the worker.
"""

from typing import Dict, List, Any

from ..settings import DEFAULT_NATS_URL
from .controller import WorkerController


class WorkerSyncInterface:
    """Provides synchronous interface methods for the worker."""

    def __init__(self, worker):
        """Initialize the sync interface with a reference to the worker."""
        self.worker = worker

    def run_sync(self) -> None:
        """
        Start the async worker in a clean AnyIO event loop using a BlockingPortal.

        Rationale:
        - Avoids mixing with any possibly running event loop or asyncio.run() constraints.
        - Provides consistent behavior when called from synchronous contexts (CLI, scripts).
        """
        try:
            from anyio.from_thread import start_blocking_portal
        except Exception as e:
            # Keep import local to avoid introducing runtime dependency unless used
            raise RuntimeError(
                "anyio is required for Worker.run_sync(). Please ensure 'anyio' is installed."
            ) from e

        # Install signal handlers in the main thread before starting the event loop thread
        self.worker.install_signal_handlers()

        # Use BlockingPortal to create and own the event loop for the duration of run()
        with start_blocking_portal() as portal:
            return portal.call(self.worker.run)

    def start_sync(self) -> "WorkerController":
        """
        Start the worker asynchronously and return a synchronous Controller.

        Usage:
            ctl = worker.start_sync()
            # ... later
            ctl.stop()
        """
        try:
            from anyio.from_thread import start_blocking_portal
        except Exception as e:
            raise RuntimeError(
                "anyio is required for Worker.start_sync(). Please ensure 'anyio' is installed."
            ) from e

        # Install signal handlers in the main thread before starting the event loop thread
        self.worker.install_signal_handlers()

        portal_cm = start_blocking_portal()
        portal = portal_cm.__enter__()

        # schedule the worker run in background; it exits when shutdown_event is set
        def _start() -> None:
            # fire-and-forget task
            async def _runner():
                await self.worker.run()

            # run as a task; no result awaited here
            import anyio

            anyio.create_task_group().start_soon  # no-op reference to satisfy linters
            # simplest is to call soon
            import asyncio
            return asyncio.create_task(_runner())

        portal.call(_start)

        return WorkerController(self.worker, portal_cm, portal)

    def stop_sync(self) -> None:
        """
        Convenience synchronous stop for a worker that was started via start_sync(),
        if a controller is not retained. This method is a no-op unless start_sync()
        was used and a controller stored on the instance.
        """
        ctl = getattr(self.worker, "_sync_controller", None)
        if ctl:
            ctl.stop()