"""Worker controller module.

This module provides the controller class for managing a Worker from synchronous code,
keeping a BlockingPortal alive.
"""

import asyncio
from typing import Any, Dict, List, Optional, Sequence

from ..utils import run_async_from_sync


class WorkerController:
    """
    Controller to manage a Worker from synchronous code, keeping a BlockingPortal alive.

    Methods:
        stop(): request graceful stop and wait for shutdown.
        status(): returns current boolean running state.
    """

    def __init__(self, worker, portal_cm, portal):
        """Initialize the worker controller.

        Args:
            worker: The worker instance to control.
            portal_cm: The BlockingPortal context manager.
            portal: The BlockingPortal instance.
        """
        self._worker = worker
        self._portal_cm = portal_cm
        self._portal = portal
        self._closed = False

    def stop(self) -> None:
        """Stop the worker gracefully."""
        if self._closed:
            return

        # Signal shutdown in the worker's event loop
        def _signal():
            self._worker._running = False
            self._worker._shutdown_event.set()
            return None

        self._portal.call(_signal)
        # allow worker.run to finish, then close portal
        self._portal_cm.__exit__(None, None, None)
        self._closed = True

    def status(self) -> bool:
        """Check if the worker is currently running."""
        # Check running flag via portal to avoid races
        def _get():
            return bool(self._worker._running)

        return self._portal.call(_get)