"""Worker controller module.

This module provides the controller class for managing a Worker from synchronous code,
keeping a BlockingPortal alive.
"""

from typing import Any, ContextManager

from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger


class WorkerController:
    """
    Controller to manage a Worker from synchronous code, keeping a BlockingPortal alive.

    Methods:
        stop(): request graceful stop and wait for shutdown.
        status(): returns current boolean running state.
    """

    def __init__(
        self, worker: Any, portal_cm: ContextManager[Any], portal: Any
    ) -> None:
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
        self._logger = StructuredLogger("worker_controller")
        self._error_handler = ErrorHandler(self._logger)

    def stop(self) -> None:
        """Stop the worker gracefully."""
        if self._closed:
            return

        with self._logger.operation_context("worker_stop"):
            try:
                # Signal shutdown in the worker's event loop
                def _signal():
                    self._worker._running = False
                    self._worker._shutdown_event.set()
                    return None

                self._portal.call(_signal)
                # allow worker.run to finish, then close portal
                self._portal_cm.__exit__(None, None, None)
                self._closed = True
                self._logger.info("Worker stopped successfully")
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to stop worker")
                self._error_handler.handle_error(
                    wrapped_error, {"operation": "worker_stop"}
                )
                raise

    def status(self) -> bool:
        """Check if the worker is currently running."""
        try:
            # Check running flag via portal to avoid races
            def _get():
                return bool(self._worker._running)

            is_running = self._portal.call(_get)
            self._logger.debug("Worker status checked", is_running=is_running)
            return is_running
        except Exception as e:
            wrapped_error = wrap_naq_exception(e, "Failed to get worker status")
            self._error_handler.handle_error(
                wrapped_error, {"operation": "worker_status"}
            )
            raise
