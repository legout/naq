"""Error handling module for the worker.

This module provides error handling functionality for job execution and unexpected errors.
"""

import time
import traceback
from typing import Optional

from nats.aio.msg import Msg

from ..nats_client import NatsClient
from ..config import get_config
from ..models.events import JobEvent
from ..models.jobs import Job
from ..utils.decorators import timing
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger


class JobErrorHandler:
    """Handles errors that occur during job execution."""

    def __init__(self, nats_client: Optional[NatsClient] = None):
        """Initialize the error handler with a NATS client.

        Args:
            nats_client: Optional NatsClient instance for accessing NATS.
        """
        self._nats_client = nats_client

        # Initialize logging and error handling
        self._logger = StructuredLogger("job_error_handler")
        self._error_handler = ErrorHandler(self._logger)

    @timing(threshold_ms=100)
    async def _get_nats_client(self) -> NatsClient:
        """Get or create a NATS client."""
        if self._nats_client is None:
            try:
                config = get_config()
                self._nats_client = NatsClient(config)
                await self._nats_client.connect()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to get NATS client")
                self._error_handler.handle_error(wrapped_error)
                raise
        return self._nats_client

    @timing(threshold_ms=500)
    async def handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        with self._logger.operation_context(
            "handle_job_execution_error", job_id=job.job_id if job else None
        ):
            try:
                nats_client = await self._get_nats_client()

                if job is None:
                    self._logger.error(
                        "Job object is None after JobExecutionError, cannot handle retry/failure"
                    )
                    await msg.term()
                    return

                self._logger.warning(
                    "Job failed execution", job_id=job.job_id, error=job.error
                )

                # --- Retry Logic ---
                attempt = msg.metadata.num_delivered
                max_retries = job.max_retries if job.max_retries is not None else 0

                if attempt <= max_retries:
                    delay = job.get_retry_delay(attempt)
                    self._logger.info(
                        "Job failed, scheduling retry",
                        job_id=job.job_id,
                        attempt=attempt,
                        max_retries=max_retries,
                        delay_seconds=delay,
                    )
                    try:
                        await msg.nak(delay=delay)
                        self._logger.debug("Message Nak'd for retry", sid=msg.sid)
                    except Exception as nak_e:
                        wrapped_error = wrap_naq_exception(
                            nak_e, "Failed to NAK message for retry"
                        )
                        self._error_handler.handle_error(
                            wrapped_error, {"sid": msg.sid, "job_id": job.job_id}
                        )
                        await msg.term()  # Terminate if NAK fails
                else:
                    # --- Terminal Failure ---
                    self._logger.error(
                        "Job failed after retries, moving to failed queue",
                        job_id=job.job_id,
                        attempt=attempt - 1,
                    )

                    # Note: JobService functionality has been removed as part of service layer removal
                    # This can be re-implemented later if needed using a different approach

                    # Note: EventService functionality has been removed as part of service layer removal
                    # This can be re-implemented later if needed using a different approach

                    try:
                        await msg.ack()  # Ack original message after handling failure
                        self._logger.debug(
                            "Message acknowledged after moving to failed queue",
                            sid=msg.sid,
                        )
                    except Exception as ack_e:
                        wrapped_error = wrap_naq_exception(
                            ack_e, "Failed to ACK message after moving to failed queue"
                        )
                        self._error_handler.handle_error(
                            wrapped_error, {"sid": msg.sid, "job_id": job.job_id}
                        )
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Error handling job execution error"
                )
                self._error_handler.handle_error(
                    wrapped_error,
                    {"job_id": job.job_id if job else None, "sid": msg.sid},
                )
                raise

    @timing(threshold_ms=500)
    async def handle_unexpected_error(
        self, job: Optional[Job], msg: Msg, error: Exception
    ) -> None:
        """Handle unexpected errors during message processing."""
        with self._logger.operation_context(
            "handle_unexpected_error", job_id=job.job_id if job else None, sid=msg.sid
        ):
            try:
                nats_client = await self._get_nats_client()

                self._logger.error(
                    "Unhandled error processing message",
                    sid=msg.sid,
                    job_id=job.job_id if job else None,
                    error=str(error),
                    error_type=type(error).__name__,
                )

                # Update status to failed if possible, otherwise terminate
                if job:
                    job.error = (
                        f"Worker processing error: {error}"  # Assign error for storage
                    )
                    job.traceback = traceback.format_exc()

                    # Note: JobService functionality has been removed as part of service layer removal
                    # This can be re-implemented later if needed using a different approach

                    # Note: EventService functionality has been removed as part of service layer removal
                    # This can be re-implemented later if needed using a different approach

                await msg.term()
                self._logger.warning(
                    "Terminated message due to unexpected processing error", sid=msg.sid
                )
            except Exception as term_e:
                wrapped_error = wrap_naq_exception(
                    term_e, "Failed to terminate message"
                )
                self._error_handler.handle_error(
                    wrapped_error,
                    {"sid": msg.sid, "job_id": job.job_id if job else None},
                )
