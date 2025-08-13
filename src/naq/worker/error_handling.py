"""Error handling module for the worker.

This module provides error handling functionality for job execution and unexpected errors.
"""

import traceback
from typing import Any, Optional

from loguru import logger
from nats.aio.msg import Msg

from ..models.jobs import Job
from ..models.enums import JOB_STATUS


class JobErrorHandler:
    """Handles errors that occur during job execution."""

    def __init__(self, worker):
        """Initialize the error handler with a reference to the worker."""
        self.worker = worker

    async def handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        if job is None:
            logger.error(
                "Job object is None after JobExecutionError, cannot handle retry/failure."
            )
            await msg.term()
            return

        logger.warning(f"Job {job.job_id} failed execution: {job.error}")

        # --- Retry Logic ---
        attempt = msg.metadata.num_delivered
        max_retries = job.max_retries if job.max_retries is not None else 0

        if attempt <= max_retries:
            delay = job.get_retry_delay(attempt)
            logger.info(
                f"Job {job.job_id} failed, scheduling retry {attempt}/{max_retries} "
                f"after {delay:.2f}s delay."
            )
            try:
                await msg.nak(delay=delay)
                logger.debug(f"Message Nak'd for retry: Sid='{msg.sid}'")
            except Exception as nak_e:
                logger.error(
                    f"Failed to NAK message Sid='{msg.sid}' for retry: {nak_e}",
                    exc_info=True,
                )
                await msg.term()  # Terminate if NAK fails
        else:
            # --- Terminal Failure ---
            logger.error(
                f"Job {job.job_id} failed after {attempt - 1} retries. Moving to failed queue."
            )
            await self.worker.job_manager.update_job_status(
                job.job_id, JOB_STATUS.FAILED
            )
            await self.worker.job_manager.store_result(job)
            await self.worker.failed_handler.publish_failed_job(job)
            try:
                await msg.ack()  # Ack original message after handling failure
                logger.debug(
                    f"Message acknowledged after moving to failed queue: Sid='{msg.sid}'"
                )
            except Exception as ack_e:
                logger.error(
                    f"Failed to ACK message Sid='{msg.sid}' after moving to failed queue: {ack_e}",
                    exc_info=True,
                )

    async def handle_unexpected_error(
        self, job: Optional[Job], msg: Msg, error: Exception
    ) -> None:
        """Handle unexpected errors during message processing."""
        logger.error(
            f"Unhandled error processing message (Sid='{msg.sid}', "
            f"JobId='{job.job_id if job else 'N/A'}'): {error}",
            exc_info=True,
        )
        try:
            # Update status to failed if possible, otherwise terminate
            if job:
                job.error = (
                    f"Worker processing error: {error}"  # Assign error for storage
                )
                job.traceback = traceback.format_exc()
                await self.worker.job_manager.update_job_status(
                    job.job_id, JOB_STATUS.FAILED
                )
                await self.worker.job_manager.store_result(job)
            await msg.term()
            logger.warning(
                f"Terminated message Sid='{msg.sid}' due to unexpected processing error."
            )
        except Exception as term_e:
            logger.error(
                f"Failed to Terminate message Sid='{msg.sid}': {term_e}", exc_info=True
            )