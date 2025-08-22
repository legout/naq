"""Error handling module for the worker.

This module provides error handling functionality for job execution and unexpected errors.
"""

import time
import traceback
from typing import Optional

from loguru import logger
from nats.aio.msg import Msg

from ..models.jobs import Job
from ..services import ServiceManager, JobService, EventService
from ..models.events import JobEvent


class JobErrorHandler:
    """Handles errors that occur during job execution."""

    def __init__(self, service_manager: ServiceManager):
        """Initialize the error handler with a service manager.

        Args:
            service_manager: The ServiceManager instance for accessing services.
        """
        self._service_manager = service_manager
        self._job_service: Optional[JobService] = None
        self._event_service: Optional[EventService] = None

    async def _get_services(self) -> None:
        """Get service instances from the service manager."""
        if self._job_service is None:
            self._job_service = await self._service_manager.get_service(
                "job_service", JobService
            )
        if self._event_service is None:
            self._event_service = await self._service_manager.get_service(
                "event_service", EventService
            )

    async def handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        await self._get_services()

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

            # Use JobService to handle job failure
            if self._job_service:
                try:
                    error = Exception(job.error or "Job execution failed")
                    await self._job_service.handle_job_failure(
                        job=job,
                        error=error,
                        worker_id="unknown-worker",
                        start_time=time.time(),
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to handle job failure via JobService: {e}",
                        exc_info=True,
                    )

            # Log failure event using EventService
            if self._event_service:
                try:
                    failure_event = JobEvent.failed(
                        job_id=job.job_id,
                        worker_id="unknown-worker",
                        error_type="JobExecutionError",
                        error_message=job.error or "Job execution failed",
                        duration_ms=0,
                        queue_name=job.queue_name,
                    )
                    await self._event_service.log_job_event(failure_event)
                except Exception as e:
                    logger.error(
                        f"Failed to log failure event via EventService: {e}",
                        exc_info=True,
                    )

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
        await self._get_services()

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

                # Use JobService to handle job failure
                if self._job_service:
                    try:
                        await self._job_service.handle_job_failure(
                            job=job,
                            error=error,
                            worker_id="unknown-worker",
                            start_time=time.time(),
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to handle job failure via JobService: {e}",
                            exc_info=True,
                        )

                # Log failure event using EventService
                if self._event_service:
                    try:
                        failure_event = JobEvent.failed(
                            job_id=job.job_id,
                            worker_id="unknown-worker",
                            error_type=type(error).__name__,
                            error_message=str(error),
                            duration_ms=0,
                            queue_name=job.queue_name,
                        )
                        await self._event_service.log_job_event(failure_event)
                    except Exception as e:
                        logger.error(
                            f"Failed to log failure event via EventService: {e}",
                            exc_info=True,
                        )

            await msg.term()
            logger.warning(
                f"Terminated message Sid='{msg.sid}' due to unexpected processing error."
            )
        except Exception as term_e:
            logger.error(
                f"Failed to Terminate message Sid='{msg.sid}': {term_e}", exc_info=True
            )
