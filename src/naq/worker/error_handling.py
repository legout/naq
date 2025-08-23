"""Error handling module for the worker.

This module provides error handling functionality for job execution and unexpected errors.
"""

import time
import traceback
from typing import Optional

from nats.aio.msg import Msg

from ..models.jobs import Job
from ..services import ServiceManager, JobService, EventService
from ..models.events import JobEvent
from ..utils.logging import StructuredLogger
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.decorators import timing


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
        
        # Initialize logging and error handling
        self._logger = StructuredLogger("job_error_handler")
        self._error_handler = ErrorHandler(self._logger)

    @timing(threshold_ms=100)
    async def _get_services(self) -> None:
        """Get service instances from the service manager."""
        with self._logger.operation_context("get_services"):
            try:
                if self._job_service is None:
                    self._job_service = await self._service_manager.get_service(
                        "job_service", JobService
                    )
                if self._event_service is None:
                    self._event_service = await self._service_manager.get_service(
                        "event_service", EventService
                    )
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to get services")
                self._error_handler.handle_error(wrapped_error)
                raise

    @timing(threshold_ms=500)
    async def handle_job_execution_error(self, job: Optional[Job], msg: Msg) -> None:
        """Handle errors from job execution."""
        with self._logger.operation_context("handle_job_execution_error", job_id=job.job_id if job else None):
            try:
                await self._get_services()

                if job is None:
                    self._logger.error("Job object is None after JobExecutionError, cannot handle retry/failure")
                    await msg.term()
                    return

                self._logger.warning("Job failed execution", job_id=job.job_id, error=job.error)

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
                        delay_seconds=delay
                    )
                    try:
                        await msg.nak(delay=delay)
                        self._logger.debug("Message Nak'd for retry", sid=msg.sid)
                    except Exception as nak_e:
                        wrapped_error = wrap_naq_exception(nak_e, "Failed to NAK message for retry")
                        self._error_handler.handle_error(
                            wrapped_error,
                            {"sid": msg.sid, "job_id": job.job_id}
                        )
                        await msg.term()  # Terminate if NAK fails
                else:
                    # --- Terminal Failure ---
                    self._logger.error(
                        "Job failed after retries, moving to failed queue",
                        job_id=job.job_id,
                        attempt=attempt - 1
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
                            wrapped_error = wrap_naq_exception(e, "Failed to handle job failure via JobService")
                            self._error_handler.handle_error(
                                wrapped_error,
                                {"job_id": job.job_id}
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
                            wrapped_error = wrap_naq_exception(e, "Failed to log failure event via EventService")
                            self._error_handler.handle_error(
                                wrapped_error,
                                {"job_id": job.job_id}
                            )

                    try:
                        await msg.ack()  # Ack original message after handling failure
                        self._logger.debug("Message acknowledged after moving to failed queue", sid=msg.sid)
                    except Exception as ack_e:
                        wrapped_error = wrap_naq_exception(ack_e, "Failed to ACK message after moving to failed queue")
                        self._error_handler.handle_error(
                            wrapped_error,
                            {"sid": msg.sid, "job_id": job.job_id}
                        )
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error handling job execution error")
                self._error_handler.handle_error(
                    wrapped_error,
                    {"job_id": job.job_id if job else None, "sid": msg.sid}
                )
                raise

    @timing(threshold_ms=500)
    async def handle_unexpected_error(
        self, job: Optional[Job], msg: Msg, error: Exception
    ) -> None:
        """Handle unexpected errors during message processing."""
        with self._logger.operation_context(
            "handle_unexpected_error",
            job_id=job.job_id if job else None,
            sid=msg.sid
        ):
            try:
                await self._get_services()

                self._logger.error(
                    "Unhandled error processing message",
                    sid=msg.sid,
                    job_id=job.job_id if job else None,
                    error=str(error),
                    error_type=type(error).__name__
                )
                
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
                            wrapped_error = wrap_naq_exception(e, "Failed to handle job failure via JobService")
                            self._error_handler.handle_error(
                                wrapped_error,
                                {"job_id": job.job_id}
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
                            wrapped_error = wrap_naq_exception(e, "Failed to log failure event via EventService")
                            self._error_handler.handle_error(
                                wrapped_error,
                                {"job_id": job.job_id}
                            )

                await msg.term()
                self._logger.warning(
                    "Terminated message due to unexpected processing error",
                    sid=msg.sid
                )
            except Exception as term_e:
                wrapped_error = wrap_naq_exception(term_e, "Failed to terminate message")
                self._error_handler.handle_error(
                    wrapped_error,
                    {"sid": msg.sid, "job_id": job.job_id if job else None}
                )
