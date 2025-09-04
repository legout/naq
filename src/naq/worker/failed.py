"""Failed job handling module.

This module provides functionality for handling failed job processing and storage.
It is responsible for managing failed job streams and publishing failed job details.
"""

from typing import Optional

from ..nats_client import NatsClient
from ..config import get_config
from ..exceptions import SerializationError
from ..models.events import JobEvent
from ..models.jobs import Job
from ..config import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX
from ..utils.decorators import timing
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger


class FailedJobHandler:
    """
    Handles failed job processing and storage.

    This class is responsible for managing failed job streams and publishing
    failed job details using the NatsClient.
    """

    def __init__(self, nats_client: Optional[NatsClient] = None):
        """Initialize the failed job handler.

        Args:
            nats_client: Optional NatsClient instance for accessing NATS.
        """
        self._nats_client = nats_client

        # Initialize logging and error handling
        self._logger = StructuredLogger("failed_job_handler")
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
    async def handle_failed_job(self, job: Job) -> None:
        """Handle a failed job by publishing it to the failed job stream."""
        with self._logger.operation_context("handle_failed_job", job_id=job.job_id):
            try:
                nats_client = await self._get_nats_client()

                subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name}"

                # Ensure the failed job stream exists
                try:
                    await nats_client.ensure_stream(
                        stream_name=FAILED_JOB_STREAM_NAME,
                        subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
                    )
                except Exception as e:
                    self._logger.warning(
                        "Failed to ensure stream exists: {error}", error=str(e)
                    )

                # Publish the failed job
                js = await nats_client.get_jetstream()
                payload = job.serialize_failed_job()
                await js.publish(subject, payload)
                self._logger.info(
                    "Published failed job to stream",
                    job_id=job.job_id,
                    subject=subject,
                )

                # Note: EventService functionality has been removed as part of service layer removal
                # This can be re-implemented later if needed using a different approach
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to publish failed job")
                self._error_handler.handle_error(
                    wrapped_error, {"job_id": job.job_id, "subject": subject}
                )
                raise

    @timing(threshold_ms=500)
    async def initialize(self) -> None:
        """Initialize the failed job handler."""
        with self._logger.operation_context("initialize"):
            try:
                nats_client = await self._get_nats_client()
                await nats_client.get_jetstream()
                await self._ensure_failed_stream()
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Failed to initialize failed job handler"
                )
                self._error_handler.handle_error(wrapped_error)
                raise

    @timing(threshold_ms=500)
    async def _ensure_failed_stream(self) -> None:
        """Ensures the stream for failed jobs exists."""
        with self._logger.operation_context("ensure_failed_stream"):
            try:
                nats_client = await self._get_nats_client()

                try:
                    await nats_client.ensure_stream(
                        stream_name=FAILED_JOB_STREAM_NAME,
                        subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
                    )
                except Exception as e:
                    # Log the error but allow the worker to continue if possible
                    wrapped_error = wrap_naq_exception(
                        e, "Failed to ensure failed jobs stream"
                    )
                    self._error_handler.handle_error(
                        wrapped_error, {"stream_name": FAILED_JOB_STREAM_NAME}
                    )
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error ensuring failed stream")
                self._error_handler.handle_error(wrapped_error)
                raise

    @timing(threshold_ms=500)
    async def publish_failed_job(self, job: Job) -> None:
        """Publishes failed job details to the failed job subject."""
        with self._logger.operation_context("publish_failed_job", job_id=job.job_id):
            try:
                nats_client = await self._get_nats_client()

                failed_subject = (
                    f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name or 'unknown'}"
                )

                # Publish the failed job
                js = await nats_client.get_jetstream()
                payload = job.serialize_failed_job()
                await js.publish(failed_subject, payload)
                self._logger.info(
                    "Published failed job details to subject",
                    job_id=job.job_id,
                    subject=failed_subject,
                )

                # Note: EventService functionality has been removed as part of service layer removal
                # This can be re-implemented later if needed using a different approach
            except SerializationError as e:
                wrapped_error = wrap_naq_exception(
                    e, "Could not serialize failed job details"
                )
                self._error_handler.handle_error(wrapped_error, {"job_id": job.job_id})
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to publish failed job")
                self._error_handler.handle_error(
                    wrapped_error, {"job_id": job.job_id, "subject": failed_subject}
                )
                raise
