# src/naq/worker/failed.py
"""
Failed job handling for NAQ workers.

This module contains the FailedJobHandler class for managing failed job
processing, retry logic, and failed job stream management.
"""

from typing import TYPE_CHECKING

from loguru import logger

from ..exceptions import SerializationError
from ..models import Job
from ..settings import (
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
)

if TYPE_CHECKING:
    from .core import Worker


class FailedJobHandler:
    """
    Handles failed job processing and storage.
    """

    def __init__(self, worker: "Worker"):
        self.worker = worker

    async def handle_failed_job(self, job: Job) -> None:
        """Handle a failed job by publishing it to the failed job stream using services."""
        subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name}"
        try:
            from ..services.streams import StreamService
            from ..services.connection import ConnectionService
            
            stream_service = await self.worker._service_manager.get_service(StreamService)
            connection_service = await self.worker._service_manager.get_service(ConnectionService)

            # Ensure the failed job stream exists
            await stream_service.ensure_stream(
                name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
            )

            # Publish the failed job using connection service
            payload = job.serialize_failed_job()
            async with connection_service.jetstream_scope() as js:
                await js.publish(subject, payload)
            logger.info(f"Published failed job {job.job_id} to {subject}")
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id}: {e}", exc_info=True
            )

    async def initialize(self) -> None:
        """Initialize the failed job handler using services."""
        # No initialization needed for services-based approach
        pass

    async def publish_failed_job(self, job: Job) -> None:
        """Publishes failed job details to the failed job subject using services."""
        # This method now delegates to handle_failed_job for consistency
        await self.handle_failed_job(job)