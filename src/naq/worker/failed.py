"""Failed job handling module.

This module provides functionality for handling failed job processing and storage.
It is responsible for managing failed job streams and publishing failed job details.
"""

from typing import Optional

from loguru import logger

from ..exceptions import SerializationError
from ..models.events import JobEvent
from ..models.enums import JobEventType
from ..models.jobs import Job
from ..services import ServiceManager, ConnectionService, StreamService, EventService
from ..settings import (
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
)


class FailedJobHandler:
    """
    Handles failed job processing and storage using the service layer.
    
    This class is responsible for managing failed job streams and publishing
    failed job details using the ConnectionService, StreamService, and EventService.
    """

    def __init__(self, service_manager: ServiceManager):
        """Initialize the failed job handler.

        Args:
            service_manager: The ServiceManager instance for accessing services.
        """
        self._service_manager = service_manager
        self._connection_service: Optional[ConnectionService] = None
        self._stream_service: Optional[StreamService] = None
        self._event_service: Optional[EventService] = None

    async def _get_services(self) -> None:
        """Get service instances from the service manager.
        
        This method retrieves the ConnectionService, StreamService, and EventService
        from the ServiceManager if they haven't been retrieved yet.
        """
        if self._connection_service is None:
            self._connection_service = await self._service_manager.get_service("connection", ConnectionService)
        if self._stream_service is None:
            self._stream_service = await self._service_manager.get_service("stream", StreamService)
        if self._event_service is None:
            self._event_service = await self._service_manager.get_service("event", EventService)

    async def handle_failed_job(self, job: Job) -> None:
        """Handle a failed job by publishing it to the failed job stream.
        
        This method uses the new context managers to ensure the failed job stream exists,
        get a JetStream context for publishing, and log the failed job event.
        """
        await self._get_services()
        
        subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name}"
        try:
            # Use the new context manager for JetStream operations
            from ..connection.context_managers import nats_jetstream
            from ..services.config import create_global_config
            
            # Create config
            config = create_global_config()
            
            # Use the JetStream context manager
            async with nats_jetstream(config) as (conn, js):
                # Ensure the failed job stream exists using the StreamService if available
                if self._stream_service:
                    await self._stream_service.ensure_stream(
                        stream_name=FAILED_JOB_STREAM_NAME,
                        subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
                    )

                # Publish the failed job
                payload = job.serialize_failed_job()
                await js.publish(subject, payload)
                logger.info(f"Published failed job {job.job_id} to {subject}")
            
            # Log the failed job event
            if self._event_service:
                event = JobEvent.failed(
                    job_id=job.job_id,
                    worker_id="unknown",  # Will be set by the caller
                    error_type="JobError",
                    error_message="Job failed during execution",
                    duration_ms=0.0,  # Will be set by the caller
                    queue_name=job.queue_name,
                    details={"subject": subject}
                )
                await self._event_service.log_job_event(event)
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id}: {e}", exc_info=True
            )

    async def initialize(self) -> None:
        """Initialize the failed job handler with services.
        
        This method retrieves the required services from the ServiceManager
        and gets a JetStream context from the ConnectionService.
        """
        await self._get_services()
        if self._connection_service:
            self._js = await self._connection_service.get_jetstream()
        await self._ensure_failed_stream()

    async def _ensure_failed_stream(self) -> None:
        """Ensures the stream for failed jobs exists."""
        await self._get_services()
        
        if not self._stream_service:
            logger.error(
                "StreamService not available, cannot ensure failed stream."
            )
            return

        try:
            await self._stream_service.ensure_stream(
                stream_name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
            )
        except Exception as e:
            # Log the error but allow the worker to continue if possible
            logger.error(
                f"Failed to ensure failed jobs stream '{FAILED_JOB_STREAM_NAME}': {e}",
                exc_info=True,
            )

    async def publish_failed_job(self, job: Job) -> None:
        """Publishes failed job details to the failed job subject.
        
        This method uses the new context managers to get a JetStream context for publishing
        and log the failed job event.
        """
        await self._get_services()
        
        failed_subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name or 'unknown'}"
        try:
            # Use the new context manager for JetStream operations
            from ..connection.context_managers import nats_jetstream
            from ..services.config import create_global_config
            
            # Create config
            config = create_global_config()
            
            # Use the JetStream context manager
            async with nats_jetstream(config) as (conn, js):
                # Publish the failed job
                payload = job.serialize_failed_job()
                await js.publish(failed_subject, payload)
                logger.info(
                    f"Published failed job {job.job_id} details "
                    f"to subject '{failed_subject}'."
                )
            
            # Log the failed job event
            if self._event_service:
                event = JobEvent.failed(
                    job_id=job.job_id,
                    worker_id="unknown",  # Will be set by the caller
                    error_type="JobError",
                    error_message="Job failed during execution",
                    duration_ms=0.0,  # Will be set by the caller
                    queue_name=job.queue_name,
                    details={"subject": failed_subject}
                )
                await self._event_service.log_job_event(event)
        except SerializationError as e:
            logger.error(
                f"Could not serialize failed job {job.job_id} details: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id} "
                f"to subject '{failed_subject}': {e}",
                exc_info=True,
            )
