"""
Failed Job Handler Module

This module contains the FailedJobHandler class responsible for handling failed job processing and storage.
"""

import asyncio
from typing import TYPE_CHECKING, Optional

from loguru import logger
from nats.js import JetStreamContext

from ..connection import ensure_stream
from ..exceptions import SerializationError
from ..models import Job
from ..settings import FAILED_JOB_STREAM_NAME, FAILED_JOB_SUBJECT_PREFIX

if TYPE_CHECKING:
    from ..worker import Worker


class FailedJobHandler:
    """
    Handles failed job processing and storage.
    """

    def __init__(self, worker: "Worker"):
        """
        Initialize the failed job handler.
        
        Args:
            worker: The worker instance this handler belongs to.
        """
        self.worker = worker
        self._js: Optional[JetStreamContext] = None

    async def handle_failed_job(self, job: Job) -> None:
        """
        Handle a failed job by publishing it to the failed job stream.
        
        Args:
            job: The failed job to handle.
        """
        if not self.worker._js:
            logger.error(
                f"Cannot handle failed job {job.job_id}, JetStream context not available"
            )
            return

        subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name}"
        try:
            # Ensure the failed job stream exists
            await ensure_stream(
                js=self.worker._js,
                stream_name=FAILED_JOB_STREAM_NAME,
                subjects=[f"{FAILED_JOB_SUBJECT_PREFIX}.*"],
            )

            # Publish the failed job
            payload = job.serialize_failed_job()
            await self.worker._js.publish(subject, payload)
            logger.info(f"Published failed job {job.job_id} to {subject}")
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id}: {e}", exc_info=True
            )

    async def initialize(self, js: JetStreamContext) -> None:
        """
        Initialize the failed job handler with a JetStream context.
        
        Args:
            js: The JetStream context to use.
        """
        self._js = js
        await self._ensure_failed_stream()

    async def _ensure_failed_stream(self) -> None:
        """
        Ensures the stream for failed jobs exists.
        """
        if not self._js:
            logger.error(
                "JetStream context not available, cannot ensure failed stream."
            )
            return

        try:
            await ensure_stream(
                js=self._js,
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
        """
        Publishes failed job details to the failed job subject.
        
        Args:
            job: The failed job to publish.
        """
        if not self._js:
            logger.error(
                f"Cannot publish failed job {job.job_id}, JetStream context not available."
            )
            return

        failed_subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name or 'unknown'}"
        try:
            payload = job.serialize_failed_job()
            await self._js.publish(failed_subject, payload)
            logger.info(
                f"Published failed job {job.job_id} details to subject '{failed_subject}'."
            )
        except SerializationError as e:
            logger.error(
                f"Could not serialize failed job {job.job_id} details: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Failed to publish failed job {job.job_id} to subject '{failed_subject}': {e}",
                exc_info=True,
            )