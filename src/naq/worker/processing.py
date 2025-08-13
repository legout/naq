"""Job processing module for the worker.

This module provides functionality for processing job messages.
"""

import asyncio
import traceback
from typing import Any, Optional

from loguru import logger
from nats.aio.msg import Msg

from ..models.jobs import Job
from ..models.enums import JOB_STATUS, WORKER_STATUS
from .error_handling import JobErrorHandler


class JobProcessor:
    """Handles the processing of job messages."""

    def __init__(self, worker):
        """Initialize the job processor with a reference to the worker."""
        self.worker = worker
        self.error_handler = JobErrorHandler(worker)

    async def process_message(self, msg: Any) -> None:
        """Process a received job message."""
        job = None
        try:
            # Deserialize the job from the message data
            if hasattr(msg, "data"):
                job = Job.deserialize(msg.data)
            else:
                # For testing where msg might be a Job directly
                job = msg

            if self.worker._shutdown_event.is_set():
                logger.info(
                    f"Shutdown in progress. Job {job.job_id if job else 'unknown'} will not be processed."
                )
                if hasattr(msg, "nak"):  # NAK the message so it can be re-queued
                    await msg.nak()
                return  # Do not process if shutdown is initiated

            # Update worker status to busy with this job
            await self.worker.status_manager.update_status(
                WORKER_STATUS.BUSY, job_id=job.job_id
            )

            # Execute the job
            try:
                if job.timeout is not None and job.timeout > 0:
                    await asyncio.wait_for(job.execute(), timeout=job.timeout)
                else:
                    await job.execute()
            except asyncio.TimeoutError:
                job.error = f"Job timed out after {job.timeout} seconds"
                job.traceback = traceback.format_exc()
            except Exception as e:
                # Catch any other exceptions from job execution
                job.error = str(e)
                job.traceback = traceback.format_exc()

            # Store result (which includes error/traceback if any)
            await self.worker.job_manager.store_result(job)

            # Handle failure if needed (e.g., publish to dead-letter queue)
            if (
                job.status == JOB_STATUS.FAILED
            ):  # status is a property derived from job.error
                await self.worker.failed_handler.handle_failed_job(job)

            # Acknowledge message processing complete
            if hasattr(msg, "ack"):
                await msg.ack()

        except Exception as e:
            logger.error(
                f"Error processing job {job.job_id if job else 'unknown'}: {e}",
                exc_info=True,
            )
            # If we have a NATS message and it has a term() method, terminate it
            if hasattr(msg, "term"):
                await msg.term()
        finally:
            # Update worker status back to idle
            await self.worker.status_manager.update_status(WORKER_STATUS.IDLE)