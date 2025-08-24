"""Job processing module for the worker.

This module provides functionality for processing job messages.
"""

import asyncio
import traceback
from typing import Any

from ..models.enums import JOB_STATUS, WORKER_STATUS
from ..models.jobs import Job
from ..utils.decorators import retry, timing
from ..utils.error_handling import ErrorHandler
from ..utils.logging import StructuredLogger
from .error_handling import JobErrorHandler


class JobProcessor:
    """Handles the processing of job messages."""

    def __init__(self, worker):
        """Initialize the job processor with a reference to the worker."""
        self.worker = worker
        self.error_handler = JobErrorHandler(worker._service_manager)
        self.logger = StructuredLogger(__name__)
        self.error_handler = ErrorHandler()

    @timing()
    @retry(max_attempts=3, delay=1.0, backoff="exponential")
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
                self.logger.info(
                    "Shutdown in progress. Job {job_id} will not be processed.",
                    job_id=job.job_id if job else "unknown",
                )
                if hasattr(msg, "nak"):  # NAK the message so it can be re-queued
                    await msg.nak()
                return  # Do not process if shutdown is initiated

            # Update worker status to busy with this job
            await self.worker.status_manager.update_status(
                WORKER_STATUS.BUSY, job_id=job.job_id
            )

            # Log worker_busy event
            if hasattr(self.worker, "_event_service") and self.worker._event_service:
                import os
                import socket

                from ..models.enums import WorkerEventType
                from ..models.events import WorkerEvent

                event = WorkerEvent.job_started(
                    worker_id=self.worker.worker_id,
                    job_id=job.job_id,
                    queue_names=self.worker.queue_names,
                    details={
                        "hostname": socket.gethostname(),
                        "pid": os.getpid(),
                        "concurrency": self.worker._concurrency,
                        "job_timeout": job.timeout,
                    },
                )
                await self.worker._event_service.log_worker_event(event)

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
            self.error_handler.handle_error(
                e,
                "Error processing job {job_id}",
                job_id=job.job_id if job else "unknown",
                exc_info=True,
            )
            # If we have a NATS message and it has a term() method, terminate it
            if hasattr(msg, "term"):
                await msg.term()
        finally:
            # Update worker status back to idle
            await self.worker.status_manager.update_status(WORKER_STATUS.IDLE)

            # Log worker_idle event
            if hasattr(self.worker, "_event_service") and self.worker._event_service:
                import os
                import socket

                from ..models.enums import WorkerEventType
                from ..models.events import WorkerEvent

                # Calculate active jobs (concurrency - available slots)
                active_jobs = 0
                if hasattr(self.worker, "_semaphore"):
                    active_jobs = (
                        self.worker._concurrency - self.worker._semaphore._value
                    )

                event = WorkerEvent(
                    worker_id=self.worker.worker_id,
                    event_type=WorkerEventType.HEARTBEAT,  # Use HEARTBEAT as base type for idle
                    queue_names=self.worker.queue_names,
                    message="Worker idle",
                    details={
                        "hostname": socket.gethostname(),
                        "pid": os.getpid(),
                        "concurrency": self.worker._concurrency,
                        "active_jobs": active_jobs,
                        "status": "idle",
                    },
                )
                await self.worker._event_service.log_worker_event(event)
