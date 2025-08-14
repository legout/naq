# src/naq/worker/jobs.py
"""
Job status management module.

This module provides the JobStatusManager class which is responsible for:
- Tracking job status and dependencies
- Storing job results
- Managing job lifecycle
"""

import asyncio
from typing import Optional

import cloudpickle
import nats
from loguru import logger
from nats.js.kv import KeyValue
from nats.js.errors import BucketNotFoundError
from nats.js import JetStreamContext

from ..models.jobs import Job
from ..models.enums import JOB_STATUS
from ..settings import (
    JOB_STATUS_KV_NAME,
    JOB_STATUS_TTL_SECONDS,
    RESULT_KV_NAME,
    DEFAULT_RESULT_TTL_SECONDS,
)
from ..results import Results


class JobStatusManager:
    """
    Manages job status tracking and dependency resolution.
    """

    def __init__(self, worker):
        self.worker = worker
        self._result_kv_store = None

    async def _get_result_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return NATS KV store for results."""
        if self._result_kv_store is None:
            if not self.worker._js:
                logger.error("JetStream context not available")
                return None
            try:
                self._result_kv_store = await self.worker._js.key_value(
                    bucket=RESULT_KV_NAME
                )
            except BucketNotFoundError:
                try:
                    self._result_kv_store = await self.worker._js.create_key_value(
                        bucket=RESULT_KV_NAME,
                        description="Stores job results and errors",
                    )
                except Exception as e:
                    logger.error(f"Failed to create result KV store: {e}")
                    self._result_kv_store = None
            except Exception as e:
                logger.error(f"Failed to get result KV store: {e}")
                self._result_kv_store = None
        return self._result_kv_store

    async def update_job(self, job: Job) -> None:
        """Update job status and result in KV store."""
        kv_store = await self._get_result_kv_store()
        if not kv_store:
            logger.warning(
                f"Result KV store not available. Cannot update status for job {job.job_id}"
            )
            return

        try:
            payload = {
                "status": job.status.value,
                "result": job.result if hasattr(job, "result") else None,
                "error": str(job.error) if job.error else None,
                "traceback": job.traceback,
                "job_id": job.job_id,
                "queue_name": job.queue_name,
                "started_at": job.started_at,
                "finished_at": job.finished_at,
            }
            serialized_payload = cloudpickle.dumps(payload)
            await kv_store.put(job.job_id, serialized_payload)
            logger.debug(f"Updated status for job {job.job_id} to {job.status.value}")
        except Exception as e:
            logger.error(f"Failed to update job status: {e}")

    async def initialize(self, js: JetStreamContext) -> None:
        """Initialize the job status manager with a JetStream context."""
        await self._initialize_status_kv(js)
        await self._initialize_result_kv(js)

    async def _initialize_status_kv(self, js: JetStreamContext) -> None:
        """Initialize the job status KV store."""
        try:
            self._status_kv = await js.key_value(bucket=JOB_STATUS_KV_NAME)
            logger.info(f"Bound to job status KV store: '{JOB_STATUS_KV_NAME}'")
        except BucketNotFoundError:
            logger.warning(
                f"Job status KV store '{JOB_STATUS_KV_NAME}' not found. Creating..."
            )
            try:
                # Use integer seconds for TTL
                status_ttl_seconds = (
                    int(JOB_STATUS_TTL_SECONDS) if JOB_STATUS_TTL_SECONDS > 0 else 0
                )
                logger.info(
                    f"Creating job status KV store '{JOB_STATUS_KV_NAME}' with default TTL: {status_ttl_seconds}s"
                )
                self._status_kv = await js.create_key_value(
                    bucket=JOB_STATUS_KV_NAME,
                    ttl=status_ttl_seconds,
                    description="Stores naq job completion status for dependencies",
                )
                logger.info(f"Created job status KV store: '{JOB_STATUS_KV_NAME}'")
            except Exception as create_e:
                logger.error(
                    f"Failed to create job status KV store '{JOB_STATUS_KV_NAME}': {create_e}",
                    exc_info=True,
                )
                # Worker might still function but dependencies won't work reliably
                self._status_kv = None
        except Exception as e:
            logger.error(
                f"Failed to bind to job status KV store '{JOB_STATUS_KV_NAME}': {e}",
                exc_info=True,
            )
            self._status_kv = None

    async def _initialize_result_kv(self, js: JetStreamContext) -> None:
        """Initialize the result KV store."""
        try:
            self._result_kv_store = await js.key_value(bucket=RESULT_KV_NAME)
            logger.info(f"Bound to result KV store: '{RESULT_KV_NAME}'")
        except BucketNotFoundError:
            logger.warning(f"Result KV store '{RESULT_KV_NAME}' not found. Creating...")
            try:
                # Use integer seconds for TTL
                default_ttl_seconds = (
                    int(DEFAULT_RESULT_TTL_SECONDS)
                    if DEFAULT_RESULT_TTL_SECONDS > 0
                    else 0
                )
                logger.info(
                    f"Creating result KV store '{RESULT_KV_NAME}' with default TTL: {default_ttl_seconds}s"
                )
                self._result_kv_store = await js.create_key_value(
                    bucket=RESULT_KV_NAME,
                    ttl=default_ttl_seconds,
                    description="Stores naq job results and errors",
                )
                logger.info(f"Created result KV store: '{RESULT_KV_NAME}'")
            except Exception as create_e:
                logger.error(
                    f"Failed to create result KV store '{RESULT_KV_NAME}': {create_e}",
                    exc_info=True,
                )
                self._result_kv_store = (
                    None  # Continue without result backend if creation fails
                )
        except Exception as e:
            logger.error(
                f"Failed to bind to result KV store '{RESULT_KV_NAME}': {e}",
                exc_info=True,
            )
            self._result_kv_store = None

    async def check_dependencies(self, job: Job) -> bool:
        """Checks if all dependencies for the job are met."""
        if not job.dependency_ids:
            return True  # No dependencies

        if not self._status_kv:
            logger.warning(
                f"Job status KV store not available. Cannot check dependencies for job {job.job_id}. Assuming met."
            )
            return True

        logger.debug(
            f"Checking dependencies for job {job.job_id}: {job.dependency_ids}"
        )
        try:
            for dep_id in job.dependency_ids:
                try:
                    entry = await self._status_kv.get(dep_id.encode("utf-8"))
                    status = entry.value.decode("utf-8")
                    if status == JOB_STATUS.COMPLETED.value:
                        logger.debug(
                            f"Dependency {dep_id} for job {job.job_id} is completed."
                        )
                        continue  # Dependency met
                    elif status == JOB_STATUS.FAILED.value:
                        logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} failed. Job {job.job_id} will not run."
                        )
                        return False
                    else:
                        # Unknown status? Treat as unmet for safety.
                        logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} has unknown status '{status}'. Treating as unmet."
                        )
                        return False
                except nats.js.errors.KeyNotFoundError:
                    # Dependency status not found, means it hasn't completed yet
                    logger.debug(
                        f"Dependency {dep_id} for job {job.job_id} not found in status KV. Not met yet."
                    )
                    return False
            # If loop completes, all dependencies were found and completed
            logger.debug(f"All dependencies met for job {job.job_id}.")
            return True
        except Exception as e:
            logger.error(
                f"Error checking dependencies for job {job.job_id}: {e}", exc_info=True
            )
            return False  # Assume dependencies not met on error

    async def update_job_status(self, job_id: str, status: JOB_STATUS) -> None:
        """Updates the job status in the KV store."""
        if not self._status_kv:
            logger.warning(
                f"Job status KV store not available. Cannot update status for job {job_id}."
            )
            return

        logger.debug(f"Updating status for job {job_id} to '{status.value}'")
        try:
            await self._status_kv.put(job_id, status.value.encode("utf-8"))
        except Exception as e:
            logger.error(
                f"Failed to update status for job {job_id} to '{status.value}': {e}",
                exc_info=True,
            )

async def store_result(self, job: Job) -> None:
        """Stores the job result or failure info using the Results class."""
        try:
            logger.debug(f"Storing result for job {job.job_id}")
            logger.debug(f"Job status: {job.status}")
            logger.debug(f"Job error: {job.error}")
            logger.debug(f"Job result: {job.result}")
            
            # Create a Results instance with service manager if available
            results_manager = Results(
                nats_url=self.worker._nats_url,
                service_manager=getattr(self.worker, '_service_manager', None)
            )
            logger.debug(f"Created Results manager with service manager: {getattr(self.worker, '_service_manager', None)}")
            
            # Prepare result data
            if job.error:
                # Store failure information
                result_data = {
                    "status": JOB_STATUS.FAILED.value,
                    "error": job.error,
                    "traceback": job.traceback,
                }
                logger.debug(f"Storing failure info for job {job.job_id}")
            else:
                # Store successful result
                result_data = {
                    "status": JOB_STATUS.COMPLETED.value,
                    "result": job.result,
                }
                logger.debug(f"Storing success result for job {job.job_id}")
            
            # Use the Results class to store the result
            logger.debug(f"Calling add_job_result for job {job.job_id}")
            await results_manager.add_job_result(
                job_id=job.job_id,
                result_data=result_data,
                result_ttl=job.result_ttl
            )
            logger.debug(f"Finished storing result for job {job.job_id}")
            # Add a small delay to ensure the write is processed
            await asyncio.sleep(0.01)
            
        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            logger.error(
                f"Failed to store result/failure info for job {job.job_id}: {e}",
                exc_info=True,
            )
            # Re-raise the exception so it can be handled by the caller
            raise