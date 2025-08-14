"""Job status management module.

This module provides functionality for managing job status tracking, dependency resolution,
and result storage. It is responsible for tracking job progress and managing job dependencies
through the service layer.
"""

import cloudpickle
from typing import Optional

from loguru import logger
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from ..models.jobs import Job
from ..models import JOB_STATUS
from ..settings import (
    DEFAULT_RESULT_TTL_SECONDS,
    JOB_STATUS_KV_NAME,
    JOB_STATUS_TTL_SECONDS,
    RESULT_KV_NAME,
)
from ..services import ServiceManager, ConnectionService, KVStoreService, JobService


class JobStatusManager:
    """
    Manages job status tracking and dependency resolution.
    """

    def __init__(self, worker, service_manager: Optional[ServiceManager] = None):
        """Initialize the job status manager.

        Args:
            worker: The worker instance this status manager belongs to.
            service_manager: Optional ServiceManager for accessing services.
        """
        self.worker = worker
        self._service_manager = service_manager
        self._connection_service: Optional[ConnectionService] = None
        self._kv_store_service: Optional[KVStoreService] = None
        self._job_service: Optional[JobService] = None
        self._result_kv_store = None

    async def _get_services(self) -> None:
        """Get service instances from ServiceManager."""
        if self._service_manager is None:
            logger.warning("ServiceManager not available, using direct worker connections")
            return

        try:
            self._connection_service = await self._service_manager.get_service("connection", ConnectionService)
            self._kv_store_service = await self._service_manager.get_service("kv_stores", KVStoreService)
            self._job_service = await self._service_manager.get_service("jobs", JobService)
            logger.debug("Successfully obtained services from ServiceManager")
        except Exception as e:
            logger.error(f"Failed to get services from ServiceManager: {e}")
            # Continue without services - will fall back to direct worker connections

    async def _get_result_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return NATS KV store for results."""
        if self._result_kv_store is None:
            # Try to use KVStoreService first
            if self._kv_store_service is not None:
                try:
                    self._result_kv_store = await self._kv_store_service.get_kv_store(RESULT_KV_NAME)
                    logger.debug(f"Got result KV store from KVStoreService: '{RESULT_KV_NAME}'")
                    return self._result_kv_store
                except Exception as e:
                    logger.error(f"Failed to get result KV store from KVStoreService: {e}")
            
            # Fall back to direct worker connection
            if not self.worker._js:
                logger.error("JetStream context not available")
                return None
            try:
                self._result_kv_store = await self.worker._js.key_value(
                    bucket=RESULT_KV_NAME
                )
            except Exception as e:
                try:
                    self._result_kv_store = await self.worker._js.create_key_value(
                        bucket=RESULT_KV_NAME,
                        description="Stores job results and errors",
                    )
                except Exception as create_e:
                    logger.error(f"Failed to create result KV store: {create_e}")
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
        # Get services from ServiceManager if available
        await self._get_services()
        await self._initialize_status_kv(js)
        await self._initialize_result_kv(js)

    async def _initialize_status_kv(self, js: JetStreamContext) -> None:
        """Initialize the job status KV store."""
        # Try to use KVStoreService first
        if self._kv_store_service is not None:
            try:
                self._status_kv = await self._kv_store_service.get_kv_store(JOB_STATUS_KV_NAME)
                logger.info(f"Bound to job status KV store from KVStoreService: '{JOB_STATUS_KV_NAME}'")
                return
            except Exception as e:
                logger.error(f"Failed to get job status KV store from KVStoreService: {e}")
        
        # Fall back to direct worker connection
        try:
            self._status_kv = await js.key_value(bucket=JOB_STATUS_KV_NAME)
            logger.info(f"Bound to job status KV store: '{JOB_STATUS_KV_NAME}'")
        except Exception as e:
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

    async def _initialize_result_kv(self, js: JetStreamContext) -> None:
        """Initialize the result KV store."""
        # Try to use KVStoreService first
        if self._kv_store_service is not None:
            try:
                self._result_kv_store = await self._kv_store_service.get_kv_store(RESULT_KV_NAME)
                logger.info(f"Bound to result KV store from KVStoreService: '{RESULT_KV_NAME}'")
                return
            except Exception as e:
                logger.error(f"Failed to get result KV store from KVStoreService: {e}")
        
        # Fall back to direct worker connection
        try:
            self._result_kv_store = await js.key_value(bucket=RESULT_KV_NAME)
            logger.info(f"Bound to result KV store: '{RESULT_KV_NAME}'")
        except Exception as e:
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

    async def check_dependencies(self, job: Job) -> bool:
        """Checks if all dependencies for the job are met."""
        if not job.dependency_ids:
            return True  # No dependencies

        # Try to use KVStoreService first
        if self._kv_store_service is not None:
            try:
                logger.debug(
                    f"Checking dependencies for job {job.job_id}: {job.dependency_ids}"
                )
                for dep_id in job.dependency_ids:
                    try:
                        status = await self._kv_store_service.get(JOB_STATUS_KV_NAME, dep_id, deserialize=False)
                        if isinstance(status, bytes):
                            status = status.decode("utf-8")
                        
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
                    except Exception:
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
                    f"Error checking dependencies for job {job.job_id} using KVStoreService: {e}", exc_info=True
                )
                # Fall back to direct KV store access
        
        # Fall back to direct KV store access
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
                except KeyNotFoundError:
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
        # Try to use KVStoreService first
        if self._kv_store_service is not None:
            try:
                logger.debug(f"Updating status for job {job_id} to '{status.value}' using KVStoreService")
                await self._kv_store_service.put(JOB_STATUS_KV_NAME, job_id, status.value, serialize=False)
                return
            except Exception as e:
                logger.error(
                    f"Failed to update status for job {job_id} to '{status.value}' using KVStoreService: {e}",
                    exc_info=True,
                )
                # Fall back to direct KV store access
        
        # Fall back to direct KV store access
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
        """Stores the job result or failure info using the service layer or direct JetStream context."""
        try:
            # Try to use JobService first
            if self._job_service is not None:
                try:
                    from ..models.jobs import JobResult
                    
                    # Create JobResult from job
                    job_result = JobResult.from_job(job)
                    if job.error:
                        job_result.status = JOB_STATUS.FAILED.value
                        job_result.error = str(job.error)
                        job_result.traceback = job.traceback
                    else:
                        job_result.status = JOB_STATUS.COMPLETED.value
                        job_result.result = job.result
                    
                    await self._job_service.store_result(job.job_id, job_result)
                    logger.debug(f"Stored result for job {job.job_id} using JobService")
                    return
                except Exception as e:
                    logger.error(f"Failed to store result for job {job.job_id} using JobService: {e}")
                    # Fall back to KVStoreService or direct connection
            
            # Try to use KVStoreService next
            if self._kv_store_service is not None:
                try:
                    # Prepare result data
                    if job.error:
                        # Store failure information
                        result_data = {
                            "status": JOB_STATUS.FAILED.value,
                            "error": job.error,
                            "traceback": job.traceback,
                        }
                        logger.debug(f"Storing failure info for job {job.job_id} using KVStoreService")
                    else:
                        # Store successful result
                        result_data = {
                            "status": JOB_STATUS.COMPLETED.value,
                            "result": job.result,
                        }
                        logger.debug(f"Storing result for job {job.job_id} using KVStoreService")

                    # Store the result with TTL
                    await self._kv_store_service.put(RESULT_KV_NAME, job.job_id, result_data, ttl=DEFAULT_RESULT_TTL_SECONDS)
                    logger.debug(f"Stored result for job {job.job_id} using KVStoreService")
                    return
                except Exception as e:
                    logger.error(f"Failed to store result for job {job.job_id} using KVStoreService: {e}")
                    # Fall back to direct connection
            
            # Fall back to direct worker connection
            if not self.worker._js:
                logger.error("JetStream context not available")
                return

            kv_store = await self.worker._js.key_value(bucket=RESULT_KV_NAME)
            if not kv_store:
                logger.warning(
                    f"Result KV store not available. Cannot store result for job {job.job_id}"
                )
                return

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
                logger.debug(f"Storing result for job {job.job_id}")

            # Serialize the result data
            serialized_result = Job.serialize_result(
                result=result_data.get("result"),
                status=result_data.get("status"),
                error=result_data.get("error"),
                traceback_str=result_data.get("traceback"),
            )

            # Store the result with TTL
            await kv_store.put(job.job_id, serialized_result)
            logger.debug(f"Stored result for job {job.job_id} using direct connection")

        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            logger.error(
                f"Failed to store result/failure info for job {job.job_id}: {e}",
                exc_info=True,
            )
