"""Job status management module.

This module provides functionality for managing job status tracking, dependency resolution,
and result storage. It is responsible for tracking job progress and managing job dependencies.
"""

from typing import Optional

import cloudpickle
from nats.js import JetStreamContext
from nats.js.kv import KeyValue

from ..nats_client import NatsClient
from ..config import get_config
from ..models import JOB_STATUS
from ..models.jobs import Job
from ..settings import DEFAULT_RESULT_TTL_SECONDS, JOB_STATUS_KV_NAME, RESULT_KV_NAME
from ..utils.decorators import timing
from ..utils.error_handling import ErrorHandler
from ..utils.logging import StructuredLogger


class JobStatusManager:
    """
    Manages job status tracking and dependency resolution.
    """

    def __init__(self, worker, nats_client: Optional[NatsClient] = None):
        """Initialize the job status manager.

        Args:
            worker: The worker instance this status manager belongs to.
            nats_client: Optional NatsClient for accessing NATS.
        """
        self.worker = worker
        self._nats_client = nats_client
        self._result_kv_store = None
        self._status_kv_store = None
        self._logger = StructuredLogger("JobStatusManager")
        self._error_handler = ErrorHandler(self._logger)

    @timing()
    async def _get_nats_client(self) -> NatsClient:
        """Get or create a NATS client."""
        if self._nats_client is None:
            try:
                config = get_config()
                self._nats_client = NatsClient(config)
                await self._nats_client.connect()
            except Exception as e:
                self._error_handler.handle_error(e, context={"operation": "get_nats_client"})
                raise
        return self._nats_client

    @timing()
    async def _get_result_kv_store(self) -> Optional[KeyValue]:
        """Initialize and return NATS KV store for results."""
        if self._result_kv_store is None:
            try:
                nats_client = await self._get_nats_client()
                self._result_kv_store = await nats_client.get_kv_store(RESULT_KV_NAME)
                self._logger.debug(
                    f"Got result KV store: '{RESULT_KV_NAME}'"
                )
                return self._result_kv_store
            except Exception as e:
                self._error_handler.handle_error(
                    e,
                    context={
                        "operation": "get_result_kv_store",
                    },
                )
                return None
        return self._result_kv_store

    @timing()
    async def update_job(self, job: Job) -> None:
        """Update job status and result in KV store."""
        kv_store = await self._get_result_kv_store()
        if not kv_store:
            self._logger.warning(
                f"Result KV store not available. Cannot update status for job {job.job_id}",
                job_id=job.job_id,
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
            }
            serialized_payload = cloudpickle.dumps(payload)
            await kv_store.put(job.job_id, serialized_payload)
            self._logger.debug(
                f"Updated status for job {job.job_id} to {job.status.value}",
                job_id=job.job_id,
                status=job.status.value,
            )
        except Exception as e:
            self._error_handler.handle_error(
                e, context={"operation": "update_job", "job_id": job.job_id}
            )

    @timing()
    async def initialize(self, js: JetStreamContext) -> None:
        """Initialize the job status manager with a JetStream context."""
        await self._initialize_status_kv(js)
        await self._initialize_result_kv(js)

    @timing()
    async def _initialize_status_kv(self, js: JetStreamContext) -> None:
        """Initialize the job status KV store."""
        try:
            nats_client = await self._get_nats_client()
            self._status_kv_store = await nats_client.get_kv_store(JOB_STATUS_KV_NAME)
            self._logger.info(
                f"Bound to job status KV store: '{JOB_STATUS_KV_NAME}'",
                kv_store_name=JOB_STATUS_KV_NAME,
            )
        except Exception as e:
            self._error_handler.handle_error(
                e,
                context={
                    "operation": "initialize_status_kv",
                },
            )
            self._status_kv_store = None

    @timing()
    async def _initialize_result_kv(self, js: JetStreamContext) -> None:
        """Initialize the result KV store."""
        try:
            nats_client = await self._get_nats_client()
            self._result_kv_store = await nats_client.get_kv_store(RESULT_KV_NAME)
            self._logger.info(
                f"Bound to result KV store: '{RESULT_KV_NAME}'",
                kv_store_name=RESULT_KV_NAME,
            )
        except Exception as e:
            self._error_handler.handle_error(
                e,
                context={
                    "operation": "initialize_result_kv",
                },
            )
            self._result_kv_store = None

    @timing()
    async def check_dependencies(self, job: Job) -> bool:
        """Checks if all dependencies for the job are met."""
        if not job.dependency_ids:
            return True  # No dependencies

        try:
            nats_client = await self._get_nats_client()
            status_kv = await nats_client.get_kv_store(JOB_STATUS_KV_NAME)
            
            self._logger.debug(
                f"Checking dependencies for job {job.job_id}: {job.dependency_ids}",
                job_id=job.job_id,
                dependency_ids=job.dependency_ids,
            )
            for dep_id in job.dependency_ids:
                try:
                    entry = await status_kv.get(dep_id.encode())
                    if entry is None:
                        # Dependency status not found, means it hasn't completed yet
                        self._logger.debug(
                            f"Dependency {dep_id} for job {job.job_id} not found in status KV. Not met yet.",
                            job_id=job.job_id,
                            dependency_id=dep_id,
                        )
                        return False
                    
                    status = entry.value.decode("utf-8")

                    if status == JOB_STATUS.COMPLETED.value:
                        self._logger.debug(
                            f"Dependency {dep_id} for job {job.job_id} is completed.",
                            job_id=job.job_id,
                            dependency_id=dep_id,
                            status=status,
                        )
                        continue  # Dependency met
                    elif status == JOB_STATUS.FAILED.value:
                        self._logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} failed. Job {job.job_id} will not run.",
                            job_id=job.job_id,
                            dependency_id=dep_id,
                            status=status,
                        )
                        return False
                    else:
                        # Unknown status? Treat as unmet for safety.
                        self._logger.warning(
                            f"Dependency {dep_id} for job {job.job_id} has unknown status '{status}'. Treating as unmet.",
                            job_id=job.job_id,
                            dependency_id=dep_id,
                            status=status,
                        )
                        return False
                except Exception:
                    # Dependency status not found, means it hasn't completed yet
                    self._logger.debug(
                        f"Dependency {dep_id} for job {job.job_id} not found in status KV. Not met yet.",
                        job_id=job.job_id,
                        dependency_id=dep_id,
                    )
                    return False
            # If loop completes, all dependencies were found and completed
            self._logger.debug(
                f"All dependencies met for job {job.job_id}.", job_id=job.job_id
            )
            return True
        except Exception as e:
            self._error_handler.handle_error(
                e,
                context={
                    "operation": "check_dependencies",
                    "job_id": job.job_id,
                },
            )
            return False  # Assume dependencies not met on error

    @timing()
    async def update_job_status(self, job_id: str, status: JOB_STATUS) -> None:
        """Updates the job status in the KV store."""
        try:
            nats_client = await self._get_nats_client()
            status_kv = await nats_client.get_kv_store(JOB_STATUS_KV_NAME)
            
            self._logger.debug(
                f"Updating status for job {job_id} to '{status.value}'",
                job_id=job_id,
                status=status.value,
            )
            await status_kv.put(job_id.encode(), status.value.encode())
        except Exception as e:
            self._error_handler.handle_error(
                e,
                context={
                    "operation": "update_job_status",
                    "job_id": job_id,
                    "status": status.value,
                },
            )

    @timing()
    async def store_result(self, job: Job) -> None:
        """Stores the job result or failure info using the service layer."""
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
                    self._logger.debug(
                        f"Stored result for job {job.job_id} using JobService",
                        job_id=job.job_id,
                    )
                    return
                except Exception as e:
                    self._error_handler.handle_error(
                        e,
                        context={
                            "operation": "store_result",
                            "job_id": job.job_id,
                            "service": "JobService",
                        },
                    )
                    # Fall back to KVStoreService

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
                        self._logger.debug(
                            f"Storing failure info for job {job.job_id} using KVStoreService",
                            job_id=job.job_id,
                        )
                    else:
                        # Store successful result
                        result_data = {
                            "status": JOB_STATUS.COMPLETED.value,
                            "result": job.result,
                        }
                        self._logger.debug(
                            f"Storing result for job {job.job_id} using KVStoreService",
                            job_id=job.job_id,
                        )

                    # Store the result with TTL
                    await self._kv_store_service.put(
                        RESULT_KV_NAME,
                        job.job_id,
                        result_data,
                        ttl=DEFAULT_RESULT_TTL_SECONDS,
                    )
                    self._logger.debug(
                        f"Stored result for job {job.job_id} using KVStoreService",
                        job_id=job.job_id,
                    )
                    return
                except Exception as e:
                    self._error_handler.handle_error(
                        e,
                        context={
                            "operation": "store_result",
                            "job_id": job.job_id,
                            "service": "KVStoreService",
                        },
                    )
            else:
                self._logger.error(
                    "Neither JobService nor KVStoreService available, cannot store result for job {job_id}",
                    job_id=job.job_id,
                )

        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            self._error_handler.handle_error(
                e, context={"operation": "store_result", "job_id": job.job_id}
            )
