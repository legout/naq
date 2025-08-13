# src/naq/services/jobs.py
"""
Job Service for NAQ.

This service centralizes job execution, result storage, and lifecycle management,
providing a unified interface for job processing across the system.
"""

import time
from typing import Any, Dict, List, Optional

from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .events import EventService
from ..models.jobs import Job
from ..models.enums import JOB_STATUS
from ..results import Results
from ..exceptions import NaqException
from ..settings import (
    JOB_STATUS_KV_NAME,
    RESULT_KV_NAME,
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
)


class JobResult:
    """Represents the result of a job execution."""
    
    def __init__(
        self,
        job_id: str,
        status: JOB_STATUS,
        result: Any = None,
        error: Optional[str] = None,
        traceback: Optional[str] = None,
        started_at: Optional[float] = None,
        finished_at: Optional[float] = None,
        duration_ms: Optional[int] = None
    ):
        self.job_id = job_id
        self.status = status
        self.result = result
        self.error = error
        self.traceback = traceback
        self.started_at = started_at
        self.finished_at = finished_at
        self.duration_ms = duration_ms

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "traceback": self.traceback,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
        }


class JobService(BaseService):
    """
    Job execution and lifecycle management service.
    
    This service provides complete job lifecycle management including
    execution, result storage, failure handling, and dependency resolution.
    """

    def __init__(
        self, 
        config: Dict[str, Any],
        connection_service: ConnectionService,
        kv_service: KVStoreService,
        event_service: EventService
    ):
        """
        Initialize the job service.
        
        Args:
            config: Service configuration
            connection_service: Connection service for NATS operations
            kv_service: KV store service for result storage
            event_service: Event service for logging job events
        """
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service
        self._results_manager: Optional[Results] = None

    async def _do_initialize(self) -> None:
        """Initialize the job service."""
        self._results_manager = Results(
            nats_url=self.connection_service.default_nats_url
        )
        logger.debug("Job service initialized")

    async def _do_cleanup(self) -> None:
        """Clean up job service resources."""
        self._results_manager = None

    async def execute_job(self, job: Job, worker_id: Optional[str] = None, *, log_events: bool = True) -> JobResult:
        """
        Execute a job with full lifecycle management and event logging.
        
        Args:
            job: The job to execute
            worker_id: ID of the worker executing the job
            log_events: Whether to log job events
            
        Returns:
            Job execution result
            
        Raises:
            NaqException: If job execution setup fails
        """
        start_time = time.time()
        job_result = JobResult(
            job_id=job.job_id,
            status=JOB_STATUS.RUNNING,
            started_at=start_time
        )
        
        try:
            # Log job started event
            if log_events and worker_id:
                try:
                    await self.event_service.log_job_started(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        queue_name=job.queue_name or "default"
                    )
                except Exception as e:
                    logger.warning(f"Failed to log job started event for {job.job_id}: {e}")

            # Check dependencies before execution
            if job.dependency_ids and not await self._check_dependencies(job):
                job_result.status = JOB_STATUS.FAILED
                job_result.error = "Job dependencies not met"
                job_result.finished_at = time.time()
                job_result.duration_ms = int((job_result.finished_at - start_time) * 1000)
                
                # Log dependency failure event
                if log_events and worker_id:
                    try:
                        await self.event_service.log_job_failed(
                            job_id=job.job_id,
                            worker_id=worker_id,
                            queue_name=job.queue_name or "default",
                            error_type="DependencyError",
                            error_message="Job dependencies not met",
                            duration_ms=job_result.duration_ms
                        )
                    except Exception as e:
                        logger.warning(f"Failed to log job dependency failure event for {job.job_id}: {e}")
                
                # Store failed result
                await self.store_result(job.job_id, job_result)
                return job_result

            # Update job status to running
            await self._update_job_status(job.job_id, JOB_STATUS.RUNNING)
            
            # Execute the job
            try:
                await job.execute()
                
                # Job completed successfully
                job_result.status = JOB_STATUS.COMPLETED
                job_result.result = job.result
                
            except Exception as e:
                # Job failed
                job_result.status = JOB_STATUS.FAILED
                job_result.error = str(e)
                job_result.traceback = job.traceback
                
                # Update job object for consistency
                job.error = str(e)
                job.traceback = job.traceback
            
            # Calculate execution time
            job_result.finished_at = time.time()
            job_result.duration_ms = int((job_result.finished_at - start_time) * 1000)
            
            # Log job completion/failure event
            if log_events and worker_id:
                try:
                    if job_result.status == JOB_STATUS.COMPLETED:
                        await self.event_service.log_job_completed(
                            job_id=job.job_id,
                            worker_id=worker_id,
                            queue_name=job.queue_name or "default",
                            duration_ms=job_result.duration_ms
                        )
                    else:
                        await self.event_service.log_job_failed(
                            job_id=job.job_id,
                            worker_id=worker_id,
                            queue_name=job.queue_name or "default",
                            error_type=type(job_result.error).__name__ if job_result.error else "UnknownError",
                            error_message=job_result.error or "Unknown error",
                            duration_ms=job_result.duration_ms
                        )
                except Exception as e:
                    logger.warning(f"Failed to log job completion event for {job.job_id}: {e}")
            
            # Store the result
            await self.store_result(job.job_id, job_result)
            
            # Update job status
            await self._update_job_status(job.job_id, job_result.status)
            
            return job_result
            
        except Exception as e:
            # Critical error during job execution setup
            job_result.status = JOB_STATUS.FAILED
            job_result.error = f"Job execution error: {str(e)}"
            job_result.finished_at = time.time()
            job_result.duration_ms = int((job_result.finished_at - start_time) * 1000)
            
            logger.error(f"Critical error executing job {job.job_id}: {e}", exc_info=True)
            
            # Log critical error event
            if log_events and worker_id:
                try:
                    await self.event_service.log_job_failed(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        queue_name=job.queue_name or "default",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        duration_ms=job_result.duration_ms
                    )
                except Exception as event_error:
                    logger.warning(f"Failed to log job critical error event for {job.job_id}: {event_error}")
            
            try:
                await self.store_result(job.job_id, job_result)
                await self._update_job_status(job.job_id, JOB_STATUS.FAILED)
            except Exception as store_error:
                logger.error(f"Failed to store error result for job {job.job_id}: {store_error}")
            
            return job_result

    async def store_result(self, job_id: str, result: JobResult) -> None:
        """
        Store job result with integrated result management.
        
        Args:
            job_id: Job ID
            result: Job execution result
        """
        if not self._results_manager:
            logger.warning(f"Results manager not available, cannot store result for job {job_id}")
            return
            
        try:
            result_data = result.to_dict()
            await self._results_manager.add_job_result(
                job_id=job_id,
                result_data=result_data,
                result_ttl=None  # Use default TTL
            )
            logger.debug(f"Stored result for job {job_id}")
            
        except Exception as e:
            logger.error(f"Failed to store result for job {job_id}: {e}", exc_info=True)

    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """
        Get job result.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job result or None if not found
        """
        if not self._results_manager:
            logger.warning("Results manager not available")
            return None
            
        try:
            result_data = await self._results_manager.get_job_result(job_id)
            if result_data is None:
                return None
                
            # Convert back to JobResult
            status = JOB_STATUS(result_data.get('status', 'unknown'))
            return JobResult(
                job_id=job_id,
                status=status,
                result=result_data.get('result'),
                error=result_data.get('error'),
                traceback=result_data.get('traceback'),
                started_at=result_data.get('started_at'),
                finished_at=result_data.get('finished_at'),
                duration_ms=result_data.get('duration_ms'),
            )
            
        except Exception as e:
            logger.error(f"Error getting result for job {job_id}: {e}")
            return None

    async def handle_job_failure(
        self, 
        job: Job, 
        error: Exception, 
        *, 
        retry_count: int = 0,
        max_retries: Optional[int] = None,
        worker_id: Optional[str] = None
    ) -> bool:
        """
        Handle job failure with retry logic and event logging.
        
        Args:
            job: The failed job
            error: The error that caused the failure
            retry_count: Current retry count
            max_retries: Maximum retries allowed (uses job setting if not provided)
            worker_id: ID of the worker handling the failure
            
        Returns:
            True if job should be retried, False if it's a terminal failure
        """
        if max_retries is None:
            max_retries = job.max_retries or 0
            
        logger.warning(
            f"Job {job.job_id} failed (attempt {retry_count + 1}/{max_retries + 1}): {error}"
        )
        
        # Check if we should retry
        if retry_count < max_retries:
            # Calculate retry delay
            delay = job.get_retry_delay(retry_count + 1)
            logger.info(f"Job {job.job_id} will be retried after {delay:.2f}s delay")
            
            # Log retry scheduled event
            if worker_id:
                try:
                    await self.event_service.log_job_retry_scheduled(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        queue_name=job.queue_name or "default",
                        retry_count=retry_count + 1,
                        retry_delay=delay
                    )
                except Exception as e:
                    logger.warning(f"Failed to log job retry scheduled event for {job.job_id}: {e}")
            
            return True
        else:
            # Terminal failure
            logger.error(f"Job {job.job_id} failed after {retry_count} retries")
            
            # Store final failure result
            job_result = JobResult(
                job_id=job.job_id,
                status=JOB_STATUS.FAILED,
                error=str(error),
                traceback=getattr(job, 'traceback', None),
                finished_at=time.time()
            )
            await self.store_result(job.job_id, job_result)
            await self._update_job_status(job.job_id, JOB_STATUS.FAILED)
            
            # Publish to failed jobs stream
            await self._publish_failed_job(job)
            
            return False

    async def _check_dependencies(self, job: Job) -> bool:
        """
        Check if all job dependencies are met.
        
        Args:
            job: Job to check dependencies for
            
        Returns:
            True if all dependencies are met, False otherwise
        """
        if not job.dependency_ids:
            return True
            
        logger.debug(f"Checking dependencies for job {job.job_id}: {job.dependency_ids}")
        
        try:
            for dep_id in job.dependency_ids:
                status = await self.kv_service.get(
                    JOB_STATUS_KV_NAME, 
                    dep_id, 
                    deserialize=False
                )
                
                if status is None:
                    logger.debug(f"Dependency {dep_id} not found for job {job.job_id}")
                    return False
                    
                if isinstance(status, bytes):
                    status = status.decode('utf-8')
                    
                if status != JOB_STATUS.COMPLETED.value:
                    if status == JOB_STATUS.FAILED.value:
                        logger.warning(f"Dependency {dep_id} failed for job {job.job_id}")
                    else:
                        logger.debug(f"Dependency {dep_id} not completed for job {job.job_id} (status: {status})")
                    return False
                    
            logger.debug(f"All dependencies met for job {job.job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error checking dependencies for job {job.job_id}: {e}", exc_info=True)
            return False

    async def _update_job_status(self, job_id: str, status: JOB_STATUS) -> None:
        """
        Update job status in the KV store.
        
        Args:
            job_id: Job ID
            status: New job status
        """
        try:
            await self.kv_service.put(
                JOB_STATUS_KV_NAME,
                job_id,
                status.value.encode('utf-8'),
                serialize=False
            )
            logger.debug(f"Updated status for job {job_id} to {status.value}")
            
        except Exception as e:
            logger.error(f"Failed to update status for job {job_id}: {e}")

    async def _publish_failed_job(self, job: Job) -> None:
        """
        Publish failed job to the failed jobs stream.
        
        Args:
            job: The failed job
        """
        try:
            subject = f"{FAILED_JOB_SUBJECT_PREFIX}.{job.queue_name or 'unknown'}"
            
            async with self.connection_service.jetstream_scope() as js:
                # Ensure failed jobs stream exists
                from .streams import StreamService
                stream_service = StreamService(self.config, self.connection_service)
                await stream_service.ensure_stream(
                    FAILED_JOB_STREAM_NAME,
                    [f"{FAILED_JOB_SUBJECT_PREFIX}.*"]
                )
                
                # Publish failed job
                payload = job.serialize_failed_job()
                await js.publish(subject, payload)
                logger.info(f"Published failed job {job.job_id} to {subject}")
                
        except Exception as e:
            logger.error(f"Failed to publish failed job {job.job_id}: {e}", exc_info=True)

    async def get_job_status(self, job_id: str) -> Optional[JOB_STATUS]:
        """
        Get the current status of a job.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job status or None if not found
        """
        try:
            status = await self.kv_service.get(
                JOB_STATUS_KV_NAME, 
                job_id, 
                deserialize=False
            )
            
            if status is None:
                return None
                
            if isinstance(status, bytes):
                status = status.decode('utf-8')
                
            return JOB_STATUS(status)
            
        except Exception as e:
            logger.error(f"Error getting status for job {job_id}: {e}")
            return None

    async def list_jobs_by_status(self, status: JOB_STATUS) -> List[str]:
        """
        List all jobs with a specific status.
        
        Args:
            status: Job status to filter by
            
        Returns:
            List of job IDs with the specified status
        """
        try:
            keys = await self.kv_service.keys(JOB_STATUS_KV_NAME)
            matching_jobs = []
            
            for job_id in keys:
                job_status = await self.get_job_status(job_id)
                if job_status == status:
                    matching_jobs.append(job_id)
                    
            return matching_jobs
            
        except Exception as e:
            logger.error(f"Error listing jobs by status {status}: {e}")
            return []