# src/naq/services/jobs.py
"""
NAQ Job Service Module

This module provides the JobService class for centralized job execution,
result storage, and lifecycle management in the NAQ job queue system.
"""

import asyncio
import time
import traceback
from typing import Any, Dict, Optional

from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .kv_stores import KVStoreService
from ..events.shared_logger import get_shared_async_logger
from ..exceptions import NaqException, JobExecutionError
from ..models import Job, JobResult, JobEvent, JobEventType
from ..models.enums import JOB_STATUS


class JobService(BaseService):
    """
    Centralized job execution and lifecycle management service.
    
    This service provides a unified interface for job execution, result storage,
    and lifecycle management. It integrates with ConnectionService for job queue
    operations, KVStoreService for result storage, and EventService for event logging.
    
    The service handles job configuration, execution, result storage, and failure
    handling with proper retry logic and event logging throughout the job lifecycle.
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        connection_service: ConnectionService,
        kv_store_service: KVStoreService,
    ):
        """
        Initialize the JobService with required dependencies.
        
        Args:
            config: Configuration dictionary for the service
            connection_service: ConnectionService for job queue operations
            kv_store_service: KVStoreService for result storage
        """
        super().__init__(config)
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_logger = None
        self._job_results_bucket = "naq_job_results"
        self._worker_id = f"job_service_{id(self)}"
        
        # Job execution configuration
        job_config = config.get('jobs', {})
        self._default_timeout = job_config.get('default_timeout', 300)  # 5 minutes
        self._max_retries = job_config.get('max_retries', 3)
        self._retry_delay = job_config.get('retry_delay', 1.0)
        
        logger.info("JobService initialized")
        
    async def _do_initialize(self) -> None:
        """Initialize the job service and its dependencies."""
        # Initialize event logger
        self._event_logger = await get_shared_async_logger()
        
        # Ensure job results bucket exists
        try:
            await self._kv_store_service.get_kv_store(
                self._job_results_bucket,
                description="NAQ job results storage"
            )
            logger.info(f"Job results bucket '{self._job_results_bucket}' initialized")
        except Exception as e:
            logger.error(f"Failed to initialize job results bucket: {e}")
            raise
            
        logger.info("JobService initialized successfully")
        
    async def cleanup(self) -> None:
        """Cleanup job service resources."""
        await super().cleanup()
        logger.info("JobService cleaned up")
        
    async def execute_job(self, job: Job) -> JobResult:
        """
        Execute a job with full lifecycle management.
        
        This method handles the complete job execution lifecycle including:
        - Job validation and configuration
        - Execution with timeout handling
        - Result storage
        - Event logging
        - Error handling and retry logic
        
        Args:
            job: The job to execute
            
        Returns:
            JobResult: The result of the job execution
            
        Raises:
            JobExecutionError: If job execution fails after retries
            NaqException: For other service-related errors
        """
        start_time = time.time()
        
        try:
            # Log job started event
            if self._event_logger:
                await self._event_logger.log_job_started(
                    job_id=job.job_id,
                    worker_id=self._worker_id,
                    queue_name=job.queue_name,
                    details={"timeout": job.timeout or self._default_timeout}
                )
            
            logger.info(f"Executing job {job.job_id} with function {getattr(job.function, '__name__', repr(job.function))}")
            
            # Set job timeout
            timeout = job.timeout if job.timeout is not None else self._default_timeout
            
            # Execute the job with timeout
            try:
                result = await asyncio.wait_for(job.execute(), timeout=timeout)
                
                # Create successful result
                job_result = JobResult(
                    job_id=job.job_id,
                    status=JOB_STATUS.COMPLETED.value,
                    result=result,
                    start_time=job._start_time or start_time,
                    finish_time=job._finish_time or time.time()
                )
                
                # Store result
                await self.store_result(job.job_id, job_result)
                
                # Log completion event
                if self._event_logger:
                    duration_ms = job_result.duration_ms or 0
                    await self._event_logger.log_job_completed(
                        job_id=job.job_id,
                        worker_id=self._worker_id,
                        duration_ms=duration_ms,
                        queue_name=job.queue_name,
                        details={"result_type": type(result).__name__}
                    )
                
                logger.info(f"Job {job.job_id} completed successfully")
                return job_result
                
            except asyncio.TimeoutError:
                error_msg = f"Job {job.job_id} timed out after {timeout} seconds"
                logger.error(error_msg)
                
                # Handle timeout as a failure
                timeout_error = TimeoutError(error_msg)
                await self.handle_job_failure(job, timeout_error)
                
                # Create failed result
                job_result = JobResult(
                    job_id=job.job_id,
                    status=JOB_STATUS.FAILED.value,
                    error=error_msg,
                    traceback="TimeoutError: " + error_msg,
                    start_time=job._start_time or start_time,
                    finish_time=time.time()
                )
                
                await self.store_result(job.job_id, job_result)
                return job_result
                
        except Exception as e:
            logger.error(f"Error executing job {job.job_id}: {e}")
            await self.handle_job_failure(job, e)
            
            # Create failed result
            job_result = JobResult(
                job_id=job.job_id,
                status=JOB_STATUS.FAILED.value,
                error=str(e),
                traceback=traceback.format_exc(),
                start_time=job._start_time or start_time,
                finish_time=time.time()
            )
            
            await self.store_result(job.job_id, job_result)
            return job_result
            
    async def store_result(self, job_id: str, result: JobResult) -> None:
        """
        Store job result with event logging.
        
        This method stores the job result in the KV store and logs appropriate
        events based on the result status.
        
        Args:
            job_id: The ID of the job
            result: The job result to store
            
        Raises:
            NaqException: If result storage fails
        """
        try:
            # Serialize the result
            import pickle
            result_data = pickle.dumps(result)
            
            # Store in KV store
            await self._kv_store_service.put(
                bucket=self._job_results_bucket,
                key=job_id,
                value=result_data
            )
            
            logger.debug(f"Stored result for job {job_id}")
            
        except Exception as e:
            error_msg = f"Failed to store result for job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqException(error_msg) from e
            
    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """
        Get job result from storage.
        
        This method retrieves a job result from the KV store.
        
        Args:
            job_id: The ID of the job to retrieve
            
        Returns:
            JobResult: The job result if found, None otherwise
            
        Raises:
            NaqException: If result retrieval fails
        """
        try:
            # Get from KV store
            result_data = await self._kv_store_service.get(
                bucket=self._job_results_bucket,
                key=job_id
            )
            
            if result_data is None:
                logger.debug(f"No result found for job {job_id}")
                return None
                
            # Deserialize the result
            import pickle
            result = pickle.loads(result_data)
            
            logger.debug(f"Retrieved result for job {job_id}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get result for job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqException(error_msg) from e
            
    async def handle_job_failure(self, job: Job, error: Exception) -> None:
        """
        Handle job failure with retry logic.
        
        This method handles job failures by implementing retry logic based on
        the job configuration and logging appropriate events.
        
        Args:
            job: The job that failed
            error: The exception that caused the failure
            
        Raises:
            JobExecutionError: If the job should not be retried or retries are exhausted
        """
        error_type = type(error).__name__
        error_message = str(error)
        
        logger.warning(f"Job {job.job_id} failed with {error_type}: {error_message}")
        
        # Log failure event
        if self._event_logger:
            duration_ms = (time.time() - (job._start_time or time.time())) * 1000
            await self._event_logger.log_job_failed(
                job_id=job.job_id,
                worker_id=self._worker_id,
                error_type=error_type,
                error_message=error_message,
                duration_ms=duration_ms,
                queue_name=job.queue_name,
                details={
                    "retry_count": job.retry_count,
                    "max_retries": job.max_retries,
                    "should_retry": job.should_retry(error)
                }
            )
        
        # Check if job should be retried
        if job.should_retry(error):
            job.increment_retry_count()
            
            # Calculate retry delay
            retry_delay = job.get_next_retry_delay()
            
            logger.info(f"Scheduling retry for job {job.job_id} (attempt {job.retry_count}/{job.max_retries}) in {retry_delay}s")
            
            # Log retry scheduled event
            if self._event_logger:
                await self._event_logger.log_job_retry_scheduled(
                    job_id=job.job_id,
                    worker_id=self._worker_id,
                    delay_seconds=retry_delay,
                    queue_name=job.queue_name,
                    details={
                        "retry_count": job.retry_count,
                        "max_retries": job.max_retries,
                        "retry_delay": retry_delay,
                        "error_type": error_type
                    }
                )
            
            # Note: In a real implementation, you would enqueue the job for retry
            # This would typically involve publishing it to a retry queue or
            # scheduling it for later execution
            # For now, we'll just log the retry intent
            
        else:
            # Job should not be retried or retries exhausted
            error_msg = f"Job {job.job_id} failed permanently after {job.retry_count} retries"
            if job.retry_count >= job.max_retries:
                error_msg += " (max retries exceeded)"
            else:
                error_msg += " (retry not configured for this error type)"
                
            logger.error(error_msg)
            raise JobExecutionError(error_msg) from error
            
    async def delete_result(self, job_id: str) -> bool:
        """
        Delete job result from storage.
        
        This method removes a job result from the KV store.
        
        Args:
            job_id: The ID of the job whose result should be deleted
            
        Returns:
            bool: True if the result was deleted, False if it didn't exist
            
        Raises:
            NaqException: If result deletion fails
        """
        try:
            # Check if result exists first
            existing_result = await self.get_result(job_id)
            if existing_result is None:
                logger.debug(f"No result to delete for job {job_id}")
                return False
                
            # Delete from KV store
            await self._kv_store_service.delete(
                bucket=self._job_results_bucket,
                key=job_id
            )
            
            logger.info(f"Deleted result for job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to delete result for job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqException(error_msg) from e
            
    async def cleanup_old_results(self, max_age_seconds: float) -> int:
        """
        Clean up old job results.
        
        This method removes job results that are older than the specified age.
        
        Args:
            max_age_seconds: Maximum age of results to keep in seconds
            
        Returns:
            int: Number of results cleaned up
            
        Raises:
            NaqException: If cleanup fails
        """
        try:
            current_time = time.time()
            cutoff_time = current_time - max_age_seconds
            cleaned_count = 0
            
            # List all keys in the results bucket
            keys = await self._kv_store_service.list_keys(self._job_results_bucket)
            
            for key in keys:
                try:
                    # Get the result to check its timestamp
                    result = await self.get_result(key)
                    if result and result.finish_time < cutoff_time:
                        await self.delete_result(key)
                        cleaned_count += 1
                except Exception as e:
                    logger.warning(f"Failed to cleanup result for job {key}: {e}")
                    continue
                    
            logger.info(f"Cleaned up {cleaned_count} old job results")
            return cleaned_count
            
        except Exception as e:
            error_msg = f"Failed to cleanup old job results: {e}"
            logger.error(error_msg)
            raise NaqException(error_msg) from e