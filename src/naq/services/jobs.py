"""Job Execution Service for NAQ.

This module provides a centralized service for job execution, result storage,
and lifecycle management with integrated event logging and failure handling.
"""

import asyncio
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from naq.exceptions import NaqException
from naq.models.jobs import Job, JobResult
from naq.services.base import BaseService
from naq.services.events import EventService


class JobService(BaseService):
    """Centralized service for job execution and lifecycle management.
    
    This service manages job execution, result storage, and failure handling
    with integrated event logging.
    """

    def __init__(
        self,
        connection_service: "ConnectionService",
        kv_store_service: "KVStoreService",
        event_service: EventService,
    ) -> None:
        """Initialize the JobService.
        
        Args:
            connection_service: The connection service for NATS operations
            kv_store_service: The KV store service for result storage
            event_service: The event service for logging job events
        """
        super().__init__()
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service

    async def _do_initialize(self) -> None:
        """Initialize the job service."""
        # Services are initialized through dependencies
        pass

    async def cleanup(self) -> None:
        """Clean up the job service resources."""
        # No specific cleanup needed
        await super().cleanup()

    async def execute_job(self, job: Job) -> JobResult:
        """Execute a job and return the result.
        
        Args:
            job: The job to execute
            
        Returns:
            The job result
            
        Raises:
            NAQError: If there's an error executing the job
        """
        job_id = str(job.id) if job.id else str(uuid4())
        
        try:
            # Log job start
            await self._event_service.log_job_started(job_id, job.function_name)
            
            # Deserialize and execute the function
            func = job.deserialize_function()
            if asyncio.iscoroutinefunction(func):
                result = await func(*job.args, **job.kwargs)
            else:
                result = func(*job.args, **job.kwargs)
            
            # Create successful result
            job_result = JobResult(
                job_id=job_id,
                status="SUCCESS",
                result=result,
                error=None,
                traceback=None
            )
            
            # Store result
            await self.store_result(job_id, job_result)
            
            # Log job completion
            await self._event_service.log_event(
                "job_completed", 
                {"job_id": job_id, "status": "SUCCESS"}
            )
            
            return job_result
        except Exception as e:
            # Handle job failure
            return await self.handle_job_failure(job_id, e)

    async def store_result(self, job_id: str, result: JobResult) -> None:
        """Store a job result in the KV store.
        
        Args:
            job_id: The ID of the job
            result: The job result to store
            
        Raises:
            NAQError: If there's an error storing the result
        """
        try:
            # Serialize the result for storage
            serialized_result = result.to_json()
            await self._kv_store_service.put("job_results", job_id, serialized_result)
        except Exception as e:
            raise NaqException(f"Failed to store result for job '{job_id}': {e}") from e

    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """Get a job result from the KV store.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            The job result if found, None otherwise
            
        Raises:
            NAQError: If there's an error retrieving the result
        """
        try:
            serialized_result = await self._kv_store_service.get("job_results", job_id)
            if serialized_result:
                return JobResult.from_json(serialized_result)
            return None
        except Exception as e:
            raise NaqException(f"Failed to get result for job '{job_id}': {e}") from e

    async def handle_job_failure(self, job_id: str, exception: Exception) -> JobResult:
        """Handle a job failure and store the error result.
        
        Args:
            job_id: The ID of the job
            exception: The exception that caused the failure
            
        Returns:
            The error job result
        """
        import traceback
        traceback_str = traceback.format_exc()
        
        # Create error result
        job_result = JobResult(
            job_id=job_id,
            status="FAILURE",
            result=None,
            error=str(exception),
            traceback=traceback_str
        )
        
        try:
            # Store error result
            await self.store_result(job_id, job_result)
            
            # Log job failure
            await self._event_service.log_event(
                "job_failed", 
                {
                    "job_id": job_id, 
                    "status": "FAILURE", 
                    "error": str(exception)
                }
            )
        except Exception as e:
            # Log the error but don't raise, as we're already handling a failure
            print(f"Failed to handle job failure for job '{job_id}': {e}")
        
        return job_result


# Import at the end to avoid circular imports
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService