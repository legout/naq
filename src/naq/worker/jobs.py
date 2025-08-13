"""
Job Processing and Status Management

This module contains classes responsible for executing jobs, handling job execution context,
managing job lifecycle, and tracking job status and dependencies.
"""

import asyncio
import importlib
import logging
import traceback
from typing import Any, Callable, Dict, Optional, Type

import cloudpickle
from loguru import logger
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from ..models import Job, JOB_STATUS
from ..exceptions import JobExecutionError
from ..results import Results
from ..settings import (
    DEFAULT_RESULT_TTL_SECONDS,
    JOB_STATUS_KV_NAME,
    JOB_STATUS_TTL_SECONDS,
    RESULT_KV_NAME,
)


class JobProcessor:
    """
    Handles the execution of jobs with proper error handling and context management.
    
    The JobProcessor is responsible for:
    - Loading and executing job functions
    - Managing job execution context
    - Handling job dependencies
    - Providing retry mechanisms for failed jobs
    """
    
    def __init__(self):
        """Initialize a new JobProcessor."""
        self._job_functions: Dict[str, Callable] = {}
        self._job_classes: Dict[str, Type] = {}
        
    def register_job_function(self, name: str, func: Callable) -> None:
        """
        Register a job function by name.
        
        Args:
            name: Name to register the function under
            func: The function to register
        """
        self._job_functions[name] = func
        logger.debug(f"Registered job function: {name}")
        
    def register_job_class(self, name: str, job_class: Type) -> None:
        """
        Register a job class by name.
        
        Args:
            name: Name to register the class under
            job_class: The job class to register
        """
        self._job_classes[name] = job_class
        logger.debug(f"Registered job class: {name}")
        
    async def process(self, job: Job) -> Any:
        """
        Process a job and return the result.
        
        Args:
            job: The job to process
            
        Returns:
            The result of job execution
            
        Raises:
            JobExecutionError: If job execution fails
        """
        try:
            logger.info(f"Processing job {job.job_id} with function {job.function}")
            
            # Get the job function or class
            job_func = job.function
            
            # Prepare job arguments
            kwargs = job.kwargs or {}
            
            # Execute the job
            if asyncio.iscoroutinefunction(job_func):
                result = await job_func(**kwargs)
            else:
                result = job_func(**kwargs)
                
            logger.info(f"Successfully completed job {job.job_id}")
            return result
            
        except Exception as e:
            error_msg = f"Job {job.job_id} failed: {str(e)}"
            logger.error(error_msg)
            logger.debug(traceback.format_exc())
            raise JobExecutionError(error_msg) from e
            
    def _get_job_function(self, job_type: str) -> Callable:
        """
        Get the job function for the given job type.
        
        Args:
            job_type: The type of job to get the function for
            
        Returns:
            The job function
            
        Raises:
            JobExecutionError: If no job function is found for the type
        """
        # Check registered functions first
        if job_type in self._job_functions:
            return self._job_functions[job_type]
            
        # Check registered classes
        if job_type in self._job_classes:
            job_class = self._job_classes[job_type]
            return job_class()
            
        # Try to import from module path
        if '.' in job_type:
            try:
                module_path, func_name = job_type.rsplit('.', 1)
                module = importlib.import_module(module_path)
                return getattr(module, func_name)
            except (ImportError, AttributeError) as e:
                logger.debug(f"Failed to import job function {job_type}: {e}")
                
        raise JobExecutionError(f"No job function found for type: {job_type}")
        
    def unregister_job_function(self, name: str) -> None:
        """
        Unregister a job function by name.
        
        Args:
            name: Name of the function to unregister
        """
        if name in self._job_functions:
            del self._job_functions[name]
            logger.debug(f"Unregistered job function: {name}")
            
    def unregister_job_class(self, name: str) -> None:
        """
        Unregister a job class by name.
        
        Args:
            name: Name of the class to unregister
        """
        if name in self._job_classes:
            del self._job_classes[name]
            logger.debug(f"Unregistered job class: {name}")
            
    def list_registered_jobs(self) -> Dict[str, str]:
        """
        Get a list of all registered job functions and classes.
        
        Returns:
            Dictionary mapping job names to their types ('function' or 'class')
        """
        result = {}
        
        for name in self._job_functions:
            result[name] = 'function'
            
        for name in self._job_classes:
            result[name] = 'class'
            
        return result


class JobStatusManager:
    """
    Manages job status tracking and dependency resolution.
    """

    def __init__(self, worker):
        self.worker = worker
        self._result_kv_store = None
        self._status_kv = None

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
                "started_at": job._start_time,
                "finished_at": job._finish_time,
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
                    entry = await self._status_kv.get(dep_id)
                    status = entry.value
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
            # Create a Results instance
            results_manager = Results(nats_url=self.worker._nats_url)
            
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

            # Use the Results class to store the result
            await results_manager.add_job_result(
                job_id=job.job_id,
                result_data=result_data,
                result_ttl=job.result_ttl
            )

        except Exception as e:
            # Log error but don't let result storage failure stop job processing
            logger.error(
                f"Failed to store result/failure info for job {job.job_id}: {e}",
                exc_info=True,
            )