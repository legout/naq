"""
Job Service for NAQ

This module provides a centralized service for managing job execution,
result storage, and lifecycle management, including integrated event logging
and failure handling.
"""

import time
import traceback
from typing import Any, Dict, Optional, Union

import cloudpickle
import msgspec
from loguru import logger

from ..exceptions import JobExecutionError, NaqException
from ..models.events import JobEvent, WorkerEvent
from ..models.enums import JobEventType, WorkerEventType, JOB_STATUS
from ..models.jobs import Job, JobResult
from .base import BaseService, ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from .connection import ConnectionService
from .events import EventService
from .kv_stores import KVStoreService


class JobServiceConfig(msgspec.Struct):
    """
    Configuration for the JobService.

    Attributes:
        results_bucket_name: Name of the KV bucket for storing job results
        default_result_ttl: Default TTL for job results in seconds
        enable_job_execution: Whether to enable job execution
        enable_result_storage: Whether to enable result storage
        enable_event_logging: Whether to enable event logging
        auto_create_buckets: Whether to automatically create buckets
        max_job_execution_time: Maximum time for job execution in seconds
    """

    results_bucket_name: str = "naq_job_results"
    default_result_ttl: Optional[int] = 86400  # 24 hours
    enable_job_execution: bool = True
    enable_result_storage: bool = True
    enable_event_logging: bool = True
    auto_create_buckets: bool = True
    max_job_execution_time: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "results_bucket_name": self.results_bucket_name,
            "default_result_ttl": self.default_result_ttl,
            "enable_job_execution": self.enable_job_execution,
            "enable_result_storage": self.enable_result_storage,
            "enable_event_logging": self.enable_event_logging,
            "auto_create_buckets": self.auto_create_buckets,
            "max_job_execution_time": self.max_job_execution_time,
        }


class JobService(BaseService):
    """
    Centralized job execution and management service.

    This service provides functionality for executing jobs, storing results,
    managing job lifecycle, and handling failures with integrated event logging.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        connection_service: Optional[ConnectionService] = None,
        kv_store_service: Optional[KVStoreService] = None,
        event_service: Optional[EventService] = None,
    ) -> None:
        """
        Initialize the job service.

        Args:
            config: Optional configuration for the service.
            connection_service: Optional ConnectionService dependency.
            kv_store_service: Optional KVStoreService dependency.
            event_service: Optional EventService dependency.
        """
        super().__init__(config)
        self._job_config = self._extract_job_config()
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service

    def _extract_job_config(self) -> JobServiceConfig:
        """
        Extract job-specific configuration from the service config.

        Returns:
            JobServiceConfig instance with job parameters.
        """
        # Start with default config
        job_config = JobServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "results_bucket_name" in custom_settings:
                job_config.results_bucket_name = custom_settings["results_bucket_name"]

            if "default_result_ttl" in custom_settings:
                job_config.default_result_ttl = custom_settings["default_result_ttl"]

            if "enable_job_execution" in custom_settings:
                job_config.enable_job_execution = custom_settings["enable_job_execution"]

            if "enable_result_storage" in custom_settings:
                job_config.enable_result_storage = custom_settings["enable_result_storage"]

            if "enable_event_logging" in custom_settings:
                job_config.enable_event_logging = custom_settings["enable_event_logging"]

            if "auto_create_buckets" in custom_settings:
                job_config.auto_create_buckets = custom_settings["auto_create_buckets"]

            if "max_job_execution_time" in custom_settings:
                job_config.max_job_execution_time = custom_settings["max_job_execution_time"]

        return job_config

    async def _do_initialize(self) -> None:
        """
        Initialize the job service.

        This method validates the configuration and ensures the required
        services are available.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing JobService")

            # Validate configuration
            if self._job_config.default_result_ttl is not None and self._job_config.default_result_ttl <= 0:
                raise ServiceInitializationError("default_result_ttl must be positive")

            if self._job_config.max_job_execution_time is not None and self._job_config.max_job_execution_time <= 0:
                raise ServiceInitializationError("max_job_execution_time must be positive")

            # Ensure connection service is available if other services are not provided
            if self._kv_store_service is None and self._connection_service is None:
                raise ServiceInitializationError("ConnectionService or KVStoreService is required")

            # Ensure connection service is initialized if provided
            if self._connection_service is not None and not self._connection_service.is_initialized:
                await self._connection_service.initialize()

            # Create KV store service if not provided
            if self._kv_store_service is None:
                from .kv_stores import KVStoreService, KVStoreServiceConfig
                
                kv_config = KVStoreServiceConfig(
                    auto_create_buckets=self._job_config.auto_create_buckets
                )
                self._kv_store_service = KVStoreService(
                    config=ServiceConfig(custom_settings=kv_config.as_dict()),
                    connection_service=self._connection_service
                )
                await self._kv_store_service.initialize()

            # Create event service if not provided
            if self._event_service is None and self._job_config.enable_event_logging:
                from .events import EventService, EventServiceConfig
                
                event_config = EventServiceConfig(
                    enable_event_logging=self._job_config.enable_event_logging,
                    auto_create_bucket=self._job_config.auto_create_buckets
                )
                self._event_service = EventService(
                    config=ServiceConfig(custom_settings=event_config.as_dict()),
                    connection_service=self._connection_service,
                    kv_store_service=self._kv_store_service
                )
                await self._event_service.initialize()

            self._logger.info("JobService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize JobService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up job service resources.

        This method cleans up the services that were created by this service.
        """
        try:
            self._logger.info("Cleaning up JobService")

            # Note: We don't clean up externally provided services
            # Only clean up if we created the services
            if self._event_service is not None and self._connection_service is not None:
                await self._event_service.cleanup()

            if self._kv_store_service is not None and self._connection_service is not None:
                await self._kv_store_service.cleanup()

            self._logger.info("JobService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup JobService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def execute_job(self, job: Job, worker_id: Optional[str] = None) -> JobResult:
        """
        Execute a job and manage its lifecycle.

        Args:
            job: The job to execute.
            worker_id: Optional ID of the worker executing the job.

        Returns:
            JobResult containing the execution result.

        Raises:
            JobExecutionError: If job execution fails.
        """
        if not self._job_config.enable_job_execution:
            raise JobExecutionError("Job execution is disabled")

        start_time = time.time()
        worker_id = worker_id or "unknown-worker"

        try:
            # Log job started event
            if self._event_service and self._job_config.enable_event_logging:
                started_event = JobEvent.started(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    queue_name=job.queue_name
                )
                await self._event_service.log_job_event(started_event)

            # Execute the job with timeout if configured
            if self._job_config.max_job_execution_time:
                import asyncio
                try:
                    result = await asyncio.wait_for(
                        job.execute(),
                        timeout=self._job_config.max_job_execution_time
                    )
                except asyncio.TimeoutError:
                    raise JobExecutionError(f"Job {job.job_id} timed out after {self._job_config.max_job_execution_time} seconds")
            else:
                result = await job.execute()

            # Create job result
            job_result = JobResult.from_job(job)
            job_result.result = result

            # Store result if enabled
            if self._job_config.enable_result_storage:
                await self.store_result(job.job_id, job_result)

            # Log job completed event
            if self._event_service and self._job_config.enable_event_logging:
                duration_ms = (time.time() - start_time) * 1000
                completed_event = JobEvent.completed(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    duration_ms=duration_ms,
                    queue_name=job.queue_name
                )
                await self._event_service.log_job_event(completed_event)

            self._logger.info(f"Job {job.job_id} completed successfully")
            return job_result

        except Exception as e:
            # Handle job failure
            await self.handle_job_failure(job, e, worker_id, start_time)
            raise

    async def store_result(self, job_id: str, result: JobResult) -> None:
        """
        Store a job result.

        Args:
            job_id: ID of the job.
            result: JobResult to store.

        Raises:
            NaqException: If storing the result fails.
        """
        if not self._job_config.enable_result_storage:
            return

        try:
            # Create result key
            result_key = f"job:{job_id}:result"
            
            # Store the result with TTL
            await self._kv_store_service.put(
                self._job_config.results_bucket_name,
                result_key,
                result,
                ttl=self._job_config.default_result_ttl,
                serialize=True
            )

            self._logger.debug(f"Stored result for job {job_id}")

        except Exception as e:
            error_msg = f"Failed to store result for job {job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """
        Get a job result.

        Args:
            job_id: ID of the job.

        Returns:
            JobResult if found, None otherwise.

        Raises:
            NaqException: If retrieving the result fails.
        """
        try:
            # Create result key
            result_key = f"job:{job_id}:result"
            
            # Get the result
            try:
                result = await self._kv_store_service.get(
                    self._job_config.results_bucket_name,
                    result_key,
                    deserialize=True
                )
                
                if isinstance(result, JobResult):
                    return result
                return None

            except NaqException:
                # Result not found
                return None

        except Exception as e:
            error_msg = f"Failed to get result for job {job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def handle_job_failure(
        self, 
        job: Job, 
        error: Exception, 
        worker_id: str, 
        start_time: float
    ) -> None:
        """
        Handle a job failure.

        Args:
            job: The job that failed.
            error: The exception that caused the failure.
            worker_id: ID of the worker that was executing the job.
            start_time: When the job started execution.

        Raises:
            JobExecutionError: If handling the failure fails.
        """
        try:
            duration_ms = (time.time() - start_time) * 1000
            
            # Log job failed event
            if self._event_service and self._job_config.enable_event_logging:
                failed_event = JobEvent.failed(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    error_type=type(error).__name__,
                    error_message=str(error),
                    duration_ms=duration_ms,
                    queue_name=job.queue_name
                )
                await self._event_service.log_job_event(failed_event)

            # Check if job should be retried
            if job.should_retry(error):
                retry_delay = job.get_next_retry_delay()
                job.increment_retry_count()
                
                # Log retry scheduled event
                if self._event_service and self._job_config.enable_event_logging:
                    retry_event = JobEvent.retry_scheduled(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        delay_seconds=retry_delay,
                        queue_name=job.queue_name
                    )
                    await self._event_service.log_job_event(retry_event)

                self._logger.warning(
                    f"Job {job.job_id} failed, retry #{job.retry_count} "
                    f"scheduled in {retry_delay} seconds"
                )
            else:
                self._logger.error(
                    f"Job {job.job_id} failed permanently after {job.retry_count} retries"
                )

            # Store failure result if enabled
            if self._job_config.enable_result_storage:
                job_result = JobResult.from_job(job)
                job_result.status = JOB_STATUS.FAILED.value
                job_result.error = str(error)
                job_result.traceback = traceback.format_exc()
                await self.store_result(job.job_id, job_result)

        except Exception as e:
            error_msg = f"Failed to handle job failure for job {job.job_id}: {e}"
            self._logger.error(error_msg)
            raise JobExecutionError(error_msg) from error

    async def delete_result(self, job_id: str) -> bool:
        """
        Delete a job result.

        Args:
            job_id: ID of the job.

        Returns:
            True if the result was deleted, False if it didn't exist.

        Raises:
            NaqException: If deleting the result fails.
        """
        try:
            # Create result key
            result_key = f"job:{job_id}:result"
            
            # Delete the result
            deleted = await self._kv_store_service.delete(
                self._job_config.results_bucket_name,
                result_key
            )

            if deleted:
                self._logger.debug(f"Deleted result for job {job_id}")

            return deleted

        except Exception as e:
            error_msg = f"Failed to delete result for job {job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    @property
    def job_config(self) -> JobServiceConfig:
        """Get the job service configuration."""
        return self._job_config

    @property
    def is_job_execution_enabled(self) -> bool:
        """Check if job execution is enabled."""
        return self._job_config.enable_job_execution

    @property
    def is_result_storage_enabled(self) -> bool:
        """Check if result storage is enabled."""
        return self._job_config.enable_result_storage

    @property
    def is_event_logging_enabled(self) -> bool:
        """Check if event logging is enabled."""
        return self._job_config.enable_event_logging