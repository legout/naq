# src/naq/services/scheduler.py
"""
Scheduler Service for NAQ.

This service centralizes scheduler operations and scheduled job management,
providing a unified interface for scheduling and triggering jobs.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .events import EventService
from ..models.jobs import Job
from ..models.schedules import Schedule
from ..exceptions import NaqException, ConfigurationError, JobNotFoundError
from ..settings import SCHEDULED_JOBS_KV_NAME, SCHEDULED_JOB_STATUS


class ScheduleInfo:
    """Information about a scheduled job."""
    
    def __init__(
        self,
        job_id: str,
        queue_name: str,
        scheduled_timestamp: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
        status: str = SCHEDULED_JOB_STATUS.ACTIVE,
        schedule_failure_count: int = 0,
        last_enqueued_utc: Optional[float] = None,
        next_run_utc: Optional[float] = None
    ):
        self.job_id = job_id
        self.queue_name = queue_name
        self.scheduled_timestamp = scheduled_timestamp
        self.cron = cron
        self.interval_seconds = interval_seconds
        self.repeat = repeat
        self.status = status
        self.schedule_failure_count = schedule_failure_count
        self.last_enqueued_utc = last_enqueued_utc
        self.next_run_utc = next_run_utc or scheduled_timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "job_id": self.job_id,
            "queue_name": self.queue_name,
            "scheduled_timestamp": self.scheduled_timestamp,
            "cron": self.cron,
            "interval_seconds": self.interval_seconds,
            "repeat": self.repeat,
            "status": self.status,
            "schedule_failure_count": self.schedule_failure_count,
            "last_enqueued_utc": self.last_enqueued_utc,
            "next_run_utc": self.next_run_utc,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScheduleInfo":
        """Create from dictionary representation."""
        return cls(
            job_id=data["job_id"],
            queue_name=data["queue_name"],
            scheduled_timestamp=data["scheduled_timestamp"],
            cron=data.get("cron"),
            interval_seconds=data.get("interval_seconds"),
            repeat=data.get("repeat"),
            status=data.get("status", SCHEDULED_JOB_STATUS.ACTIVE),
            schedule_failure_count=data.get("schedule_failure_count", 0),
            last_enqueued_utc=data.get("last_enqueued_utc"),
            next_run_utc=data.get("next_run_utc"),
        )


class SchedulerService(BaseService):
    """
    Scheduler operations service.
    
    This service provides centralized scheduler operations including
    job scheduling, due job detection, and schedule management.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        connection_service: ConnectionService,
        kv_service: KVStoreService,
        event_service: EventService
    ):
        """
        Initialize the scheduler service.
        
        Args:
            config: Service configuration
            connection_service: Connection service for NATS operations
            kv_service: KV store service for schedule storage
            event_service: Event service for logging
        """
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service

    async def _do_initialize(self) -> None:
        """Initialize the scheduler service."""
        # Ensure the scheduled jobs KV store exists
        await self.kv_service.get_kv_store(
            SCHEDULED_JOBS_KV_NAME,
            description="Stores NAQ scheduled job details",
            create_if_not_exists=True
        )
        logger.debug("Scheduler service initialized")

    async def _do_cleanup(self) -> None:
        """Clean up scheduler service resources."""
        pass

    async def schedule_job(
        self,
        job: Job,
        scheduled_timestamp: float,
        *,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None
    ) -> str:
        """
        Schedule a job for future execution.
        
        Args:
            job: The job to schedule
            scheduled_timestamp: When the job should run (UTC timestamp)
            cron: Optional cron expression for recurring jobs
            interval_seconds: Optional interval for recurring jobs
            repeat: Optional number of times to repeat
            
        Returns:
            The scheduled job ID
            
        Raises:
            NaqException: If scheduling fails
        """
        try:
            # Create schedule info
            schedule_info = ScheduleInfo(
                job_id=job.job_id,
                queue_name=job.queue_name,
                scheduled_timestamp=scheduled_timestamp,
                cron=cron,
                interval_seconds=interval_seconds,
                repeat=repeat
            )
            
            # Store schedule in KV store
            await self.kv_service.put(
                SCHEDULED_JOBS_KV_NAME,
                job.job_id,
                {
                    **schedule_info.to_dict(),
                    "_orig_job_payload": job.serialize(),
                    "_serializer": "cloudpickle"
                }
            )
            
            # Log schedule created event
            schedule_expr = cron or f"interval:{interval_seconds}s" if interval_seconds else "one-time"
            await self.event_service.log_schedule_created(
                job_id=job.job_id,
                queue_name=job.queue_name,
                schedule_expression=schedule_expr
            )
            
            logger.info(f"Scheduled job {job.job_id} for {datetime.fromtimestamp(scheduled_timestamp, timezone.utc)}")
            return job.job_id
            
        except Exception as e:
            raise NaqException(f"Failed to schedule job {job.job_id}: {e}") from e

    async def trigger_due_jobs(self, current_time: Optional[float] = None) -> List[str]:
        """
        Find and trigger jobs that are due for execution.
        
        Args:
            current_time: Current timestamp (defaults to now)
            
        Returns:
            List of job IDs that were triggered
            
        Raises:
            NaqException: If triggering fails
        """
        if current_time is None:
            current_time = time.time()
            
        triggered_jobs = []
        
        try:
            # Get all scheduled job keys
            job_keys = await self.kv_service.keys(SCHEDULED_JOBS_KV_NAME)
            
            for job_id in job_keys:
                try:
                    # Get schedule data
                    schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
                    if not schedule_data:
                        continue
                        
                    schedule_info = ScheduleInfo.from_dict(schedule_data)
                    
                    # Skip if not active
                    if schedule_info.status != SCHEDULED_JOB_STATUS.ACTIVE:
                        continue
                        
                    # Check if job is due
                    if schedule_info.next_run_utc <= current_time:
                        success = await self._trigger_scheduled_job(schedule_info, schedule_data)
                        if success:
                            triggered_jobs.append(job_id)
                            
                except Exception as e:
                    logger.error(f"Error processing scheduled job {job_id}: {e}")
                    
                    # Log scheduler error event
                    try:
                        await self.event_service.log_scheduler_error(
                            error_type=type(e).__name__,
                            error_message=str(e),
                            schedule_id=job_id,
                            queue_name=None
                        )
                    except Exception as log_error:
                        logger.warning(f"Failed to log scheduler error event: {log_error}")
                    
                    continue
                    
            if triggered_jobs:
                logger.info(f"Triggered {len(triggered_jobs)} scheduled jobs")
                
            return triggered_jobs
            
        except Exception as e:
            raise NaqException(f"Failed to trigger due jobs: {e}") from e

    async def _trigger_scheduled_job(
        self, 
        schedule_info: ScheduleInfo, 
        schedule_data: Dict[str, Any]
    ) -> bool:
        """
        Trigger a specific scheduled job.
        
        Args:
            schedule_info: Schedule information
            schedule_data: Full schedule data from KV store
            
        Returns:
            True if job was successfully triggered
        """
        try:
            # Recreate the job from stored payload
            job_payload = schedule_data.get("_orig_job_payload")
            if not job_payload:
                logger.error(f"No job payload found for scheduled job {schedule_info.job_id}")
                return False
                
            job = Job.deserialize(job_payload)
            
            # Enqueue the job
            from ..queue import enqueue
            await enqueue(
                job.function,
                *job.args,
                queue_name=job.queue_name,
                nats_url=self.connection_service.default_nats_url,
                max_retries=job.max_retries,
                retry_delay=job.retry_delay,
                depends_on=job.depends_on,
                timeout=job.timeout,
                **job.kwargs
            )
            
            # Log trigger event
            await self.event_service.log_schedule_triggered(
                job_id=schedule_info.job_id,
                queue_name=schedule_info.queue_name,
                trigger_timestamp=time.time()
            )
            
            # Update schedule info
            now = time.time()
            schedule_info.last_enqueued_utc = now
            
            # Calculate next run time for recurring jobs
            next_run = self._calculate_next_run_time(schedule_info, now)
            
            if next_run is not None:
                # Update for next run
                schedule_info.next_run_utc = next_run
                schedule_info.scheduled_timestamp = next_run
                
                # Decrease repeat count if specified
                if schedule_info.repeat is not None and schedule_info.repeat > 0:
                    schedule_info.repeat -= 1
                    if schedule_info.repeat == 0:
                        # Last repetition, deactivate
                        schedule_info.status = SCHEDULED_JOB_STATUS.COMPLETED
                        
                # Update in KV store
                updated_data = {**schedule_data, **schedule_info.to_dict()}
                await self.kv_service.put(SCHEDULED_JOBS_KV_NAME, schedule_info.job_id, updated_data)
                
            else:
                # One-time job, mark as completed and optionally remove
                schedule_info.status = SCHEDULED_JOB_STATUS.COMPLETED
                updated_data = {**schedule_data, **schedule_info.to_dict()}
                await self.kv_service.put(SCHEDULED_JOBS_KV_NAME, schedule_info.job_id, updated_data)
                
                # Could also delete completed one-time jobs:
                # await self.kv_service.delete(SCHEDULED_JOBS_KV_NAME, schedule_info.job_id)
            
            logger.info(f"Triggered scheduled job {schedule_info.job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to trigger scheduled job {schedule_info.job_id}: {e}")
            
            # Log scheduler error event
            try:
                await self.event_service.log_scheduler_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    schedule_id=schedule_info.job_id,
                    queue_name=schedule_info.queue_name
                )
            except Exception as log_error:
                logger.warning(f"Failed to log scheduler error event: {log_error}")
            
            # Increment failure count
            schedule_info.schedule_failure_count += 1
            try:
                updated_data = {**schedule_data, **schedule_info.to_dict()}
                await self.kv_service.put(SCHEDULED_JOBS_KV_NAME, schedule_info.job_id, updated_data)
            except Exception:
                pass  # Don't fail on failure count update
                
            return False

    def _calculate_next_run_time(self, schedule_info: ScheduleInfo, current_time: float) -> Optional[float]:
        """
        Calculate the next run time for a recurring job.
        
        Args:
            schedule_info: Schedule information
            current_time: Current timestamp
            
        Returns:
            Next run timestamp or None if no more runs
        """
        try:
            if schedule_info.cron:
                # Use cron expression
                from croniter import croniter
                now_dt = datetime.fromtimestamp(current_time, timezone.utc)
                cron_iter = croniter(schedule_info.cron, now_dt)
                next_run_dt = cron_iter.get_next(datetime)
                return next_run_dt.timestamp()
                
            elif schedule_info.interval_seconds:
                # Use interval
                return current_time + schedule_info.interval_seconds
                
        except Exception as e:
            logger.error(f"Error calculating next run time for job {schedule_info.job_id}: {e}")
            
        return None

    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job.
        
        Args:
            job_id: ID of the job to cancel
            
        Returns:
            True if job was found and canceled, False if not found
        """
        try:
            # Check if job exists
            schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
            if not schedule_data:
                logger.warning(f"Scheduled job {job_id} not found for cancellation")
                return False
                
            # Delete the job
            await self.kv_service.delete(SCHEDULED_JOBS_KV_NAME, job_id, purge=True)
            
            # Log cancel event
            schedule_info = ScheduleInfo.from_dict(schedule_data)
            await self.event_service.log_schedule_cancelled(
                job_id=job_id,
                queue_name=schedule_info.queue_name
            )
            
            logger.info(f"Cancelled scheduled job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling scheduled job {job_id}: {e}")
            return False

    async def pause_scheduled_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.
        
        Args:
            job_id: ID of the job to pause
            
        Returns:
            True if successful, False if not found or failed
        """
        return await self._update_job_status(job_id, SCHEDULED_JOB_STATUS.PAUSED, "paused")

    async def resume_scheduled_job(self, job_id: str) -> bool:
        """
        Resume a paused scheduled job.
        
        Args:
            job_id: ID of the job to resume
            
        Returns:
            True if successful, False if not found or failed
        """
        return await self._update_job_status(job_id, SCHEDULED_JOB_STATUS.ACTIVE, "resumed")

    async def _update_job_status(self, job_id: str, new_status: str, action: str) -> bool:
        """Update the status of a scheduled job."""
        try:
            schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
            if not schedule_data:
                logger.warning(f"Scheduled job {job_id} not found for {action}")
                return False
                
            schedule_info = ScheduleInfo.from_dict(schedule_data)
            if schedule_info.status == new_status:
                logger.info(f"Scheduled job {job_id} already {new_status}")
                return True
                
            schedule_info.status = new_status
            
            # Update in KV store
            updated_data = {**schedule_data, **schedule_info.to_dict()}
            await self.kv_service.put(SCHEDULED_JOBS_KV_NAME, job_id, updated_data)
            
            # Log event
            if action == "paused":
                await self.event_service.log_schedule_paused(
                    job_id=job_id,
                    queue_name=schedule_info.queue_name
                )
            elif action == "resumed":
                await self.event_service.log_schedule_resumed(
                    job_id=job_id,
                    queue_name=schedule_info.queue_name
                )
                
            logger.info(f"Scheduled job {job_id} {action}")
            return True
            
        except Exception as e:
            logger.error(f"Error {action} scheduled job {job_id}: {e}")
            return False

    async def modify_scheduled_job(self, job_id: str, **updates: Any) -> bool:
        """
        Modify parameters of a scheduled job.
        
        Args:
            job_id: ID of the job to modify
            **updates: Parameters to update
            
        Returns:
            True if successful, False if not found or failed
        """
        try:
            schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
            if not schedule_data:
                raise JobNotFoundError(f"Scheduled job {job_id} not found")
                
            schedule_info = ScheduleInfo.from_dict(schedule_data)
            
            # Validate and apply updates
            supported_keys = {"cron", "interval", "repeat", "scheduled_timestamp_utc"}
            update_keys = set(updates.keys())
            
            if not update_keys.issubset(supported_keys):
                raise ConfigurationError(
                    f"Unsupported modification keys: {update_keys - supported_keys}. "
                    f"Supported: {supported_keys}"
                )
            
            needs_next_run_recalc = False
            
            if "cron" in updates:
                schedule_info.cron = updates["cron"]
                schedule_info.interval_seconds = None  # Clear interval
                needs_next_run_recalc = True
                
            if "interval" in updates:
                interval = updates["interval"]
                if isinstance(interval, (int, float)):
                    schedule_info.interval_seconds = float(interval)
                elif isinstance(interval, timedelta):
                    schedule_info.interval_seconds = interval.total_seconds()
                else:
                    raise ConfigurationError("'interval' must be timedelta or numeric seconds")
                schedule_info.cron = None  # Clear cron
                needs_next_run_recalc = True
                
            if "repeat" in updates:
                schedule_info.repeat = updates["repeat"]
                
            if "scheduled_timestamp_utc" in updates:
                schedule_info.scheduled_timestamp = updates["scheduled_timestamp_utc"]
                schedule_info.next_run_utc = updates["scheduled_timestamp_utc"]
                needs_next_run_recalc = False
                
            # Recalculate next run time if needed
            if needs_next_run_recalc:
                current_time = time.time()
                next_run_ts = self._calculate_next_run_time(schedule_info, current_time)
                if next_run_ts is not None:
                    schedule_info.scheduled_timestamp = next_run_ts
                    schedule_info.next_run_utc = next_run_ts
                    
            # Update in KV store
            updated_data = {**schedule_data, **schedule_info.to_dict()}
            await self.kv_service.put(SCHEDULED_JOBS_KV_NAME, job_id, updated_data)
            
            # Log modification event
            await self.event_service.log_schedule_modified(
                job_id=job_id,
                queue_name=schedule_info.queue_name,
                modifications=updates
            )
            
            logger.info(f"Modified scheduled job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error modifying scheduled job {job_id}: {e}")
            return False

    async def get_scheduled_job(self, job_id: str) -> Optional[ScheduleInfo]:
        """
        Get information about a scheduled job.
        
        Args:
            job_id: Job ID
            
        Returns:
            Schedule information or None if not found
        """
        try:
            schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
            if schedule_data:
                return ScheduleInfo.from_dict(schedule_data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting scheduled job {job_id}: {e}")
            return None

    async def list_scheduled_jobs(self, *, status: Optional[str] = None) -> List[ScheduleInfo]:
        """
        List all scheduled jobs, optionally filtered by status.
        
        Args:
            status: Optional status filter
            
        Returns:
            List of schedule information
        """
        try:
            job_keys = await self.kv_service.keys(SCHEDULED_JOBS_KV_NAME)
            schedules = []
            
            for job_id in job_keys:
                try:
                    schedule_data = await self.kv_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
                    if schedule_data:
                        schedule_info = ScheduleInfo.from_dict(schedule_data)
                        if status is None or schedule_info.status == status:
                            schedules.append(schedule_info)
                except Exception as e:
                    logger.error(f"Error getting scheduled job {job_id}: {e}")
                    continue
                    
            return schedules
            
        except Exception as e:
            logger.error(f"Error listing scheduled jobs: {e}")
            return []