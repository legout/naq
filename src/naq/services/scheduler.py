"""
NAQ Scheduler Service Module

This module provides the SchedulerService class that centralizes scheduler operations
and scheduled job management for the NAQ job queue system.
"""

import time
import uuid
from typing import Dict, List, Optional, Any

import cloudpickle
from loguru import logger

from .base import BaseService
from .connection import ConnectionService
from .kv_stores import KVStoreService
from .events import EventService
from ..models import Job, Schedule
from ..models.events import JobEvent, JobEventType
from ..settings import (
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULED_JOB_STATUS,
    NAQ_PREFIX,
)
from ..exceptions import NaqConnectionError, NaqException


class SchedulerService(BaseService):
    """
    Centralized scheduler operations and scheduled job management service.
    
    This service provides a unified interface for all scheduler-related operations
    in the NAQ system, integrating with ConnectionService for NATS operations,
    KVStoreService for scheduled job storage, and EventService for event logging.
    
    The service handles job scheduling, triggering due jobs, and managing scheduled
    job lifecycle with proper error handling and event logging throughout.
    """

    def __init__(
        self, 
        config: Dict[str, Any],
        connection_service: ConnectionService,
        kv_store_service: KVStoreService,
        event_service: EventService
    ):
        """
        Initialize the SchedulerService.
        
        Args:
            config: Configuration dictionary
            connection_service: ConnectionService for NATS operations
            kv_store_service: KVStoreService for scheduled job storage
            event_service: EventService for event logging
        """
        super().__init__(config)
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service
        self._scheduler_config = config.get('scheduler', {})
        self._scheduled_jobs_bucket = SCHEDULED_JOBS_KV_NAME
        
    async def _do_initialize(self) -> None:
        """Initialize the scheduler service and its dependencies."""
        try:
            # Extract scheduler configuration
            self._poll_interval = self._scheduler_config.get('poll_interval', 1.0)
            self._max_schedule_failures = self._scheduler_config.get('max_schedule_failures', 5)
            
            # Ensure the scheduled jobs KV bucket exists
            await self._kv_store_service.get_kv_store(self._scheduled_jobs_bucket)
            
            logger.info("SchedulerService initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize SchedulerService: {e}")
            raise NaqConnectionError(f"SchedulerService initialization failed: {e}") from e

    async def cleanup(self) -> None:
        """Cleanup scheduler service resources."""
        try:
            await super().cleanup()
            logger.info("SchedulerService cleaned up successfully")
            
        except Exception as e:
            logger.error(f"Error during SchedulerService cleanup: {e}")
            # Continue cleanup even if there are errors

    async def schedule_job(self, job: Job, schedule: Schedule) -> str:
        """
        Schedule a job for future execution.
        
        Args:
            job: Job to be scheduled
            schedule: Schedule configuration for the job
            
        Returns:
            str: The scheduled job ID
            
        Raises:
            NaqConnectionError: If scheduling fails due to connection issues
            NaqException: If scheduling fails due to other errors
        """
        try:
            # Create schedule data structure
            schedule_data = {
                'job_id': job.job_id,
                'scheduled_timestamp_utc': schedule.scheduled_timestamp_utc,
                '_orig_job_payload': job.serialize(),
                'cron': schedule.cron,
                'interval_seconds': schedule.interval_seconds,
                'repeat': schedule.repeat,
                'status': schedule.status,
                'last_enqueued_utc': schedule.last_enqueued_utc,
                'schedule_failure_count': schedule.schedule_failure_count,
                'queue_name': job.queue_name,
            }
            
            # Store the schedule in KV store
            serialized_schedule = cloudpickle.dumps(schedule_data)
            await self._kv_store_service.put(
                self._scheduled_jobs_bucket, 
                job.job_id, 
                serialized_schedule
            )
            
            # Log job scheduled event
            await self._event_service.log_job_scheduled(
                job_id=job.job_id,
                queue_name=job.queue_name,
                scheduled_timestamp_utc=schedule.scheduled_timestamp_utc,
                details={
                    'cron': schedule.cron,
                    'interval_seconds': schedule.interval_seconds,
                    'repeat': schedule.repeat,
                    'is_recurring': schedule.is_recurring,
                }
            )
            
            logger.info(f"Scheduled job {job.job_id} for execution at {schedule.scheduled_timestamp_utc}")
            return job.job_id
            
        except Exception as e:
            error_msg = f"Failed to schedule job {job.job_id}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def trigger_due_jobs(self) -> List[str]:
        """
        Find and trigger jobs that are due for execution.
        
        Returns:
            List[str]: List of job IDs that were triggered
            
        Raises:
            NaqConnectionError: If triggering fails due to connection issues
            NaqException: If triggering fails due to other errors
        """
        triggered_jobs = []
        now_ts = time.time()
        
        try:
            # Get all scheduled job keys
            job_keys = await self._kv_store_service.list_keys(self._scheduled_jobs_bucket)
            
            if not job_keys:
                logger.debug("No scheduled jobs found")
                return triggered_jobs
                
            logger.debug(f"Checking {len(job_keys)} scheduled jobs for due execution")
            
            for job_id in job_keys:
                try:
                    # Get schedule data
                    schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
                    if not schedule_data_bytes:
                        continue
                        
                    schedule_data = cloudpickle.loads(schedule_data_bytes)
                    
                    # Skip paused or failed jobs
                    status = schedule_data.get('status')
                    if status == SCHEDULED_JOB_STATUS.PAUSED.value:
                        logger.debug(f"Skipping paused job '{job_id}'")
                        continue
                        
                    if status == SCHEDULED_JOB_STATUS.FAILED.value:
                        logger.debug(f"Skipping failed job '{job_id}'")
                        continue
                        
                    # Check if job is due
                    scheduled_ts = schedule_data.get('scheduled_timestamp_utc')
                    if scheduled_ts is None or scheduled_ts > now_ts:
                        continue
                        
                    # Job is due, enqueue it
                    if await self._enqueue_scheduled_job(schedule_data):
                        triggered_jobs.append(job_id)
                        
                except Exception as e:
                    logger.error(f"Error processing scheduled job {job_id}: {e}")
                    continue
                    
            logger.info(f"Triggered {len(triggered_jobs)} due jobs")
            return triggered_jobs
            
        except Exception as e:
            error_msg = f"Failed to trigger due jobs: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def _enqueue_scheduled_job(self, schedule_data: Dict[str, Any]) -> bool:
        """
        Enqueue a scheduled job.
        
        Args:
            schedule_data: Schedule data dictionary
            
        Returns:
            bool: True if enqueuing was successful, False otherwise
        """
        job_id = schedule_data.get('job_id', 'unknown')
        queue_name = schedule_data.get('queue_name')
        original_payload = schedule_data.get('_orig_job_payload')
        scheduled_ts = schedule_data.get('scheduled_timestamp_utc')
        
        if not queue_name or not original_payload:
            logger.error(f"Invalid schedule data for job '{job_id}', missing queue_name or payload")
            return False
            
        try:
            # Get JetStream context and publish the job
            js = await self._connection_service.get_jetstream()
            target_subject = f"{NAQ_PREFIX}.queue.{queue_name}"
            
            ack = await js.publish(subject=target_subject, payload=original_payload)
            logger.debug(f"Enqueued scheduled job {job_id} to {target_subject}. Stream: {ack.stream}, Seq: {ack.seq}")
            
            # Log job schedule triggered event
            await self._event_service.log_event(
                JobEvent.schedule_triggered(
                    job_id=job_id,
                    queue_name=queue_name,
                    worker_id="scheduler",
                    nats_subject=target_subject,
                    nats_sequence=ack.seq,
                    details={
                        'scheduled_timestamp_utc': scheduled_ts,
                        'cron': schedule_data.get('cron'),
                        'interval_seconds': schedule_data.get('interval_seconds'),
                        'repeat': schedule_data.get('repeat'),
                    }
                )
            )
            
            # Handle recurring jobs
            await self._handle_recurring_job(schedule_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to enqueue scheduled job {job_id}: {e}")
            
            # Update failure count
            failure_count = schedule_data.get('schedule_failure_count', 0) + 1
            schedule_data['schedule_failure_count'] = failure_count
            
            # Check if we should mark the job as permanently failed
            if self._max_schedule_failures and failure_count >= self._max_schedule_failures:
                logger.warning(f"Job {job_id} has failed scheduling {failure_count} times, marking as failed")
                schedule_data['status'] = SCHEDULED_JOB_STATUS.FAILED.value
                
            # Update the schedule data
            serialized_schedule = cloudpickle.dumps(schedule_data)
            await self._kv_store_service.put(self._scheduled_jobs_bucket, job_id, serialized_schedule)
            
            return False

    async def _handle_recurring_job(self, schedule_data: Dict[str, Any]) -> None:
        """
        Handle recurring job rescheduling or cleanup.
        
        Args:
            schedule_data: Schedule data dictionary
        """
        job_id = schedule_data.get('job_id')
        cron = schedule_data.get('cron')
        interval_seconds = schedule_data.get('interval_seconds')
        repeat = schedule_data.get('repeat')
        scheduled_ts = schedule_data.get('scheduled_timestamp_utc')
        
        # Calculate next runtime if this is a recurring job
        if scheduled_ts is not None:
            next_scheduled_ts = self._calculate_next_runtime(schedule_data, scheduled_ts)
        else:
            next_scheduled_ts = None
        
        if next_scheduled_ts is None:
            # Not a recurring job, delete the schedule
            if job_id is not None:
                await self._kv_store_service.delete(self._scheduled_jobs_bucket, job_id)
            logger.debug(f"Deleted schedule entry for non-recurring job {job_id}")
            return
            
        # Check repeat count
        if repeat is not None:
            if repeat > 1:
                schedule_data['repeat'] = repeat - 1
                schedule_data['scheduled_timestamp_utc'] = next_scheduled_ts
                logger.debug(f"Rescheduling job {job_id} for {next_scheduled_ts}. Repeats left: {repeat - 1}")
            else:
                # Last repetition, delete the schedule
                if job_id is not None:
                    await self._kv_store_service.delete(self._scheduled_jobs_bucket, job_id)
                logger.debug(f"Job {job_id} finished its repetitions")
                return
        else:
            # Infinite repeat
            schedule_data['scheduled_timestamp_utc'] = next_scheduled_ts
            logger.debug(f"Rescheduling job {job_id} for {next_scheduled_ts} (infinite)")
            
        # Update the schedule data
        serialized_schedule = cloudpickle.dumps(schedule_data)
        if job_id is not None:
            await self._kv_store_service.put(self._scheduled_jobs_bucket, job_id, serialized_schedule)

    def _calculate_next_runtime(
        self, schedule_data: Dict[str, Any], scheduled_ts: float
    ) -> Optional[float]:
        """
        Calculate the next runtime for a recurring job.
        
        Args:
            schedule_data: The job schedule data
            scheduled_ts: The previous scheduled timestamp
            
        Returns:
            Next runtime timestamp or None if not recurring
        """
        try:
            from croniter import croniter
        except ImportError:
            croniter = None
            
        cron = schedule_data.get("cron")
        interval_seconds = schedule_data.get("interval_seconds")
        next_scheduled_ts = None

        if cron:
            if croniter is None:
                logger.error("Cannot reschedule cron job: 'croniter' library not installed.")
                return None

            # Calculate next run time based on the previous scheduled time
            import datetime
            from datetime import timezone
            
            base_dt = datetime.datetime.fromtimestamp(scheduled_ts, timezone.utc)
            cron_iter = croniter(cron, base_dt)
            next_scheduled_ts = cron_iter.get_next(datetime.datetime).timestamp()

        elif interval_seconds:
            # Calculate next run time based on the previous scheduled time + interval
            next_scheduled_ts = scheduled_ts + interval_seconds

        return next_scheduled_ts

    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job.
        
        Args:
            job_id: ID of the job to cancel
            
        Returns:
            bool: True if cancellation was successful, False otherwise
            
        Raises:
            NaqConnectionError: If cancellation fails due to connection issues
            NaqException: If cancellation fails due to other errors
        """
        try:
            # Check if the job exists
            schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
            if not schedule_data_bytes:
                logger.warning(f"Cannot cancel job {job_id}: job not found in scheduled jobs")
                return False
                
            schedule_data = cloudpickle.loads(schedule_data_bytes)
            queue_name = schedule_data.get('queue_name')
            
            # Delete the schedule
            await self._kv_store_service.delete(self._scheduled_jobs_bucket, job_id)
            
            # Log job cancelled event
            await self._event_service.log_job_cancelled(
                job_id=job_id,
                queue_name=queue_name,
                details={
                    'cancelled_timestamp_utc': time.time(),
                    'was_recurring': schedule_data.get('cron') is not None or schedule_data.get('interval_seconds') is not None,
                }
            )
            
            logger.info(f"Cancelled scheduled job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to cancel scheduled job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def pause_scheduled_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.
        
        Args:
            job_id: ID of the job to pause
            
        Returns:
            bool: True if pausing was successful, False otherwise
            
        Raises:
            NaqConnectionError: If pausing fails due to connection issues
            NaqException: If pausing fails due to other errors
        """
        try:
            # Check if the job exists
            schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
            if not schedule_data_bytes:
                logger.warning(f"Cannot pause job {job_id}: job not found in scheduled jobs")
                return False
                
            schedule_data = cloudpickle.loads(schedule_data_bytes)
            
            # Update status to paused
            schedule_data['status'] = SCHEDULED_JOB_STATUS.PAUSED.value
            
            # Save the updated schedule
            serialized_schedule = cloudpickle.dumps(schedule_data)
            await self._kv_store_service.put(self._scheduled_jobs_bucket, job_id, serialized_schedule)
            
            # Log job paused event
            await self._event_service.log_job_cancelled(
                job_id=job_id,
                queue_name=schedule_data.get('queue_name'),
                details={
                    'paused_timestamp_utc': time.time(),
                    'action': 'paused',
                }
            )
            
            logger.info(f"Paused scheduled job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to pause scheduled job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def resume_scheduled_job(self, job_id: str) -> bool:
        """
        Resume a paused scheduled job.
        
        Args:
            job_id: ID of the job to resume
            
        Returns:
            bool: True if resuming was successful, False otherwise
            
        Raises:
            NaqConnectionError: If resuming fails due to connection issues
            NaqException: If resuming fails due to other errors
        """
        try:
            # Check if the job exists
            schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
            if not schedule_data_bytes:
                logger.warning(f"Cannot resume job {job_id}: job not found in scheduled jobs")
                return False
                
            schedule_data = cloudpickle.loads(schedule_data_bytes)
            
            # Check if job is actually paused
            if schedule_data.get('status') != SCHEDULED_JOB_STATUS.PAUSED.value:
                logger.warning(f"Cannot resume job {job_id}: job is not paused")
                return False
                
            # Update status to active
            schedule_data['status'] = SCHEDULED_JOB_STATUS.ACTIVE.value
            
            # Save the updated schedule
            serialized_schedule = cloudpickle.dumps(schedule_data)
            await self._kv_store_service.put(self._scheduled_jobs_bucket, job_id, serialized_schedule)
            
            # Log job resumed event
            await self._event_service.log_job_scheduled(
                job_id=job_id,
                queue_name=schedule_data.get('queue_name'),
                scheduled_timestamp_utc=schedule_data.get('scheduled_timestamp_utc'),
                details={
                    'resumed_timestamp_utc': time.time(),
                    'action': 'resumed',
                }
            )
            
            logger.info(f"Resumed scheduled job {job_id}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to resume scheduled job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def get_scheduled_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a scheduled job.
        
        Args:
            job_id: ID of the job to retrieve
            
        Returns:
            Dictionary with job schedule information or None if not found
            
        Raises:
            NaqConnectionError: If retrieval fails due to connection issues
        """
        try:
            schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
            if not schedule_data_bytes:
                return None
                
            schedule_data = cloudpickle.loads(schedule_data_bytes)
            return schedule_data
            
        except Exception as e:
            error_msg = f"Failed to get scheduled job {job_id}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def list_scheduled_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all scheduled jobs, optionally filtered by status.
        
        Args:
            status: Optional status filter ('active', 'paused', 'failed', 'cancelled')
            
        Returns:
            List of dictionaries with job schedule information
            
        Raises:
            NaqConnectionError: If listing fails due to connection issues
        """
        try:
            job_keys = await self._kv_store_service.list_keys(self._scheduled_jobs_bucket)
            jobs = []
            
            for job_id in job_keys:
                try:
                    schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
                    if not schedule_data_bytes:
                        continue
                        
                    schedule_data = cloudpickle.loads(schedule_data_bytes)
                    
                    # Filter by status if specified
                    if status and schedule_data.get('status') != status:
                        continue
                        
                    jobs.append(schedule_data)
                    
                except Exception as e:
                    logger.error(f"Error retrieving scheduled job {job_id}: {e}")
                    continue
                    
            return jobs
            
        except Exception as e:
            error_msg = f"Failed to list scheduled jobs: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the scheduler.
        
        Returns:
            Dictionary containing scheduler statistics
            
        Raises:
            NaqConnectionError: If getting stats fails due to connection issues
        """
        try:
            job_keys = await self._kv_store_service.list_keys(self._scheduled_jobs_bucket)
            
            stats = {
                'total_scheduled_jobs': len(job_keys),
                'status_counts': {
                    'active': 0,
                    'paused': 0,
                    'failed': 0,
                    'cancelled': 0,
                },
                'recurring_jobs': 0,
            }
            
            for job_id in job_keys:
                try:
                    schedule_data_bytes = await self._kv_store_service.get(self._scheduled_jobs_bucket, job_id)
                    if not schedule_data_bytes:
                        continue
                        
                    schedule_data = cloudpickle.loads(schedule_data_bytes)
                    
                    # Count by status
                    status = schedule_data.get('status', 'active')
                    if status in stats['status_counts']:
                        stats['status_counts'][status] += 1
                    
                    # Count recurring jobs
                    if schedule_data.get('cron') or schedule_data.get('interval_seconds'):
                        stats['recurring_jobs'] += 1
                        
                except Exception as e:
                    logger.error(f"Error retrieving scheduled job {job_id} for stats: {e}")
                    continue
                    
            return stats
            
        except Exception as e:
            error_msg = f"Failed to get scheduler stats: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e