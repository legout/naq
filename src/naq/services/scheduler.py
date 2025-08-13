"""
Scheduler Service for NAQ

This module provides a centralized service for managing scheduled jobs,
including job scheduling, triggering, cancellation, and pausing functionality.
"""

import time
from typing import Any, Dict, List, Optional

import cloudpickle
import msgspec
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from ..exceptions import NaqException
from ..models.enums import SCHEDULED_JOB_STATUS, JobEventType
from ..models.jobs import Job
from ..models.schedules import Schedule
from .base import (
    BaseService,
    ServiceConfig,
    ServiceInitializationError,
    ServiceRuntimeError,
)
from .connection import ConnectionService
from .events import EventService
from .kv_stores import KVStoreService


class SchedulerServiceConfig(msgspec.Struct):
    """
    Configuration for the SchedulerService.

    Attributes:
        scheduled_jobs_bucket_name: Name of the KV bucket for storing scheduled jobs
        max_schedule_failures: Maximum number of schedule failures before marking as
            failed
        enable_scheduling: Whether to enable job scheduling
        enable_event_logging: Whether to enable event logging
        auto_create_bucket: Whether to automatically create the scheduled jobs bucket
        default_poll_interval: Default interval for checking due jobs in seconds
    """

    scheduled_jobs_bucket_name: str = "naq_scheduled_jobs"
    max_schedule_failures: int = 5
    enable_scheduling: bool = True
    enable_event_logging: bool = True
    auto_create_bucket: bool = True
    default_poll_interval: float = 1.0

    def as_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "scheduled_jobs_bucket_name": self.scheduled_jobs_bucket_name,
            "max_schedule_failures": self.max_schedule_failures,
            "enable_scheduling": self.enable_scheduling,
            "enable_event_logging": self.enable_event_logging,
            "auto_create_bucket": self.auto_create_bucket,
            "default_poll_interval": self.default_poll_interval,
        }


class SchedulerService(BaseService):
    """
    Centralized scheduler service for managing scheduled jobs.

    This service provides functionality for scheduling jobs, triggering due jobs,
    canceling scheduled jobs, and pausing scheduled jobs. It integrates with
    the ConnectionService, KVStoreService, and EventService for a complete
    scheduling solution.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        connection_service: Optional[ConnectionService] = None,
        kv_store_service: Optional[KVStoreService] = None,
        event_service: Optional[EventService] = None,
    ) -> None:
        """
        Initialize the scheduler service.

        Args:
            config: Optional configuration for the service.
            connection_service: Optional ConnectionService dependency.
            kv_store_service: Optional KVStoreService dependency.
            event_service: Optional EventService dependency.
        """
        super().__init__(config)
        self._scheduler_config = self._extract_scheduler_config()
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service
        self._scheduled_jobs_kv: Optional[KeyValue] = None

    def _extract_scheduler_config(self) -> SchedulerServiceConfig:
        """
        Extract scheduler-specific configuration from the service config.

        Returns:
            SchedulerServiceConfig instance with scheduler parameters.
        """
        # Start with default config
        scheduler_config = SchedulerServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "scheduled_jobs_bucket_name" in custom_settings:
                scheduler_config.scheduled_jobs_bucket_name = custom_settings[
                    "scheduled_jobs_bucket_name"
                ]

            if "max_schedule_failures" in custom_settings:
                scheduler_config.max_schedule_failures = custom_settings[
                    "max_schedule_failures"
                ]

            if "enable_scheduling" in custom_settings:
                scheduler_config.enable_scheduling = custom_settings[
                    "enable_scheduling"
                ]

            if "enable_event_logging" in custom_settings:
                scheduler_config.enable_event_logging = custom_settings[
                    "enable_event_logging"
                ]

            if "auto_create_bucket" in custom_settings:
                scheduler_config.auto_create_bucket = custom_settings[
                    "auto_create_bucket"
                ]

            if "default_poll_interval" in custom_settings:
                scheduler_config.default_poll_interval = custom_settings[
                    "default_poll_interval"
                ]

        return scheduler_config

    async def _do_initialize(self) -> None:
        """
        Initialize the scheduler service.

        This method validates the configuration and ensures the required
        services are available.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing SchedulerService")

            # Validate configuration
            if self._scheduler_config.max_schedule_failures <= 0:
                raise ServiceInitializationError(
                    "max_schedule_failures must be positive"
                )

            if self._scheduler_config.default_poll_interval <= 0:
                raise ServiceInitializationError(
                    "default_poll_interval must be positive"
                )

            # Ensure connection service is available if other services are not provided
            if self._kv_store_service is None and self._connection_service is None:
                raise ServiceInitializationError(
                    "ConnectionService or KVStoreService is required"
                )

            # Ensure connection service is initialized if provided
            if (
                self._connection_service is not None
                and not self._connection_service.is_initialized
            ):
                await self._connection_service.initialize()

            # Create KV store service if not provided
            if self._kv_store_service is None:
                from .kv_stores import KVStoreService, KVStoreServiceConfig

                kv_config = KVStoreServiceConfig(
                    auto_create_buckets=self._scheduler_config.auto_create_bucket
                )
                self._kv_store_service = KVStoreService(
                    config=ServiceConfig(custom_settings=kv_config.as_dict()),
                    connection_service=self._connection_service,
                )
                await self._kv_store_service.initialize()

            # Create event service if not provided
            if (
                self._event_service is None
                and self._scheduler_config.enable_event_logging
            ):
                from .events import EventService, EventServiceConfig

                event_config = EventServiceConfig(
                    enable_event_logging=self._scheduler_config.enable_event_logging,
                    auto_create_bucket=self._scheduler_config.auto_create_bucket,
                )
                self._event_service = EventService(
                    config=ServiceConfig(custom_settings=event_config.as_dict()),
                    connection_service=self._connection_service,
                    kv_store_service=self._kv_store_service,
                )
                await self._event_service.initialize()

            # Get scheduled jobs KV store
            await self._get_scheduled_jobs_kv()

            self._logger.info("SchedulerService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize SchedulerService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up scheduler service resources.

        This method cleans up the services that were created by this service.
        """
        try:
            self._logger.info("Cleaning up SchedulerService")

            # Note: We don't clean up externally provided services
            # Only clean up if we created the services
            if self._event_service is not None and self._connection_service is not None:
                await self._event_service.cleanup()

            if (
                self._kv_store_service is not None
                and self._connection_service is not None
            ):
                await self._kv_store_service.cleanup()

            self._scheduled_jobs_kv = None

            self._logger.info("SchedulerService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup SchedulerService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def _get_scheduled_jobs_kv(self) -> KeyValue:
        """
        Get the KeyValue store for scheduled jobs.

        Returns:
            KeyValue store for scheduled jobs.

        Raises:
            NaqException: If getting the KV store fails.
        """
        if self._scheduled_jobs_kv is not None:
            return self._scheduled_jobs_kv

        try:
            self._scheduled_jobs_kv = await self._kv_store_service.get_kv_store(
                self._scheduler_config.scheduled_jobs_bucket_name
            )
            return self._scheduled_jobs_kv

        except Exception as e:
            error_msg = f"Failed to get scheduled jobs KV store: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def schedule_job(
        self,
        job: Job,
        scheduled_timestamp: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ) -> str:
        """
        Schedule a job for execution at a specific time or on a recurring basis.

        Args:
            job: The job to schedule.
            scheduled_timestamp: When the job should run (UTC timestamp).
            cron: Optional cron expression for recurring jobs.
            interval_seconds: Optional interval in seconds for recurring jobs.
            repeat: Optional number of times to repeat (None=infinite).

        Returns:
            The job ID of the scheduled job.

        Raises:
            NaqException: If scheduling the job fails.
        """
        if not self._scheduler_config.enable_scheduling:
            raise NaqException("Job scheduling is disabled")

        try:
            # Create schedule data
            schedule_data = {
                "job_id": job.job_id,
                "scheduled_timestamp_utc": scheduled_timestamp,
                "queue_name": job.queue_name,
                "cron": cron,
                "interval_seconds": interval_seconds,
                "repeat": repeat,
                "_orig_job_payload": job.serialize(),
                "status": SCHEDULED_JOB_STATUS.ACTIVE.value,
                "schedule_failure_count": 0,
                "last_enqueued_utc": None,
                "next_run_utc": scheduled_timestamp,
            }

            # Store the scheduled job
            kv = await self._get_scheduled_jobs_kv()
            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.put(job.job_id.encode("utf-8"), serialized_schedule_data)

            # Log job scheduled event
            if self._event_service and self._scheduler_config.enable_event_logging:
                from ..models.events import JobEvent

                scheduled_event = JobEvent.scheduled(
                    job_id=job.job_id, worker_id="scheduler", queue_name=job.queue_name
                )
                await self._event_service.log_job_event(scheduled_event)

            self._logger.info(f"Scheduled job {job.job_id} for {scheduled_timestamp}")
            return job.job_id

        except Exception as e:
            error_msg = f"Failed to schedule job {job.job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def trigger_due_jobs(self) -> tuple[int, int]:
        """
        Check for and trigger jobs that are due to run.

        Returns:
            Tuple of (processed_count, error_count) indicating how many jobs
            were processed and how many errors occurred.

        Raises:
            NaqException: If triggering due jobs fails.
        """
        if not self._scheduler_config.enable_scheduling:
            return 0, 0

        processed_count = 0
        error_count = 0
        now_ts = time.time()

        try:
            kv = await self._get_scheduled_jobs_kv()
            keys = await kv.keys()

            if not keys:
                self._logger.debug("No scheduled jobs found")
                return 0, 0

            self._logger.debug(f"Found {len(keys)} scheduled jobs")

            for key_bytes in keys:
                try:
                    processed, had_error = await self._process_single_scheduled_job(
                        key_bytes, now_ts
                    )
                    processed_count += processed
                    error_count += had_error
                except Exception as e:
                    self._logger.error(f"Error processing scheduled job: {e}")
                    error_count += 1

            return processed_count, error_count

        except Exception as e:
            error_msg = f"Failed to trigger due jobs: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def _process_single_scheduled_job(
        self, key_bytes: bytes, now_ts: float
    ) -> tuple[int, int]:
        """
        Process a single scheduled job.

        Args:
            key_bytes: The KV store key.
            now_ts: Current timestamp.

        Returns:
            Tuple of (processed_count, error_count).
        """
        processed = 0
        errors = 0
        key = key_bytes.decode("utf-8") if isinstance(key_bytes, bytes) else key_bytes

        try:
            kv = await self._get_scheduled_jobs_kv()
            entry = await kv.get(key_bytes)
            if entry is None:
                return 0, 0

            schedule_data: Dict[str, Any] = cloudpickle.loads(entry.value)

            # Skip paused jobs
            status = schedule_data.get("status")
            if status == SCHEDULED_JOB_STATUS.PAUSED.value:
                self._logger.debug(f"Skipping paused job '{key}'")
                return 0, 0

            # Skip failed jobs that exceeded retry attempts
            if status == SCHEDULED_JOB_STATUS.FAILED.value:
                self._logger.debug(
                    f"Skipping failed job '{key}' that exceeded retry limits"
                )
                return 0, 0

            # Check if job is ready to run
            scheduled_ts = schedule_data.get("scheduled_timestamp_utc")
            if scheduled_ts is None or scheduled_ts > now_ts:
                return 0, 0  # Not ready yet

            # Job is ready to run
            job_id = schedule_data.get("job_id", "unknown")
            queue_name = schedule_data.get("queue_name")
            original_payload = schedule_data.get("_orig_job_payload")
            repeat = schedule_data.get("repeat")

            # Validate job data
            if not queue_name or not original_payload:
                self._logger.error(
                    f"Invalid schedule data for key '{key}' (missing queue_name or "
                    f"payload). Deleting."
                )
                await kv.delete(key_bytes)
                return 0, 1

            # Enqueue the job
            self._logger.info(
                f"Job {job_id} is ready. Enqueueing to queue '{queue_name}'."
            )
            enqueue_success = await self._enqueue_job(queue_name, original_payload)

            # Track success/failure
            if enqueue_success:
                processed += 1
                # Reset failure count on success
                schedule_data["schedule_failure_count"] = 0
                schedule_data["last_enqueued_utc"] = now_ts

                # Log schedule triggered event
                if self._event_service and self._scheduler_config.enable_event_logging:
                    from ..models.events import JobEvent

                    triggered_event = JobEvent.schedule_triggered(
                        job_id=job_id, worker_id="scheduler", queue_name=queue_name
                    )
                    await self._event_service.log_job_event(triggered_event)
            else:
                errors += 1
                # Track failures for potential retry limiting
                failure_count = schedule_data.get("schedule_failure_count", 0) + 1
                schedule_data["schedule_failure_count"] = failure_count

                # Check if we should mark the job as permanently failed
                if failure_count >= self._scheduler_config.max_schedule_failures:
                    self._logger.warning(
                        f"Job {job_id} has failed scheduling {failure_count} times, "
                        f"marking as failed"
                    )
                    schedule_data["status"] = SCHEDULED_JOB_STATUS.FAILED.value
                    serialized_data = cloudpickle.dumps(schedule_data)
                    await kv.put(key_bytes, serialized_data)
                    return 0, 1
                else:
                    # Just log and continue - will retry on next check cycle
                    self._logger.warning(
                        f"Failed to enqueue job {job_id} (attempt {failure_count}). "
                        f"Will retry on next cycle."
                    )
                    serialized_data = cloudpickle.dumps(schedule_data)
                    await kv.put(key_bytes, serialized_data)
                    return 0, 1

            # Handle recurrence or deletion
            if not enqueue_success:
                return processed, errors

            # Calculate next run time if this is a recurring job
            next_scheduled_ts = self._calculate_next_runtime(
                schedule_data, scheduled_ts
            )
            delete_entry = True  # Assume deletion unless rescheduled

            if next_scheduled_ts is not None:
                # Check repeat count
                if repeat is not None:
                    if repeat > 1:
                        schedule_data["repeat"] = repeat - 1
                        schedule_data["scheduled_timestamp_utc"] = next_scheduled_ts
                        schedule_data["next_run_utc"] = next_scheduled_ts
                        delete_entry = False  # Reschedule
                        self._logger.debug(
                            f"Rescheduling job {job_id} for {next_scheduled_ts}. "
                            f"Repeats left: {repeat - 1}"
                        )
                    else:
                        # Last repetition
                        self._logger.debug(f"Job {job_id} finished its repetitions.")
                        delete_entry = True
                else:
                    # Infinite repeat
                    schedule_data["scheduled_timestamp_utc"] = next_scheduled_ts
                    schedule_data["next_run_utc"] = next_scheduled_ts
                    delete_entry = False  # Reschedule
                    self._logger.debug(
                        f"Rescheduling job {job_id} for {next_scheduled_ts} (infinite)."
                    )

            # Update or delete the KV entry
            if delete_entry:
                self._logger.debug(f"Deleting schedule entry for job {job_id}.")
                await kv.delete(key_bytes)
            else:
                self._logger.debug(f"Updating schedule entry for job {job_id}.")
                updated_payload = cloudpickle.dumps(schedule_data)
                await kv.put(key_bytes, updated_payload)

            return processed, errors

        except Exception as e:
            self._logger.exception(f"Error processing schedule key '{key}': {e}")
            return 0, 1

    async def _enqueue_job(self, queue_name: str, payload: bytes) -> bool:
        """
        Enqueue a job payload to the specified queue.

        Args:
            queue_name: Name of the queue to enqueue to.
            payload: Serialized job payload.

        Returns:
            True if enqueuing was successful, False otherwise.
        """
        try:
            if self._connection_service is None:
                self._logger.error("ConnectionService not available for enqueuing job")
                return False

            # Get JetStream context
            js = await self._connection_service.get_jetstream()

            # Publish to queue subject
            from ..settings import NAQ_PREFIX

            subject = f"{NAQ_PREFIX}.queue.{queue_name}"
            ack = await js.publish(subject=subject, payload=payload)

            self._logger.debug(
                f"Enqueued job to {subject}. Stream: {ack.stream}, Seq: {ack.seq}"
            )
            return True

        except Exception as e:
            self._logger.error(f"Failed to enqueue job to queue '{queue_name}': {e}")
            return False

    def _calculate_next_runtime(
        self, schedule_data: Dict[str, Any], scheduled_ts: float
    ) -> Optional[float]:
        """
        Calculate the next runtime for a recurring job.

        Args:
            schedule_data: The job schedule data.
            scheduled_ts: The previous scheduled timestamp.

        Returns:
            Next runtime timestamp or None if not recurring.
        """
        cron = schedule_data.get("cron")
        interval_seconds = schedule_data.get("interval_seconds")
        next_scheduled_ts = None

        if cron:
            try:
                from croniter import croniter
                import datetime

                # Calculate next run time based on the previous scheduled time
                base_dt = datetime.datetime.fromtimestamp(
                    scheduled_ts, datetime.timezone.utc
                )
                cron_iter = croniter(cron, base_dt)
                next_scheduled_ts = cron_iter.get_next(datetime.datetime).timestamp()
            except ImportError:
                self._logger.error(
                    "Cannot reschedule cron job: 'croniter' library not installed."
                )
                return None
            except Exception as e:
                self._logger.error(f"Error calculating next cron runtime: {e}")
                return None

        elif interval_seconds:
            # Calculate next run time based on the previous scheduled time + interval
            next_scheduled_ts = scheduled_ts + interval_seconds

        return next_scheduled_ts

    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job by deleting it from the KV store.

        Args:
            job_id: ID of the job to cancel.

        Returns:
            True if job was found and canceled, False if not found.

        Raises:
            NaqException: For errors other than job not found.
        """
        self._logger.info(f"Attempting to cancel scheduled job '{job_id}'")
        try:
            kv = await self._get_scheduled_jobs_kv()

            # Check if job exists first
            try:
                entry = await kv.get(job_id.encode("utf-8"))
                if entry is None:
                    self._logger.warning(
                        f"Scheduled job '{job_id}' not found. Cannot cancel."
                    )
                    return False
            except KeyNotFoundError:
                self._logger.warning(
                    f"Scheduled job '{job_id}' not found. Cannot cancel."
                )
                return False

            # Delete the job
            await kv.delete(job_id.encode("utf-8"), purge=True)

            # Log schedule cancelled event
            if self._event_service and self._scheduler_config.enable_event_logging:
                from ..models.events import JobEvent

                cancelled_event = JobEvent.schedule_cancelled(
                    job_id=job_id,
                    worker_id="scheduler",
                    queue_name="unknown",  # We don't have queue name here
                )
                await self._event_service.log_job_event(cancelled_event)

            self._logger.info(f"Scheduled job '{job_id}' cancelled successfully.")
            return True

        except Exception as e:
            self._logger.error(f"Error cancelling scheduled job '{job_id}': {e}")
            raise NaqException(f"Failed to cancel scheduled job: {e}") from e

    async def pause_scheduled_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job by updating its status to PAUSED.

        Args:
            job_id: ID of the job to pause.

        Returns:
            True if job was found and paused, False if not found.

        Raises:
            NaqException: For errors other than job not found.
        """
        return await self._update_job_status(
            job_id, SCHEDULED_JOB_STATUS.PAUSED, "schedule_paused"
        )

    async def resume_scheduled_job(self, job_id: str) -> bool:
        """
        Resume a paused scheduled job by updating its status to ACTIVE.

        Args:
            job_id: ID of the job to resume.

        Returns:
            True if job was found and resumed, False if not found.

        Raises:
            NaqException: For errors other than job not found.
        """
        return await self._update_job_status(
            job_id, SCHEDULED_JOB_STATUS.ACTIVE, "schedule_resumed"
        )

    async def _update_job_status(
        self, job_id: str, status: SCHEDULED_JOB_STATUS, event_type: str
    ) -> bool:
        """
        Update the status of a scheduled job.

        Args:
            job_id: ID of the job to update.
            status: New status to set.
            event_type: Event type to log.

        Returns:
            True if update was successful, False if job not found.

        Raises:
            NaqException: For errors other than job not found.
        """
        try:
            kv = await self._get_scheduled_jobs_kv()

            # Get current job data
            try:
                entry = await kv.get(job_id.encode("utf-8"))
                if entry is None:
                    self._logger.warning(
                        f"Scheduled job '{job_id}' not found. Cannot update status."
                    )
                    return False
            except KeyNotFoundError:
                self._logger.warning(
                    f"Scheduled job '{job_id}' not found. Cannot update status."
                )
                return False

            schedule_data = cloudpickle.loads(entry.value)
            if schedule_data.get("status") == status.value:
                self._logger.info(
                    f"Scheduled job '{job_id}' already has status '{status.value}'."
                )
                return True  # No change needed

            # Update status
            schedule_data["status"] = status.value
            serialized_schedule_data = cloudpickle.dumps(schedule_data)

            # Update the KV entry
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)

            # Log status change event
            if self._event_service and self._scheduler_config.enable_event_logging:
                from ..models.events import JobEvent

                status_event = JobEvent(
                    job_id=job_id,
                    event_type=JobEventType(event_type),
                    worker_id="scheduler",
                    queue_name=schedule_data.get("queue_name", "unknown"),
                    timestamp=time.time(),
                )
                await self._event_service.log_job_event(status_event)

            self._logger.info(
                f"Scheduled job '{job_id}' status updated to '{status.value}'."
            )
            return True

        except Exception as e:
            self._logger.error(f"Error updating status for job '{job_id}': {e}")
            raise NaqException(f"Failed to update job status: {e}") from e

    async def get_scheduled_job(self, job_id: str) -> Optional[Schedule]:
        """
        Get a scheduled job by ID.

        Args:
            job_id: ID of the job to retrieve.

        Returns:
            Schedule object if found, None otherwise.

        Raises:
            NaqException: If retrieving the job fails.
        """
        try:
            kv = await self._get_scheduled_jobs_kv()

            try:
                entry = await kv.get(job_id.encode("utf-8"))
                if entry is None:
                    return None

                schedule_data = cloudpickle.loads(entry.value)

                # Convert to Schedule object
                return Schedule(
                    job_id=schedule_data["job_id"],
                    scheduled_timestamp_utc=schedule_data["scheduled_timestamp_utc"],
                    _orig_job_payload=schedule_data["_orig_job_payload"],
                    cron=schedule_data.get("cron"),
                    interval_seconds=schedule_data.get("interval_seconds"),
                    repeat=schedule_data.get("repeat"),
                    status=schedule_data.get("status", "active"),
                    last_enqueued_utc=schedule_data.get("last_enqueued_utc"),
                    schedule_failure_count=schedule_data.get(
                        "schedule_failure_count", 0
                    ),
                )

            except KeyNotFoundError:
                return None

        except Exception as e:
            error_msg = f"Failed to get scheduled job {job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def list_scheduled_jobs(
        self, status: Optional[SCHEDULED_JOB_STATUS] = None
    ) -> List[Schedule]:
        """
        List all scheduled jobs, optionally filtered by status.

        Args:
            status: Optional status to filter by.

        Returns:
            List of Schedule objects.

        Raises:
            NaqException: If listing jobs fails.
        """
        try:
            kv = await self._get_scheduled_jobs_kv()
            keys = await kv.keys()

            schedules = []
            for key_bytes in keys:
                try:
                    entry = await kv.get(key_bytes)
                    if entry is None:
                        continue

                    schedule_data = cloudpickle.loads(entry.value)

                    # Filter by status if specified
                    if (
                        status is not None
                        and schedule_data.get("status") != status.value
                    ):
                        continue

                    # Convert to Schedule object
                    schedule = Schedule(
                        job_id=schedule_data["job_id"],
                        scheduled_timestamp_utc=schedule_data[
                            "scheduled_timestamp_utc"
                        ],
                        _orig_job_payload=schedule_data["_orig_job_payload"],
                        cron=schedule_data.get("cron"),
                        interval_seconds=schedule_data.get("interval_seconds"),
                        repeat=schedule_data.get("repeat"),
                        status=schedule_data.get("status", "active"),
                        last_enqueued_utc=schedule_data.get("last_enqueued_utc"),
                        schedule_failure_count=schedule_data.get(
                            "schedule_failure_count", 0
                        ),
                    )
                    schedules.append(schedule)

                except Exception as e:
                    self._logger.error(f"Error processing scheduled job key: {e}")
                    continue

            return schedules

        except Exception as e:
            error_msg = f"Failed to list scheduled jobs: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    @property
    def scheduler_config(self) -> SchedulerServiceConfig:
        """Get the scheduler service configuration."""
        return self._scheduler_config

    @property
    def is_scheduling_enabled(self) -> bool:
        """Check if job scheduling is enabled."""
        return self._scheduler_config.enable_scheduling
