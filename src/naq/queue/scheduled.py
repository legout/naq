"""Scheduled job management functionality.

This module contains the ScheduledJobManager class for handling scheduled jobs.
"""

import datetime
from datetime import timedelta, timezone
from typing import Any, Dict, Optional

import cloudpickle
from nats.js.errors import KeyNotFoundError, APIError
from ..exceptions import (
    ConfigurationError,
    JobNotFoundError,
    NaqException,
)
from ..models.jobs import Job
from ..models.enums import SCHEDULED_JOB_STATUS
from ..services import ServiceManager, ConnectionService, KVStoreService
from ..services.config import create_global_config, GlobalServiceConfig
from ..settings import (
    JOB_SERIALIZER,
    SCHEDULED_JOBS_KV_NAME,
)
from ..utils.error_handling import wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter


class ScheduledJobManager:
    """
    Manager for scheduled jobs within a Queue.
    Handles storing, retrieving, and managing scheduled jobs in the NATS KV store.
    """

    def __init__(
        self,
        queue_name: str,
        nats_url: str,
        config: Optional[GlobalServiceConfig] = None,
        service_manager: Optional[ServiceManager] = None,
    ):
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        self.queue_name = queue_name
        self._nats_url = nats_url
        self._config = config or create_global_config()
        self._service_manager = service_manager or self._config.service_manager
        self._connection_service: Optional[ConnectionService] = None
        self._kv_store_service: Optional[KVStoreService] = None
        self._logger = StructuredLogger(__name__)

    async def _get_services(self) -> tuple[ConnectionService, KVStoreService]:
        """Get the connection and KV store services from the service manager."""
        with self._logger.operation_context("get_services", queue_name=self.queue_name):
            if self._service_manager is None:
                error_msg = "ServiceManager is required for service-based operations"
                self._logger.error("service_manager_missing", error=error_msg)
                raise NaqException(error_msg)

            if self._connection_service is None:
                self._logger.debug("getting_connection_service")
                self._connection_service = await self._service_manager.get_service(
                    "connection", ConnectionService
                )

            if self._kv_store_service is None:
                self._logger.debug("getting_kv_store_service")
                self._kv_store_service = await self._service_manager.get_service(
                    "kv_store", KVStoreService
                )

            self._logger.debug("services_retrieved")
            return self._connection_service, self._kv_store_service

    async def store_job(
        self,
        job: Job,
        scheduled_timestamp: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ) -> None:
        """
        Stores a job in the scheduled jobs KV store.

        Args:
            job: The job to schedule
            scheduled_timestamp: When the job should run (UTC timestamp)
            cron: Optional cron expression for recurring jobs
            interval_seconds: Optional interval in seconds for recurring jobs
            repeat: Optional number of times to repeat (None=infinite)

        Raises:
            NaqException: If storing the job fails
        """
        with self._logger.operation_context(
            "store_job",
            job_id=job.job_id,
            queue_name=job.queue_name,
            scheduled_timestamp=scheduled_timestamp,
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        ):
            validate_parameter(job, "job", Job)
            validate_parameter(scheduled_timestamp, "scheduled_timestamp", (int, float))

            original_job_payload = job.serialize()

            schedule_data = {
                "job_id": job.job_id,
                "scheduled_timestamp_utc": scheduled_timestamp,
                "queue_name": job.queue_name,
                "cron": cron,
                "interval_seconds": interval_seconds,
                "repeat": repeat,
                "_orig_job_payload": original_job_payload,
                "_serializer": JOB_SERIALIZER,
                "status": SCHEDULED_JOB_STATUS.ACTIVE,  # Initial status
                "schedule_failure_count": 0,  # Initial failure count
                "last_enqueued_utc": None,  # Track last enqueue time
                "next_run_utc": scheduled_timestamp,  # Explicitly store next run time
            }

            try:
                # Use the KV store service
                self._logger.debug("storing_scheduled_job", job_id=job.job_id)
                _, kv_store_service = await self._get_services()
                await kv_store_service.put(
                    SCHEDULED_JOBS_KV_NAME, job.job_id, schedule_data
                )
                self._logger.info("job_stored", job_id=job.job_id)
            except Exception as e:
                error_msg = (
                    f"Failed to store scheduled job {job.job_id} in KV store: {e}"
                )
                self._logger.error(
                    "job_storage_failed", error=str(e), job_id=job.job_id
                )
                raise wrap_naq_exception(e, error_msg) from e

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancels a scheduled job by deleting it from the KV store.

        Args:
            job_id: ID of the job to cancel

        Returns:
            True if job was found and canceled, False if not found

        Raises:
            NaqException: For errors other than job not found
        """
        with self._logger.operation_context(
            "cancel_job", job_id=job_id, queue_name=self.queue_name
        ):
            validate_parameter(job_id, "job_id", str)

            self._logger.info("cancelling_job", job_id=job_id)
            try:
                # Use the KV store service
                _, kv_store_service = await self._get_services()
                # Use delete with purge=True to ensure it's fully removed
                try:
                    await kv_store_service.delete(
                        SCHEDULED_JOBS_KV_NAME, job_id, purge=True
                    )
                    self._logger.info("job_cancelled", job_id=job_id)
                    return True
                except KeyNotFoundError:
                    self._logger.warning("job_not_found", job_id=job_id)
                    return False
            except Exception as e:
                error_msg = f"Failed to cancel scheduled job: {e}"
                self._logger.error(
                    "job_cancellation_failed", error=str(e), job_id=job_id
                )
                raise wrap_naq_exception(e, error_msg) from e

    async def update_job_status(self, job_id: str, status: str) -> bool:
        """
        Updates the status of a scheduled job.

        Args:
            job_id: ID of the job to update
            status: New status (ACTIVE, PAUSED, etc.)

        Returns:
            True if update was successful, False on concurrency conflict

        Raises:
            JobNotFoundError: If job doesn't exist
            NaqException: For other errors
        """
        validate_parameter(job_id, "job_id", str)
        validate_parameter(status, "status", str)

        return await self._update_job_data(
            job_id=job_id,
            update_func=lambda data: self._update_status_in_data(data, status),
            operation_name="update_job_status",
            log_context={"status": status},
        )

    async def modify_job(self, job_id: str, **updates: Any) -> bool:
        """
        Modifies parameters of a scheduled job.

        Args:
            job_id: ID of the job to modify
            **updates: Parameters to update (cron, interval, repeat, etc.)

        Returns:
            True if modification was successful, False on concurrency conflict

        Raises:
            JobNotFoundError: If job doesn't exist
            ConfigurationError: If invalid parameters are provided
            NaqException: For other errors
        """
        with self._logger.operation_context(
            "modify_job", job_id=job_id, updates=updates, queue_name=self.queue_name
        ):
            validate_parameter(job_id, "job_id", str)

            self._logger.info("modifying_job", job_id=job_id, updates=updates)
            supported_keys = {"cron", "interval", "repeat", "scheduled_timestamp_utc"}
            update_keys = set(updates.keys())

            if not update_keys.issubset(supported_keys):
                unsupported_keys = update_keys - supported_keys
                self._logger.error(
                    "unsupported_modification_keys",
                    unsupported_keys=list(unsupported_keys),
                    supported_keys=list(supported_keys),
                )
                raise ConfigurationError(
                    f"Unsupported modification keys: {unsupported_keys}. Supported: {supported_keys}"
                )

            try:
                # Use the KV store service
                _, kv_store_service = await self._get_services()
                # Get the current job data
                entry = await kv_store_service.get(SCHEDULED_JOBS_KV_NAME, job_id)
                schedule_data = cloudpickle.loads(entry.value)

                # Apply updates
                needs_next_run_recalc = False
                if "cron" in updates:
                    schedule_data["cron"] = updates["cron"]
                    schedule_data["interval_seconds"] = (
                        None  # Clear interval if cron is set
                    )
                    needs_next_run_recalc = True

                if "interval" in updates:
                    interval = updates["interval"]
                    if isinstance(interval, (int, float)):
                        interval = timedelta(seconds=interval)
                    if isinstance(interval, timedelta):
                        schedule_data["interval_seconds"] = interval.total_seconds()
                        schedule_data["cron"] = None  # Clear cron if interval is set
                        needs_next_run_recalc = True
                    else:
                        self._logger.error("invalid_interval_type", job_id=job_id)
                        raise ConfigurationError(
                            "'interval' must be timedelta or numeric seconds."
                        )

                if "repeat" in updates:
                    schedule_data["repeat"] = updates["repeat"]

                if "scheduled_timestamp_utc" in updates:
                    # Allow explicitly setting the next run time
                    schedule_data["scheduled_timestamp_utc"] = updates[
                        "scheduled_timestamp_utc"
                    ]
                    schedule_data["next_run_utc"] = updates["scheduled_timestamp_utc"]
                    needs_next_run_recalc = (
                        False  # Explicitly set, no recalc needed now
                    )

                # Recalculate next run time if cron/interval changed and not explicitly set
                if needs_next_run_recalc:
                    next_run_ts = self._calculate_next_run_time(schedule_data)

                    if next_run_ts is not None:
                        schedule_data["scheduled_timestamp_utc"] = next_run_ts
                        schedule_data["next_run_utc"] = next_run_ts
                    else:
                        # This case might occur if a one-off job's time is modified without providing a new time
                        self._logger.warning(
                            "next_run_time_calculation_failed", job_id=job_id
                        )

                # Put the updated data
                await kv_store_service.put(
                    SCHEDULED_JOBS_KV_NAME, job_id, schedule_data
                )

                self._logger.info("job_modified", job_id=job_id)
                return True

            except KeyNotFoundError:
                self._logger.error("job_not_found", job_id=job_id)
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
            except APIError as e:
                if "wrong last sequence" in str(e).lower():
                    self._logger.warning("concurrent_modification", job_id=job_id)
                    return False
                else:
                    error_msg = f"Failed to modify job: {e}"
                    self._logger.error(
                        "job_modification_failed", error=str(e), job_id=job_id
                    )
                    raise wrap_naq_exception(e, error_msg) from e
            except Exception as e:
                error_msg = f"Failed to modify job: {e}"
                self._logger.error(
                    "job_modification_failed", error=str(e), job_id=job_id
                )
                raise wrap_naq_exception(e, error_msg) from e

    def _calculate_next_run_time(
        self, schedule_data: Dict[str, Any]
    ) -> Optional[float]:
        """
        Calculates the next run time based on cron or interval.

        Args:
            schedule_data: The scheduled job data

        Returns:
            Next run timestamp or None if it couldn't be calculated
        """
        with self._logger.operation_context(
            "calculate_next_run_time", schedule_data_keys=list(schedule_data.keys())
        ):
            validate_parameter(schedule_data, "schedule_data", dict)

            now_utc = datetime.datetime.now(timezone.utc)

            if schedule_data.get("cron"):
                try:
                    from croniter import croniter

                    self._logger.debug(
                        "calculating_cron_next_run", cron=schedule_data["cron"]
                    )
                    cron_iter = croniter(schedule_data["cron"], now_utc)
                    next_run = cron_iter.get_next(datetime.datetime).timestamp()
                    self._logger.debug("cron_next_run_calculated", next_run=next_run)
                    return next_run
                except ImportError:
                    self._logger.error("croniter_missing")
                    raise ImportError(
                        "Please install 'croniter' to use cron scheduling."
                    )
                except Exception as e:
                    error_msg = f"Invalid cron format '{schedule_data['cron']}': {e}"
                    self._logger.error(
                        "cron_format_invalid", error=str(e), cron=schedule_data["cron"]
                    )
                    raise ConfigurationError(error_msg)

            elif schedule_data.get("interval_seconds"):
                # Base next run on 'now' for simplicity when modifying
                interval_seconds = schedule_data["interval_seconds"]
                self._logger.debug(
                    "calculating_interval_next_run", interval_seconds=interval_seconds
                )
                next_run = (now_utc + timedelta(seconds=interval_seconds)).timestamp()
                self._logger.debug("interval_next_run_calculated", next_run=next_run)
                return next_run

            self._logger.debug("no_schedule_found")
            return None
