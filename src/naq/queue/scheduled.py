"""Scheduled job management functionality.

This module contains the ScheduledJobManager class for handling scheduled jobs.
"""

import datetime
from datetime import timedelta, timezone
from typing import Any, Dict, Optional

import cloudpickle
from loguru import logger
from nats.js.errors import KeyNotFoundError, APIError
from nats.js.kv import KeyValue

from ..connection.context_managers import nats_kv_store
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
    ):
        self.queue_name = queue_name
        self._nats_url = nats_url
        self._config = config or create_global_config()
        self._service_manager: Optional[ServiceManager] = None
        self._connection_service: Optional[ConnectionService] = None
        self._kv_store_service: Optional[KVStoreService] = None
        self._kv: Optional[KeyValue] = None

    async def _get_services(self) -> tuple[ConnectionService, KVStoreService]:
        """Get the connection and KV store services from the service manager."""
        if self._service_manager is None:
            raise NaqException(
                "ServiceManager is required for service-based operations"
            )

        if self._connection_service is None:
            self._connection_service = await self._service_manager.get_service(
                "connection", ConnectionService
            )

        if self._kv_store_service is None:
            self._kv_store_service = await self._service_manager.get_service(
                "kv_store", KVStoreService
            )

        return self._connection_service, self._kv_store_service

    async def get_kv(self):
        """Gets the KeyValue store for scheduled jobs, creating it if needed."""
        if self._kv is not None:
            return self._kv

        # Get the KV store service
        _, kv_store_service = await self._get_services()

        try:
            # Get the KV store
            self._kv = await kv_store_service.get_kv_store(SCHEDULED_JOBS_KV_NAME)
            logger.debug(f"Connected to KV store '{SCHEDULED_JOBS_KV_NAME}'")
            return self._kv
        except Exception as e:
            raise NaqException(
                f"Failed to access or create KV store '{SCHEDULED_JOBS_KV_NAME}': {e}"
            ) from e

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
            # Use the context manager for KV store operations
            config = create_global_config()
            config.nats_url = self._nats_url

            async with nats_kv_store(SCHEDULED_JOBS_KV_NAME, config) as kv:
                await kv.put(job.job_id, schedule_data)
        except Exception as e:
            raise NaqException(
                f"Failed to store scheduled job {job.job_id} in KV store: {e}"
            ) from e

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
        logger.info(f"Attempting to cancel scheduled job '{job_id}'")
        try:
            # Use the context manager for KV store operations
            config = create_global_config()
            config.nats_url = self._nats_url

            async with nats_kv_store(SCHEDULED_JOBS_KV_NAME, config) as kv:
                # Use delete with purge=True to ensure it's fully removed
                try:
                    await kv.delete(job_id, purge=True)
                    logger.info(f"Scheduled job '{job_id}' cancelled successfully.")
                    return True
                except KeyNotFoundError:
                    logger.warning(
                        f"Scheduled job '{job_id}' not found. Cannot cancel."
                    )
                    return False
        except Exception as e:
            logger.error(f"Error cancelling scheduled job '{job_id}': {e}")
            raise NaqException(f"Failed to cancel scheduled job: {e}") from e

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
        try:
            # Use the context manager for KV store operations
            config = create_global_config()
            config.nats_url = self._nats_url

            async with nats_kv_store(SCHEDULED_JOBS_KV_NAME, config) as kv:
                # Get the current job data
                entry = await kv.get(job_id)
                schedule_data = cloudpickle.loads(entry.value)

                if schedule_data.get("status") == status:
                    logger.info(
                        f"Scheduled job '{job_id}' already has status '{status}'."
                    )
                    return True  # No change needed

                # Update the status
                schedule_data["status"] = status

                # Put the updated data
                await kv.put(job_id, schedule_data)

            logger.info(f"Scheduled job '{job_id}' status updated to '{status}'.")
            return True
        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except APIError as e:
            # Handle potential revision mismatch (another process updated it)
            if "wrong last sequence" in str(e).lower():
                logger.warning(
                    f"Concurrent modification detected for job '{job_id}'. Update failed. Please retry."
                )
                return False  # Indicate update failed due to concurrency
            else:
                logger.error(f"NATS API error updating status for job '{job_id}': {e}")
                raise NaqException(f"Failed to update job status: {e}") from e
        except Exception as e:
            logger.error(f"Error updating status for job '{job_id}': {e}")
            raise NaqException(f"Failed to update job status: {e}") from e

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
        logger.info(
            f"Attempting to modify scheduled job '{job_id}' with updates: {updates}"
        )
        supported_keys = {"cron", "interval", "repeat", "scheduled_timestamp_utc"}
        update_keys = set(updates.keys())

        if not update_keys.issubset(supported_keys):
            raise ConfigurationError(
                f"Unsupported modification keys: {update_keys - supported_keys}. Supported: {supported_keys}"
            )

        try:
            # Use the context manager for KV store operations
            config = create_global_config()
            config.nats_url = self._nats_url

            async with nats_kv_store(SCHEDULED_JOBS_KV_NAME, config) as kv:
                # Get the current job data
                entry = await kv.get(job_id)
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
                        logger.warning(
                            f"Could not determine next run time for job '{job_id}' after modification. Check parameters."
                        )

                # Put the updated data
                await kv.put(job_id, schedule_data)

            logger.info(f"Scheduled job '{job_id}' modified successfully.")
            return True

        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except APIError as e:
            if "wrong last sequence" in str(e).lower():
                logger.warning(
                    f"Concurrent modification detected for job '{job_id}'. Update failed. Please retry."
                )
                return False
            else:
                logger.error(f"NATS API error modifying job '{job_id}': {e}")
                raise NaqException(f"Failed to modify job: {e}") from e
        except Exception as e:
            logger.error(f"Error modifying job '{job_id}': {e}")
            raise NaqException(f"Failed to modify job: {e}") from e

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
        now_utc = datetime.datetime.now(timezone.utc)

        if schedule_data.get("cron"):
            try:
                from croniter import croniter

                cron_iter = croniter(schedule_data["cron"], now_utc)
                return cron_iter.get_next(datetime.datetime).timestamp()
            except ImportError:
                raise ImportError("Please install 'croniter' to use cron scheduling.")
            except Exception as e:
                raise ConfigurationError(
                    f"Invalid cron format '{schedule_data['cron']}': {e}"
                )

        elif schedule_data.get("interval_seconds"):
            # Base next run on 'now' for simplicity when modifying
            return (
                now_utc + timedelta(seconds=schedule_data["interval_seconds"])
            ).timestamp()

        return None
