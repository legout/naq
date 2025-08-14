# src/naq/queue/scheduled.py
"""
Scheduled job management for naq queues.

This module provides the ScheduledJobManager class which handles storing,
retrieving, and managing scheduled jobs in the NATS KV store.
"""

import datetime
import re
from datetime import timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import cloudpickle
import nats
from loguru import logger
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from ..connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from ..exceptions import ConfigurationError
from ..exceptions import NaqConnectionError
from ..exceptions import JobNotFoundError, NaqException
from ..models import Job, RetryDelayType
from ..settings import (
    DEFAULT_NATS_URL,
    JOB_SERIALIZER,
    NAQ_PREFIX,
    SCHEDULED_JOB_STATUS,
    SCHEDULED_JOBS_KV_NAME,
)


class ScheduledJobManager:
    """
    Manager for scheduled jobs within a Queue.
    Handles storing, retrieving, and managing scheduled jobs in the NATS KV store.
    """

    def __init__(self, queue_name: str, nats_url: str = DEFAULT_NATS_URL, service_manager = None):
        self.queue_name = queue_name
        self._nats_url = nats_url
        self._service_manager = service_manager
        self._kv: Optional[KeyValue] = None

    async def get_kv(self) -> KeyValue:
        """Gets the KeyValue store for scheduled jobs, creating it if needed."""
        if self._kv is not None:
            return self._kv

        # Ensure the KV store is created if it doesn't exist
        nc = await get_nats_connection(url=self._nats_url)
        js = await get_jetstream_context(nc=nc)
        try:
            # Try to connect to existing KV store
            self._kv = await js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
            logger.debug(f"Connected to KV store '{SCHEDULED_JOBS_KV_NAME}'")
            return self._kv
        except Exception:
            # Attempt to create if not found
            try:
                logger.info(
                    f"KV store '{SCHEDULED_JOBS_KV_NAME}' not found, creating..."
                )
                self._kv = await js.create_key_value(
                    bucket=SCHEDULED_JOBS_KV_NAME,
                    description="Stores naq scheduled job details",
                )
                logger.info(f"KV store '{SCHEDULED_JOBS_KV_NAME}' created.")
                return self._kv
            except Exception as create_e:
                raise NaqConnectionError(
                    f"Failed to access or create KV store '{SCHEDULED_JOBS_KV_NAME}': {create_e}"
                ) from create_e

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
        kv = await self.get_kv()
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
            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.put(job.job_id, serialized_schedule_data)
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
        kv = await self.get_kv()
        try:
            # Use delete with purge=True to ensure it's fully removed
            await kv.delete(job_id, purge=True)
            logger.info(f"Scheduled job '{job_id}' cancelled successfully.")
            return True
        except KeyNotFoundError:
            logger.warning(f"Scheduled job '{job_id}' not found. Cannot cancel.")
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
        kv = await self.get_kv()
        try:
            entry = await kv.get(job_id)
            if not entry:
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

            schedule_data = cloudpickle.loads(entry.value)
            if schedule_data.get("status") == status:
                logger.info(f"Scheduled job '{job_id}' already has status '{status}'.")
                return True  # No change needed

            schedule_data["status"] = status
            serialized_schedule_data = cloudpickle.dumps(schedule_data)

            # Use update with revision check for optimistic concurrency control
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            logger.info(f"Scheduled job '{job_id}' status updated to '{status}'.")
            return True
        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
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
        kv = await self.get_kv()
        supported_keys = {"cron", "interval", "repeat", "scheduled_timestamp_utc"}
        update_keys = set(updates.keys())

        if not update_keys.issubset(supported_keys):
            raise ConfigurationError(
                f"Unsupported modification keys: {update_keys - supported_keys}. Supported: {supported_keys}"
            )

        try:
            entry = await kv.get(job_id)
            if not entry:
                raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")

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
                needs_next_run_recalc = False  # Explicitly set, no recalc needed now

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

            serialized_schedule_data = cloudpickle.dumps(schedule_data)
            await kv.update(entry.key, serialized_schedule_data, last=entry.revision)
            logger.info(f"Scheduled job '{job_id}' modified successfully.")
            return True

        except KeyNotFoundError:
            raise JobNotFoundError(f"Scheduled job '{job_id}' not found.")
        except nats.js.errors.APIError as e:
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