"""
NAQ Schedule Models Module

This module contains Schedule model definitions for the NAQ job queue system.
"""

import time
from typing import Optional, TYPE_CHECKING

import msgspec

from ..settings import MAX_SCHEDULE_FAILURES

if TYPE_CHECKING:
    from .jobs import Job


class Schedule(msgspec.Struct):
    """
    Represents a scheduled job configuration.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.
    """

    job_id: str
    scheduled_timestamp_utc: float
    _orig_job_payload: bytes
    cron: Optional[str] = None
    interval_seconds: Optional[float] = None
    repeat: Optional[int] = None
    status: str = "active"
    last_enqueued_utc: Optional[float] = None
    schedule_failure_count: int = 0

    @property
    def is_recurring(self) -> bool:
        """Check if this is a recurring schedule."""
        return self.cron is not None or self.interval_seconds is not None

    @property
    def is_infinite_repeat(self) -> bool:
        """Check if this schedule repeats infinitely."""
        return self.repeat is None

    def should_retry_schedule(self) -> bool:
        """Check if the schedule should be retried after a failure."""
        if MAX_SCHEDULE_FAILURES is None:
            return True
        return self.schedule_failure_count < MAX_SCHEDULE_FAILURES

    def increment_failure_count(self) -> None:
        """Increment the schedule failure count."""
        self.schedule_failure_count += 1

    def reset_failure_count(self) -> None:
        """Reset the schedule failure count."""
        self.schedule_failure_count = 0

    def update_last_enqueued(self) -> None:
        """Update the last enqueued timestamp to now."""
        self.last_enqueued_utc = time.time()

    @classmethod
    def from_job(
        cls,
        job: "Job",
        scheduled_timestamp_utc: float,
        cron: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        repeat: Optional[int] = None,
    ) -> "Schedule":
        """Create a Schedule from a Job object."""
        return cls(
            job_id=job.job_id,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            _orig_job_payload=job.serialize(),
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        )