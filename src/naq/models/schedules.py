# src/naq/models/schedules.py
"""
Schedule model definitions for the NAQ job queue system.

This module contains the Schedule class that represents scheduled job configurations.
The Schedule class manages job scheduling information, including recurring schedules,
timestamps, and failure tracking.
"""

import time
from typing import Optional

import msgspec

from ..settings import MAX_SCHEDULE_FAILURES
from .jobs import Job


class Schedule(msgspec.Struct):
    """
    Represents a scheduled job configuration.

    This class uses msgspec.Struct for efficient serialization and deserialization.
    All fields are properly typed with default values where appropriate.

    The Schedule class manages job scheduling information, including recurring
    schedules, timestamps, and failure tracking. It provides methods to check
    if a schedule is recurring, should be retried, and to manage failure counts.

    Attributes:
        job_id: Unique identifier for the job
        scheduled_timestamp_utc: Unix timestamp when the job should run
        _orig_job_payload: Original serialized job data
        cron: Cron expression for recurring schedules (optional)
        interval_seconds: Interval in seconds for recurring schedules (optional)
        repeat: Number of times to repeat the schedule (None for infinite)
        status: Current status of the schedule (default: "active")
        last_enqueued_utc: Last time the job was enqueued (Unix timestamp)
        schedule_failure_count: Number of consecutive schedule failures

    Example:
        ```python
        from naq.models.schedules import Schedule
        from naq.models.jobs import Job
        import time

        # Create a job first
        job = Job(function=lambda: print("Hello World"))

        # Create a recurring schedule
        schedule = Schedule(
            job_id=job.job_id,
            scheduled_timestamp_utc=time.time() + 3600,  # 1 hour from now
            _orig_job_payload=job.serialize(),
            interval_seconds=60,  # Every minute
            repeat=10  # Repeat 10 times
        )

        if schedule.is_recurring:
            print("This is a recurring schedule")

        # Create a schedule from a job object
        schedule_from_job = Schedule.from_job(
            job=job,
            scheduled_timestamp_utc=time.time() + 7200,  # 2 hours from now
            cron="0 9 * * *"  # Every day at 9 AM
        )
        ```
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
        """
        Check if this is a recurring schedule.

        Returns:
            bool: True if this schedule uses cron or interval_seconds, False otherwise

        Example:
            ```python
            if schedule.is_recurring:
                print("Schedule will repeat automatically")
            else:
                print("Schedule will run only once")
            ```
        """
        return self.cron is not None or self.interval_seconds is not None

    @property
    def is_infinite_repeat(self) -> bool:
        """
        Check if this schedule repeats infinitely.

        Returns:
            bool: True if repeat is None (infinite), False otherwise

        Example:
            ```python
            if schedule.is_infinite_repeat:
                print("Schedule will repeat forever")
            else:
                print(f"Schedule will repeat {schedule.repeat} times")
            ```
        """
        return self.repeat is None

    def should_retry_schedule(self) -> bool:
        """
        Check if the schedule should be retried after a failure.

        Returns:
            bool: True if the schedule should be retried, False if max failures reached

        Example:
            ```python
            if schedule.should_retry_schedule():
                print("Will retry this schedule")
            else:
                print("Max schedule failures reached")
            ```
        """
        if MAX_SCHEDULE_FAILURES is None:
            return True
        return self.schedule_failure_count < MAX_SCHEDULE_FAILURES

    def increment_failure_count(self) -> None:
        """
        Increment the schedule failure count.

        Example:
            ```python
            # After a schedule fails
            schedule.increment_failure_count()
            print(f"Schedule failure count: {schedule.schedule_failure_count}")
            ```
        """
        self.schedule_failure_count += 1

    def reset_failure_count(self) -> None:
        """
        Reset the schedule failure count.

        Example:
            ```python
            # After a successful schedule execution
            schedule.reset_failure_count()
            print("Schedule failure count reset to 0")
            ```
        """
        self.schedule_failure_count = 0

    def update_last_enqueued(self) -> None:
        """
        Update the last enqueued timestamp to now.

        Example:
            ```python
            # When the schedule is triggered
            schedule.update_last_enqueued()
            print(f"Schedule last enqueued at: {schedule.last_enqueued_utc}")
            ```
        """
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
        """
        Create a Schedule from a Job object.

        Args:
            job: The Job object to create a schedule from
            scheduled_timestamp_utc: When the job should be scheduled
            cron: Optional cron expression for recurring schedules
            interval_seconds: Optional interval in seconds for recurring schedules
            repeat: Optional number of repetitions (None for infinite)

        Returns:
            Schedule: A new Schedule instance

        Example:
            ```python
            from naq.models.jobs import Job
            import time

            # Create a job
            job = Job(function=lambda: print("Hello from scheduled job"))

            # Create a schedule from the job
            schedule = Schedule.from_job(
                job=job,
                scheduled_timestamp_utc=time.time() + 3600,
                interval_seconds=60,  # Every minute
                repeat=5  # Repeat 5 times
            )

            print(f"Created schedule for job {schedule.job_id}")
            ```
        """
        return cls(
            job_id=job.job_id,
            scheduled_timestamp_utc=scheduled_timestamp_utc,
            _orig_job_payload=job.serialize(),
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        )
