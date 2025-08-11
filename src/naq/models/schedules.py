# src/naq/models/schedules.py
"""
Schedule model classes for NAQ job scheduling.

This module contains the Schedule class and related functionality
for managing scheduled and recurring jobs.
"""

import time
from typing import Optional

import msgspec


class Schedule(msgspec.Struct):
    """
    Represents a scheduled job configuration.
    
    This replaces the dictionary-based schedule representation used in
    the NATS KV store with a typed structure.
    """
    
    job_id: str
    scheduled_timestamp_utc: float
    status: str
    schedule_failure_count: int = 0
    _orig_job_payload: bytes = b""
    cron: Optional[str] = None
    interval_seconds: Optional[float] = None
    repeat: Optional[int] = None
    last_enqueued_utc: Optional[float] = None
    
    def is_due(self, current_time: Optional[float] = None) -> bool:
        """Check if the scheduled job is due for execution."""
        if current_time is None:
            current_time = time.time()
        return current_time >= self.scheduled_timestamp_utc
    
    def is_active(self) -> bool:
        """Check if the schedule is active (not paused or cancelled)."""
        from ..settings import SCHEDULED_JOB_STATUS
        return self.status == SCHEDULED_JOB_STATUS.ACTIVE.value
    
    def is_paused(self) -> bool:
        """Check if the schedule is paused."""
        from ..settings import SCHEDULED_JOB_STATUS
        return self.status == SCHEDULED_JOB_STATUS.PAUSED.value