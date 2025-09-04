"""
Schema constants for NAQ.

This module provides constants used throughout the NAQ library for naming
and configuration purposes. These constants are imported from settings
for backward compatibility.
"""

from .settings import (
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
    DEFAULT_NATS_URL,
    DEFAULT_RESULT_TTL_SECONDS,
    RESULT_KV_NAME,
)

# Re-export all constants for direct import
__all__ = [
    "SCHEDULED_JOBS_KV_NAME",
    "SCHEDULER_LOCK_KEY",
    "SCHEDULER_LOCK_KV_NAME",
    "SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS",
    "SCHEDULER_LOCK_TTL_SECONDS",
    "DEFAULT_NATS_URL",
    "DEFAULT_RESULT_TTL_SECONDS",
    "RESULT_KV_NAME",
]