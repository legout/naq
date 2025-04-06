# src/naq/settings.py
import os

# Default NATS server URL
DEFAULT_NATS_URL = os.getenv("NAQ_NATS_URL", "nats://localhost:4222")

# Default queue name (maps to a NATS subject/stream)
DEFAULT_QUEUE_NAME = os.getenv("NAQ_DEFAULT_QUEUE", "naq_default_queue")

# Prefix for NATS subjects/streams used by naq
NAQ_PREFIX = "naq"

# How jobs are serialized
# Options: 'pickle' (default, more flexible), 'json' (safer, less flexible)
JOB_SERIALIZER = os.getenv("NAQ_JOB_SERIALIZER", "pickle")

# --- Scheduler Settings ---
# KV bucket name for scheduled jobs
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"
# KV bucket name for scheduler leader election lock
SCHEDULER_LOCK_KV_NAME = f"{NAQ_PREFIX}_scheduler_lock"
# Key used within the lock KV store
SCHEDULER_LOCK_KEY = "leader_lock"
# TTL (in seconds) for the leader lock. A scheduler renews the lock periodically.
SCHEDULER_LOCK_TTL_SECONDS = int(os.getenv("NAQ_SCHEDULER_LOCK_TTL", "30"))
# How often the leader tries to renew the lock (should be less than TTL)
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = int(os.getenv("NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", "15"))
# Maximum number of times the scheduler will try to enqueue a job before marking it as failed.
# Set to 0 or None for infinite retries by the scheduler itself.
MAX_SCHEDULE_FAILURES = os.getenv("NAQ_MAX_SCHEDULE_FAILURES")
if MAX_SCHEDULE_FAILURES is not None:
    try:
        MAX_SCHEDULE_FAILURES = int(MAX_SCHEDULE_FAILURES)
    except ValueError:
        print(f"Warning: Invalid NAQ_MAX_SCHEDULE_FAILURES value '{MAX_SCHEDULE_FAILURES}'. Disabling limit.")
        MAX_SCHEDULE_FAILURES = None
else:
    # Default to a reasonable limit, e.g., 5, or None for infinite
    MAX_SCHEDULE_FAILURES = 5

# Status values for scheduled jobs
SCHEDULED_JOB_STATUS_ACTIVE = "active"
SCHEDULED_JOB_STATUS_PAUSED = "paused"
SCHEDULED_JOB_STATUS_FAILED = "schedule_failed" # Failed to be scheduled/enqueued repeatedly