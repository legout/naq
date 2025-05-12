# src/naq/settings.py
import os
from enum import Enum
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
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = int(
    os.getenv("NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", "15")
)
# Maximum number of times the scheduler will try to enqueue a job before marking it as failed.
# Set to 0 or None for infinite retries by the scheduler itself.
MAX_SCHEDULE_FAILURES = os.getenv("NAQ_MAX_SCHEDULE_FAILURES")
if MAX_SCHEDULE_FAILURES is not None:
    try:
        MAX_SCHEDULE_FAILURES = int(MAX_SCHEDULE_FAILURES)
    except ValueError:
        print(
            f"Warning: Invalid NAQ_MAX_SCHEDULE_FAILURES value '{MAX_SCHEDULE_FAILURES}'. Disabling limit."
        )
        MAX_SCHEDULE_FAILURES = None
else:
    # Default to a reasonable limit, e.g., 5, or None for infinite
    MAX_SCHEDULE_FAILURES = 5

# --- Job Status Settings ---
class JOB_STATUS(Enum):
    """Enum representing the possible states of a job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    SCHEDULED = "scheduled"
    PAUSED = "paused"
    CANCELLED = "cancelled"

# KV bucket name for tracking job completion status (for dependencies)
JOB_STATUS_KV_NAME = f"{NAQ_PREFIX}_job_status"
# Status values stored in the job status KV

# TTL for job status entries (e.g., 1 day) - adjust as needed
JOB_STATUS_TTL_SECONDS = int(os.getenv("NAQ_JOB_STATUS_TTL", 86400))

# Define subject for failed jobs
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
# Define stream name for failed jobs (could be same or different)
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"

class SCHEDULED_JOB_STATUS(Enum):
    """Enum representing the possible states of a scheduled job."""
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    CANCELLED = "cancelled"

# --- Result Backend Settings ---
# KV bucket name for storing job results/errors
RESULT_KV_NAME = f"{NAQ_PREFIX}_results"
# Default TTL (in seconds) for job results stored in the KV store (e.g., 7 days)
DEFAULT_RESULT_TTL_SECONDS = int(os.getenv("NAQ_DEFAULT_RESULT_TTL", 604800))

# --- Worker Monitoring Settings ---
class WORKER_STATUS(Enum):
    """Enum representing the possible states of a worker."""
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    STOPPING = "stopping"


# KV bucket name for storing worker status and heartbeats
WORKER_KV_NAME = f"{NAQ_PREFIX}_workers"
# Default TTL (in seconds) for worker heartbeat entries. Should be longer than heartbeat interval.
DEFAULT_WORKER_TTL_SECONDS = int(os.getenv("NAQ_WORKER_TTL", "60"))
# Default interval (in seconds) for worker heartbeats
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = int(
    os.getenv("NAQ_WORKER_HEARTBEAT_INTERVAL", "15")
)

DEPENDENCY_CHECK_DELAY_SECONDS = 5

# --- Job Retry Settings ---
class RETRY_STRATEGY(Enum):
    """Enum representing the retry strategies for job execution."""
    LINEAR = "linear"
    EXPONENTIAL = "exponential"