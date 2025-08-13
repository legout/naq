# src/naq/settings.py
"""
NAQ Settings - Backward compatibility layer for environment-based configuration.

This module maintains backward compatibility with the original environment variable
based configuration system while integrating with the new YAML configuration system.

For new projects, use the YAML configuration system via config package.
For existing projects, this module will automatically bridge to the new system.
"""

import os
from enum import Enum
from typing import Optional

from loguru import logger

# Try to import the new configuration system
try:
    from .config import load_config, get_config, NAQConfig
    _CONFIG_SYSTEM_AVAILABLE = True
except ImportError:
    _CONFIG_SYSTEM_AVAILABLE = False
    logger.warning("New configuration system not available, using environment variables only")


def _get_config() -> Optional['NAQConfig']:
    """Get the current configuration from the new system if available."""
    if not _CONFIG_SYSTEM_AVAILABLE:
        return None
    
    try:
        return get_config()
    except Exception as e:
        logger.warning(f"Failed to load configuration from new system: {e}")
        return None


def _get_config_value(config_path: str, default_value, env_var: str = None):
    """
    Get configuration value with fallback chain:
    1. New config system (if available)
    2. Environment variable (if specified)
    3. Default value
    """
    # Try new configuration system first
    config = _get_config()
    if config:
        try:
            value = config.get_nested(config_path)
            if value is not None:
                return value
        except Exception:
            pass
    
    # Fallback to environment variable
    if env_var:
        env_value = os.getenv(env_var)
        if env_value is not None:
            return env_value
    
    # Use default value
    return default_value


# Default NATS server URL
DEFAULT_NATS_URL = _get_config_value("nats.servers[0]", "nats://localhost:4222", "NAQ_NATS_URL")

# Default queue name (maps to a NATS subject/stream)
DEFAULT_QUEUE_NAME = _get_config_value("queues.default", "naq_default_queue", "NAQ_DEFAULT_QUEUE")

# Prefix for NATS subjects/streams used by naq
NAQ_PREFIX = _get_config_value("queues.prefix", "naq", "NAQ_QUEUE_PREFIX")

# How jobs are serialized
# Options: 'pickle' (default, more flexible), 'json' (safer, less flexible)
JOB_SERIALIZER = _get_config_value("serialization.job_serializer", "pickle", "NAQ_JOB_SERIALIZER")

# Optional: Dotted paths to JSON encoder/decoder classes for custom types
# Defaults use Python's built-in json.JSONEncoder/JSONDecoder
JSON_ENCODER = _get_config_value("serialization.json.encoder", "json.JSONEncoder", "NAQ_JSON_ENCODER")
JSON_DECODER = _get_config_value("serialization.json.decoder", "json.JSONDecoder", "NAQ_JSON_DECODER")

# --- Scheduler Settings ---
# KV bucket name for scheduled jobs
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"
# KV bucket name for scheduler leader election lock
SCHEDULER_LOCK_KV_NAME = f"{NAQ_PREFIX}_scheduler_lock"
# Key used within the lock KV store
SCHEDULER_LOCK_KEY = "leader_lock"
# TTL (in seconds) for the leader lock. A scheduler renews the lock periodically.
SCHEDULER_LOCK_TTL_SECONDS = int(_get_config_value("scheduler.lock_ttl", 30, "NAQ_SCHEDULER_LOCK_TTL"))
# How often the leader tries to renew the lock (should be less than TTL)
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = int(_get_config_value("scheduler.lock_renew_interval", 15, "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL"))
# Maximum number of times the scheduler will try to enqueue a job before marking it as failed.
MAX_SCHEDULE_FAILURES = int(_get_config_value("scheduler.max_failures", 5, "NAQ_MAX_SCHEDULE_FAILURES"))


# KV bucket name for tracking job completion status (for dependencies)
JOB_STATUS_KV_NAME = f"{NAQ_PREFIX}_job_status"
# Status values stored in the job status KV

# TTL for job status entries (e.g., 1 day) - adjust as needed
JOB_STATUS_TTL_SECONDS = int(_get_config_value("results.ttl", 604800, "NAQ_JOB_STATUS_TTL"))

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
RESULT_KV_NAME = _get_config_value("results.kv_bucket", f"{NAQ_PREFIX}_results", "NAQ_RESULT_KV_BUCKET")
# Default TTL (in seconds) for job results stored in the KV store (e.g., 7 days)
DEFAULT_RESULT_TTL_SECONDS = int(_get_config_value("results.ttl", 604800, "NAQ_DEFAULT_RESULT_TTL"))


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
DEFAULT_WORKER_TTL_SECONDS = int(_get_config_value("workers.ttl", 60, "NAQ_WORKER_TTL"))
# Default interval (in seconds) for worker heartbeats
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = int(_get_config_value("workers.heartbeat_interval", 15, "NAQ_WORKER_HEARTBEAT_INTERVAL"))


# --- Event Logging Settings ---
# Enable/disable the entire event logging system
NAQ_EVENTS_ENABLED = _get_config_value("events.enabled", True, "NAQ_EVENTS_ENABLED")

# Storage backend type for events (currently only 'nats' supported) 
NAQ_EVENT_STORAGE_TYPE = "nats"  # Always NATS for now

# NATS URL for the event system (defaults to main NATS URL)
NAQ_EVENT_STORAGE_URL = _get_config_value("events.storage_url", DEFAULT_NATS_URL, "NAQ_EVENT_STORAGE_URL") or DEFAULT_NATS_URL

# JetStream stream name for job events
NAQ_EVENT_STREAM_NAME = _get_config_value("events.stream.name", "NAQ_JOB_EVENTS", "NAQ_EVENT_STREAM_NAME")

# Base subject prefix for event routing
NAQ_EVENT_SUBJECT_PREFIX = _get_config_value("events.subject_prefix", "naq.jobs.events", "NAQ_EVENT_SUBJECT_PREFIX")

# Event logger buffer settings
NAQ_EVENT_LOGGER_BATCH_SIZE = int(_get_config_value("events.batch_size", 100, "NAQ_EVENT_LOGGER_BATCH_SIZE"))
NAQ_EVENT_LOGGER_FLUSH_INTERVAL = float(_get_config_value("events.flush_interval", 5.0, "NAQ_EVENT_LOGGER_FLUSH_INTERVAL"))
NAQ_EVENT_LOGGER_MAX_BUFFER_SIZE = int(_get_config_value("events.max_buffer_size", 10000, "NAQ_EVENT_LOGGER_MAX_BUFFER_SIZE"))

# Default ack_wait (in seconds) for JetStream consumers. Must be >= max expected job duration.
# Can be overridden per-worker by passing ack_wait in Worker(...) or via env var below.
def _get_default_ack_wait():
    """Get default ack wait, trying queue configs first."""
    config = _get_config()
    if config and config.queues.configs.get("default"):
        return config.queues.configs["default"].ack_wait
    return int(_get_config_value("queues.configs.default.ack_wait", 60, "NAQ_DEFAULT_ACK_WAIT"))

DEFAULT_ACK_WAIT_SECONDS = _get_default_ack_wait()

# Optional per-queue overrides via environment, JSON object mapping queue_name -> seconds.
# Example: NAQ_ACK_WAIT_PER_QUEUE='{"email": 120, "reports": 300}'
import json as _json

def _get_ack_wait_per_queue():
    """Get per-queue ack wait settings from config or environment."""
    config = _get_config()
    if config and config.queues.configs:
        return {name: cfg.ack_wait for name, cfg in config.queues.configs.items()}
    
    # Fallback to environment variable
    _ACK_PER_QUEUE_ENV = os.getenv("NAQ_ACK_WAIT_PER_QUEUE")
    if _ACK_PER_QUEUE_ENV:
        try:
            parsed = _json.loads(_ACK_PER_QUEUE_ENV)
            if isinstance(parsed, dict):
                return {str(k): int(v) for k, v in parsed.items()}
        except Exception:
            pass
    return {}

ACK_WAIT_PER_QUEUE: dict[str, int] = _get_ack_wait_per_queue()

DEPENDENCY_CHECK_DELAY_SECONDS = 5


# --- Job Retry Settings ---
class RETRY_STRATEGY(Enum):
    """Enum representing the retry strategies for job execution."""

    LINEAR = "linear"
    EXPONENTIAL = "exponential"


# --- Logging Settings ---
# Default log level for the application. Can be one of:
# "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
# Set to "CRITICAL" by default to effectively disable logging.
LOG_LEVEL = _get_config_value("logging.level", "CRITICAL", "NAQ_LOG_LEVEL").upper()

# Whether to enable logging to a file.
LOG_TO_FILE_ENABLED = bool(_get_config_value("logging.to_file_enabled", False, "NAQ_LOG_TO_FILE_ENABLED"))

# Path for the log file. Can include placeholders like {time}.
LOG_FILE_PATH = _get_config_value("logging.file", "naq_{time}.log", "NAQ_LOG_FILE_PATH")
