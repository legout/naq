# src/naq/settings.py
import os
from enum import Enum
from typing import Any, Dict, Optional, Union

import msgspec

# Default NATS server URL
DEFAULT_NATS_URL = os.getenv("NAQ_NATS_URL", "nats://localhost:4222")

# Default queue name (maps to a NATS subject/stream)
DEFAULT_QUEUE_NAME = os.getenv("NAQ_DEFAULT_QUEUE", "naq_default_queue")

# Prefix for NATS subjects/streams used by naq
NAQ_PREFIX = "naq"

# How jobs are serialized
# Options: 'pickle' (default, more flexible), 'json' (safer, less flexible)
JOB_SERIALIZER = os.getenv("NAQ_JOB_SERIALIZER", "pickle")

# Optional: Dotted paths to JSON encoder/decoder classes for custom types
# Defaults use Python's built-in json.JSONEncoder/JSONDecoder
JSON_ENCODER = os.getenv("NAQ_JSON_ENCODER", "json.JSONEncoder")
JSON_DECODER = os.getenv("NAQ_JSON_DECODER", "json.JSONDecoder")

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


# KV bucket name for tracking job completion status (for dependencies)
JOB_STATUS_KV_NAME = f"{NAQ_PREFIX}_job_status"
# Status values stored in the job status KV

# TTL for job status entries (e.g., 1 day) - adjust as needed
JOB_STATUS_TTL_SECONDS = int(os.getenv("NAQ_JOB_STATUS_TTL", 86400))

# Define subject for failed jobs
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
# Define stream name for failed jobs (could be same or different)
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"


# --- Result Backend Settings ---
# KV bucket name for storing job results/errors
RESULT_KV_NAME = f"{NAQ_PREFIX}_results"
# Default TTL (in seconds) for job results stored in the KV store (e.g., 7 days)
DEFAULT_RESULT_TTL_SECONDS = int(os.getenv("NAQ_DEFAULT_RESULT_TTL", 604800))


# KV bucket name for storing worker status and heartbeats
WORKER_KV_NAME = f"{NAQ_PREFIX}_workers"
# Default TTL (in seconds) for worker heartbeat entries. Should be longer than heartbeat interval.
DEFAULT_WORKER_TTL_SECONDS = int(os.getenv("NAQ_WORKER_TTL", "60"))
# Default interval (in seconds) for worker heartbeats
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = int(
    os.getenv("NAQ_WORKER_HEARTBEAT_INTERVAL", "15")
)

# Default ack_wait (in seconds) for JetStream consumers. Must be >= max expected job duration.
# Can be overridden per-worker by passing ack_wait in Worker(...) or via env var below.
DEFAULT_ACK_WAIT_SECONDS = int(os.getenv("NAQ_DEFAULT_ACK_WAIT", "60"))

# Optional per-queue overrides via environment, JSON object mapping queue_name -> seconds.
# Example: NAQ_ACK_WAIT_PER_QUEUE='{"email": 120, "reports": 300}'
import json as _json

_ACK_PER_QUEUE_ENV = os.getenv("NAQ_ACK_WAIT_PER_QUEUE")
ACK_WAIT_PER_QUEUE: dict[str, int] = {}
if _ACK_PER_QUEUE_ENV:
    try:
        parsed = _json.loads(_ACK_PER_QUEUE_ENV)
        if isinstance(parsed, dict):
            ACK_WAIT_PER_QUEUE = {str(k): int(v) for k, v in parsed.items()}
    except Exception:
        # Leave as empty on parse error
        ACK_WAIT_PER_QUEUE = {}

DEPENDENCY_CHECK_DELAY_SECONDS = 5


# --- Job Retry Settings ---
# Import RETRY_STRATEGY from models package
from .models.enums import RETRY_STRATEGY


# --- Logging Settings ---
# Default log level for the application. Can be one of:
# "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
# Set to "CRITICAL" by default to effectively disable logging.
LOG_LEVEL = os.getenv("NAQ_LOG_LEVEL", "CRITICAL").upper()

# Whether to enable logging to a file.
LOG_TO_FILE_ENABLED = os.getenv("NAQ_LOG_TO_FILE_ENABLED", "False").lower() == "true"

# Path for the log file. Can include placeholders like {time}.
LOG_FILE_PATH = os.getenv("NAQ_LOG_FILE_PATH", "naq_{time}.log")


# --- NATS Connection Configuration ---

class NATSConnectionConfig(msgspec.Struct):
    """
    Configuration for NATS connections.
    
    This class provides a structured way to configure NATS connection parameters
    with support for environment variable overrides and default values.
    
    Attributes:
        servers: List of NATS server URLs (e.g., ["nats://localhost:4222"])
        max_reconnect_attempts: Maximum number of reconnection attempts
        reconnect_time_wait: Time to wait between reconnection attempts (in seconds)
        connection_timeout: Timeout for establishing a connection (in seconds)
        ping_interval: Interval for sending ping messages (in seconds)
        max_outstanding_pings: Maximum number of outstanding pings before connection is considered dead
        prefer_thread_local: Whether to prefer thread-local connections for sync helpers
        name: Name to identify this client connection
        no_randomize: Whether to disable server randomization
        tls: TLS configuration for secure connections
        user: Username for authentication
        password: Password for authentication
        token: Token for authentication
        nkey: NKey for authentication
        credentials: Path to credentials file for authentication
    """
    
    servers: list[str] = msgspec.field(default_factory=lambda: [DEFAULT_NATS_URL])
    max_reconnect_attempts: int = msgspec.field(default_factory=lambda: int(os.getenv("NAQ_MAX_RECONNECT_ATTEMPTS", "5")))
    reconnect_time_wait: float = msgspec.field(default_factory=lambda: float(os.getenv("NAQ_RECONNECT_TIME_WAIT", "2.0")))
    connection_timeout: float = msgspec.field(default_factory=lambda: float(os.getenv("NAQ_CONNECTION_TIMEOUT", "10.0")))
    ping_interval: float = msgspec.field(default_factory=lambda: float(os.getenv("NAQ_PING_INTERVAL", "30.0")))
    max_outstanding_pings: int = msgspec.field(default_factory=lambda: int(os.getenv("NAQ_MAX_OUTSTANDING_PINGS", "3")))
    prefer_thread_local: bool = msgspec.field(default_factory=lambda: os.getenv("NAQ_PREFER_THREAD_LOCAL", "false").lower() == "true")
    name: str = msgspec.field(default_factory=lambda: os.getenv("NAQ_CLIENT_NAME", "naq_client"))
    no_randomize: bool = msgspec.field(default_factory=lambda: os.getenv("NAQ_NO_RANDOMIZE", "false").lower() == "true")
    tls: Optional[dict[str, Any]] = None
    user: Optional[str] = msgspec.field(default_factory=lambda: os.getenv("NAQ_USER"))
    password: Optional[str] = msgspec.field(default_factory=lambda: os.getenv("NAQ_PASSWORD"))
    token: Optional[str] = msgspec.field(default_factory=lambda: os.getenv("NAQ_TOKEN"))
    nkey: Optional[str] = msgspec.field(default_factory=lambda: os.getenv("NAQ_NKEY"))
    credentials: Optional[str] = msgspec.field(default_factory=lambda: os.getenv("NAQ_CREDENTIALS"))
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Ensure servers is not empty
        if not self.servers:
            self.servers = [DEFAULT_NATS_URL]
        
        # Validate numeric values
        if self.max_reconnect_attempts < 0:
            raise ValueError("max_reconnect_attempts must be non-negative")
        if self.reconnect_time_wait < 0:
            raise ValueError("reconnect_time_wait must be non-negative")
        if self.connection_timeout < 0:
            raise ValueError("connection_timeout must be non-negative")
        if self.ping_interval < 0:
            raise ValueError("ping_interval must be non-negative")
        if self.max_outstanding_pings < 0:
            raise ValueError("max_outstanding_pings must be non-negative")


class Config:
    """
    Main configuration class for NAQ.
    
    This class integrates all configuration settings including NATS connection
    configuration, job settings, scheduler settings, and other system parameters.
    It supports loading from environment variables, configuration files, and
    provides validation and default values.
    
    Attributes:
        nats_connection: NATS connection configuration
        queue_name: Default queue name for jobs
        job_serializer: Job serialization method
        json_encoder: JSON encoder class path
        json_decoder: JSON decoder class path
        scheduler_lock_ttl_seconds: TTL for scheduler lock (in seconds)
        scheduler_lock_renew_interval_seconds: Interval for scheduler lock renewal (in seconds)
        max_schedule_failures: Maximum number of scheduling failures before giving up
        job_status_ttl_seconds: TTL for job status entries (in seconds)
        default_result_ttl_seconds: Default TTL for job results (in seconds)
        worker_ttl_seconds: Default TTL for worker entries (in seconds)
        worker_heartbeat_interval_seconds: Default interval for worker heartbeats (in seconds)
        default_ack_wait_seconds: Default ack wait time for JetStream consumers (in seconds)
        ack_wait_per_queue: Per-queue ack wait time overrides
        dependency_check_delay_seconds: Delay between dependency checks (in seconds)
        log_level: Logging level
        log_to_file_enabled: Whether to enable file logging
        log_file_path: Path for the log file
    """
    
    def __init__(
        self,
        nats_connection: Optional[NATSConnectionConfig] = None,
        queue_name: Optional[str] = None,
        job_serializer: Optional[str] = None,
        json_encoder: Optional[str] = None,
        json_decoder: Optional[str] = None,
        scheduler_lock_ttl_seconds: Optional[int] = None,
        scheduler_lock_renew_interval_seconds: Optional[int] = None,
        max_schedule_failures: Optional[int] = None,
        job_status_ttl_seconds: Optional[int] = None,
        default_result_ttl_seconds: Optional[int] = None,
        worker_ttl_seconds: Optional[int] = None,
        worker_heartbeat_interval_seconds: Optional[int] = None,
        default_ack_wait_seconds: Optional[int] = None,
        ack_wait_per_queue: Optional[dict[str, int]] = None,
        dependency_check_delay_seconds: Optional[int] = None,
        log_level: Optional[str] = None,
        log_to_file_enabled: Optional[bool] = None,
        log_file_path: Optional[str] = None,
    ) -> None:
        """Initialize the configuration with optional overrides."""
        # NATS connection configuration
        self.nats_connection = nats_connection or NATSConnectionConfig()
        
        # Queue and job configuration
        self.queue_name = queue_name or DEFAULT_QUEUE_NAME
        self.job_serializer = job_serializer or JOB_SERIALIZER
        self.json_encoder = json_encoder or JSON_ENCODER
        self.json_decoder = json_decoder or JSON_DECODER
        
        # Scheduler configuration
        self.scheduler_lock_ttl_seconds = scheduler_lock_ttl_seconds or SCHEDULER_LOCK_TTL_SECONDS
        self.scheduler_lock_renew_interval_seconds = scheduler_lock_renew_interval_seconds or SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS
        self.max_schedule_failures = max_schedule_failures or MAX_SCHEDULE_FAILURES
        
        # TTL and timeout configuration
        self.job_status_ttl_seconds = job_status_ttl_seconds or JOB_STATUS_TTL_SECONDS
        self.default_result_ttl_seconds = default_result_ttl_seconds or DEFAULT_RESULT_TTL_SECONDS
        self.worker_ttl_seconds = worker_ttl_seconds or DEFAULT_WORKER_TTL_SECONDS
        self.worker_heartbeat_interval_seconds = worker_heartbeat_interval_seconds or DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS
        self.default_ack_wait_seconds = default_ack_wait_seconds or DEFAULT_ACK_WAIT_SECONDS
        self.ack_wait_per_queue = ack_wait_per_queue or ACK_WAIT_PER_QUEUE
        self.dependency_check_delay_seconds = dependency_check_delay_seconds or DEPENDENCY_CHECK_DELAY_SECONDS
        
        # Logging configuration
        self.log_level = log_level or LOG_LEVEL
        self.log_to_file_enabled = log_to_file_enabled or LOG_TO_FILE_ENABLED
        self.log_file_path = log_file_path or LOG_FILE_PATH
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the configuration values."""
        # Validate numeric values
        if self.scheduler_lock_ttl_seconds <= 0:
            raise ValueError("scheduler_lock_ttl_seconds must be positive")
        if self.scheduler_lock_renew_interval_seconds <= 0:
            raise ValueError("scheduler_lock_renew_interval_seconds must be positive")
        if self.job_status_ttl_seconds < 0:
            raise ValueError("job_status_ttl_seconds must be non-negative")
        if self.default_result_ttl_seconds < 0:
            raise ValueError("default_result_ttl_seconds must be non-negative")
        if self.worker_ttl_seconds <= 0:
            raise ValueError("worker_ttl_seconds must be positive")
        if self.worker_heartbeat_interval_seconds <= 0:
            raise ValueError("worker_heartbeat_interval_seconds must be positive")
        if self.default_ack_wait_seconds <= 0:
            raise ValueError("default_ack_wait_seconds must be positive")
        if self.dependency_check_delay_seconds < 0:
            raise ValueError("dependency_check_delay_seconds must be non-negative")
        
        # Validate ack wait per queue
        for queue, ack_wait in self.ack_wait_per_queue.items():
            if ack_wait <= 0:
                raise ValueError(f"ack_wait for queue '{queue}' must be positive")
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Create a configuration instance from environment variables.
        
        Returns:
            A Config instance with values loaded from environment variables.
        """
        # Create NATS connection config from environment
        nats_config = NATSConnectionConfig()
        
        # Create main config with environment overrides
        return cls(
            nats_connection=nats_config,
            queue_name=os.getenv("NAQ_DEFAULT_QUEUE"),
            job_serializer=os.getenv("NAQ_JOB_SERIALIZER"),
            json_encoder=os.getenv("NAQ_JSON_ENCODER"),
            json_decoder=os.getenv("NAQ_JSON_DECODER"),
            scheduler_lock_ttl_seconds=int(os.getenv("NAQ_SCHEDULER_LOCK_TTL", SCHEDULER_LOCK_TTL_SECONDS)),
            scheduler_lock_renew_interval_seconds=int(os.getenv("NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS)),
            max_schedule_failures=int(os.getenv("NAQ_MAX_SCHEDULE_FAILURES", MAX_SCHEDULE_FAILURES)) if os.getenv("NAQ_MAX_SCHEDULE_FAILURES") else None,
            job_status_ttl_seconds=int(os.getenv("NAQ_JOB_STATUS_TTL", JOB_STATUS_TTL_SECONDS)),
            default_result_ttl_seconds=int(os.getenv("NAQ_DEFAULT_RESULT_TTL", DEFAULT_RESULT_TTL_SECONDS)),
            worker_ttl_seconds=int(os.getenv("NAQ_WORKER_TTL", DEFAULT_WORKER_TTL_SECONDS)),
            worker_heartbeat_interval_seconds=int(os.getenv("NAQ_WORKER_HEARTBEAT_INTERVAL", DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS)),
            default_ack_wait_seconds=int(os.getenv("NAQ_DEFAULT_ACK_WAIT", DEFAULT_ACK_WAIT_SECONDS)),
            ack_wait_per_queue=ACK_WAIT_PER_QUEUE,  # Already loaded from environment
            dependency_check_delay_seconds=DEPENDENCY_CHECK_DELAY_SECONDS,
            log_level=os.getenv("NAQ_LOG_LEVEL", LOG_LEVEL),
            log_to_file_enabled=os.getenv("NAQ_LOG_TO_FILE_ENABLED", "false").lower() == "true",
            log_file_path=os.getenv("NAQ_LOG_FILE_PATH", LOG_FILE_PATH),
        )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Config":
        """
        Create a configuration instance from a dictionary.
        
        Args:
            config_dict: Dictionary containing configuration values.
            
        Returns:
            A Config instance with values from the dictionary.
        """
        # Extract NATS connection config if provided
        nats_config_dict = config_dict.pop("nats_connection", {})
        if nats_config_dict:
            nats_config = NATSConnectionConfig(**nats_config_dict)
        else:
            nats_config = None
        
        # Create main config
        return cls(
            nats_connection=nats_config,
            **config_dict
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the configuration to a dictionary.
        
        Returns:
            A dictionary containing all configuration values.
        """
        return {
            "nats_connection": msgspec.to_builtins(self.nats_connection),
            "queue_name": self.queue_name,
            "job_serializer": self.job_serializer,
            "json_encoder": self.json_encoder,
            "json_decoder": self.json_decoder,
            "scheduler_lock_ttl_seconds": self.scheduler_lock_ttl_seconds,
            "scheduler_lock_renew_interval_seconds": self.scheduler_lock_renew_interval_seconds,
            "max_schedule_failures": self.max_schedule_failures,
            "job_status_ttl_seconds": self.job_status_ttl_seconds,
            "default_result_ttl_seconds": self.default_result_ttl_seconds,
            "worker_ttl_seconds": self.worker_ttl_seconds,
            "worker_heartbeat_interval_seconds": self.worker_heartbeat_interval_seconds,
            "default_ack_wait_seconds": self.default_ack_wait_seconds,
            "ack_wait_per_queue": self.ack_wait_per_queue,
            "dependency_check_delay_seconds": self.dependency_check_delay_seconds,
            "log_level": self.log_level,
            "log_to_file_enabled": self.log_to_file_enabled,
            "log_file_path": self.log_file_path,
        }


# Create a global configuration instance
_global_config: Optional[Config] = None


def get_global_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        The global configuration instance, creating it if necessary.
    """
    global _global_config
    if _global_config is None:
        _global_config = Config.from_env()
    return _global_config


def set_global_config(config: Config) -> None:
    """
    Set the global configuration instance.
    
    Args:
        config: The configuration instance to set as global.
    """
    global _global_config
    _global_config = config


def reset_global_config() -> None:
    """Reset the global configuration instance."""
    global _global_config
    _global_config = None
