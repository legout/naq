# src/naq/settings.py
import json as _json
import os
from typing import Any, Dict, List, Optional

import msgspec

from naq.config import get_config


def _get_env_or_config(
    env_var: str, config_path: List[str], default: Optional[Any] = None
) -> Any:
    """
    Retrieves a configuration value, prioritizing environment variables over the new configuration system.

    Args:
        env_var: The name of the environment variable (e.g., "NAQ_NATS_URL").
        config_path: A list of keys representing the path to the value in the NAQConfig object
                     (e.g., ["nats", "servers"] for config.nats.servers).
        default: The default value to return if neither the environment variable nor the config path yields a value.

    Returns:
        The configuration value.
    """
    env_value = os.getenv(env_var)
    if env_value is not None:
        # Attempt to convert environment variable to appropriate type if possible
        # This is a simplification; a more robust solution might involve type introspection
        # or a dedicated conversion utility. For now, return as string.
        return env_value

    # Get the global NAQConfig instance
    cfg = get_config()

    # Traverse the config object using the config_path
    current_value = cfg
    try:
        for key in config_path:
            if isinstance(current_value, dict):
                current_value = current_value.get(key)
            else:  # Assume it's a msgspec.Struct or similar object
                current_value = getattr(current_value, key)
            if current_value is None:  # Path not found or value is None
                return default
    except AttributeError:  # Attribute not found on a struct
        return default
    except KeyError:  # Key not found in a dict
        return default
    except Exception:  # Catch any other unexpected errors during traversal
        return default

    return current_value if current_value is not None else default


# Default NATS server URL
DEFAULT_NATS_URL = _get_env_or_config(
    "NAQ_NATS_URL", ["nats", "servers"], "nats://localhost:4222"
)

# Default queue name (maps to a NATS subject/stream)
DEFAULT_QUEUE_NAME = _get_env_or_config(
    "NAQ_DEFAULT_QUEUE", ["queues", "default_name"], "naq_default_queue"
)

# Prefix for NATS subjects/streams used by naq
NAQ_PREFIX = "naq"

# How jobs are serialized
# Options: 'pickle' (default, more flexible), 'json' (safer, less flexible)
JOB_SERIALIZER = _get_env_or_config(
    "NAQ_JOB_SERIALIZER", ["serialization", "method"], "pickle"
)

# Optional: Dotted paths to JSON encoder/decoder classes for custom types
# Defaults use Python's built-in json.JSONEncoder/JSONDecoder
JSON_ENCODER = _get_env_or_config(
    "NAQ_JSON_ENCODER", ["serialization", "json_encoder"], "json.JSONEncoder"
)
JSON_DECODER = _get_env_or_config(
    "NAQ_JSON_DECODER", ["serialization", "json_decoder"], "json.JSONDecoder"
)

# --- Scheduler Settings ---
# KV bucket name for scheduled jobs
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"
# KV bucket name for scheduler leader election lock
SCHEDULER_LOCK_KV_NAME = f"{NAQ_PREFIX}_scheduler_lock"
# Key used within the lock KV store
SCHEDULER_LOCK_KEY = "leader_lock"
# TTL (in seconds) for the leader lock. A scheduler renews the lock periodically.
SCHEDULER_LOCK_TTL_SECONDS = int(
    _get_env_or_config("NAQ_SCHEDULER_LOCK_TTL", ["scheduler", "lock_ttl"], "30")
)
# How often the leader tries to renew the lock (should be less than TTL)
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = int(
    _get_env_or_config(
        "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", ["scheduler", "lock_renew_interval"], "15"
    )
)
# Maximum number of times the scheduler will try to enqueue a job before marking it as failed.
# Set to 0 or None for infinite retries by the scheduler itself.
MAX_SCHEDULE_FAILURES = _get_env_or_config(
    "NAQ_MAX_SCHEDULE_FAILURES", ["scheduler", "max_failures"]
)
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
JOB_STATUS_TTL_SECONDS = int(
    _get_env_or_config("NAQ_JOB_STATUS_TTL", ["scheduler", "job_status_ttl"], 86400)
)

# Define subject for failed jobs
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
# Define stream name for failed jobs (could be same or different)
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"


# --- Result Backend Settings ---
# KV bucket name for storing job results/errors
RESULT_KV_NAME = f"{NAQ_PREFIX}_results"
# Default TTL (in seconds) for job results stored in the KV store (e.g., 7 days)
DEFAULT_RESULT_TTL_SECONDS = int(
    _get_env_or_config("NAQ_DEFAULT_RESULT_TTL", ["results", "ttl"], 604800)
)


# KV bucket name for storing worker status and heartbeats
WORKER_KV_NAME = f"{NAQ_PREFIX}_workers"
# Default TTL (in seconds) for worker heartbeat entries. Should be longer than heartbeat interval.
DEFAULT_WORKER_TTL_SECONDS = int(
    _get_env_or_config("NAQ_WORKER_TTL", ["workers", "ttl"], "60")
)
# Default interval (in seconds) for worker heartbeats
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = int(
    _get_env_or_config(
        "NAQ_WORKER_HEARTBEAT_INTERVAL", ["workers", "heartbeat_interval"], "15"
    )
)

# Default ack_wait (in seconds) for JetStream consumers. Must be >= max expected job duration.
# Can be overridden per-worker by passing ack_wait in Worker(...) or via env var below.
DEFAULT_ACK_WAIT_SECONDS = int(
    _get_env_or_config("NAQ_DEFAULT_ACK_WAIT", ["queues", "ack_wait"], "60")
)

# Optional per-queue overrides via environment, JSON object mapping queue_name -> seconds.
# Example: NAQ_ACK_WAIT_PER_QUEUE='{"email": 120, "reports": 300}'

_ACK_PER_QUEUE_ENV = _get_env_or_config(
    "NAQ_ACK_WAIT_PER_QUEUE", ["queues", "ack_wait_per_queue"]
)
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


# --- Logging Settings ---
# Default log level for the application. Can be one of:
# "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
# Set to "CRITICAL" by default to effectively disable logging.
LOG_LEVEL = _get_env_or_config(
    "NAQ_LOG_LEVEL", ["logging", "level"], "CRITICAL"
).upper()

# Whether to enable logging to a file.
LOG_TO_FILE_ENABLED = (
    _get_env_or_config(
        "NAQ_LOG_TO_FILE_ENABLED", ["logging", "to_file_enabled"], "False"
    ).lower()
    == "true"
)

# Path for the log file. Can include placeholders like {time}.
LOG_FILE_PATH = _get_env_or_config(
    "NAQ_LOG_FILE_PATH", ["logging", "file_path"], "naq_{time}.log"
)


# --- Event System Configuration ---

# Default event system configuration
EVENTS_ENABLED = _get_env_or_config(
    "NAQ_EVENTS_ENABLED", ["events", "enabled"], "False"
).lower() == "true"

EVENTS_BATCH_SIZE = int(
    _get_env_or_config("NAQ_EVENTS_BATCH_SIZE", ["events", "batch_size"], "100")
)

EVENTS_FLUSH_INTERVAL = float(
    _get_env_or_config("NAQ_EVENTS_FLUSH_INTERVAL", ["events", "flush_interval"], "5.0")
)

EVENTS_MAX_BUFFER_SIZE = int(
    _get_env_or_config("NAQ_EVENTS_MAX_BUFFER_SIZE", ["events", "max_buffer_size"], "1000")
)

EVENTS_STREAM_NAME = _get_env_or_config(
    "NAQ_EVENTS_STREAM_NAME", ["events", "stream"], "naq_events"
)

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
    max_reconnect_attempts: int = msgspec.field(
        default_factory=lambda: int(
            _get_env_or_config(
                "NAQ_MAX_RECONNECT_ATTEMPTS", ["nats", "max_reconnect_attempts"], "5"
            )
        )
    )
    reconnect_time_wait: float = msgspec.field(
        default_factory=lambda: float(
            _get_env_or_config(
                "NAQ_RECONNECT_TIME_WAIT", ["nats", "reconnect_time_wait"], "2.0"
            )
        )
    )
    connection_timeout: float = msgspec.field(
        default_factory=lambda: float(
            _get_env_or_config(
                "NAQ_CONNECTION_TIMEOUT", ["nats", "connection_timeout"], "10.0"
            )
        )
    )
    ping_interval: float = msgspec.field(
        default_factory=lambda: float(
            _get_env_or_config("NAQ_PING_INTERVAL", ["nats", "ping_interval"], "30.0")
        )
    )
    max_outstanding_pings: int = msgspec.field(
        default_factory=lambda: int(
            _get_env_or_config(
                "NAQ_MAX_OUTSTANDING_PINGS", ["nats", "max_outstanding_pings"], "3"
            )
        )
    )
    prefer_thread_local: bool = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_PREFER_THREAD_LOCAL", ["nats", "prefer_thread_local"], "false"
        ).lower()
        == "true"
    )
    name: str = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_CLIENT_NAME", ["nats", "client_name"], "naq_client"
        )
    )
    no_randomize: bool = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_NO_RANDOMIZE", ["nats", "no_randomize"], "false"
        ).lower()
        == "true"
    )
    tls: Optional[dict[str, Any]] = None
    user: Optional[str] = msgspec.field(
        default_factory=lambda: _get_env_or_config("NAQ_USER", ["nats", "auth", "user"])
    )
    password: Optional[str] = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_PASSWORD", ["nats", "auth", "password"]
        )
    )
    token: Optional[str] = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_TOKEN", ["nats", "auth", "token"]
        )
    )
    nkey: Optional[str] = msgspec.field(
        default_factory=lambda: _get_env_or_config("NAQ_NKEY", ["nats", "auth", "nkey"])
    )
    credentials: Optional[str] = msgspec.field(
        default_factory=lambda: _get_env_or_config(
            "NAQ_CREDENTIALS", ["nats", "auth", "credentials"]
        )
    )

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
        events_enabled: Whether event processing is enabled
        events_batch_size: Number of events to batch before flushing
        events_flush_interval: Maximum time to wait before flushing batched events (in seconds)
        events_max_buffer_size: Maximum number of events to hold in the in-memory buffer
        events_stream_name: Name of the event stream
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
        events_enabled: Optional[bool] = None,
        events_batch_size: Optional[int] = None,
        events_flush_interval: Optional[float] = None,
        events_max_buffer_size: Optional[int] = None,
        events_stream_name: Optional[str] = None,
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
        self.scheduler_lock_ttl_seconds = (
            scheduler_lock_ttl_seconds or SCHEDULER_LOCK_TTL_SECONDS
        )
        self.scheduler_lock_renew_interval_seconds = (
            scheduler_lock_renew_interval_seconds
            or SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS
        )
        self.max_schedule_failures = max_schedule_failures or MAX_SCHEDULE_FAILURES

        # TTL and timeout configuration
        self.job_status_ttl_seconds = job_status_ttl_seconds or JOB_STATUS_TTL_SECONDS
        self.default_result_ttl_seconds = (
            default_result_ttl_seconds or DEFAULT_RESULT_TTL_SECONDS
        )
        self.worker_ttl_seconds = worker_ttl_seconds or DEFAULT_WORKER_TTL_SECONDS
        self.worker_heartbeat_interval_seconds = (
            worker_heartbeat_interval_seconds
            or DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS
        )
        self.default_ack_wait_seconds = (
            default_ack_wait_seconds or DEFAULT_ACK_WAIT_SECONDS
        )
        self.ack_wait_per_queue = ack_wait_per_queue or ACK_WAIT_PER_QUEUE
        self.dependency_check_delay_seconds = (
            dependency_check_delay_seconds or DEPENDENCY_CHECK_DELAY_SECONDS
        )

        # Logging configuration
        self.log_level = log_level or LOG_LEVEL
        self.log_to_file_enabled = log_to_file_enabled or LOG_TO_FILE_ENABLED
        self.log_file_path = log_file_path or LOG_FILE_PATH

        # Event system configuration
        self.events_enabled = events_enabled or EVENTS_ENABLED
        self.events_batch_size = events_batch_size or EVENTS_BATCH_SIZE
        self.events_flush_interval = events_flush_interval or EVENTS_FLUSH_INTERVAL
        self.events_max_buffer_size = events_max_buffer_size or EVENTS_MAX_BUFFER_SIZE
        self.events_stream_name = events_stream_name or EVENTS_STREAM_NAME

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

        # Validate event system configuration
        if self.events_batch_size <= 0:
            raise ValueError("events_batch_size must be positive")
        if self.events_flush_interval < 0:
            raise ValueError("events_flush_interval must be non-negative")
        if self.events_max_buffer_size <= 0:
            raise ValueError("events_max_buffer_size must be positive")

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
            queue_name=_get_env_or_config(
                "NAQ_DEFAULT_QUEUE", ["queues", "default_name"]
            ),
            job_serializer=_get_env_or_config(
                "NAQ_JOB_SERIALIZER", ["serialization", "method"]
            ),
            json_encoder=_get_env_or_config(
                "NAQ_JSON_ENCODER", ["serialization", "json_encoder"]
            ),
            json_decoder=_get_env_or_config(
                "NAQ_JSON_DECODER", ["serialization", "json_decoder"]
            ),
            scheduler_lock_ttl_seconds=int(
                _get_env_or_config(
                    "NAQ_SCHEDULER_LOCK_TTL",
                    ["scheduler", "lock_ttl"],
                    SCHEDULER_LOCK_TTL_SECONDS,
                )
            ),
            scheduler_lock_renew_interval_seconds=int(
                _get_env_or_config(
                    "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL",
                    ["scheduler", "lock_renew_interval"],
                    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
                )
            ),
            max_schedule_failures=int(
                _get_env_or_config(
                    "NAQ_MAX_SCHEDULE_FAILURES",
                    ["scheduler", "max_failures"],
                    MAX_SCHEDULE_FAILURES,
                )
            )
            if _get_env_or_config(
                "NAQ_MAX_SCHEDULE_FAILURES", ["scheduler", "max_failures"]
            )
            else None,
            job_status_ttl_seconds=int(
                _get_env_or_config(
                    "NAQ_JOB_STATUS_TTL",
                    ["scheduler", "job_status_ttl"],
                    JOB_STATUS_TTL_SECONDS,
                )
            ),
            default_result_ttl_seconds=int(
                _get_env_or_config(
                    "NAQ_DEFAULT_RESULT_TTL",
                    ["results", "ttl"],
                    DEFAULT_RESULT_TTL_SECONDS,
                )
            ),
            worker_ttl_seconds=int(
                _get_env_or_config(
                    "NAQ_WORKER_TTL", ["workers", "ttl"], DEFAULT_WORKER_TTL_SECONDS
                )
            ),
            worker_heartbeat_interval_seconds=int(
                _get_env_or_config(
                    "NAQ_WORKER_HEARTBEAT_INTERVAL",
                    ["workers", "heartbeat_interval"],
                    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
                )
            ),
            default_ack_wait_seconds=int(
                _get_env_or_config(
                    "NAQ_DEFAULT_ACK_WAIT",
                    ["queues", "ack_wait"],
                    DEFAULT_ACK_WAIT_SECONDS,
                )
            ),
            ack_wait_per_queue=ACK_WAIT_PER_QUEUE,  # Already loaded from environment
            dependency_check_delay_seconds=DEPENDENCY_CHECK_DELAY_SECONDS,
            log_level=_get_env_or_config(
                "NAQ_LOG_LEVEL", ["logging", "level"], LOG_LEVEL
            ),
            log_to_file_enabled=_get_env_or_config(
                "NAQ_LOG_TO_FILE_ENABLED", ["logging", "to_file_enabled"], "false"
            ).lower()
            == "true",
            log_file_path=_get_env_or_config(
                "NAQ_LOG_FILE_PATH", ["logging", "file_path"], LOG_FILE_PATH
            ),
            events_enabled=_get_env_or_config(
                "NAQ_EVENTS_ENABLED", ["events", "enabled"], "False"
            ).lower()
            == "true",
            events_batch_size=int(
                _get_env_or_config(
                    "NAQ_EVENTS_BATCH_SIZE",
                    ["events", "batch_size"],
                    EVENTS_BATCH_SIZE,
                )
            ),
            events_flush_interval=float(
                _get_env_or_config(
                    "NAQ_EVENTS_FLUSH_INTERVAL",
                    ["events", "flush_interval"],
                    EVENTS_FLUSH_INTERVAL,
                )
            ),
            events_max_buffer_size=int(
                _get_env_or_config(
                    "NAQ_EVENTS_MAX_BUFFER_SIZE",
                    ["events", "max_buffer_size"],
                    EVENTS_MAX_BUFFER_SIZE,
                )
            ),
            events_stream_name=_get_env_or_config(
                "NAQ_EVENTS_STREAM_NAME",
                ["events", "stream"],
                EVENTS_STREAM_NAME,
            ),
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
        return cls(nats_connection=nats_config, **config_dict)

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
            "events_enabled": self.events_enabled,
            "events_batch_size": self.events_batch_size,
            "events_flush_interval": self.events_flush_interval,
            "events_max_buffer_size": self.events_max_buffer_size,
            "events_stream_name": self.events_stream_name,
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
