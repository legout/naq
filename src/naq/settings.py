"""Configuration settings for NAQ.

This module provides centralized configuration management for the NAQ library,
using msgspec.Struct for type safety and validation.
"""

import os
from typing import Any, Dict, List, Optional, Union

import msgspec
from loguru import logger

from .config import get_config
from .exceptions import ConfigurationError


class NatsConnectionConfig(msgspec.Struct):
    """Configuration for NATS connections.

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

    servers: list[str] = msgspec.field(default_factory=lambda: ["nats://localhost:4222"])
    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    connection_timeout: float = 10.0
    ping_interval: float = 30.0
    max_outstanding_pings: int = 3
    prefer_thread_local: bool = False
    name: str = "naq_client"
    no_randomize: bool = False
    tls: Optional[dict[str, Any]] = None
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    nkey: Optional[str] = None
    credentials: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Ensure servers is not empty
        if not self.servers:
            self.servers = ["nats://localhost:4222"]

        # Validate numeric values
        if self.max_reconnect_attempts < 0:
            raise ConfigurationError("max_reconnect_attempts must be non-negative")
        if self.reconnect_time_wait < 0:
            raise ConfigurationError("reconnect_time_wait must be non-negative")
        if self.connection_timeout < 0:
            raise ConfigurationError("connection_timeout must be non-negative")
        if self.ping_interval < 0:
            raise ConfigurationError("ping_interval must be non-negative")
        if self.max_outstanding_pings < 0:
            raise ConfigurationError("max_outstanding_pings must be non-negative")


class SerializationConfig(msgspec.Struct):
    """Configuration for job serialization.

    Attributes:
        method: Serialization method ('pickle' or 'json')
        json_encoder: JSON encoder class path
        json_decoder: JSON decoder class path
        checksum_enabled: Whether to enable checksum verification
        checksum_algorithm: Algorithm to use for checksum calculation
        signature_key: Secret key for HMAC signature
        max_size_bytes: Maximum size for serialized data in bytes
        pickle_debug_logging_enabled: Whether debug logging is enabled for PickleSerializer
        pickle_debug_logging_level: Log level for PickleSerializer debug messages
        pickle_debug_logging_include_objects: Whether to include object analysis in debug logs
    """

    method: str = "pickle"
    json_encoder: str = "json.JSONEncoder"
    json_decoder: str = "json.JSONDecoder"
    checksum_enabled: bool = False
    checksum_algorithm: str = "sha256"
    signature_key: Optional[str] = None
    max_size_bytes: int = 10485760  # 10MB
    pickle_debug_logging_enabled: bool = False
    pickle_debug_logging_level: str = "DEBUG"
    pickle_debug_logging_include_objects: bool = True


class QueueConfig(msgspec.Struct):
    """Configuration for queue settings.

    Attributes:
        default_name: Default queue name
        ack_wait: Default ack wait time in seconds
        ack_wait_per_queue: Per-queue ack wait time overrides
        dependency_check_delay_seconds: Delay between dependency checks in seconds
    """

    default_name: str = "naq_default_queue"
    ack_wait: int = 60
    ack_wait_per_queue: dict[str, int] = msgspec.field(default_factory=dict)
    dependency_check_delay_seconds: int = 5


class SchedulerConfig(msgspec.Struct):
    """Configuration for scheduler settings.

    Attributes:
        lock_ttl_seconds: TTL for scheduler lock in seconds
        lock_renew_interval_seconds: Interval for scheduler lock renewal in seconds
        max_failures: Maximum number of scheduling failures before giving up
        job_status_ttl_seconds: TTL for job status entries in seconds
    """

    lock_ttl_seconds: int = 30
    lock_renew_interval_seconds: int = 15
    max_failures: int = 5
    job_status_ttl_seconds: int = 86400


class ResultsConfig(msgspec.Struct):
    """Configuration for job results.

    Attributes:
        ttl_seconds: Default TTL for job results in seconds
    """

    ttl_seconds: int = 604800  # 7 days


class WorkersConfig(msgspec.Struct):
    """Configuration for worker settings.

    Attributes:
        ttl_seconds: Default TTL for worker entries in seconds
        heartbeat_interval_seconds: Default interval for worker heartbeats in seconds
    """

    ttl_seconds: int = 60
    heartbeat_interval_seconds: int = 15


class EventsConfig(msgspec.Struct):
    """Configuration for event processing.

    Attributes:
        enabled: Whether event processing is enabled
        batch_size: Number of events to batch before flushing
        flush_interval: Maximum time to wait before flushing batched events in seconds
        max_buffer_size: Maximum number of events to hold in the in-memory buffer
        stream_name: Name of the event stream
    """

    enabled: bool = False
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 1000
    stream_name: str = "naq_events"


class LoggingConfig(msgspec.Struct):
    """Configuration for logging.

    Attributes:
        level: Logging level
        to_file_enabled: Whether to enable file logging
        file_path: Path for the log file
    """

    level: str = "CRITICAL"
    to_file_enabled: bool = False
    file_path: str = "naq_{time}.log"


class Settings(msgspec.Struct):
    """Main configuration class for NAQ.

    This class integrates all configuration settings including NATS connection
    configuration, job settings, scheduler settings, and other system parameters.
    It supports loading from environment variables, configuration files, and
    provides validation and default values.

    Attributes:
        nats_connection: NATS connection configuration
        serialization: Serialization configuration
        queues: Queue configuration
        scheduler: Scheduler configuration
        results: Results configuration
        workers: Workers configuration
        events: Events configuration
        logging: Logging configuration
        prefix: Prefix for NATS subjects/streams used by naq
    """

    nats_connection: NatsConnectionConfig = msgspec.field(default_factory=NatsConnectionConfig)
    serialization: SerializationConfig = msgspec.field(default_factory=SerializationConfig)
    queues: QueueConfig = msgspec.field(default_factory=QueueConfig)
    scheduler: SchedulerConfig = msgspec.field(default_factory=SchedulerConfig)
    results: ResultsConfig = msgspec.field(default_factory=ResultsConfig)
    workers: WorkersConfig = msgspec.field(default_factory=WorkersConfig)
    events: EventsConfig = msgspec.field(default_factory=EventsConfig)
    logging: LoggingConfig = msgspec.field(default_factory=LoggingConfig)
    prefix: str = "naq"
    
    # Backward compatibility properties
    @property
    def queue_name(self) -> str:
        """Get the default queue name (backward compatibility)."""
        return self.queues.default_name
    
    @queue_name.setter
    def queue_name(self, value: str) -> None:
        """Set the default queue name (backward compatibility)."""
        self.queues.default_name = value
    
    @property
    def job_serializer(self) -> str:
        """Get the job serializer method (backward compatibility)."""
        return self.serialization.method
    
    @job_serializer.setter
    def job_serializer(self, value: str) -> None:
        """Set the job serializer method (backward compatibility)."""
        self.serialization.method = value
    
    @property
    def json_encoder(self) -> str:
        """Get the JSON encoder class path (backward compatibility)."""
        return self.serialization.json_encoder
    
    @json_encoder.setter
    def json_encoder(self, value: str) -> None:
        """Set the JSON encoder class path (backward compatibility)."""
        self.serialization.json_encoder = value
    
    @property
    def json_decoder(self) -> str:
        """Get the JSON decoder class path (backward compatibility)."""
        return self.serialization.json_decoder
    
    @json_decoder.setter
    def json_decoder(self, value: str) -> None:
        """Set the JSON decoder class path (backward compatibility)."""
        self.serialization.json_decoder = value
    
    @property
    def scheduler_lock_ttl_seconds(self) -> int:
        """Get the scheduler lock TTL in seconds (backward compatibility)."""
        return self.scheduler.lock_ttl_seconds
    
    @scheduler_lock_ttl_seconds.setter
    def scheduler_lock_ttl_seconds(self, value: int) -> None:
        """Set the scheduler lock TTL in seconds (backward compatibility)."""
        self.scheduler.lock_ttl_seconds = value
    
    @property
    def scheduler_lock_renew_interval_seconds(self) -> int:
        """Get the scheduler lock renew interval in seconds (backward compatibility)."""
        return self.scheduler.lock_renew_interval_seconds
    
    @scheduler_lock_renew_interval_seconds.setter
    def scheduler_lock_renew_interval_seconds(self, value: int) -> None:
        """Set the scheduler lock renew interval in seconds (backward compatibility)."""
        self.scheduler.lock_renew_interval_seconds = value
    
    @property
    def max_schedule_failures(self) -> int:
        """Get the maximum schedule failures (backward compatibility)."""
        return self.scheduler.max_failures
    
    @max_schedule_failures.setter
    def max_schedule_failures(self, value: int) -> None:
        """Set the maximum schedule failures (backward compatibility)."""
        self.scheduler.max_failures = value
    
    @property
    def job_status_ttl_seconds(self) -> int:
        """Get the job status TTL in seconds (backward compatibility)."""
        return self.scheduler.job_status_ttl_seconds
    
    @job_status_ttl_seconds.setter
    def job_status_ttl_seconds(self, value: int) -> None:
        """Set the job status TTL in seconds (backward compatibility)."""
        self.scheduler.job_status_ttl_seconds = value
    
    @property
    def default_result_ttl_seconds(self) -> int:
        """Get the default result TTL in seconds (backward compatibility)."""
        return self.results.ttl_seconds
    
    @default_result_ttl_seconds.setter
    def default_result_ttl_seconds(self, value: int) -> None:
        """Set the default result TTL in seconds (backward compatibility)."""
        self.results.ttl_seconds = value
    
    @property
    def worker_ttl_seconds(self) -> int:
        """Get the worker TTL in seconds (backward compatibility)."""
        return self.workers.ttl_seconds
    
    @worker_ttl_seconds.setter
    def worker_ttl_seconds(self, value: int) -> None:
        """Set the worker TTL in seconds (backward compatibility)."""
        self.workers.ttl_seconds = value
    
    @property
    def worker_heartbeat_interval_seconds(self) -> int:
        """Get the worker heartbeat interval in seconds (backward compatibility)."""
        return self.workers.heartbeat_interval_seconds
    
    @worker_heartbeat_interval_seconds.setter
    def worker_heartbeat_interval_seconds(self, value: int) -> None:
        """Set the worker heartbeat interval in seconds (backward compatibility)."""
        self.workers.heartbeat_interval_seconds = value
    
    @property
    def default_ack_wait_seconds(self) -> int:
        """Get the default ack wait in seconds (backward compatibility)."""
        return self.queues.ack_wait
    
    @default_ack_wait_seconds.setter
    def default_ack_wait_seconds(self, value: int) -> None:
        """Set the default ack wait in seconds (backward compatibility)."""
        self.queues.ack_wait = value
    
    @property
    def ack_wait_per_queue(self) -> dict[str, int]:
        """Get the ack wait per queue (backward compatibility)."""
        return self.queues.ack_wait_per_queue
    
    @ack_wait_per_queue.setter
    def ack_wait_per_queue(self, value: dict[str, int]) -> None:
        """Set the ack wait per queue (backward compatibility)."""
        self.queues.ack_wait_per_queue = value
    
    @property
    def dependency_check_delay_seconds(self) -> int:
        """Get the dependency check delay in seconds (backward compatibility)."""
        return self.queues.dependency_check_delay_seconds
    
    @dependency_check_delay_seconds.setter
    def dependency_check_delay_seconds(self, value: int) -> None:
        """Set the dependency check delay in seconds (backward compatibility)."""
        self.queues.dependency_check_delay_seconds = value
    
    @property
    def log_level(self) -> str:
        """Get the log level (backward compatibility)."""
        return self.logging.level
    
    @log_level.setter
    def log_level(self, value: str) -> None:
        """Set the log level (backward compatibility)."""
        self.logging.level = value
    
    @property
    def log_to_file_enabled(self) -> bool:
        """Get whether file logging is enabled (backward compatibility)."""
        return self.logging.to_file_enabled
    
    @log_to_file_enabled.setter
    def log_to_file_enabled(self, value: bool) -> None:
        """Set whether file logging is enabled (backward compatibility)."""
        self.logging.to_file_enabled = value
    
    @property
    def log_file_path(self) -> str:
        """Get the log file path (backward compatibility)."""
        return self.logging.file_path
    
    @log_file_path.setter
    def log_file_path(self, value: str) -> None:
        """Set the log file path (backward compatibility)."""
        self.logging.file_path = value
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Validate numeric values
        if self.scheduler.lock_ttl_seconds <= 0:
            raise ConfigurationError("scheduler_lock_ttl_seconds must be positive")
        if self.scheduler.lock_renew_interval_seconds <= 0:
            raise ConfigurationError("scheduler_lock_renew_interval_seconds must be positive")
        if self.workers.ttl_seconds <= 0:
            raise ConfigurationError("worker_ttl_seconds must be positive")
        if self.workers.heartbeat_interval_seconds <= 0:
            raise ConfigurationError("worker_heartbeat_interval_seconds must be positive")
        if self.queues.ack_wait <= 0:
            raise ConfigurationError("default_ack_wait_seconds must be positive")
        
        # Validate ack_wait_per_queue values
        for queue_name, ack_wait in self.queues.ack_wait_per_queue.items():
            if ack_wait <= 0:
                raise ConfigurationError(f"ack_wait for queue '{queue_name}' must be positive")
    
    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "Settings":
        """Create a configuration instance from a dictionary.
        
        Args:
            config_dict: Dictionary containing configuration values
            
        Returns:
            A new Settings instance
        """
        # Extract nested configurations
        nats_dict = config_dict.get("nats_connection", {})
        nats_config = NatsConnectionConfig(**nats_dict)
        
        # Create the settings instance
        settings = cls(nats_connection=nats_config)
        
        # Set other properties if provided
        if "queue_name" in config_dict:
            settings.queue_name = config_dict["queue_name"]
        if "job_serializer" in config_dict:
            settings.job_serializer = config_dict["job_serializer"]
        if "ack_wait_per_queue" in config_dict:
            settings.ack_wait_per_queue = config_dict["ack_wait_per_queue"]
            
        return settings

    @classmethod
    def from_env(cls) -> "Settings":
        """Create a configuration instance from environment variables."""
        return cls(
            nats_connection=NatsConnectionConfig(
                servers=os.getenv("NAQ_NATS_URL", "nats://localhost:4222").split(","),
                max_reconnect_attempts=int(os.getenv("NAQ_MAX_RECONNECT_ATTEMPTS", "5")),
                reconnect_time_wait=float(os.getenv("NAQ_RECONNECT_TIME_WAIT", "2.0")),
                connection_timeout=float(os.getenv("NAQ_CONNECTION_TIMEOUT", "10.0")),
                ping_interval=float(os.getenv("NAQ_PING_INTERVAL", "30.0")),
                max_outstanding_pings=int(os.getenv("NAQ_MAX_OUTSTANDING_PINGS", "3")),
                prefer_thread_local=os.getenv("NAQ_PREFER_THREAD_LOCAL", "false").lower() == "true",
                name=os.getenv("NAQ_CLIENT_NAME", "naq_client"),
                no_randomize=os.getenv("NAQ_NO_RANDOMIZE", "false").lower() == "true",
                user=os.getenv("NAQ_USER"),
                password=os.getenv("NAQ_PASSWORD"),
                token=os.getenv("NAQ_TOKEN"),
                nkey=os.getenv("NAQ_NKEY"),
                credentials=os.getenv("NAQ_CREDENTIALS"),
            ),
            serialization=SerializationConfig(
                method=os.getenv("NAQ_JOB_SERIALIZER", "pickle"),
                json_encoder=os.getenv("NAQ_JSON_ENCODER", "json.JSONEncoder"),
                json_decoder=os.getenv("NAQ_JSON_DECODER", "json.JSONDecoder"),
                checksum_enabled=os.getenv("NAQ_SERIALIZATION_CHECKSUM_ENABLED", "false").lower() == "true",
                checksum_algorithm=os.getenv("NAQ_SERIALIZATION_CHECKSUM_ALGORITHM", "sha256"),
                signature_key=os.getenv("NAQ_SERIALIZATION_SIGNATURE_KEY"),
                max_size_bytes=int(os.getenv("NAQ_SERIALIZATION_MAX_SIZE_BYTES", "10485760")),
                pickle_debug_logging_enabled=os.getenv("NAQ_PICKLE_DEBUG_LOGGING_ENABLED", "false").lower() == "true",
                pickle_debug_logging_level=os.getenv("NAQ_PICKLE_DEBUG_LOGGING_LEVEL", "DEBUG").upper(),
                pickle_debug_logging_include_objects=os.getenv("NAQ_PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS", "true").lower() == "true",
            ),
            queues=QueueConfig(
                default_name=os.getenv("NAQ_DEFAULT_QUEUE", "naq_default_queue"),
                ack_wait=int(os.getenv("NAQ_DEFAULT_ACK_WAIT", "60")),
                dependency_check_delay_seconds=int(os.getenv("NAQ_DEPENDENCY_CHECK_DELAY_SECONDS", "5")),
            ),
            scheduler=SchedulerConfig(
                lock_ttl_seconds=int(os.getenv("NAQ_SCHEDULER_LOCK_TTL", "30")),
                lock_renew_interval_seconds=int(os.getenv("NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", "15")),
                max_failures=int(os.getenv("NAQ_MAX_SCHEDULE_FAILURES", "5")),
                job_status_ttl_seconds=int(os.getenv("NAQ_JOB_STATUS_TTL", "86400")),
            ),
            results=ResultsConfig(
                ttl_seconds=int(os.getenv("NAQ_DEFAULT_RESULT_TTL", "604800")),
            ),
            workers=WorkersConfig(
                ttl_seconds=int(os.getenv("NAQ_WORKER_TTL", "60")),
                heartbeat_interval_seconds=int(os.getenv("NAQ_WORKER_HEARTBEAT_INTERVAL", "15")),
            ),
            events=EventsConfig(
                enabled=os.getenv("NAQ_EVENTS_ENABLED", "false").lower() == "true",
                batch_size=int(os.getenv("NAQ_EVENTS_BATCH_SIZE", "100")),
                flush_interval=float(os.getenv("NAQ_EVENTS_FLUSH_INTERVAL", "5.0")),
                max_buffer_size=int(os.getenv("NAQ_EVENTS_MAX_BUFFER_SIZE", "1000")),
                stream_name=os.getenv("NAQ_EVENTS_STREAM_NAME", "naq_events"),
            ),
            logging=LoggingConfig(
                level=os.getenv("NAQ_LOG_LEVEL", "CRITICAL").upper(),
                to_file_enabled=os.getenv("NAQ_LOG_TO_FILE_ENABLED", "false").lower() == "true",
                file_path=os.getenv("NAQ_LOG_FILE_PATH", "naq_{time}.log"),
            ),
            prefix=os.getenv("NAQ_PREFIX", "naq"),
        )

    @classmethod
    def from_config(cls) -> "Settings":
        """Create a configuration instance from the loaded config."""
        config = get_config()
        
        # Handle ack_wait_per_queue JSON parsing
        ack_wait_per_queue = {}
        if config.queues and "ack_wait_per_queue" in config.queues:
            ack_wait_per_queue = config.queues["ack_wait_per_queue"] or {}
        
        return cls(
            nats_connection=NatsConnectionConfig(
                servers=config.nats.servers,
                max_reconnect_attempts=config.nats.max_reconnect_attempts,
                reconnect_time_wait=config.nats.reconnect_time_wait,
                connection_timeout=config.nats.connection_timeout,
                ping_interval=config.nats.get("ping_interval", 30.0),
                max_outstanding_pings=config.nats.get("max_outstanding_pings", 3),
                prefer_thread_local=config.nats.get("prefer_thread_local", False),
                name=config.nats.client_name,
                no_randomize=config.nats.get("no_randomize", False),
                tls=config.nats.tls,
                user=config.nats.auth.get("user") if config.nats.auth else None,
                password=config.nats.auth.get("password") if config.nats.auth else None,
                token=config.nats.auth.get("token") if config.nats.auth else None,
                nkey=config.nats.auth.get("nkey") if config.nats.auth else None,
                credentials=config.nats.auth.get("credentials") if config.nats.auth else None,
            ),
            serialization=SerializationConfig(
                method=config.serialization.get("method", "pickle") if config.serialization else "pickle",
                json_encoder=config.serialization.get("json_encoder", "json.JSONEncoder") if config.serialization else "json.JSONEncoder",
                json_decoder=config.serialization.get("json_decoder", "json.JSONDecoder") if config.serialization else "json.JSONDecoder",
                checksum_enabled=config.serialization.get("checksum_enabled", False) if config.serialization else False,
                checksum_algorithm=config.serialization.get("checksum_algorithm", "sha256") if config.serialization else "sha256",
                signature_key=config.serialization.get("signature_key") if config.serialization else None,
                max_size_bytes=config.serialization.get("max_size_bytes", 10485760) if config.serialization else 10485760,
                pickle_debug_logging_enabled=config.serialization.get("pickle_debug_logging_enabled", False) if config.serialization else False,
                pickle_debug_logging_level=config.serialization.get("pickle_debug_logging_level", "DEBUG") if config.serialization else "DEBUG",
                pickle_debug_logging_include_objects=config.serialization.get("pickle_debug_logging_include_objects", True) if config.serialization else True,
            ),
            queues=QueueConfig(
                default_name=config.queues.get("default_name", "naq_default_queue") if config.queues else "naq_default_queue",
                ack_wait=config.queues.get("ack_wait", 60) if config.queues else 60,
                ack_wait_per_queue=ack_wait_per_queue,
                dependency_check_delay_seconds=5,
            ),
            scheduler=SchedulerConfig(
                lock_ttl_seconds=config.scheduler.get("lock_ttl", 30) if config.scheduler else 30,
                lock_renew_interval_seconds=config.scheduler.get("lock_renew_interval", 15) if config.scheduler else 15,
                max_failures=config.scheduler.get("max_failures", 5) if config.scheduler else 5,
                job_status_ttl_seconds=config.scheduler.get("job_status_ttl", 86400) if config.scheduler else 86400,
            ),
            results=ResultsConfig(
                ttl_seconds=config.results.get("ttl", 604800) if config.results else 604800,
            ),
            workers=WorkersConfig(
                ttl_seconds=config.workers.ttl,
                heartbeat_interval_seconds=config.workers.heartbeat_interval,
            ),
            events=EventsConfig(
                enabled=config.events.enabled,
                batch_size=config.events.batch_size,
                flush_interval=config.events.flush_interval,
                max_buffer_size=config.events.max_buffer_size,
                stream_name=config.events.stream,
            ),
            logging=LoggingConfig(
                level=config.logging.get("level", "CRITICAL") if config.logging else "CRITICAL",
                to_file_enabled=config.logging.get("to_file_enabled", False) if config.logging else False,
                file_path=config.logging.get("file_path", "naq_{time}.log") if config.logging else "naq_{time}.log",
            ),
            prefix="naq",
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return msgspec.to_builtins(self)


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance.
    
    Returns:
        The global Settings instance.
    """
    global _settings
    if _settings is None:
        try:
            _settings = Settings.from_config()
        except Exception:
            logger.debug("Failed to load from config, using environment variables")
            _settings = Settings.from_env()
    return _settings


def set_settings(settings: Settings) -> None:
    """Set the global settings instance.
    
    Args:
        settings: The settings instance to set as global.
    """
    global _settings
    _settings = settings


def reset_settings() -> None:
    """Reset the global settings instance."""
    global _settings
    _settings = None


# Backward compatibility functions
def get_global_config() -> Settings:
    """Get the global settings instance (backward compatibility).
    
    Returns:
        The global Settings instance.
    """
    return get_settings()


def set_global_config(settings: Settings) -> None:
    """Set the global settings instance (backward compatibility).
    
    Args:
        settings: The settings instance to set as global.
    """
    set_settings(settings)


def reset_global_config() -> None:
    """Reset the global settings instance (backward compatibility)."""
    reset_settings()


# Backward compatibility constants
DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_QUEUE_NAME = "naq_default_queue"
NAQ_PREFIX = "naq"

# Backward compatibility aliases
Config = Settings  # Alias for backward compatibility
JOB_SERIALIZER = "pickle"
JSON_ENCODER = "json.JSONEncoder"
JSON_DECODER = "json.JSONDecoder"
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"
SCHEDULER_LOCK_KV_NAME = f"{NAQ_PREFIX}_scheduler_lock"
SCHEDULER_LOCK_KEY = "leader_lock"
SCHEDULER_LOCK_TTL_SECONDS = 30
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = 15
MAX_SCHEDULE_FAILURES = 5
JOB_STATUS_KV_NAME = f"{NAQ_PREFIX}_job_status"
JOB_STATUS_TTL_SECONDS = 86400
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"
RESULT_KV_NAME = f"{NAQ_PREFIX}_results"
DEFAULT_RESULT_TTL_SECONDS = 604800
WORKER_KV_NAME = f"{NAQ_PREFIX}_workers"
DEFAULT_WORKER_TTL_SECONDS = 60
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = 15
DEFAULT_ACK_WAIT_SECONDS = 60
ACK_WAIT_PER_QUEUE: dict[str, int] = {}
DEPENDENCY_CHECK_DELAY_SECONDS = 5
LOG_LEVEL = "CRITICAL"
LOG_TO_FILE_ENABLED = False
LOG_FILE_PATH = "naq_{time}.log"
EVENTS_ENABLED = False
EVENTS_BATCH_SIZE = 100
EVENTS_FLUSH_INTERVAL = 5.0
EVENTS_MAX_BUFFER_SIZE = 1000
EVENTS_STREAM_NAME = "naq_events"
PICKLE_DEBUG_LOGGING_ENABLED = False
PICKLE_DEBUG_LOGGING_LEVEL = "DEBUG"
PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS = True
SERIALIZATION_CHECKSUM_ENABLED = False
SERIALIZATION_CHECKSUM_ALGORITHM = "sha256"
SERIALIZATION_SIGNATURE_KEY = None
SERIALIZATION_MAX_SIZE_BYTES = 10485760