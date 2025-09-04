"""Configuration module for NAQ."""

import contextlib
from typing import Any, Dict, Iterator, Optional

import msgspec

from .defaults import DEFAULT_CONFIG
from .loader import ConfigLoader
from .merger import merge_config
from .schema import ConfigValidator
from .types import EventsConfig, NAQConfig, NatsConfig, WorkerConfig
from .validation import (
    validate_naq_config,
    validate_connection_config,
    validate_job_service_config,
    validate_worker_service_config,
    validate_scheduler_service_config,
    validate_stream_service_config,
    validate_kv_store_config,
    validate_event_service_config,
)

# Legacy constants for backward compatibility
NAQ_PREFIX = "naq"
DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_QUEUE_NAME = "naq_default_queue"
MAX_SCHEDULE_FAILURES = 5
SCHEDULED_JOBS_KV_NAME = f"{NAQ_PREFIX}_scheduled_jobs"
SCHEDULER_LOCK_KV_NAME = f"{NAQ_PREFIX}_scheduler_lock"
SCHEDULER_LOCK_KEY = "leader_lock"
SCHEDULER_LOCK_TTL_SECONDS = 30
SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS = 15
RESULT_KV_NAME = f"{NAQ_PREFIX}_results"
DEFAULT_RESULT_TTL_SECONDS = 604800
FAILED_JOB_SUBJECT_PREFIX = f"{NAQ_PREFIX}.failed"
FAILED_JOB_STREAM_NAME = f"{NAQ_PREFIX}_failed_jobs"
JOB_STATUS_KV_NAME = f"{NAQ_PREFIX}_job_status"
DEFAULT_ACK_WAIT_SECONDS = 60
DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS = 15
DEFAULT_WORKER_TTL_SECONDS = 60
DEPENDENCY_CHECK_DELAY_SECONDS = 5
EVENTS_BATCH_SIZE = 100
EVENTS_ENABLED = False
EVENTS_FLUSH_INTERVAL = 5.0
EVENTS_MAX_BUFFER_SIZE = 1000
EVENTS_STREAM_NAME = "naq_events"
JOB_SERIALIZER = "pickle"
JOB_STATUS_TTL_SECONDS = 86400
JSON_DECODER = "json.JSONDecoder"
JSON_ENCODER = "json.JSONEncoder"
LOG_FILE_PATH = "naq_{time}.log"
LOG_LEVEL = "CRITICAL"
LOG_TO_FILE_ENABLED = False
PICKLE_DEBUG_LOGGING_ENABLED = False
PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS = True
PICKLE_DEBUG_LOGGING_LEVEL = "DEBUG"
SERIALIZATION_CHECKSUM_ALGORITHM = "sha256"
SERIALIZATION_CHECKSUM_ENABLED = False
SERIALIZATION_MAX_SIZE_BYTES = 10485760
SERIALIZATION_SIGNATURE_KEY = None
WORKER_KV_NAME = f"{NAQ_PREFIX}_workers"

# Global configuration instance
_config_instance: Optional[NAQConfig] = None


def _dict_to_naq_config(config_dict: Dict[str, Any]) -> NAQConfig:
    """Convert a dictionary to a NAQConfig instance.

    Args:
        config_dict: Dictionary containing configuration data.

    Returns:
        A NAQConfig instance.
    """
    return msgspec.json.decode(msgspec.json.encode(config_dict), type=NAQConfig)


__all__ = [
    "DEFAULT_CONFIG",
    "ConfigLoader",
    "NAQConfig",
    "EventsConfig",
    "NatsConfig",
    "WorkerConfig",
    "load_config",
    "get_config",
    "reload_config",
    "temp_config",
    # Legacy constants
    "NAQ_PREFIX",
    "DEFAULT_NATS_URL",
    "DEFAULT_QUEUE_NAME",
    "MAX_SCHEDULE_FAILURES",
    "SCHEDULED_JOBS_KV_NAME",
    "SCHEDULER_LOCK_KV_NAME",
    "SCHEDULER_LOCK_KEY",
    "SCHEDULER_LOCK_TTL_SECONDS",
    "SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS",
    "RESULT_KV_NAME",
    "DEFAULT_RESULT_TTL_SECONDS",
    "FAILED_JOB_SUBJECT_PREFIX",
    "FAILED_JOB_STREAM_NAME",
    "JOB_STATUS_KV_NAME",
    "DEFAULT_ACK_WAIT_SECONDS",
    "DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS",
    "DEFAULT_WORKER_TTL_SECONDS",
    "DEPENDENCY_CHECK_DELAY_SECONDS",
    "EVENTS_BATCH_SIZE",
    "EVENTS_ENABLED",
    "EVENTS_FLUSH_INTERVAL",
    "EVENTS_MAX_BUFFER_SIZE",
    "EVENTS_STREAM_NAME",
    "JOB_SERIALIZER",
    "JOB_STATUS_TTL_SECONDS",
    "JSON_DECODER",
    "JSON_ENCODER",
    "LOG_FILE_PATH",
    "LOG_LEVEL",
    "LOG_TO_FILE_ENABLED",
    "PICKLE_DEBUG_LOGGING_ENABLED",
    "PICKLE_DEBUG_LOGGING_INCLUDE_OBJECTS",
    "PICKLE_DEBUG_LOGGING_LEVEL",
    "SERIALIZATION_CHECKSUM_ALGORITHM",
    "SERIALIZATION_CHECKSUM_ENABLED",
    "SERIALIZATION_MAX_SIZE_BYTES",
    "SERIALIZATION_SIGNATURE_KEY",
    "WORKER_KV_NAME",
    "merge_config",
    "validate_naq_config",
    "validate_connection_config",
    "validate_job_service_config",
    "validate_worker_service_config",
    "validate_scheduler_service_config",
    "validate_stream_service_config",
    "validate_kv_store_config",
    "validate_event_service_config",
    "_dict_to_naq_config",
]


def load_config(config_path: Optional[str] = None, validate: bool = True) -> NAQConfig:
    """Load configuration from file and optionally validate it.

    Args:
        config_path: Optional path to the configuration file.
                    If not provided, default locations will be checked.
        validate: Whether to validate the configuration against the schema.
                 Defaults to True.

    Returns:
        The loaded and validated NAQConfig instance.

    Raises:
        ConfigurationError: If loading or validation fails.
    """
    global _config_instance

    # Load configuration using ConfigLoader
    loader = ConfigLoader(config_path)
    config_dict = loader.load_config()

    # Validate if requested
    if validate:
        validator = ConfigValidator()
        validator.validate(config_dict)

    # Convert to NAQConfig using msgspec
    config = _dict_to_naq_config(config_dict)

    # Store in global instance
    _config_instance = config

    return config


def get_config() -> NAQConfig:
    """Get the current configuration instance.

    If no configuration has been loaded yet, this will call load_config()
    with default parameters.

    Returns:
        The current NAQConfig instance.
    """
    global _config_instance

    if _config_instance is None:
        load_config()

    return _config_instance


def reload_config(
    config_path: Optional[str] = None, validate: bool = True
) -> NAQConfig:
    """Force a reload of the configuration.

    This will clear the current configuration instance and load a new one.

    Args:
        config_path: Optional path to the configuration file.
                    If not provided, default locations will be checked.
        validate: Whether to validate the configuration against the schema.
                 Defaults to True.

    Returns:
        The newly loaded NAQConfig instance.
    """
    global _config_instance

    # Clear current instance
    _config_instance = None

    # Load new configuration
    return load_config(config_path, validate)


@contextlib.contextmanager
def temp_config(config_data: Optional[Dict[str, Any]] = None) -> Iterator[NAQConfig]:
    """Temporarily override the global configuration for testing purposes.

    Args:
        config_data: Optional dictionary to use as the temporary configuration.
                    If not provided, a default configuration will be used.

    Yields:
        The temporary NAQConfig instance.

    Example:
        with temp_config({"nats": {"servers": ["nats://localhost:4222"], ...}}):
            # Use temporary configuration
            config = get_config()
            # ... test code here ...
    """
    global _config_instance

    # Save the original configuration
    original_config = _config_instance

    try:
        if config_data is not None:
            # Convert provided config data to NAQConfig
            _config_instance = _dict_to_naq_config(config_data)
        else:
            # Create a default configuration
            _config_instance = _dict_to_naq_config(DEFAULT_CONFIG)

        yield _config_instance
    finally:
        # Restore the original configuration
        _config_instance = original_config
