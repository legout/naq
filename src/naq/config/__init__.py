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

# Global configuration instance
_config_instance: Optional[NAQConfig] = None

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
    "merge_config",
    "validate_naq_config",
    "validate_connection_config",
    "validate_job_service_config",
    "validate_worker_service_config",
    "validate_scheduler_service_config",
    "validate_stream_service_config",
    "validate_kv_store_config",
    "validate_event_service_config",
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
    config = msgspec.json.decode(msgspec.json.encode(config_dict), type=NAQConfig)

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
            temp_config_instance = msgspec.json.decode(
                msgspec.json.encode(config_data), type=NAQConfig
            )
            _config_instance = temp_config_instance
        else:
            # Create a default configuration
            default_config = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "naq-test",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0,
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0,
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events",
                },
            }
            temp_config_instance = msgspec.json.decode(
                msgspec.json.encode(default_config), type=NAQConfig
            )
            _config_instance = temp_config_instance

        yield _config_instance
    finally:
        # Restore the original configuration
        _config_instance = original_config
