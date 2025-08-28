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
