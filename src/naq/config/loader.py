"""Configuration loading logic for NAQ."""

import os
import re
from typing import Any, Dict, Optional

import yaml

from ..exceptions import ConfigurationError
from .defaults import DEFAULT_CONFIG
from .merger import merge_config
from .validation import validate_naq_config


class ConfigLoader:
    """Loads configuration from various sources with proper priority handling.

    This class handles loading configuration from YAML files and environment variables,
    with a clear priority order: explicit config file > default config files >
    environment variables > defaults.
    """

    DEFAULT_CONFIG_PATHS = [
        "./naq.yaml",
        "./naq.yml",
        "./config/naq.yaml",
        "./config/naq.yml",
        "~/.naq.yaml",
        "~/.naq.yml",
        "/etc/naq/naq.yaml",
        "/etc/naq/naq.yml",
    ]

    def __init__(self, config_path: Optional[str] = None) -> None:
        """Initialize the config loader.

        Args:
            config_path: Optional explicit path to a configuration file.
                        If provided, this file will take highest priority.
        """
        self.config_path = config_path

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from all sources with proper priority handling.

        The priority order is:
        1. Explicit config file (if provided)
        2. Default config files (first found)
        3. Environment variables
        4. Default values

        Returns:
            A dictionary containing the merged configuration.
        """
        # Start with default configuration as the baseline
        config = DEFAULT_CONFIG.copy()

        # Load from default config files
        for path in self.DEFAULT_CONFIG_PATHS:
            expanded_path = os.path.expanduser(path)
            if os.path.exists(expanded_path):
                try:
                    file_config = self._load_yaml_file(expanded_path)
                    config = self._merge_config(config, file_config)
                    break
                except (FileNotFoundError, yaml.YAMLError):
                    continue

        # Load from explicit config file if provided
        if self.config_path:
            try:
                explicit_config = self._load_yaml_file(self.config_path)
                config = self._merge_config(config, explicit_config)
            except (FileNotFoundError, yaml.YAMLError) as e:
                raise ConfigurationError(f"Failed to load explicit config file: {e}")

        # Apply environment variable overrides
        config = self._apply_environment_overrides(config)

        # Validate the final configuration
        try:
            validate_naq_config(config)
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}")

        return config

    def _load_yaml_file(self, file_path: str) -> Dict[str, Any]:
        """Read and parse a YAML file.

        Args:
            file_path: Path to the YAML file to load.

        Returns:
            A dictionary containing the parsed YAML content.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            yaml.YAMLError: If the file contains invalid YAML.
            ConfigurationError: For other configuration-related errors.
        """
        try:
            with open(file_path, "r") as f:
                content = f.read()

            # Interpolate environment variables before parsing
            content = self._interpolate_env_vars(content)

            return yaml.safe_load(content) or {}
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML in configuration file {file_path}: {e}"
            )

    def _interpolate_env_vars(self, content: str) -> str:
        """Replace ${VAR:default} patterns in YAML content with env variable values.

        Args:
            content: The YAML content as a string.

        Returns:
            The content with environment variables interpolated.
        """

        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ""
            return os.getenv(var_name, default_value)

        # Match ${VAR:default} or ${VAR} patterns
        pattern = r"\$\{([^}:]+)(?::([^}]*))?\}"
        return re.sub(pattern, replace_var, content)

    def _load_env_variables(self) -> Dict[str, Any]:
        """Map NAQ_* environment variables to a nested dictionary structure.

        Supports both new nested format and legacy flat format for backward compatibility:

        New nested format:
        NAQ_NATS__SERVERS=nats://localhost:4222
        NAQ_WORKERS__CONCURRENCY=4
        NAQ_QUEUES__DEFAULT_NAME=my_queue

        Legacy flat format (for backward compatibility):
        NAQ_NATS_URL=nats://localhost:4222
        NAQ_DEFAULT_QUEUE=my_queue

        Returns:
            A dictionary containing the environment variables in a nested structure.
        """
        env_config = {}

        for key, value in os.environ.items():
            if key.startswith("NAQ_"):
                # Remove NAQ_ prefix
                config_key = key[4:]

                # Handle legacy flat format environment variables
                if "__" not in config_key:
                    # Legacy format: convert to nested structure
                    env_config = self._handle_legacy_env_var(config_key, value, env_config)
                else:
                    # New nested format
                    parts = config_key.split("__")

                    # Navigate/create nested structure
                    current = env_config
                    for part in parts[:-1]:
                        # Convert part to lowercase to match config structure
                        part_lower = part.lower()
                        if part_lower not in current:
                            current[part_lower] = {}
                        current = current[part_lower]

                    # Set the final value with type conversion
                    # Convert the last part to lowercase as well
                    current[parts[-1].lower()] = self._convert_env_value(value)

        return env_config

    def _handle_legacy_env_var(self, key: str, value: str, env_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle legacy flat format environment variables.

        Converts old format variables to new nested structure:
        - NAQ_NATS_URL -> nats.servers
        - NAQ_DEFAULT_QUEUE -> queues.default_name
        - NAQ_LOG_LEVEL -> logging.level
        etc.

        Args:
            key: The environment variable key without NAQ_ prefix
            value: The environment variable value
            env_config: The current environment config dictionary

        Returns:
            Updated environment config dictionary
        """
        # Legacy environment variable mappings
        legacy_mappings = {
            "NATS_URL": ("nats", "servers", lambda x: [x]),  # Convert to list
            "DEFAULT_QUEUE": ("queues", "default_name", str),
            "LOG_LEVEL": ("logging", "level", str),
            "WORKER_CONCURRENCY": ("workers", "concurrency", int),
            "WORKER_HEARTBEAT_INTERVAL": ("workers", "heartbeat_interval", float),
            "DEFAULT_ACK_WAIT": ("queues", "ack_wait", int),
            "SCHEDULER_LOCK_TTL": ("scheduler", "lock_ttl", float),
            "SCHEDULER_LOCK_RENEW_INTERVAL": ("scheduler", "lock_renew_interval", int),
            "MAX_SCHEDULE_FAILURES": ("scheduler", "max_failures", int),
            "JOB_STATUS_TTL": ("scheduler", "job_status_ttl", int),
            "DEFAULT_RESULT_TTL": ("results", "ttl", int),
            "WORKER_TTL": ("workers", "ttl", int),
        }

        if key in legacy_mappings:
            section, prop, converter = legacy_mappings[key]
            converted_value = converter(value)

            if section not in env_config:
                env_config[section] = {}
            env_config[section][prop] = converted_value

        return env_config

    def _convert_env_value(self, value: str) -> Any:
        """Convert string environment variable values to appropriate types.

        Args:
            value: The string value from the environment variable.

        Returns:
            The value converted to the appropriate type (bool, int, float, list, or str).
        """
        # Try to parse as JSON first (for lists and complex objects)
        try:
            import json
            parsed = json.loads(value)
            return parsed
        except (json.JSONDecodeError, ValueError):
            pass

        # Convert boolean values
        if value.lower() in ("true", "yes", "1", "on"):
            return True
        elif value.lower() in ("false", "no", "0", "off"):
            return False

        # Try to convert to int
        try:
            return int(value)
        except ValueError:
            pass

        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            pass

        # Return as string if no other conversion worked
        return value

    def _merge_config(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recursively merge two dictionaries.

        The override dictionary takes precedence over the base dictionary.
        Nested dictionaries are merged recursively.
        Lists are replaced rather than merged.

        Args:
            base: The base dictionary.
            override: The dictionary with overriding values.

        Returns:
            A new dictionary containing the merged result.
        """
        return merge_config(base, override)

    def _apply_environment_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variables on top of the loaded configuration.

        Args:
            config: The configuration dictionary loaded from files.

        Returns:
            The configuration dictionary with environment variable overrides applied.
        """
        env_config = self._load_env_variables()
        return self._merge_config(config, env_config)
