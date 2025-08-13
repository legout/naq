"""
Configuration utilities for NAQ.

This module provides utilities for managing configuration across the application,
including environment variable handling, configuration validation, and default values.
"""
import re
import os
import json
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, TypeVar, Type, get_type_hints
from dataclasses import dataclass, field, is_dataclass

from .validation import ValidationError, validate_type, validate_required

T = TypeVar('T')


class ConfigError(Exception):
    """Raised when there's an error with configuration."""
    pass


@dataclass
class ConfigSource:
    """Base class for configuration sources."""
    priority: int = 0
    
    def load(self) -> Dict[str, Any]:
        """Load configuration from this source."""
        raise NotImplementedError


@dataclass
class EnvironmentConfigSource(ConfigSource):
    """Configuration source that reads from environment variables."""
    prefix: str = "NAQ_"
    priority: int = 10
    
    def load(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(self.prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(self.prefix):].lower()
                
                # Try to parse as JSON, fall back to string
                try:
                    config[config_key] = json.loads(value)
                except json.JSONDecodeError:
                    config[config_key] = value
        
        return config


@dataclass
class FileConfigSource(ConfigSource):
    """Configuration source that reads from a file."""
    priority: int = 20
    file_path: Union[str, Path] = field(default_factory=lambda: "config.yaml")
    
    def load(self) -> Dict[str, Any]:
        """Load configuration from file."""
        file_path = Path(self.file_path)
        
        if not file_path.exists():
            return {}
        
        with open(file_path, 'r') as f:
            if file_path.suffix.lower() in ['.yaml', '.yml']:
                return yaml.safe_load(f) or {}
            elif file_path.suffix.lower() == '.json':
                return json.load(f)
            else:
                raise ConfigError(f"Unsupported config file format: {file_path.suffix}")


@dataclass
class DictConfigSource(ConfigSource):
    """Configuration source that uses a provided dictionary."""
    priority: int = 30
    config_dict: Dict[str, Any] = field(default_factory=dict)
    
    def load(self) -> Dict[str, Any]:
        """Return the provided dictionary."""
        return self.config_dict.copy()


class ConfigManager:
    """
    Manages configuration from multiple sources with priority-based merging.
    """
    
    def __init__(self, sources: Optional[List[ConfigSource]] = None):
        """
        Initialize the configuration manager.
        
        Args:
            sources: List of configuration sources (ordered by priority)
        """
        self.sources = sources or []
        self._config_cache: Optional[Dict[str, Any]] = None
    
    def add_source(self, source: ConfigSource) -> None:
        """
        Add a configuration source.
        
        Args:
            source: Configuration source to add
        """
        self.sources.append(source)
        self.sources.sort(key=lambda s: s.priority)
        self._config_cache = None
    
    def load_config(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load and merge configuration from all sources.
        
        Args:
            force_reload: Force reload even if cache is valid
            
        Returns:
            Merged configuration dictionary
        """
        if self._config_cache is not None and not force_reload:
            return self._config_cache
        
        # Sort sources by priority (lower = higher priority)
        sorted_sources = sorted(self.sources, key=lambda s: s.priority)
        
        # Merge configs (higher priority sources override lower priority)
        merged_config = {}
        for source in sorted_sources:
            config = source.load()
            merged_config.update(config)
        
        self._config_cache = merged_config
        return merged_config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key (supports dot notation)
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        config = self.load_config()
        
        # Handle dot notation
        keys = key.split('.')
        value = config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_typed(self, key: str, expected_type: Type[T], default: Optional[T] = None) -> T:
        """
        Get a configuration value with type checking.
        
        Args:
            key: Configuration key
            expected_type: Expected type
            default: Default value if key not found
            
        Returns:
            Configuration value with correct type
            
        Raises:
            ConfigError: If value exists but is not of expected type
        """
        value = self.get(key, default)
        
        if value is not None and not isinstance(value, expected_type):
            raise ConfigError(
                f"Config value '{key}' must be of type {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )
        
        return value
    
    def get_required(self, key: str) -> Any:
        """
        Get a required configuration value.
        
        Args:
            key: Configuration key
            
        Returns:
            Configuration value
            
        Raises:
            ConfigError: If key is not found
        """
        value = self.get(key)
        
        if value is None:
            raise ConfigError(f"Required configuration key '{key}' not found")
        
        return value
    
    def get_required_typed(self, key: str, expected_type: Type[T]) -> T:
        """
        Get a required configuration value with type checking.
        
        Args:
            key: Configuration key
            expected_type: Expected type
            
        Returns:
            Configuration value with correct type
            
        Raises:
            ConfigError: If key is not found or value is not of expected type
        """
        value = self.get_required(key)
        
        if not isinstance(value, expected_type):
            raise ConfigError(
                f"Config value '{key}' must be of type {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value in the cache.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Configuration value
        """
        if self._config_cache is None:
            self._config_cache = {}
        
        # Handle dot notation
        keys = key.split('.')
        config = self._config_cache
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache = None


def create_default_config_manager() -> ConfigManager:
    """
    Create a default configuration manager with common sources.
    
    Returns:
        ConfigManager instance with default sources
    """
    sources = [
        # Default values (lowest priority)
        DictConfigSource(
            config_dict={
                "job": {
                    "max_retries": 3,
                    "retry_delay": 1.0,
                    "timeout": 30,
                    "retry_strategy": "exponential"
                },
                "connection": {
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "client_name": "naq_client"
                },
                "event": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "storage_type": "memory"
                },
                "logging": {
                    "level": "INFO",
                    "format": "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
                    "file": None
                }
            },
            priority=0
        ),
        
        # Environment variables (medium priority)
        EnvironmentConfigSource(priority=10),
        
        # Config files (high priority)
        FileConfigSource(file_path="naq.yaml", priority=20),
        FileConfigSource(file_path="naq.yml", priority=20),
        FileConfigSource(file_path="naq.json", priority=20),

        # User home config (highest priority)
        FileConfigSource(file_path=Path.home() / ".naq.yaml", priority=30),
        FileConfigSource(file_path=Path.home() / ".naq.yml", priority=30),
        FileConfigSource(file_path=Path.home() / ".naq.json", priority=30),
    ]
    
    return ConfigManager(sources)


def load_dataclass_from_config(
    config_manager: ConfigManager,
    dataclass_type: Type[T],
    prefix: str = ""
) -> T:
    """
    Load a dataclass from configuration.
    
    Args:
        config_manager: Configuration manager instance
        dataclass_type: Dataclass type to load
        prefix: Configuration key prefix
        
    Returns:
        Populated dataclass instance
        
    Raises:
        ConfigError: If configuration is invalid or missing required fields
    """
    if not is_dataclass(dataclass_type):
        raise ConfigError(f"{dataclass_type.__name__} is not a dataclass")
    
    # Get type hints for the dataclass
    type_hints = get_type_hints(dataclass_type)
    
    # Create kwargs for dataclass constructor
    kwargs = {}
    
    for field_name, field_type in type_hints.items():
        config_key = f"{prefix}.{field_name}" if prefix else field_name
        
        try:
            # Get the value from config
            if hasattr(dataclass_type, "__dataclass_fields__"):
                field_info = dataclass_type.__dataclass_fields__[field_name]
                
                # Check if field has a default value
                has_default = field_info.default is not field_info.default_factory
                
                if has_default:
                    # Use default if not in config
                    value = config_manager.get_typed(
                        config_key,
                        field_type,
                        field_info.default
                    )
                else:
                    # Field is required
                    value = config_manager.get_required_typed(config_key, field_type)
            else:
                # Fallback for older Python versions
                value = config_manager.get_typed(config_key, field_type)
            
            kwargs[field_name] = value
            
        except ConfigError as e:
            raise ConfigError(f"Error loading field '{field_name}': {str(e)}")
    
    try:
        return dataclass_type(**kwargs)
    except Exception as e:
        raise ConfigError(f"Error creating {dataclass_type.__name__}: {str(e)}")


@dataclass
class JobConfig:
    """Configuration for job execution."""
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: int = 30
    retry_strategy: str = "exponential"
    queue_name: Optional[str] = None


@dataclass
class ConnectionConfig:
    """Configuration for connections."""
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    client_name: str = "naq_client"
    credentials: Optional[Dict[str, Any]] = None


@dataclass
class EventConfig:
    """Configuration for event handling."""
    enabled: bool = True
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 1000
    storage_type: str = "memory"
    storage_url: Optional[str] = None


@dataclass
class LoggingConfig:
    """Configuration for logging."""
    level: str = "INFO"
    format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}"
    file: Optional[str] = None


@dataclass
class NaqConfig:
    """Main NAQ configuration."""
    job: JobConfig = field(default_factory=JobConfig)
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    event: EventConfig = field(default_factory=EventConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_naq_config(config_manager: Optional[ConfigManager] = None) -> NaqConfig:
    """
    Load the main NAQ configuration.
    
    Args:
        config_manager: Configuration manager instance (creates default if None)
        
    Returns:
        NaqConfig instance
    """
    if config_manager is None:
        config_manager = create_default_config_manager()
    
    return load_dataclass_from_config(config_manager, NaqConfig)


def validate_config(config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    """
    Validate a configuration dictionary against a schema.
    
    Args:
        config: Configuration to validate
        schema: Schema definition
        
    Returns:
        List of validation error messages
    """
    errors = []
    
    def validate_value(value: Any, schema_def: Any, path: str) -> None:
        if isinstance(schema_def, dict):
            if "type" in schema_def:
                expected_type = schema_def["type"]
                # Handle tuple types (e.g., (int, float))
                if isinstance(expected_type, tuple):
                    if not any(isinstance(value, t) for t in expected_type):
                        type_names = [t.__name__ for t in expected_type]
                        errors.append(f"{path}: expected one of {type_names}, got {type(value).__name__}")
                elif not isinstance(value, expected_type):
                    errors.append(f"{path}: expected {expected_type.__name__}, got {type(value).__name__}")
            
            if "required" in schema_def and schema_def["required"] and value is None:
                errors.append(f"{path}: is required")
            
            if "choices" in schema_def and value not in schema_def["choices"]:
                errors.append(f"{path}: must be one of {schema_def['choices']}")
            
            if "min" in schema_def and value < schema_def["min"]:
                errors.append(f"{path}: must be at least {schema_def['min']}")
            
            if "max" in schema_def and value > schema_def["max"]:
                errors.append(f"{path}: must be at most {schema_def['max']}")
            
            if "pattern" in schema_def and not re.match(schema_def["pattern"], str(value)):
                errors.append(f"{path}: does not match pattern '{schema_def['pattern']}'")
        
        elif isinstance(schema_def, list) and len(schema_def) == 1:
            if not isinstance(value, list):
                errors.append(f"{path}: expected list, got {type(value).__name__}")
            else:
                for i, item in enumerate(value):
                    validate_value(item, schema_def[0], f"{path}[{i}]")
    
    for key, schema_def in schema.items():
        if key in config:
            if isinstance(schema_def, dict) and schema_def.get("type") == dict:
                # This is a nested dictionary validation
                if isinstance(config[key], dict):
                    for sub_key, sub_schema in schema_def.items():
                        if sub_key != "type" and sub_key != "required":
                            if sub_key in config[key]:
                                validate_value(config[key][sub_key], sub_schema, f"{key}.{sub_key}")
                            elif isinstance(sub_schema, dict) and sub_schema.get("required", False):
                                errors.append(f"{key}.{sub_key}: is required")
                else:
                    errors.append(f"{key}: expected dict, got {type(config[key]).__name__}")
            else:
                validate_value(config[key], schema_def, key)
        elif isinstance(schema_def, dict) and schema_def.get("required", False):
            errors.append(f"{key}: is required")
    
    return errors


# Default configuration schema
DEFAULT_CONFIG_SCHEMA = {
    "job": {
        "type": dict,
        "required": False,
        "max_retries": {"type": int, "min": 0},
        "retry_delay": {"type": (int, float), "min": 0},
        "timeout": {"type": int, "min": 1},
        "retry_strategy": {"type": str, "choices": ["linear", "exponential"]}
    },
    "connection": {
        "type": dict,
        "required": False,
        "servers": {"type": list, "required": True},
        "max_reconnect_attempts": {"type": int, "min": 0},
        "reconnect_time_wait": {"type": (int, float), "min": 0},
        "client_name": {"type": str}
    },
    "event": {
        "type": dict,
        "required": False,
        "enabled": {"type": bool},
        "batch_size": {"type": int, "min": 1},
        "flush_interval": {"type": (int, float), "min": 0.1},
        "max_buffer_size": {"type": int, "min": 1},
        "storage_type": {"type": str, "choices": ["nats", "memory", "file"]}
    },
    "logging": {
        "type": dict,
        "required": False,
        "level": {"type": str, "choices": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]},
        "format": {"type": str},
        "file": {"type": (str, type(None))}
    }
}