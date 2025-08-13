"""
NAQ Configuration System

This module provides a comprehensive configuration system for NAQ that supports:
- YAML configuration file loading from multiple locations
- Configuration hierarchy (YAML → environment variables → defaults)
- Configuration validation and schema enforcement
- Backward compatibility with existing environment variables
- Hot-reloading support for configuration changes
"""

from .loader import ConfigLoader, ConfigurationError
from .schema import ConfigSchema
from .defaults import get_default_config
from .merger import ConfigMerger
from .validator import ConfigValidator
from .types import (
    ConfigDict,
    ConfigSource,
    NatsAuthConfig,
    NatsTLSConfig,
    NatsConfig,
    WorkerPoolConfig,
    WorkerConfig,
    EventStreamConfig,
    EventFilterConfig,
    EventsConfig,
    QueueConfig,
    SchedulerConfig,
    ResultsConfig,
    JSONSerializationConfig,
    SerializationConfig,
    LoggingConfig,
    EnvironmentConfig,
    NAQConfig,
)

__all__ = [
    "ConfigLoader",
    "ConfigurationError",
    "ConfigSchema",
    "get_default_config",
    "ConfigMerger",
    "ConfigValidator",
    "ConfigDict",
    "ConfigSource",
    "NatsAuthConfig",
    "NatsTLSConfig",
    "NatsConfig",
    "WorkerPoolConfig",
    "WorkerConfig",
    "EventStreamConfig",
    "EventFilterConfig",
    "EventsConfig",
    "QueueConfig",
    "SchedulerConfig",
    "ResultsConfig",
    "JSONSerializationConfig",
    "SerializationConfig",
    "LoggingConfig",
    "EnvironmentConfig",
    "NAQConfig",
]