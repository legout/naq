"""
Configuration Validation Schema

This module defines the schema for validating configuration values.
"""

from typing import Dict, Any, List, Optional, Union, Type
from dataclasses import dataclass
from enum import Enum
import re
from .types import ConfigDict


class ValidationType(Enum):
    """Enumeration of validation types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    DICT = "dict"
    ANY = "any"


@dataclass
class ValidationRule:
    """Represents a validation rule for a configuration value"""
    type: ValidationType
    required: bool = False
    default: Any = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    choices: Optional[List[Any]] = None
    description: Optional[str] = None


class ConfigSchema:
    """
    Configuration schema definition and validation
    
    This class defines the schema for configuration validation and provides
    methods to validate configuration values against the schema.
    """
    
    def __init__(self):
        """Initialize the configuration schema"""
        self._schema = self._build_schema()
    
    def _build_schema(self) -> Dict[str, ValidationRule]:
        """
        Build the configuration schema
        
        Returns:
            Dict[str, ValidationRule]: Schema definition
        """
        return {
            # NATS configuration
            "nats.servers": ValidationRule(
                type=ValidationType.LIST,
                required=True,
                default=["nats://localhost:4222"],
                min_length=1,
                description="NATS server URLs"
            ),
            "nats.client_name": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="naq-client",
                description="NATS client name"
            ),
            "nats.max_reconnect_attempts": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=5,
                min_value=0,
                description="Maximum reconnection attempts"
            ),
            "nats.reconnect_time_wait": ValidationRule(
                type=ValidationType.FLOAT,
                required=False,
                default=2.0,
                min_value=0.1,
                description="Reconnect time wait in seconds"
            ),
            "nats.connection_timeout": ValidationRule(
                type=ValidationType.FLOAT,
                required=False,
                default=10.0,
                min_value=0.1,
                description="Connection timeout in seconds"
            ),
            "nats.drain_timeout": ValidationRule(
                type=ValidationType.FLOAT,
                required=False,
                default=30.0,
                min_value=1.0,
                description="Drain timeout in seconds"
            ),
            "nats.auth.username": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="NATS username"
            ),
            "nats.auth.password": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="NATS password"
            ),
            "nats.auth.token": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="NATS auth token"
            ),
            "nats.tls.enabled": ValidationRule(
                type=ValidationType.BOOLEAN,
                required=False,
                default=False,
                description="Enable TLS"
            ),
            "nats.tls.cert_file": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="TLS certificate file path"
            ),
            "nats.tls.key_file": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="TLS key file path"
            ),
            "nats.tls.ca_file": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="TLS CA file path"
            ),
            
            # Queues configuration
            "queues.default": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="naq_default_queue",
                pattern=r"^[a-zA-Z0-9_-]+$",
                description="Default queue name"
            ),
            "queues.prefix": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="naq",
                pattern=r"^[a-zA-Z0-9_-]+$",
                description="Queue prefix"
            ),
            "queues.configs": ValidationRule(
                type=ValidationType.DICT,
                required=False,
                default={},
                description="Queue-specific configurations"
            ),
            
            # Workers configuration
            "workers.concurrency": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=10,
                min_value=1,
                description="Worker concurrency"
            ),
            "workers.heartbeat_interval": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=15,
                min_value=1,
                description="Heartbeat interval in seconds"
            ),
            "workers.ttl": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=60,
                min_value=1,
                description="Worker TTL in seconds"
            ),
            "workers.max_job_duration": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=3600,
                min_value=1,
                description="Maximum job duration in seconds"
            ),
            "workers.shutdown_timeout": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=30,
                min_value=1,
                description="Shutdown timeout in seconds"
            ),
            "workers.pools": ValidationRule(
                type=ValidationType.DICT,
                required=False,
                default={},
                description="Worker pool configurations"
            ),
            
            # Scheduler configuration
            "scheduler.enabled": ValidationRule(
                type=ValidationType.BOOLEAN,
                required=False,
                default=True,
                description="Enable scheduler"
            ),
            "scheduler.lock_ttl": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=30,
                min_value=1,
                description="Lock TTL in seconds"
            ),
            "scheduler.lock_renew_interval": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=15,
                min_value=1,
                description="Lock renew interval in seconds"
            ),
            "scheduler.max_failures": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=5,
                min_value=0,
                description="Maximum failures"
            ),
            "scheduler.scan_interval": ValidationRule(
                type=ValidationType.FLOAT,
                required=False,
                default=1.0,
                min_value=0.1,
                description="Scan interval in seconds"
            ),
            
            # Events configuration
            "events.enabled": ValidationRule(
                type=ValidationType.BOOLEAN,
                required=False,
                default=True,
                description="Enable events"
            ),
            "events.batch_size": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=100,
                min_value=1,
                description="Event batch size"
            ),
            "events.flush_interval": ValidationRule(
                type=ValidationType.FLOAT,
                required=False,
                default=5.0,
                min_value=0.1,
                description="Flush interval in seconds"
            ),
            "events.max_buffer_size": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=10000,
                min_value=1,
                description="Maximum buffer size"
            ),
            "events.stream.name": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="NAQ_JOB_EVENTS",
                description="Event stream name"
            ),
            "events.stream.max_age": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="168h",
                description="Stream max age"
            ),
            "events.stream.max_bytes": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="1GB",
                description="Stream max bytes"
            ),
            "events.stream.replicas": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=1,
                min_value=1,
                description="Stream replicas"
            ),
            "events.filters.exclude_heartbeats": ValidationRule(
                type=ValidationType.BOOLEAN,
                required=False,
                default=True,
                description="Exclude heartbeats"
            ),
            "events.filters.min_job_duration": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=0,
                min_value=0,
                description="Minimum job duration in ms"
            ),
            
            # Results configuration
            "results.ttl": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=604800,
                min_value=0,
                description="Result TTL in seconds"
            ),
            "results.cleanup_interval": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=3600,
                min_value=1,
                description="Cleanup interval in seconds"
            ),
            
            # Serialization configuration
            "serialization.job_serializer": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="pickle",
                choices=["pickle", "json"],
                description="Job serializer"
            ),
            "serialization.compression": ValidationRule(
                type=ValidationType.BOOLEAN,
                required=False,
                default=False,
                description="Enable compression"
            ),
            "serialization.json.encoder": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="json.JSONEncoder",
                description="JSON encoder"
            ),
            "serialization.json.decoder": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="json.JSONDecoder",
                description="JSON decoder"
            ),
            
            # Logging configuration
            "logging.level": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="INFO",
                choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                description="Logging level"
            ),
            "logging.format": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="json",
                choices=["json", "text"],
                description="Log format"
            ),
            "logging.file": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="",
                description="Log file path"
            ),
            "logging.max_size": ValidationRule(
                type=ValidationType.STRING,
                required=False,
                default="100MB",
                description="Maximum log size"
            ),
            "logging.backup_count": ValidationRule(
                type=ValidationType.INTEGER,
                required=False,
                default=5,
                min_value=0,
                description="Backup count"
            ),
            
            # Environment-specific configuration
            "environments.development": ValidationRule(
                type=ValidationType.DICT,
                required=False,
                default={},
                description="Development environment overrides"
            ),
            "environments.production": ValidationRule(
                type=ValidationType.DICT,
                required=False,
                default={},
                description="Production environment overrides"
            ),
            "environments.testing": ValidationRule(
                type=ValidationType.DICT,
                required=False,
                default={},
                description="Testing environment overrides"
            ),
        }
    
    def validate_config(self, config: ConfigDict) -> List[str]:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List[str]: List of validation errors
        """
        errors = []
        
        for key_path, rule in self._schema.items():
            value = config.get_nested(key_path)
            
            # Check if required value is missing
            if rule.required and value is None:
                errors.append(f"Required configuration '{key_path}' is missing")
                continue
            
            # Skip validation if value is None and not required
            if value is None:
                continue
            
            # Validate type
            type_error = self._validate_type(key_path, value, rule.type)
            if type_error:
                errors.append(type_error)
                continue
            
            # Validate value constraints
            constraint_errors = self._validate_constraints(key_path, value, rule)
            errors.extend(constraint_errors)
        
        return errors
    
    def _validate_type(self, key_path: str, value: Any, expected_type: ValidationType) -> Optional[str]:
        """
        Validate value type
        
        Args:
            key_path: Configuration key path
            value: Value to validate
            expected_type: Expected validation type
            
        Returns:
            Optional[str]: Error message if validation fails, None otherwise
        """
        actual_type = type(value)
        
        if expected_type == ValidationType.STRING:
            if not isinstance(value, str):
                return f"Configuration '{key_path}' must be a string, got {actual_type.__name__}"
        
        elif expected_type == ValidationType.INTEGER:
            if not isinstance(value, int) or isinstance(value, bool):
                return f"Configuration '{key_path}' must be an integer, got {actual_type.__name__}"
        
        elif expected_type == ValidationType.FLOAT:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return f"Configuration '{key_path}' must be a float, got {actual_type.__name__}"
        
        elif expected_type == ValidationType.BOOLEAN:
            if not isinstance(value, bool):
                return f"Configuration '{key_path}' must be a boolean, got {actual_type.__name__}"
        
        elif expected_type == ValidationType.LIST:
            if not isinstance(value, list):
                return f"Configuration '{key_path}' must be a list, got {actual_type.__name__}"
        
        elif expected_type == ValidationType.DICT:
            if not isinstance(value, dict):
                return f"Configuration '{key_path}' must be a dictionary, got {actual_type.__name__}"
        
        return None
    
    def _validate_constraints(self, key_path: str, value: Any, rule: ValidationRule) -> List[str]:
        """
        Validate value constraints
        
        Args:
            key_path: Configuration key path
            value: Value to validate
            rule: Validation rule
            
        Returns:
            List[str]: List of validation errors
        """
        errors = []
        
        # Min/max value validation
        if rule.min_value is not None and value < rule.min_value:
            errors.append(f"Configuration '{key_path}' must be >= {rule.min_value}, got {value}")
        
        if rule.max_value is not None and value > rule.max_value:
            errors.append(f"Configuration '{key_path}' must be <= {rule.max_value}, got {value}")
        
        # Min/max length validation
        if rule.min_length is not None and len(value) < rule.min_length:
            errors.append(f"Configuration '{key_path}' length must be >= {rule.min_length}, got {len(value)}")
        
        if rule.max_length is not None and len(value) > rule.max_length:
            errors.append(f"Configuration '{key_path}' length must be <= {rule.max_length}, got {len(value)}")
        
        # Pattern validation
        if rule.pattern is not None and isinstance(value, str):
            if not re.match(rule.pattern, value):
                errors.append(f"Configuration '{key_path}' does not match pattern '{rule.pattern}'")
        
        # Choices validation
        if rule.choices is not None and value not in rule.choices:
            errors.append(f"Configuration '{key_path}' must be one of {rule.choices}, got {value}")
        
        return errors
    
    def get_schema(self) -> Dict[str, ValidationRule]:
        """
        Get the schema definition
        
        Returns:
            Dict[str, ValidationRule]: Schema definition
        """
        return self._schema.copy()
    
    def get_rule(self, key_path: str) -> Optional[ValidationRule]:
        """
        Get validation rule for a specific key path
        
        Args:
            key_path: Configuration key path
            
        Returns:
            Optional[ValidationRule]: Validation rule or None if not found
        """
        return self._schema.get(key_path)