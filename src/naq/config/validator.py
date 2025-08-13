# src/naq/config/validator.py
"""
Configuration validation logic for NAQ.

This module provides comprehensive validation for NAQ configuration using
JSON Schema validation along with custom business logic validation.
"""

from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

from loguru import logger

from .schema import get_schema, format_validation_error, VALIDATION_ERROR_MESSAGES
from .types import NAQConfig
from ..exceptions import ConfigurationError


class ConfigValidator:
    """
    Validates NAQ configuration against schema and business rules.
    
    This validator performs both JSON Schema validation and custom business
    logic validation to ensure configuration integrity and compatibility.
    """
    
    def __init__(self):
        """Initialize configuration validator."""
        self.schema = get_schema()
        self._validation_errors: List[str] = []
        
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration dictionary against schema and business rules.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        self._validation_errors = []
        
        # Perform JSON Schema validation
        schema_valid = self._validate_schema(config)
        
        # Perform business logic validation
        business_valid = self._validate_business_rules(config)
        
        return (schema_valid and business_valid, self._validation_errors)
    
    def validate_typed_config(self, config: NAQConfig) -> Tuple[bool, List[str]]:
        """
        Validate typed NAQConfig instance.
        
        Args:
            config: NAQConfig instance to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        # Convert to dict for schema validation
        config_dict = config.to_dict()
        return self.validate_config(config_dict)
    
    def _validate_schema(self, config: Dict[str, Any]) -> bool:
        """
        Validate configuration against JSON schema.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True if schema validation passes
        """
        try:
            import jsonschema
        except ImportError:
            logger.warning(
                "jsonschema package not available. Install with: pip install jsonschema"
            )
            return True  # Skip schema validation if package not available
        
        try:
            jsonschema.validate(config, self.schema)
            logger.debug("Configuration passed JSON schema validation")
            return True
        except jsonschema.ValidationError as e:
            self._add_schema_error(e)
            return False
        except jsonschema.SchemaError as e:
            logger.error(f"Invalid schema definition: {e}")
            self._validation_errors.append(f"Schema error: {e.message}")
            return False
    
    def _add_schema_error(self, error: 'jsonschema.ValidationError') -> None:
        """
        Add formatted schema validation error to error list.
        
        Args:
            error: JSON schema validation error
        """
        path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
        
        if error.validator in VALIDATION_ERROR_MESSAGES:
            context = {
                "field": path,
                "expected_type": error.validator_value if error.validator == "type" else None,
                "minimum": error.validator_value if error.validator == "minimum" else None,
                "maximum": error.validator_value if error.validator == "maximum" else None,
                "minLength": error.validator_value if error.validator == "minLength" else None,
                "maxLength": error.validator_value if error.validator == "maxLength" else None,
                "minItems": error.validator_value if error.validator == "minItems" else None,
                "maxItems": error.validator_value if error.validator == "maxItems" else None,
                "enum_values": ", ".join(map(str, error.validator_value)) if error.validator == "enum" else None,
            }
            
            formatted_msg = format_validation_error(
                VALIDATION_ERROR_MESSAGES[error.validator], 
                context
            )
            self._validation_errors.append(formatted_msg)
        else:
            self._validation_errors.append(f"Validation error at {path}: {error.message}")
    
    def _validate_business_rules(self, config: Dict[str, Any]) -> bool:
        """
        Validate business logic rules not covered by JSON schema.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True if all business rules pass
        """
        valid = True
        
        # Validate NATS configuration
        valid &= self._validate_nats_config(config.get("nats", {}))
        
        # Validate queue configuration
        valid &= self._validate_queue_config(config.get("queues", {}))
        
        # Validate worker configuration
        valid &= self._validate_worker_config(config.get("workers", {}))
        
        # Validate scheduler configuration
        valid &= self._validate_scheduler_config(config.get("scheduler", {}))
        
        # Validate events configuration
        valid &= self._validate_events_config(config.get("events", {}))
        
        # Validate results configuration
        valid &= self._validate_results_config(config.get("results", {}))
        
        # Validate serialization configuration
        valid &= self._validate_serialization_config(config.get("serialization", {}))
        
        # Validate logging configuration
        valid &= self._validate_logging_config(config.get("logging", {}))
        
        # Validate environment configurations
        valid &= self._validate_environments_config(config.get("environments", {}))
        
        return valid
    
    def _validate_nats_config(self, nats_config: Dict[str, Any]) -> bool:
        """Validate NATS configuration business rules."""
        valid = True
        
        servers = nats_config.get("servers", [])
        if not servers:
            self._validation_errors.append("At least one NATS server must be configured")
            valid = False
        
        # Validate server URLs
        for i, server in enumerate(servers):
            if not isinstance(server, str):
                continue
                
            if not (server.startswith("nats://") or server.startswith("nats-tls://")):
                self._validation_errors.append(f"NATS server {i+1} must use nats:// or nats-tls:// scheme: {server}")
                valid = False
        
        # Validate auth configuration consistency
        auth = nats_config.get("auth")
        if auth:
            has_user_pass = auth.get("username") and auth.get("password")
            has_token = auth.get("token")
            
            if has_user_pass and has_token:
                self._validation_errors.append("NATS auth cannot use both username/password and token")
                valid = False
                
            if not has_user_pass and not has_token:
                self._validation_errors.append("NATS auth must specify either username/password or token")
                valid = False
        
        # Validate TLS configuration
        tls = nats_config.get("tls")
        if tls and tls.get("enabled"):
            cert_file = tls.get("cert_file")
            key_file = tls.get("key_file")
            
            if cert_file and not Path(cert_file).exists():
                self._validation_errors.append(f"NATS TLS cert file not found: {cert_file}")
                valid = False
                
            if key_file and not Path(key_file).exists():
                self._validation_errors.append(f"NATS TLS key file not found: {key_file}")
                valid = False
            
            # Both cert and key are required for client certificates
            if (cert_file or key_file) and not (cert_file and key_file):
                self._validation_errors.append("NATS TLS requires both cert_file and key_file for client certificates")
                valid = False
        
        return valid
    
    def _validate_queue_config(self, queues_config: Dict[str, Any]) -> bool:
        """Validate queue configuration business rules."""
        valid = True
        
        default_queue = queues_config.get("default")
        if default_queue and not isinstance(default_queue, str):
            self._validation_errors.append("Default queue name must be a string")
            valid = False
        
        prefix = queues_config.get("prefix", "")
        if prefix and not prefix.replace("_", "").replace("-", "").replace(".", "").isalnum():
            self._validation_errors.append("Queue prefix must contain only alphanumeric characters, underscores, hyphens, and dots")
            valid = False
        
        return valid
    
    def _validate_worker_config(self, workers_config: Dict[str, Any]) -> bool:
        """Validate worker configuration business rules."""
        valid = True
        
        concurrency = workers_config.get("concurrency", 1)
        max_job_duration = workers_config.get("max_job_duration", 3600)
        shutdown_timeout = workers_config.get("shutdown_timeout", 30)
        
        # Validate reasonable limits
        if concurrency > 1000:
            self._validation_errors.append("Worker concurrency should not exceed 1000 for performance reasons")
            valid = False
        
        if max_job_duration > 86400:  # 24 hours
            self._validation_errors.append("Maximum job duration should not exceed 24 hours")
            valid = False
        
        if shutdown_timeout > max_job_duration:
            self._validation_errors.append("Worker shutdown timeout should not exceed max job duration")
            valid = False
        
        # Validate worker pools
        pools = workers_config.get("pools", {})
        for pool_name, pool_config in pools.items():
            if not isinstance(pool_config, dict):
                continue
                
            pool_concurrency = pool_config.get("concurrency", 1)
            if pool_concurrency > concurrency:
                self._validation_errors.append(f"Pool '{pool_name}' concurrency ({pool_concurrency}) exceeds worker concurrency ({concurrency})")
                valid = False
        
        return valid
    
    def _validate_scheduler_config(self, scheduler_config: Dict[str, Any]) -> bool:
        """Validate scheduler configuration business rules."""
        valid = True
        
        if not scheduler_config.get("enabled", True):
            return valid  # Skip validation if scheduler is disabled
        
        lock_ttl = scheduler_config.get("lock_ttl", 30)
        lock_renew_interval = scheduler_config.get("lock_renew_interval", 15)
        
        if lock_renew_interval >= lock_ttl:
            self._validation_errors.append("Scheduler lock renewal interval must be less than lock TTL")
            valid = False
        
        if lock_renew_interval < 5:
            self._validation_errors.append("Scheduler lock renewal interval should be at least 5 seconds")
            valid = False
        
        return valid
    
    def _validate_events_config(self, events_config: Dict[str, Any]) -> bool:
        """Validate events configuration business rules."""
        valid = True
        
        if not events_config.get("enabled", True):
            return valid  # Skip validation if events are disabled
        
        batch_size = events_config.get("batch_size", 100)
        max_buffer_size = events_config.get("max_buffer_size", 10000)
        
        if batch_size > max_buffer_size:
            self._validation_errors.append("Events batch size should not exceed max buffer size")
            valid = False
        
        # Validate stream configuration
        stream_config = events_config.get("stream", {})
        if stream_config:
            max_age = stream_config.get("max_age", "")
            if max_age and not self._validate_duration_string(max_age):
                self._validation_errors.append(f"Invalid stream max_age format: {max_age}. Use format like '168h', '7d'")
                valid = False
            
            max_bytes = stream_config.get("max_bytes", "")
            if max_bytes and not self._validate_bytes_string(max_bytes):
                self._validation_errors.append(f"Invalid stream max_bytes format: {max_bytes}. Use format like '1GB', '500MB'")
                valid = False
        
        return valid
    
    def _validate_results_config(self, results_config: Dict[str, Any]) -> bool:
        """Validate results configuration business rules."""
        valid = True
        
        ttl = results_config.get("ttl", 604800)
        cleanup_interval = results_config.get("cleanup_interval", 3600)
        
        if cleanup_interval > ttl:
            self._validation_errors.append("Results cleanup interval should not exceed TTL")
            valid = False
        
        return valid
    
    def _validate_serialization_config(self, serialization_config: Dict[str, Any]) -> bool:
        """Validate serialization configuration business rules."""
        valid = True
        
        job_serializer = serialization_config.get("job_serializer", "pickle")
        
        # Validate JSON encoder/decoder if using JSON serialization
        if job_serializer == "json":
            json_config = serialization_config.get("json", {})
            encoder = json_config.get("encoder")
            decoder = json_config.get("decoder")
            
            if encoder:
                valid &= self._validate_class_path(encoder, "JSON encoder")
            if decoder:
                valid &= self._validate_class_path(decoder, "JSON decoder")
        
        return valid
    
    def _validate_logging_config(self, logging_config: Dict[str, Any]) -> bool:
        """Validate logging configuration business rules."""
        valid = True
        
        level = logging_config.get("level", "INFO").upper()
        valid_levels = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        if level not in valid_levels:
            self._validation_errors.append(f"Invalid log level: {level}. Must be one of: {', '.join(valid_levels)}")
            valid = False
        
        # Validate log file path if specified
        log_file = logging_config.get("file")
        if log_file:
            log_path = Path(log_file)
            parent_dir = log_path.parent
            
            if not parent_dir.exists():
                self._validation_errors.append(f"Log file directory does not exist: {parent_dir}")
                valid = False
            elif not parent_dir.is_dir():
                self._validation_errors.append(f"Log file parent path is not a directory: {parent_dir}")
                valid = False
        
        return valid
    
    def _validate_environments_config(self, environments_config: Dict[str, Any]) -> bool:
        """Validate environment configurations."""
        valid = True
        
        for env_name, env_config in environments_config.items():
            if not isinstance(env_config, dict):
                continue
            
            # Validate environment name
            if not env_name.replace("_", "").replace("-", "").isalnum():
                self._validation_errors.append(f"Environment name '{env_name}' must contain only alphanumeric characters, underscores, and hyphens")
                valid = False
            
            # Validate override paths
            overrides = env_config.get("overrides", {})
            for path, value in overrides.items():
                if not self._validate_config_path(path):
                    self._validation_errors.append(f"Invalid configuration path in environment '{env_name}': {path}")
                    valid = False
        
        return valid
    
    def _validate_duration_string(self, duration: str) -> bool:
        """Validate duration string format (e.g., '168h', '7d')."""
        import re
        pattern = r'^\d+[smhd]$'
        return bool(re.match(pattern, duration))
    
    def _validate_bytes_string(self, bytes_str: str) -> bool:
        """Validate bytes string format (e.g., '1GB', '500MB')."""
        import re
        pattern = r'^\d+[KMGT]?B$'
        return bool(re.match(pattern, bytes_str))
    
    def _validate_class_path(self, class_path: str, description: str) -> bool:
        """Validate that a class path string is properly formatted."""
        if not class_path or not isinstance(class_path, str):
            self._validation_errors.append(f"{description} class path cannot be empty")
            return False
        
        if not '.' in class_path:
            self._validation_errors.append(f"{description} class path must include module: {class_path}")
            return False
        
        # Try to import the class to verify it exists
        try:
            module_path, class_name = class_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            getattr(module, class_name)
            return True
        except (ImportError, AttributeError):
            self._validation_errors.append(f"{description} class not found: {class_path}")
            return False
    
    def _validate_config_path(self, path: str) -> bool:
        """Validate configuration path format."""
        if not path or not isinstance(path, str):
            return False
        
        # Basic validation - path should contain only valid characters
        import re
        pattern = r'^[a-zA-Z0-9_.\[\]]+$'
        return bool(re.match(pattern, path))


def validate_config(config: Dict[str, Any], raise_on_error: bool = True) -> Tuple[bool, List[str]]:
    """
    Validate configuration dictionary.
    
    Args:
        config: Configuration dictionary to validate
        raise_on_error: Whether to raise ConfigurationError on validation failure
        
    Returns:
        Tuple of (is_valid, error_messages)
        
    Raises:
        ConfigurationError: If validation fails and raise_on_error is True
    """
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config)
    
    if not is_valid and raise_on_error:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
        raise ConfigurationError(error_msg)
    
    return is_valid, errors


def validate_typed_config(config: NAQConfig, raise_on_error: bool = True) -> Tuple[bool, List[str]]:
    """
    Validate typed NAQConfig instance.
    
    Args:
        config: NAQConfig instance to validate
        raise_on_error: Whether to raise ConfigurationError on validation failure
        
    Returns:
        Tuple of (is_valid, error_messages)
        
    Raises:
        ConfigurationError: If validation fails and raise_on_error is True
    """
    validator = ConfigValidator()
    is_valid, errors = validator.validate_typed_config(config)
    
    if not is_valid and raise_on_error:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
        raise ConfigurationError(error_msg)
    
    return is_valid, errors


def quick_validate(config: Dict[str, Any]) -> bool:
    """
    Quick validation that returns only True/False without detailed errors.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid
    """
    is_valid, _ = validate_config(config, raise_on_error=False)
    return is_valid