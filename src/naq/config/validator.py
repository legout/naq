"""
Configuration Validation

This module handles validation of configuration values against defined schemas.
"""

import logging
from copy import deepcopy
from typing import Dict, Any, List, Optional, Set, Tuple
from .types import ConfigDict, ConfigSource, NAQConfig
from .schema import ConfigSchema, ValidationRule


class ConfigValidationError(Exception):
    """Exception raised for configuration validation errors"""
    
    def __init__(self, message: str, key_path: Optional[str] = None):
        """
        Initialize validation error
        
        Args:
            message: Error message
            key_path: Configuration key path that caused the error
        """
        self.message = message
        self.key_path = key_path
        super().__init__(message)


class ConfigValidator:
    """
    Configuration validation handler
    
    This class provides methods to validate configuration values against
    defined schemas and handle validation errors.
    """
    
    def __init__(self, schema: Optional[ConfigSchema] = None):
        """
        Initialize the configuration validator
        
        Args:
            schema: Optional configuration schema to use
        """
        self._schema = schema or ConfigSchema()
        self._logger = logging.getLogger(__name__)
    
    def validate(self, config: ConfigDict, raise_on_error: bool = True) -> List[str]:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration dictionary to validate
            raise_on_error: Whether to raise exception on validation errors
            
        Returns:
            List[str]: List of validation error messages
            
        Raises:
            ConfigValidationError: If validation fails and raise_on_error is True
        """
        errors = self._schema.validate_config(config)
        
        if errors and raise_on_error:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ConfigValidationError(error_msg)
        
        return errors
    
    def validate_with_details(self, config: ConfigDict) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Validate configuration and return detailed results
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple[bool, List[Dict[str, Any]]]: 
                - Whether validation passed
                - List of validation error details
        """
        errors = self._schema.validate_config(config)
        error_details = []
        
        for error in errors:
            # Parse error message to extract key path and message
            if "Configuration '" in error and "' is" in error:
                key_start = error.find("Configuration '") + len("Configuration '")
                key_end = error.find("' is")
                key_path = error[key_start:key_end]
                message = error[key_end + 4:]  # Skip "' is"
            else:
                key_path = "unknown"
                message = error
            
            error_details.append({
                "key_path": key_path,
                "message": message,
                "full_message": error
            })
        
        return len(errors) == 0, error_details
    
    def validate_section(self, config: ConfigDict, section: str) -> List[str]:
        """
        Validate a specific section of the configuration
        
        Args:
            config: Configuration dictionary to validate
            section: Section name to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        section_config = config.get(section, {})
        if not isinstance(section_config, dict):
            return [f"Section '{section}' is not a dictionary"]
        
        # Create a temporary config with only the section to validate
        temp_config = ConfigDict()
        temp_config[section] = section_config
        
        # Filter schema to only include rules for this section
        section_errors = []
        for key_path, rule in self._schema.get_schema().items():
            if key_path.startswith(f"{section}."):
                value = temp_config.get_nested(key_path)
                if rule.required and value is None:
                    section_errors.append(f"Required configuration '{key_path}' is missing")
                    continue
                
                if value is None:
                    continue
                
                # Validate type
                type_error = self._schema._validate_type(key_path, value, rule.type)
                if type_error:
                    section_errors.append(type_error)
                    continue
                
                # Validate constraints
                constraint_errors = self._schema._validate_constraints(key_path, value, rule)
                section_errors.extend(constraint_errors)
        
        return section_errors
    
    def validate_key(self, config: ConfigDict, key_path: str, value: Any) -> Optional[str]:
        """
        Validate a specific configuration key
        
        Args:
            config: Configuration dictionary
            key_path: Configuration key path
            value: Value to validate
            
        Returns:
            Optional[str]: Error message if validation fails, None otherwise
        """
        rule = self._schema.get_rule(key_path)
        if not rule:
            # No rule defined for this key, consider it valid
            return None
        
        # Check if required value is missing
        if rule.required and value is None:
            return f"Required configuration '{key_path}' is missing"
        
        # Skip validation if value is None and not required
        if value is None:
            return None
        
        # Validate type
        type_error = self._schema._validate_type(key_path, value, rule.type)
        if type_error:
            return type_error
        
        # Validate constraints
        constraint_errors = self._schema._validate_constraints(key_path, value, rule)
        if constraint_errors:
            return constraint_errors[0]  # Return first constraint error
        
        return None
    
    def get_validation_rules(self, section: Optional[str] = None) -> Dict[str, ValidationRule]:
        """
        Get validation rules, optionally filtered by section
        
        Args:
            section: Optional section name to filter rules
            
        Returns:
            Dict[str, ValidationRule]: Validation rules
        """
        schema = self._schema.get_schema()
        
        if section is None:
            return schema
        
        return {
            key_path: rule 
            for key_path, rule in schema.items() 
            if key_path.startswith(f"{section}.")
        }
    
    def get_required_keys(self, section: Optional[str] = None) -> List[str]:
        """
        Get list of required configuration keys
        
        Args:
            section: Optional section name to filter keys
            
        Returns:
            List[str]: List of required key paths
        """
        rules = self.get_validation_rules(section)
        return [
            key_path 
            for key_path, rule in rules.items() 
            if rule.required
        ]
    
    def get_missing_keys(self, config: ConfigDict, section: Optional[str] = None) -> List[str]:
        """
        Get list of missing required configuration keys
        
        Args:
            config: Configuration dictionary to check
            section: Optional section name to filter keys
            
        Returns:
            List[str]: List of missing required key paths
        """
        required_keys = self.get_required_keys(section)
        missing_keys = []
        
        for key_path in required_keys:
            value = config.get_nested(key_path)
            if value is None:
                missing_keys.append(key_path)
        
        return missing_keys
    
    def check_completeness(self, config: ConfigDict) -> Tuple[bool, List[str]]:
        """
        Check if configuration is complete (all required keys present)
        
        Args:
            config: Configuration dictionary to check
            
        Returns:
            Tuple[bool, List[str]]: 
                - Whether configuration is complete
                - List of missing required keys
        """
        missing_keys = self.get_missing_keys(config)
        return len(missing_keys) == 0, missing_keys
    
    def get_default_values(self, section: Optional[str] = None) -> Dict[str, Any]:
        """
        Get default values for configuration keys
        
        Args:
            section: Optional section name to filter keys
            
        Returns:
            Dict[str, Any]: Dictionary of default values
        """
        rules = self.get_validation_rules(section)
        defaults = {}
        
        for key_path, rule in rules.items():
            if rule.default is not None:
                defaults[key_path] = rule.default
        
        return defaults
    
    def apply_defaults(self, config: ConfigDict, section: Optional[str] = None) -> ConfigDict:
        """
        Apply default values to configuration
        
        Args:
            config: Configuration dictionary to update
            section: Optional section name to filter defaults
            
        Returns:
            ConfigDict: Configuration with defaults applied
        """
        defaults = self.get_default_values(section)
        result = ConfigDict(deepcopy(config))
        
        for key_path, default_value in defaults.items():
            current_value = result.get_nested(key_path)
            if current_value is None:
                result.set_nested(key_path, default_value)
        
        return result
    
    def validate_environment_compatibility(self, config: ConfigDict) -> List[str]:
        """
        Validate configuration for environment variable compatibility
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List[str]: List of compatibility warnings
        """
        warnings = []
        
        # Check for complex nested structures that can't be represented as env vars
        for key_path, value in config.items():
            if isinstance(value, dict):
                warnings.append(f"Nested configuration '{key_path}' cannot be fully represented as environment variables")
            elif isinstance(value, list):
                warnings.append(f"List configuration '{key_path}' cannot be represented as environment variables")
        
        return warnings
    
    def validate_hot_reload_compatibility(self, config: ConfigDict) -> List[str]:
        """
        Validate configuration for hot-reload compatibility
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List[str]: List of compatibility warnings
        """
        warnings = []
        
        # Check for values that shouldn't be changed at runtime
        runtime_safe_keys = {
            'server.host', 'server.port', 'server.max_payload',
            'queue.max_retries', 'queue.retry_delay', 'queue.timeout',
            'worker.max_jobs', 'worker.job_timeout',
            'scheduler.enabled', 'scheduler.timezone',
            'events.enabled', 'events.level',
            'dashboard.enabled', 'dashboard.host', 'dashboard.port',
            'logging.level', 'logging.file',
            'development.debug', 'development.hot_reload'
        }
        
        for key_path in config.keys():
            if key_path not in runtime_safe_keys:
                warnings.append(f"Configuration '{key_path}' may not be safe to change at runtime")
        
        return warnings
    
    def log_validation_results(self, errors: List[str], warnings: Optional[List[str]] = None) -> None:
        """
        Log validation results
        
        Args:
            errors: List of validation errors
            warnings: Optional list of validation warnings
        """
        if errors:
            self._logger.error("Configuration validation failed:")
            for error in errors:
                self._logger.error(f"  - {error}")
        
        if warnings:
            self._logger.warning("Configuration validation warnings:")
            for warning in warnings:
                self._logger.warning(f"  - {warning}")
        
        if not errors and not warnings:
            self._logger.info("Configuration validation passed")
    
    def set_schema(self, schema: ConfigSchema) -> None:
        """
        Set the configuration schema
        
        Args:
            schema: Configuration schema to use
        """
        self._schema = schema
    
    def get_schema(self) -> ConfigSchema:
        """
        Get the current configuration schema
        
        Returns:
            ConfigSchema: Current configuration schema
        """
        return self._schema
    
    def validate_naq_config(self, config: NAQConfig, raise_on_error: bool = True) -> List[str]:
        """
        Validate NAQConfig object against schema
        
        Args:
            config: NAQConfig object to validate
            raise_on_error: Whether to raise exception on validation errors
            
        Returns:
            List[str]: List of validation error messages
            
        Raises:
            ConfigValidationError: If validation fails and raise_on_error is True
        """
        # Convert NAQConfig to dictionary for validation
        config_dict = ConfigDict(config.to_dict())
        return self.validate(config_dict, raise_on_error)
    
    def validate_nats_config(self, config: NAQConfig) -> List[str]:
        """
        Validate NATS configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "nats")
    
    def validate_workers_config(self, config: NAQConfig) -> List[str]:
        """
        Validate workers configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "workers")
    
    def validate_events_config(self, config: NAQConfig) -> List[str]:
        """
        Validate events configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "events")
    
    def validate_queues_config(self, config: NAQConfig) -> List[str]:
        """
        Validate queues configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "queues")
    
    def validate_scheduler_config(self, config: NAQConfig) -> List[str]:
        """
        Validate scheduler configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "scheduler")
    
    def validate_results_config(self, config: NAQConfig) -> List[str]:
        """
        Validate results configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "results")
    
    def validate_serialization_config(self, config: NAQConfig) -> List[str]:
        """
        Validate serialization configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "serialization")
    
    def validate_logging_config(self, config: NAQConfig) -> List[str]:
        """
        Validate logging configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "logging")
    
    def validate_environments_config(self, config: NAQConfig) -> List[str]:
        """
        Validate environments configuration section
        
        Args:
            config: NAQConfig object to validate
            
        Returns:
            List[str]: List of validation error messages
        """
        config_dict = ConfigDict(config.to_dict())
        return self.validate_section(config_dict, "environments")