# src/naq/config/loader.py
"""
Configuration loading logic for NAQ.

This module handles loading configuration from multiple sources with proper
priority ordering and merging. It supports YAML files, environment variables,
and default values with environment variable interpolation.
"""

import os
import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Union

from loguru import logger

from .defaults import get_default_config, ENVIRONMENT_VARIABLE_MAPPINGS
from .merger import merge_configurations
from ..exceptions import ConfigurationError


class ConfigLoader:
    """
    Loads configuration from multiple sources with priority.
    
    Configuration sources in order of priority (highest to lowest):
    1. Explicit config file (via config_path parameter)
    2. Current directory: ./naq.yaml or ./naq.yml
    3. User config: ~/.naq/config.yaml
    4. System config: /etc/naq/config.yaml
    5. Environment variables: NAQ_* variables
    6. Built-in defaults
    """
    
    DEFAULT_CONFIG_PATHS = [
        Path.cwd() / "naq.yaml",
        Path.cwd() / "naq.yml", 
        Path.home() / ".naq" / "config.yaml",
        Path("/etc/naq/config.yaml"),
    ]
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path: Optional explicit path to configuration file
        """
        self.config_path = Path(config_path) if config_path else None
        self._loaded_files: List[Path] = []
        
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from all sources with proper priority.
        
        Returns:
            Merged configuration dictionary
            
        Raises:
            ConfigurationError: If configuration loading fails
        """
        logger.debug("Loading NAQ configuration from all sources")
        
        # Start with default configuration
        config = get_default_config()
        logger.debug("Loaded default configuration")
        
        # Load from files (lowest priority first, so later sources override)
        file_configs = self._load_file_configs()
        for file_config in file_configs:
            config = merge_configurations(config, file_config)
        
        # Environment variables override file configs
        env_config = self._load_env_variables()
        if env_config:
            logger.debug(f"Loading environment variable overrides: {list(env_config.keys())}")
            config = merge_configurations(config, env_config)
        
        # Apply environment-specific overrides
        config = self._apply_environment_overrides(config)
        
        logger.debug(f"Configuration loaded successfully (files: {[str(f) for f in self._loaded_files]})")
        return config
        
    def _load_file_configs(self) -> List[Dict[str, Any]]:
        """
        Load configuration from YAML files.
        
        Returns:
            List of configuration dictionaries from files
        """
        configs = []
        
        if self.config_path:
            # Explicit config file has highest priority among files
            if self.config_path.exists():
                config = self._load_yaml_file(self.config_path)
                if config:
                    configs.append(config)
                    self._loaded_files.append(self.config_path)
                    logger.info(f"Loaded explicit config file: {self.config_path}")
            else:
                raise ConfigurationError(f"Specified config file not found: {self.config_path}")
        else:
            # Search default locations (in priority order)
            for path in self.DEFAULT_CONFIG_PATHS:
                if path.exists():
                    config = self._load_yaml_file(path)
                    if config:
                        configs.append(config)
                        self._loaded_files.append(path)
                        logger.info(f"Loaded config file: {path}")
                        break  # Use first found file only
                        
        return configs
        
    def _load_yaml_file(self, path: Path) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            path: Path to YAML file
            
        Returns:
            Configuration dictionary from file
            
        Raises:
            ConfigurationError: If file cannot be loaded or parsed
        """
        try:
            import yaml
        except ImportError:
            raise ConfigurationError(
                "PyYAML is required for YAML configuration files. "
                "Install with: pip install pyyaml"
            )
            
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Support environment variable interpolation
            content = self._interpolate_env_vars(content)
            
            # Parse YAML content
            config = yaml.safe_load(content)
            return config or {}
            
        except FileNotFoundError:
            logger.debug(f"Config file not found: {path}")
            return {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
        except UnicodeDecodeError as e:
            raise ConfigurationError(f"Cannot decode file {path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading config from {path}: {e}")
    
    def _interpolate_env_vars(self, content: str) -> str:
        """
        Replace ${VAR:default} patterns with environment variables.
        
        Supports patterns like:
        - ${NAQ_NATS_URL} - Use environment variable or empty string
        - ${NAQ_NATS_URL:nats://localhost:4222} - Use environment variable or default
        - ${NATS_HOST:localhost} - Use environment variable or default
        
        Args:
            content: YAML content with interpolation patterns
            
        Returns:
            Content with environment variables interpolated
        """
        def replace_env_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default = var_expr.split(':', 1)
            else:
                var_name, default = var_expr, ""
                
            value = os.getenv(var_name.strip(), default.strip())
            logger.debug(f"Interpolated ${{{var_expr}}} -> {value}")
            return value
        
        # Match ${VARIABLE} and ${VARIABLE:default} patterns
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_env_var, content)
    
    def _load_env_variables(self) -> Dict[str, Any]:
        """
        Load configuration from environment variables.
        
        Uses the mapping defined in defaults.py to convert environment
        variables to configuration paths.
        
        Returns:
            Configuration dictionary with environment variable values
        """
        env_config = {}
        
        for env_var, config_path in ENVIRONMENT_VARIABLE_MAPPINGS.items():
            value = os.getenv(env_var)
            if value is not None:
                converted_value = self._convert_env_value(value)
                self._set_nested_value(env_config, config_path, converted_value)
                logger.debug(f"Environment variable {env_var}={value} -> {config_path}")
                
        return env_config
    
    def _convert_env_value(self, value: str) -> Union[str, int, float, bool]:
        """
        Convert environment variable string to appropriate type.
        
        Args:
            value: Environment variable string value
            
        Returns:
            Converted value with appropriate type
        """
        # Boolean conversion
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off'):
            return False
        
        # Numeric conversion
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # String value (strip quotes if present)
        return value.strip('\'"')
    
    def _set_nested_value(self, config: Dict[str, Any], path: str, value: Any) -> None:
        """
        Set nested configuration value using dot notation.
        
        Supports paths like:
        - "nats.servers[0]" - Set first server in servers list
        - "workers.concurrency" - Set workers concurrency value
        - "events.stream.name" - Set nested stream name
        
        Args:
            config: Configuration dictionary to modify
            path: Dot-separated path
            value: Value to set
        """
        parts = path.split('.')
        current = config
        
        # Navigate to the parent of the final key
        for part in parts[:-1]:
            if '[' in part and part.endswith(']'):
                # Handle array access like "servers[0]"
                attr_name = part[:part.index('[')]
                index_str = part[part.index('[') + 1:-1]
                
                try:
                    index = int(index_str)
                    
                    # Ensure the attribute exists and is a list
                    if attr_name not in current:
                        current[attr_name] = []
                    elif not isinstance(current[attr_name], list):
                        current[attr_name] = []
                    
                    # Extend list if necessary
                    while len(current[attr_name]) <= index:
                        current[attr_name].append(None)
                    
                    # Move to the list item (create dict if needed)
                    if current[attr_name][index] is None:
                        current[attr_name][index] = {}
                    current = current[attr_name][index]
                    
                except (ValueError, IndexError):
                    logger.warning(f"Invalid array index in path: {path}")
                    return
            else:
                # Regular nested object
                if part not in current:
                    current[part] = {}
                elif not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
        
        # Set the final value
        final_part = parts[-1]
        if '[' in final_part and final_part.endswith(']'):
            # Handle array assignment like "servers[0]"
            attr_name = final_part[:final_part.index('[')]
            index_str = final_part[final_part.index('[') + 1:-1]
            
            try:
                index = int(index_str)
                
                # Ensure the attribute exists and is a list
                if attr_name not in current:
                    current[attr_name] = []
                elif not isinstance(current[attr_name], list):
                    current[attr_name] = []
                
                # Extend list if necessary
                while len(current[attr_name]) <= index:
                    current[attr_name].append(None)
                
                current[attr_name][index] = value
                
            except ValueError:
                logger.warning(f"Invalid array index in path: {path}")
        else:
            current[final_part] = value
    
    def _apply_environment_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment-specific configuration overrides.
        
        If NAQ_ENVIRONMENT is set and matches an environment in the config,
        applies the overrides for that environment.
        
        Args:
            config: Base configuration dictionary
            
        Returns:
            Configuration with environment overrides applied
        """
        env_name = os.getenv('NAQ_ENVIRONMENT')
        if not env_name:
            return config
            
        environments = config.get('environments', {})
        if env_name not in environments:
            logger.warning(f"Environment '{env_name}' not found in configuration")
            return config
            
        env_config = environments[env_name]
        overrides = env_config.get('overrides', {})
        
        if overrides:
            logger.info(f"Applying environment overrides for '{env_name}'")
            # Apply each override path
            for path, value in overrides.items():
                self._set_nested_value(config, path, value)
                logger.debug(f"Environment override: {path} = {value}")
        
        return config
    
    def get_loaded_files(self) -> List[Path]:
        """
        Get list of configuration files that were loaded.
        
        Returns:
            List of paths to loaded configuration files
        """
        return self._loaded_files.copy()
    
    def get_search_paths(self) -> List[Path]:
        """
        Get list of default configuration file search paths.
        
        Returns:
            List of default search paths
        """
        return self.DEFAULT_CONFIG_PATHS.copy()