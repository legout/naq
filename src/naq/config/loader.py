"""
Configuration Loading Logic

This module handles loading configuration from various sources including:
- YAML files from multiple locations
- Environment variables
- Default values
"""

import os
import re
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from .types import ConfigDict, ConfigSource, ConfigLocation, NAQConfig
from .defaults import get_default_config, get_environment_mapping
from .merger import ConfigMerger


class ConfigurationError(Exception):
    """Exception raised for configuration-related errors"""
    pass


class ConfigLoader:
    """
    Handles loading configuration from multiple sources
    
    This class is responsible for loading configuration from:
    1. Default values
    2. YAML files from multiple locations
    3. Environment variables
    4. Environment-specific overrides
    
    The configuration is merged with the following priority (highest first):
    1. Environment variables
    2. YAML files (by priority)
    3. Default values
    """
    
    DEFAULT_CONFIG_PATHS = [
        Path.cwd() / "naq.yaml",
        Path.cwd() / "naq.yml",
        Path.home() / ".naq" / "config.yaml",
        Path("/etc/naq/config.yaml"),
    ]
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration loader
        
        Args:
            config_path: Optional path to a specific configuration file
        """
        self.config_path = Path(config_path) if config_path else None
        self._logger = logging.getLogger(__name__)
        self._merger = ConfigMerger()
        self._environment_mapping = get_environment_mapping()
    
    def load_config(self) -> ConfigDict:
        """
        Load configuration from all sources with proper priority
        
        Returns:
            ConfigDict: Merged configuration dictionary
        """
        # Start with defaults
        config = self._load_defaults()
        
        # Load from YAML files (lowest priority first)
        yaml_configs = []
        yaml_sources = []
        
        if self.config_path and self.config_path.exists():
            # Explicit config file has highest priority among files
            try:
                file_config = self._load_yaml_file(self.config_path)
                yaml_configs.append(file_config)
                yaml_sources.append(ConfigSource.YAML_FILE)
                self._logger.info(f"Loaded configuration from {self.config_path}")
            except ConfigurationError as e:
                self._logger.error(f"Error loading config from {self.config_path}: {e}")
        else:
            # Search default locations (reverse priority order)
            for path in reversed(self.DEFAULT_CONFIG_PATHS):
                if path.exists():
                    try:
                        file_config = self._load_yaml_file(path)
                        yaml_configs.insert(0, file_config)  # Insert at beginning for priority
                        yaml_sources.insert(0, ConfigSource.YAML_FILE)
                        self._logger.info(f"Loaded configuration from {path}")
                    except ConfigurationError as e:
                        self._logger.error(f"Error loading config from {path}: {e}")
        
        # Merge YAML configs with proper priority
        if yaml_configs:
            config = self._merger.merge_configs(config, yaml_configs, yaml_sources)
        
        # Environment variables override file configs
        env_config = self._load_env_variables()
        if env_config:
            config = self._merger.apply_environment_overrides(config, env_config)
            self._logger.debug("Applied environment variable overrides")
        
        # Apply environment-specific overrides
        config = self._apply_environment_overrides(config)
        
        return config
    
    def _load_defaults(self) -> ConfigDict:
        """
        Load default configuration values
        
        Returns:
            ConfigDict: Default configuration dictionary
        """
        return ConfigDict(get_default_config())
    
    def _load_yaml_file(self, path: Path) -> ConfigDict:
        """
        Load configuration from YAML file
        
        Args:
            path: Path to the YAML file
            
        Returns:
            ConfigDict: Configuration from YAML file
            
        Raises:
            ConfigurationError: If there's an error loading the file
        """
        try:
            with open(path, 'r') as f:
                content = f.read()
                
            # Support environment variable interpolation
            content = self._interpolate_env_vars(content)
            
            yaml_data = yaml.safe_load(content)
            return ConfigDict(yaml_data or {})
            
        except FileNotFoundError:
            return ConfigDict()
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading config from {path}: {e}")
    
    def _interpolate_env_vars(self, content: str) -> str:
        """
        Replace ${VAR:default} patterns with environment variables
        
        Args:
            content: YAML content as string
            
        Returns:
            str: Content with environment variables interpolated
        """
        def replace_env_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default = var_expr.split(':', 1)
            else:
                var_name, default = var_expr, ""
            return os.getenv(var_name.strip(), default.strip())
        
        return re.sub(r'\$\{([^}]+)\}', replace_env_var, content)
    
    def _load_env_variables(self) -> ConfigDict:
        """
        Load configuration from environment variables
        
        Returns:
            ConfigDict: Configuration from environment variables
        """
        env_config = ConfigDict()
        
        # Map environment variables to config structure
        for env_var, config_path in self._environment_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                converted_value = self._convert_env_value(value)
                env_config.set_nested(config_path, converted_value)
                
        return env_config
    
    def _convert_env_value(self, value: str) -> Any:
        """
        Convert environment variable string to appropriate type
        
        Args:
            value: String value from environment variable
            
        Returns:
            Any: Converted value
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
        
        # String value
        return value
    
    def _apply_environment_overrides(self, config: ConfigDict) -> ConfigDict:
        """
        Apply environment-specific overrides to configuration
        
        Args:
            config: Base configuration dictionary
            
        Returns:
            ConfigDict: Configuration with environment overrides applied
        """
        # Get current environment from environment variable
        current_env = os.getenv('NAQ_ENVIRONMENT', '').lower()
        
        if not current_env:
            return config
        
        # Check if we have environment-specific overrides
        environments = config.get('environments', {})
        env_overrides = environments.get(current_env, {})
        
        if not env_overrides:
            self._logger.debug(f"No environment overrides found for '{current_env}'")
            return config
        
        # Apply overrides
        self._logger.info(f"Applying environment overrides for '{current_env}'")
        return self._merger.merge_configs(config, [env_overrides], [ConfigSource.ENVIRONMENT])
    
    def get_config_locations(self) -> List[Path]:
        """
        Get all configuration file locations
        
        Returns:
            List[Path]: List of configuration file paths
        """
        locations = []
        
        if self.config_path:
            locations.append(self.config_path)
        
        locations.extend(self.DEFAULT_CONFIG_PATHS)
        
        return locations
    
    def find_config_file(self) -> Optional[Path]:
        """
        Find the first existing configuration file
        
        Returns:
            Optional[Path]: Path to the first existing config file or None
        """
        if self.config_path and self.config_path.exists():
            return self.config_path
        
        for path in self.DEFAULT_CONFIG_PATHS:
            if path.exists():
                return path
        
        return None
    
    def watch_config_files(self, callback=None) -> None:
        """
        Watch configuration files for changes (placeholder for hot-reload)
        
        Args:
            callback: Optional callback function to call when config changes
        """
        # This will be implemented for hot-reload support
        # For now, it's a placeholder
        self._logger.info("Hot-reload support not yet implemented")
    
    def load_naq_config(self) -> NAQConfig:
        """
        Load configuration as a NAQConfig object
        
        Returns:
            NAQConfig: Configuration object
        """
        config_dict = self.load_config()
        return NAQConfig.from_dict(config_dict)
    
    def reload_config(self) -> ConfigDict:
        """
        Reload configuration from all sources
        
        Returns:
            ConfigDict: Reloaded configuration dictionary
        """
        self._logger.info("Reloading configuration")
        return self.load_config()