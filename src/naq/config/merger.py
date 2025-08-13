"""
Configuration Merging Logic

This module handles merging configuration from multiple sources with proper precedence.
"""

from typing import Dict, Any, List, Optional, Union
from copy import deepcopy
from .types import ConfigDict, ConfigSource, ConfigChange


class ConfigMerger:
    """
    Handles merging configuration from multiple sources
    
    This class provides methods to merge configuration dictionaries with proper
    precedence handling and change tracking.
    """
    
    def __init__(self):
        """Initialize the configuration merger"""
        self._source_priority = {
            ConfigSource.DEFAULT: 0,
            ConfigSource.YAML_FILE: 1,
            ConfigSource.ENVIRONMENT: 2,
            ConfigSource.COMMAND_LINE: 3,
        }
    
    def merge_configs(self, 
                     base_config: ConfigDict, 
                     override_configs: List[ConfigDict],
                     sources: List[ConfigSource],
                     track_changes: bool = False) -> ConfigDict:
        """
        Merge multiple configuration dictionaries
        
        Args:
            base_config: Base configuration dictionary
            override_configs: List of configuration dictionaries to merge
            sources: List of sources corresponding to override_configs
            track_changes: Whether to track changes during merge
            
        Returns:
            ConfigDict: Merged configuration dictionary
        """
        if track_changes:
            return self._merge_with_tracking(base_config, override_configs, sources)
        else:
            return self._merge_without_tracking(base_config, override_configs)
    
    def _merge_without_tracking(self,
                               base_config: ConfigDict,
                               override_configs: List[ConfigDict]) -> ConfigDict:
        """
        Merge configurations without tracking changes
        
        Args:
            base_config: Base configuration dictionary
            override_configs: List of configuration dictionaries to merge
            
        Returns:
            ConfigDict: Merged configuration dictionary
        """
        # Start with a deep copy of the base configuration
        merged = ConfigDict(deepcopy(base_config))
        
        # Apply each override configuration in order
        for override_config in override_configs:
            merged = ConfigDict(self._deep_merge(merged, override_config))
        
        return merged
    
    def _merge_with_tracking(self,
                            base_config: ConfigDict,
                            override_configs: List[ConfigDict],
                            sources: List[ConfigSource]) -> ConfigDict:
        """
        Merge configurations with change tracking
        
        Args:
            base_config: Base configuration dictionary
            override_configs: List of configuration dictionaries to merge
            sources: List of sources corresponding to override_configs
            
        Returns:
            ConfigDict: Merged configuration dictionary
        """
        # Start with a deep copy of the base configuration
        merged = ConfigDict(deepcopy(base_config))
        changes = []
        
        # Apply each override configuration in order
        for override_config, source in zip(override_configs, sources):
            merged_dict, new_changes = self._deep_merge_with_tracking(merged, override_config, source)
            merged = ConfigDict(merged_dict)
            changes.extend(new_changes)
        
        # Store changes in the merged config for later access
        setattr(merged, '_changes', changes)
        
        return merged
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries
        
        Args:
            base: Base dictionary
            override: Override dictionary
            
        Returns:
            Dict[str, Any]: Merged dictionary
        """
        result = deepcopy(base)
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = deepcopy(value)
        
        return result
    
    def _deep_merge_with_tracking(self, 
                                 base: Dict[str, Any], 
                                 override: Dict[str, Any],
                                 source: ConfigSource) -> tuple[Dict[str, Any], List[ConfigChange]]:
        """
        Deep merge two dictionaries with change tracking
        
        Args:
            base: Base dictionary
            override: Override dictionary
            source: Source of the override configuration
            
        Returns:
            tuple: (merged dictionary, list of changes)
        """
        result = deepcopy(base)
        changes = []
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                nested_result, nested_changes = self._deep_merge_with_tracking(
                    result[key], value, source
                )
                result[key] = nested_result
                
                # Prefix nested keys with the current key
                for change in nested_changes:
                    change.key_path = f"{key}.{change.key_path}"
                    changes.append(change)
            else:
                # Track the change
                old_value = result.get(key)
                if old_value != value:
                    change = ConfigChange(key, old_value, value, source)
                    changes.append(change)
                
                result[key] = deepcopy(value)
        
        return result, changes
    
    def merge_with_priority(self, 
                           configs: List[ConfigDict], 
                           sources: List[ConfigSource],
                           track_changes: bool = False) -> ConfigDict:
        """
        Merge configurations based on source priority
        
        Args:
            configs: List of configuration dictionaries
            sources: List of sources corresponding to configs
            track_changes: Whether to track changes during merge
            
        Returns:
            ConfigDict: Merged configuration dictionary
        """
        if len(configs) != len(sources):
            raise ValueError("Number of configs must match number of sources")
        
        # Sort configs by source priority
        sorted_configs = sorted(
            zip(configs, sources),
            key=lambda x: self._source_priority[x[1]],
            reverse=True
        )
        
        # Separate configs and sources
        sorted_configs_list = [config for config, _ in sorted_configs]
        sorted_sources = [source for _, source in sorted_configs]
        
        # Start with an empty base config
        base_config = ConfigDict()
        
        # Merge all configs
        return self.merge_configs(base_config, sorted_configs_list, sorted_sources, track_changes)
    
    def apply_environment_overrides(self, 
                                  base_config: ConfigDict, 
                                  env_config: ConfigDict,
                                  track_changes: bool = False) -> ConfigDict:
        """
        Apply environment variable overrides to base configuration
        
        Args:
            base_config: Base configuration dictionary
            env_config: Environment configuration dictionary
            track_changes: Whether to track changes
            
        Returns:
            ConfigDict: Configuration with environment overrides applied
        """
        return self.merge_configs(
            base_config, 
            [env_config], 
            [ConfigSource.ENVIRONMENT], 
            track_changes
        )
    
    def apply_yaml_overrides(self, 
                            base_config: ConfigDict, 
                            yaml_config: ConfigDict,
                            track_changes: bool = False) -> ConfigDict:
        """
        Apply YAML configuration overrides to base configuration
        
        Args:
            base_config: Base configuration dictionary
            yaml_config: YAML configuration dictionary
            track_changes: Whether to track changes
            
        Returns:
            ConfigDict: Configuration with YAML overrides applied
        """
        return self.merge_configs(
            base_config, 
            [yaml_config], 
            [ConfigSource.YAML_FILE], 
            track_changes
        )
    
    def get_changes(self, config: ConfigDict) -> List[ConfigChange]:
        """
        Get changes tracked during merge
        
        Args:
            config: Configuration dictionary with tracked changes
            
        Returns:
            List[ConfigChange]: List of changes
        """
        return getattr(config, '_changes', [])
    
    def clear_changes(self, config: ConfigDict) -> None:
        """
        Clear tracked changes from configuration
        
        Args:
            config: Configuration dictionary with tracked changes
        """
        if hasattr(config, '_changes'):
            delattr(config, '_changes')
    
    def flatten_config(self, config: ConfigDict, prefix: str = "") -> Dict[str, Any]:
        """
        Flatten nested configuration dictionary
        
        Args:
            config: Configuration dictionary to flatten
            prefix: Prefix for nested keys
            
        Returns:
            Dict[str, Any]: Flattened configuration dictionary
        """
        result = {}
        
        for key, value in config.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                nested_result = self.flatten_config(ConfigDict(value), full_key)
                result.update(nested_result)
            else:
                result[full_key] = value
        
        return result
    
    def unflatten_config(self, flat_config: Dict[str, Any]) -> ConfigDict:
        """
        Unflatten configuration dictionary
        
        Args:
            flat_config: Flattened configuration dictionary
            
        Returns:
            ConfigDict: Nested configuration dictionary
        """
        result = ConfigDict()
        
        for key, value in flat_config.items():
            self._set_nested_value(result, key, value)
        
        return result
    
    def _set_nested_value(self, config: Dict[str, Any], key_path: str, value: Any) -> None:
        """
        Set nested value in configuration dictionary
        
        Args:
            config: Configuration dictionary
            key_path: Dot-separated key path
            value: Value to set
        """
        keys = key_path.split('.')
        current = config
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def diff_configs(self, 
                    config1: ConfigDict, 
                    config2: ConfigDict,
                    source: ConfigSource = ConfigSource.YAML_FILE) -> List[ConfigChange]:
        """
        Compare two configurations and return differences
        
        Args:
            config1: First configuration dictionary
            config2: Second configuration dictionary
            source: Source of the changes
            
        Returns:
            List[ConfigChange]: List of differences
        """
        flat_config1 = self.flatten_config(config1)
        flat_config2 = self.flatten_config(config2)
        
        changes = []
        
        # Check for changed and new values
        for key, value2 in flat_config2.items():
            if key not in flat_config1:
                # New value
                changes.append(ConfigChange(key, None, value2, source))
            elif flat_config1[key] != value2:
                # Changed value
                changes.append(ConfigChange(key, flat_config1[key], value2, source))
        
        # Check for removed values
        for key, value1 in flat_config1.items():
            if key not in flat_config2:
                # Removed value
                changes.append(ConfigChange(key, value1, None, source))
        
        return changes