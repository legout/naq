# src/naq/config/merger.py
"""
Configuration merging utilities for NAQ.

This module provides utilities for merging configuration dictionaries
from multiple sources while preserving proper priority and handling
complex nested structures.
"""

from typing import Dict, Any, Union
from copy import deepcopy


def merge_configurations(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two configuration dictionaries with deep merging.
    
    The override configuration takes precedence over the base configuration.
    Lists are replaced entirely (not merged) to avoid unexpected behavior.
    
    Args:
        base: Base configuration dictionary
        override: Override configuration dictionary
        
    Returns:
        Merged configuration dictionary
    """
    if not isinstance(base, dict) or not isinstance(override, dict):
        # If either is not a dict, override takes precedence
        return deepcopy(override)
    
    result = deepcopy(base)
    
    for key, value in override.items():
        if key in result:
            if isinstance(result[key], dict) and isinstance(value, dict):
                # Recursive merge for nested dictionaries
                result[key] = merge_configurations(result[key], value)
            else:
                # Direct replacement for non-dict values (including lists)
                result[key] = deepcopy(value)
        else:
            # New key from override
            result[key] = deepcopy(value)
    
    return result


def merge_multiple_configurations(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple configuration dictionaries.
    
    Later configurations in the argument list take precedence over earlier ones.
    
    Args:
        *configs: Configuration dictionaries to merge
        
    Returns:
        Merged configuration dictionary
    """
    if not configs:
        return {}
    
    result = deepcopy(configs[0])
    
    for config in configs[1:]:
        result = merge_configurations(result, config)
    
    return result


def override_config_values(config: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply configuration overrides using dot notation paths.
    
    This function handles overrides like:
    {
        "nats.servers[0]": "nats://new-server:4222",
        "workers.concurrency": 20
    }
    
    Args:
        config: Base configuration dictionary
        overrides: Dictionary with dot-notation keys and override values
        
    Returns:
        Configuration with overrides applied
    """
    result = deepcopy(config)
    
    for path, value in overrides.items():
        set_nested_value(result, path, value)
    
    return result


def set_nested_value(config: Dict[str, Any], path: str, value: Any) -> None:
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
                    current[attr_name].append({})
                
                # Move to the list item
                current = current[attr_name][index]
                
            except (ValueError, IndexError):
                raise ValueError(f"Invalid array index in path: {path}")
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
            raise ValueError(f"Invalid array index in path: {path}")
    else:
        current[final_part] = value


def get_nested_value(config: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Get nested configuration value using dot notation.
    
    Args:
        config: Configuration dictionary
        path: Dot-separated path
        default: Default value if path not found
        
    Returns:
        Configuration value or default
    """
    try:
        current = config
        parts = path.split('.')
        
        for part in parts:
            # Handle array access like "servers[0]"
            if '[' in part and part.endswith(']'):
                attr_name = part[:part.index('[')]
                index_str = part[part.index('[') + 1:-1]
                
                try:
                    index = int(index_str)
                    current = current[attr_name][index]
                except (ValueError, IndexError, KeyError, TypeError):
                    return default
            else:
                current = current[part]
                
        return current
    except (KeyError, TypeError):
        return default


def flatten_config(config: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Flatten nested configuration dictionary to dot notation.
    
    Args:
        config: Nested configuration dictionary
        prefix: Key prefix for recursion
        
    Returns:
        Flattened dictionary with dot-notation keys
    """
    result = {}
    
    for key, value in config.items():
        full_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            result.update(flatten_config(value, full_key))
        elif isinstance(value, list):
            # Handle lists by creating indexed keys
            for i, item in enumerate(value):
                item_key = f"{full_key}[{i}]"
                if isinstance(item, dict):
                    result.update(flatten_config(item, item_key))
                else:
                    result[item_key] = item
        else:
            result[full_key] = value
    
    return result


def unflatten_config(flat_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Unflatten dot-notation configuration dictionary to nested structure.
    
    Args:
        flat_config: Flattened configuration with dot-notation keys
        
    Returns:
        Nested configuration dictionary
    """
    result = {}
    
    for path, value in flat_config.items():
        set_nested_value(result, path, value)
    
    return result


def validate_config_structure(config: Dict[str, Any]) -> bool:
    """
    Validate that configuration has expected structure.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if structure is valid
    """
    required_sections = ['nats', 'queues', 'workers', 'events', 'serialization', 'logging']
    
    if not isinstance(config, dict):
        return False
    
    # Check that required sections exist
    for section in required_sections:
        if section not in config:
            return False
        if not isinstance(config[section], dict):
            return False
    
    return True


def diff_configurations(config1: Dict[str, Any], config2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare two configurations and return differences.
    
    Args:
        config1: First configuration dictionary
        config2: Second configuration dictionary
        
    Returns:
        Dictionary containing differences
    """
    flat1 = flatten_config(config1)
    flat2 = flatten_config(config2)
    
    differences = {}
    
    all_keys = set(flat1.keys()) | set(flat2.keys())
    
    for key in all_keys:
        val1 = flat1.get(key, '<missing>')
        val2 = flat2.get(key, '<missing>')
        
        if val1 != val2:
            differences[key] = {
                'config1': val1,
                'config2': val2
            }
    
    return differences


class ConfigMerger:
    """Configuration merger utility class."""
    
    @staticmethod
    def merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Merge configurations using deep merge strategy."""
        return merge_configurations(base, override)
    
    @staticmethod
    def apply_overrides(config: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Apply overrides using dot notation."""
        return apply_overrides(config, overrides)
    
    @staticmethod
    def validate_structure(config: Dict[str, Any]) -> bool:
        """Validate configuration structure."""
        return validate_config_structure(config)