# src/naq/config/__init__.py
"""
NAQ Configuration System

This package provides a comprehensive YAML configuration system with support for:
- Hierarchical configuration loading (YAML files → environment variables → defaults)
- Configuration validation and schema enforcement  
- Backward compatibility with existing environment variables
- Hot-reloading support for configuration changes
- Environment-specific configuration overrides

Usage:
    from naq.config import load_config, get_config, reload_config
    
    # Load configuration from default sources
    config = load_config()
    
    # Load from specific file
    config = load_config("/path/to/config.yaml")
    
    # Get cached configuration instance
    config = get_config()
    
    # Reload configuration
    config = reload_config()
"""

from typing import Optional, Dict, Any
from functools import lru_cache
from contextlib import contextmanager

from .loader import ConfigLoader
from .types import NAQConfig
from .validator import ConfigValidator
from .defaults import get_default_config
from ..utils.types import ConfigDict

# Global configuration instance
_config_instance: Optional[NAQConfig] = None
_config_path: Optional[str] = None


def load_config(config_path: Optional[str] = None, validate: bool = True) -> NAQConfig:
    """
    Load and return configuration from all sources.
    
    Configuration is loaded with the following priority (highest to lowest):
    1. Command-line specified config file (if config_path provided)
    2. Current directory: ./naq.yaml or ./naq.yml
    3. User config: ~/.naq/config.yaml
    4. System config: /etc/naq/config.yaml  
    5. Environment variables: NAQ_* variables
    6. Built-in defaults
    
    Args:
        config_path: Optional path to specific configuration file
        validate: Whether to validate configuration against schema
        
    Returns:
        Loaded and validated NAQConfig instance
        
    Raises:
        ConfigurationError: If configuration is invalid or cannot be loaded
    """
    global _config_instance, _config_path
    
    # Force reload if config path changed or no instance exists
    if _config_instance is None or _config_path != config_path:
        loader = ConfigLoader(config_path)
        config_dict = loader.load_config()
        
        if validate:
            validator = ConfigValidator()
            is_valid, errors = validator.validate_config(config_dict)
            if not is_valid:
                from ..exceptions import ConfigurationError
                error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
                raise ConfigurationError(error_msg)
            
        _config_instance = NAQConfig.from_dict(config_dict)
        _config_path = config_path
    
    return _config_instance


def get_config() -> NAQConfig:
    """
    Get current configuration instance.
    
    If no configuration has been loaded yet, loads configuration from
    default sources with validation enabled.
    
    Returns:
        Current NAQConfig instance
    """
    if _config_instance is None:
        return load_config()
    return _config_instance


def reload_config(config_path: Optional[str] = None) -> NAQConfig:
    """
    Force reload configuration from sources.
    
    This clears the cached configuration instance and loads fresh
    configuration from all sources.
    
    Args:
        config_path: Optional path to specific configuration file
        
    Returns:
        Newly loaded NAQConfig instance
    """
    global _config_instance, _config_path
    _config_instance = None
    _config_path = None
    return load_config(config_path)


def is_config_loaded() -> bool:
    """
    Check if configuration has been loaded.
    
    Returns:
        True if configuration instance exists, False otherwise
    """
    return _config_instance is not None


def get_config_source_info() -> Dict[str, Any]:
    """
    Get information about configuration sources.
    
    Returns:
        Dictionary with information about configuration loading
    """
    if _config_instance is None:
        return {"loaded": False}
    
    loader = ConfigLoader(_config_path)
    return {
        "loaded": True,
        "config_path": _config_path,
        "search_paths": [str(p) for p in loader.DEFAULT_CONFIG_PATHS],
        "found_files": [str(p) for p in loader.DEFAULT_CONFIG_PATHS if p.exists()],
    }


@contextmanager
def temp_config(config_dict: Dict[str, Any]):
    """
    Temporarily override configuration for testing.
    
    This context manager allows temporary configuration overrides
    without affecting the global configuration state.
    
    Args:
        config_dict: Configuration dictionary to use temporarily
        
    Usage:
        with temp_config({"nats": {"servers": ["nats://test:4222"]}}):
            config = get_config()
            assert config.nats.servers[0] == "nats://test:4222"
    """
    global _config_instance
    original = _config_instance
    _config_instance = NAQConfig.from_dict(config_dict)
    try:
        yield _config_instance
    finally:
        _config_instance = original


# Configuration validation utilities
def validate_config_file(config_path: str) -> bool:
    """
    Validate a configuration file without loading it globally.
    
    Args:
        config_path: Path to configuration file to validate
        
    Returns:
        True if configuration is valid
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    loader = ConfigLoader(config_path)
    config_dict = loader.load_config()
    validator = ConfigValidator()
    is_valid, errors = validator.validate_config(config_dict)
    if not is_valid:
        from ..exceptions import ConfigurationError
        error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
        raise ConfigurationError(error_msg)
    return True


def get_effective_config_dict() -> Dict[str, Any]:
    """
    Get the effective configuration as a dictionary.
    
    This includes all resolved values from all sources, useful for
    debugging and displaying current configuration.
    
    Returns:
        Configuration dictionary with all resolved values
    """
    config = get_config()
    return config.to_dict()


# Export main API
__all__ = [
    "load_config",
    "get_config", 
    "reload_config",
    "is_config_loaded",
    "get_config_source_info",
    "temp_config",
    "validate_config_file",
    "get_effective_config_dict",
    "NAQConfig",
    "ConfigLoader",
    "ConfigValidator",
    "ConfigDict",
]