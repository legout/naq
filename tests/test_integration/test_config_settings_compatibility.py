"""Integration tests for backward compatibility with settings.py."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from naq.config import load_config, get_config, reload_config
from naq.settings import _get_env_or_config, DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME


class TestConfigSettingsCompatibility:
    """Test backward compatibility with settings.py."""

    def test_get_env_or_config_with_env_var_priority(self):
        """Test that environment variables take priority over config values."""
        # Set environment variable
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        
        try:
            # Create a config file with different value
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file = Path(temp_dir) / "config.yaml"
                config_file.write_text("""
nats:
  servers: nats://config-server:4222
""")
                
                # Load config
                load_config(str(config_file))
                
                # _get_env_or_config should return the environment variable value
                result = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"])
                assert result == "nats://env-server:4222"
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()

    def test_get_env_or_config_with_config_value(self):
        """Test that config values are used when no environment variable is set."""
        # Ensure environment variable is not set
        if "NAQ_NATS_URL" in os.environ:
            del os.environ["NAQ_NATS_URL"]
        
        # Create a config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers: nats://config-server:4222
""")
            
            # Load config
            load_config(str(config_file))
            
            # _get_env_or_config should return the config value
            result = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"])
            assert result == "nats://config-server:4222"
            
            # Clean up
            reload_config()

    def test_get_env_or_config_with_default_value(self):
        """Test that default values are used when neither env var nor config is set."""
        # Ensure environment variable is not set
        if "NAQ_NATS_URL" in os.environ:
            del os.environ["NAQ_NATS_URL"]
        
        # Load default config (no config file)
        reload_config()
        
        # _get_env_or_config should return the default value
        result = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"], "nats://default-server:4222")
        assert result == "nats://default-server:4222"

    def test_get_env_or_config_with_nested_path(self):
        """Test that nested config paths work correctly."""
        # Ensure environment variable is not set
        if "NAQ_NATS_URL" in os.environ:
            del os.environ["NAQ_NATS_URL"]
        
        # Create a config file with nested values
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  auth:
    user: testuser
    password: testpass
""")
            
            # Load config
            load_config(str(config_file))
            
            # _get_env_or_config should return the nested config value
            result = _get_env_or_config("NAQ_USER", ["nats", "auth", "user"])
            assert result == "testuser"
            
            # Clean up
            reload_config()

    def test_get_env_or_config_with_invalid_path(self):
        """Test that invalid paths return the default value."""
        # Ensure environment variable is not set
        if "NAQ_NONEXISTENT" in os.environ:
            del os.environ["NAQ_NONEXISTENT"]
        
        # Load default config
        reload_config()
        
        # _get_env_or_config should return the default value for invalid path
        result = _get_env_or_config("NAQ_NONEXISTENT", ["nonexistent", "path"], "default_value")
        assert result == "default_value"

    def test_get_env_or_config_with_type_conversion(self):
        """Test that environment variables are returned as strings (no type conversion)."""
        # Set environment variable with a number
        os.environ["NAQ_SCHEDULER_LOCK_TTL"] = "60"
        
        try:
            # Load default config
            reload_config()
            
            # _get_env_or_config should return the string value
            result = _get_env_or_config("NAQ_SCHEDULER_LOCK_TTL", ["scheduler", "lock_ttl"])
            assert result == "60"
            assert isinstance(result, str)
        finally:
            # Clean up
            if "NAQ_SCHEDULER_LOCK_TTL" in os.environ:
                del os.environ["NAQ_SCHEDULER_LOCK_TTL"]
            reload_config()

    def test_default_constants_use_get_env_or_config(self):
        """Test that default constants in settings.py use _get_env_or_config."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://custom-server:4222"
        os.environ["NAQ_DEFAULT_QUEUE"] = "custom_queue"
        
        try:
            # Reload config to pick up environment variables
            reload_config()
            
            # Import the constants again to see if they pick up the environment variables
            # Note: This might not work in all cases due to module caching, but we can test
            # the function directly
            
            # Test that _get_env_or_config returns the environment variable values
            nats_url = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"], "nats://localhost:4222")
            queue_name = _get_env_or_config("NAQ_DEFAULT_QUEUE", ["queues", "default_name"], "naq_default_queue")
            
            assert nats_url == "nats://custom-server:4222"
            assert queue_name == "custom_queue"
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            if "NAQ_DEFAULT_QUEUE" in os.environ:
                del os.environ["NAQ_DEFAULT_QUEUE"]
            reload_config()

    def test_get_env_or_config_with_none_value(self):
        """Test that None values in config are handled correctly."""
        # Ensure environment variable is not set
        if "NAQ_NATS_URL" in os.environ:
            del os.environ["NAQ_NATS_URL"]
        
        # Create a config file with None value
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers: null
""")
            
            # Load config
            load_config(str(config_file))
            
            # _get_env_or_config should return the default value when config value is None
            result = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"], "nats://default-server:4222")
            assert result == "nats://default-server:4222"
            
            # Clean up
            reload_config()

    def test_get_env_or_config_with_empty_string(self):
        """Test that empty string environment variables are handled correctly."""
        # Set environment variable to empty string
        os.environ["NAQ_NATS_URL"] = ""
        
        try:
            # Load default config
            reload_config()
            
            # _get_env_or_config should return the empty string
            result = _get_env_or_config("NAQ_NATS_URL", ["nats", "servers"], "nats://default-server:4222")
            assert result == ""
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()