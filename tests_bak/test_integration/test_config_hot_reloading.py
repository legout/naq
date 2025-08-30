"""Integration tests for configuration hot-reloading behavior."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from naq.config import load_config, get_config, reload_config, temp_config
from naq.exceptions import ConfigurationError


class TestConfigHotReloading:
    """Test configuration hot-reloading behavior."""

    def test_reload_config_clears_global_instance(self):
        """Test that reload_config clears the global configuration instance."""
        # Load initial config
        initial_config = load_config()
        
        # Verify that the global instance is set
        assert get_config() is initial_config
        
        # Reload config
        reloaded_config = reload_config()
        
        # Verify that the global instance is updated
        assert get_config() is reloaded_config
        assert get_config() is not initial_config

    def test_reload_config_with_new_file(self):
        """Test reloading configuration with a new file."""
        # Create a temporary config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file1 = Path(temp_dir) / "config1.yaml"
            config_file1.write_text("""
nats:
  servers:
    - nats://server1:4222
queues:
  default_name: queue1
""")
            
            # Load initial config
            config1 = load_config(str(config_file1))
            assert config1.nats.servers == ["nats://server1:4222"]
            assert config1.queues.default_name == "queue1"
            
            # Create a new config file
            config_file2 = Path(temp_dir) / "config2.yaml"
            config_file2.write_text("""
nats:
  servers:
    - nats://server2:4222
queues:
  default_name: queue2
""")
            
            # Reload config with new file
            config2 = reload_config(str(config_file2))
            
            # Verify that the config was updated
            assert config2.nats.servers == ["nats://server2:4222"]
            assert config2.queues.default_name == "queue2"
            assert get_config() is config2

    def test_reload_config_with_environment_variables(self):
        """Test reloading configuration with environment variables."""
        # Set initial environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env1:4222"
        
        try:
            # Load initial config
            config1 = load_config()
            assert config1.nats.servers == ["nats://env1:4222"]
            
            # Change environment variables
            os.environ["NAQ_NATS_URL"] = "nats://env2:4222"
            
            # Reload config
            config2 = reload_config()
            
            # Verify that the config was updated with new environment variables
            assert config2.nats.servers == ["nats://env2:4222"]
            assert get_config() is config2
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()

    def test_reload_config_with_validation(self):
        """Test reloading configuration with validation enabled."""
        # Create a temporary valid config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
queues:
  default_name: test_queue
""")
            
            # Load initial config with validation
            config1 = load_config(str(config_file), validate=True)
            
            # Modify the config file to be invalid
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Try to reload with validation - should raise an error
            with pytest.raises(ConfigurationError):
                reload_config(str(config_file), validate=True)
            
            # The global instance should still be the original valid config
            assert get_config() is config1

    def test_reload_config_without_validation(self):
        """Test reloading configuration without validation."""
        # Create a temporary config file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://localhost:4222
queues:
  default_name: test_queue
""")
            
            # Load initial config
            config1 = load_config(str(config_file))
            
            # Modify the config file to be invalid
            config_file.write_text("""
nats:
  servers: invalid_value  # Should be a list
""")
            
            # Reload without validation - should not raise an error
            config2 = reload_config(str(config_file), validate=False)
            
            # The global instance should be updated
            assert get_config() is config2
            # But the config will have default values for invalid fields
            assert config2.nats.servers == ["nats://localhost:4222"]  # Default value

    def test_reload_config_with_no_file(self):
        """Test reloading configuration with no file (uses defaults)."""
        # Load initial config with a file
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / "config.yaml"
            config_file.write_text("""
nats:
  servers:
    - nats://custom-server:4222
""")
            
            config1 = load_config(str(config_file))
            assert config1.nats.servers == ["nats://custom-server:4222"]
            
            # Reload with no file (should use defaults)
            config2 = reload_config()
            
            # Verify that the config was updated to defaults
            assert config2.nats.servers == ["nats://localhost:4222"]
            assert get_config() is config2

    def test_get_config_lazy_loading(self):
        """Test that get_config performs lazy loading when no config is loaded."""
        # Clear the global instance
        reload_config._config_instance = None
        
        # Call get_config - should load default config
        config = get_config()
        
        # Verify that default config was loaded
        assert config.nats.servers == ["nats://localhost:4222"]
        assert config.queues.default_name == "naq_default_queue"

    def test_temp_config_context_manager(self):
        """Test the temp_config context manager."""
        # Load initial config
        initial_config = load_config()
        initial_server = initial_config.nats.servers[0]
        
        # Use temp_config with custom data
        custom_data = {
            "nats": {
                "servers": ["nats://temp-server:4222"]
            }
        }
        
        with temp_config(custom_data) as temp_cfg:
            # Verify that the global config is temporarily changed
            assert get_config() is temp_cfg
            assert temp_cfg.nats.servers == ["nats://temp-server:4222"]
        
        # Verify that the original config is restored
        assert get_config() is initial_config
        assert get_config().nats.servers[0] == initial_server

    def test_temp_config_with_no_data(self):
        """Test the temp_config context manager with no custom data."""
        # Load initial config
        initial_config = load_config()
        initial_server = initial_config.nats.servers[0]
        
        # Use temp_config with no data
        with temp_config() as temp_cfg:
            # Verify that the global config is temporarily changed to defaults
            assert get_config() is temp_cfg
            assert temp_cfg.nats.servers == ["nats://localhost:4222"]
        
        # Verify that the original config is restored
        assert get_config() is initial_config
        assert get_config().nats.servers[0] == initial_server

    def test_temp_config_with_exception(self):
        """Test that temp_config restores original config even if an exception occurs."""
        # Load initial config
        initial_config = load_config()
        initial_server = initial_config.nats.servers[0]
        
        # Use temp_config with custom data and raise an exception
        custom_data = {
            "nats": {
                "servers": ["nats://temp-server:4222"]
            }
        }
        
        try:
            with temp_config(custom_data) as temp_cfg:
                # Verify that the global config is temporarily changed
                assert get_config() is temp_cfg
                assert temp_cfg.nats.servers == ["nats://temp-server:4222"]
                # Raise an exception
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected exception
        
        # Verify that the original config is restored despite the exception
        assert get_config() is initial_config
        assert get_config().nats.servers[0] == initial_server

    def test_multiple_reload_configs(self):
        """Test multiple consecutive reload operations."""
        # Create temporary config files
        with tempfile.TemporaryDirectory() as temp_dir:
            config_files = []
            for i in range(3):
                config_file = Path(temp_dir) / f"config{i}.yaml"
                config_file.write_text(f"""
nats:
  servers:
    - nats://server{i}:4222
queues:
  default_name: queue{i}
""")
                config_files.append(config_file)
            
            # Load and reload configs multiple times
            configs = []
            for config_file in config_files:
                config = reload_config(str(config_file))
                configs.append(config)
                
                # Verify that the global instance is updated
                assert get_config() is config
                assert config.nats.servers == [f"nats://server{len(configs)-1}:4222"]
                assert config.queues.default_name == f"queue{len(configs)-1}"
            
            # Verify that all configs are different instances
            assert len(set(configs)) == 3

    def test_reload_config_preserves_environment_priority(self):
        """Test that reload_config preserves environment variable priority."""
        # Set environment variable
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        
        try:
            # Create a config file with different value
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file = Path(temp_dir) / "config.yaml"
                config_file.write_text("""
nats:
  servers:
    - nats://file-server:4222
""")
                
                # Load initial config
                config1 = load_config(str(config_file))
                assert config1.nats.servers == ["nats://env-server:4222"]  # Env takes precedence
                
                # Reload config
                config2 = reload_config(str(config_file))
                
                # Verify that environment variable still takes precedence
                assert config2.nats.servers == ["nats://env-server:4222"]
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()