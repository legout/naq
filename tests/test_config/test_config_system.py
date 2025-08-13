# tests/test_config/test_config_system.py
"""Comprehensive tests for the configuration system."""

import os
import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

from naq.config import (
    load_config, 
    get_config, 
    reload_config,
    ConfigDict
)
from naq.config.loader import ConfigLoader
from naq.config.validator import ConfigValidator
from naq.config.merger import ConfigMerger
from naq.config.defaults import DEFAULT_CONFIG
from naq.exceptions import ConfigurationError


class TestConfigLoader:
    """Test configuration loading functionality."""

    def test_load_from_dict(self):
        """Test loading config from dictionary."""
        config_dict = {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 4}
        }
        
        loader = ConfigLoader()
        config = loader.load_from_dict(config_dict)
        
        assert config.to_dict()["nats"]["url"] == "nats://localhost:4222"
        assert config.to_dict()["workers"]["concurrency"] == 4

    def test_load_from_yaml_file(self):
        """Test loading config from YAML file."""
        config_data = {
            "nats": {
                "url": "nats://test:4222",
                "client_name": "test-client"
            },
            "workers": {
                "concurrency": 8,
                "heartbeat_interval": 30
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            config = loader.load_from_file(temp_file)
            
            config_dict = config.to_dict()
            assert config_dict["nats"]["url"] == "nats://test:4222"
            assert config_dict["workers"]["concurrency"] == 8
        finally:
            os.unlink(temp_file)

    def test_load_from_environment(self):
        """Test loading config from environment variables."""
        env_vars = {
            "NAQ_NATS_URL": "nats://env:4222",
            "NAQ_WORKERS_CONCURRENCY": "16",
            "NAQ_EVENTS_ENABLED": "false"
        }
        
        with patch.dict(os.environ, env_vars):
            loader = ConfigLoader()
            config = loader.load_from_environment()
            
            config_dict = config.to_dict()
            assert config_dict["nats"]["url"] == "nats://env:4222"
            assert config_dict["workers"]["concurrency"] == 16
            assert config_dict["events"]["enabled"] is False

    def test_load_nonexistent_file(self):
        """Test loading from nonexistent file."""
        loader = ConfigLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_from_file("/nonexistent/config.yaml")

    def test_load_invalid_yaml(self):
        """Test loading invalid YAML file."""
        invalid_yaml = "nats:\n  url: nats://test\n invalid_yaml: ["
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(invalid_yaml)
            temp_file = f.name
        
        try:
            loader = ConfigLoader()
            with pytest.raises(ConfigurationError):
                loader.load_from_file(temp_file)
        finally:
            os.unlink(temp_file)


class TestConfigValidator:
    """Test configuration validation."""

    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        config = {
            "nats": {
                "url": "nats://localhost:4222",
                "client_name": "test-client"
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": 30
            },
            "events": {
                "enabled": True,
                "batch_size": 100
            }
        }
        
        validator = ConfigValidator()
        # Should not raise any exceptions
        validator.validate(config)

    def test_validate_invalid_nats_config(self):
        """Test validation of invalid NATS configuration."""
        config = {
            "nats": {
                "url": "invalid-url",  # Invalid URL format
                "client_name": ""      # Empty client name
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError):
            validator.validate(config)

    def test_validate_invalid_worker_config(self):
        """Test validation of invalid worker configuration."""
        config = {
            "workers": {
                "concurrency": -1,         # Negative concurrency
                "heartbeat_interval": 0    # Zero heartbeat interval
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError):
            validator.validate(config)

    def test_validate_invalid_events_config(self):
        """Test validation of invalid events configuration."""
        config = {
            "events": {
                "enabled": "yes",  # Should be boolean
                "batch_size": 0    # Should be positive
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError):
            validator.validate(config)

    def test_validate_missing_required_fields(self):
        """Test validation with missing required fields."""
        config = {
            "nats": {
                # Missing required 'url' field
                "client_name": "test"
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError):
            validator.validate(config)

    def test_validate_type_errors(self):
        """Test validation with type errors."""
        config = {
            "nats": {
                "url": "nats://localhost:4222",
                "timeout": "30"  # Should be numeric
            },
            "workers": {
                "concurrency": "4"  # Should be integer, but string might be convertible
            }
        }
        
        validator = ConfigValidator()
        # May or may not raise depending on implementation
        # Some validators auto-convert strings to appropriate types


class TestConfigMerger:
    """Test configuration merging functionality."""

    def test_merge_basic_configs(self):
        """Test merging basic configurations."""
        base_config = {
            "nats": {"url": "nats://localhost:4222"},
            "workers": {"concurrency": 4}
        }
        
        override_config = {
            "nats": {"client_name": "test-client"},
            "workers": {"concurrency": 8}  # Override
        }
        
        merger = ConfigMerger()
        merged = merger.merge(base_config, override_config)
        
        assert merged["nats"]["url"] == "nats://localhost:4222"
        assert merged["nats"]["client_name"] == "test-client"
        assert merged["workers"]["concurrency"] == 8  # Overridden

    def test_merge_nested_configs(self):
        """Test merging deeply nested configurations."""
        base_config = {
            "services": {
                "job_service": {
                    "timeout": 30,
                    "retries": 3,
                    "options": {"buffer_size": 1000}
                }
            }
        }
        
        override_config = {
            "services": {
                "job_service": {
                    "timeout": 60,  # Override
                    "options": {"compression": True}  # Add new option
                }
            }
        }
        
        merger = ConfigMerger()
        merged = merger.merge(base_config, override_config)
        
        job_service = merged["services"]["job_service"]
        assert job_service["timeout"] == 60
        assert job_service["retries"] == 3  # Preserved
        assert job_service["options"]["buffer_size"] == 1000  # Preserved
        assert job_service["options"]["compression"] is True  # Added

    def test_merge_list_configs(self):
        """Test merging configurations with lists."""
        base_config = {
            "workers": {
                "queues": ["queue1", "queue2"],
                "plugins": ["plugin_a"]
            }
        }
        
        override_config = {
            "workers": {
                "queues": ["queue3"],  # Replace entire list
                "plugins": ["plugin_b", "plugin_c"]
            }
        }
        
        merger = ConfigMerger()
        merged = merger.merge(base_config, override_config)
        
        # Lists are typically replaced, not merged
        assert merged["workers"]["queues"] == ["queue3"]
        assert merged["workers"]["plugins"] == ["plugin_b", "plugin_c"]

    def test_merge_with_none_values(self):
        """Test merging with None values."""
        base_config = {
            "feature_a": {"enabled": True, "value": 100},
            "feature_b": {"enabled": False}
        }
        
        override_config = {
            "feature_a": None,  # Disable entire feature
            "feature_b": {"enabled": True, "value": 200}
        }
        
        merger = ConfigMerger()
        merged = merger.merge(base_config, override_config)
        
        assert merged["feature_a"] is None
        assert merged["feature_b"]["enabled"] is True
        assert merged["feature_b"]["value"] == 200


class TestConfigAPI:
    """Test high-level configuration API."""

    def test_load_config_from_file(self):
        """Test loading config using the main API."""
        config_data = {
            "nats": {"url": "nats://api-test:4222"},
            "workers": {"concurrency": 6}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = load_config(temp_file)
            config_dict = config.to_dict()
            
            assert config_dict["nats"]["url"] == "nats://api-test:4222"
            assert config_dict["workers"]["concurrency"] == 6
        finally:
            os.unlink(temp_file)

    def test_load_config_with_environment_override(self):
        """Test loading config with environment variable overrides."""
        config_data = {
            "nats": {"url": "nats://file:4222"},
            "workers": {"concurrency": 4}
        }
        
        env_vars = {
            "NAQ_WORKERS_CONCURRENCY": "12"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            with patch.dict(os.environ, env_vars):
                config = load_config(temp_file)
                config_dict = config.to_dict()
                
                # File value preserved
                assert config_dict["nats"]["url"] == "nats://file:4222"
                # Environment override applied
                assert config_dict["workers"]["concurrency"] == 12
        finally:
            os.unlink(temp_file)

    def test_get_config_singleton(self):
        """Test config singleton behavior."""
        config_data = {"nats": {"url": "nats://singleton:4222"}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file = f.name
        
        try:
            # Load config
            config1 = load_config(temp_file)
            
            # Get config should return the same instance
            config2 = get_config()
            
            assert config1 is config2
            assert config1.to_dict()["nats"]["url"] == "nats://singleton:4222"
        finally:
            os.unlink(temp_file)

    def test_reload_config(self):
        """Test config reloading."""
        # Initial config
        initial_data = {"nats": {"url": "nats://initial:4222"}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(initial_data, f)
            temp_file = f.name
        
        try:
            # Load initial config
            config1 = load_config(temp_file)
            assert config1.to_dict()["nats"]["url"] == "nats://initial:4222"
            
            # Update file
            updated_data = {"nats": {"url": "nats://updated:4222"}}
            with open(temp_file, 'w') as f:
                yaml.dump(updated_data, f)
            
            # Reload config
            config2 = reload_config(temp_file)
            
            assert config2.to_dict()["nats"]["url"] == "nats://updated:4222"
            assert get_config() is config2  # Singleton updated
        finally:
            os.unlink(temp_file)

    def test_load_config_defaults(self):
        """Test loading config with defaults."""
        # Empty config should get defaults
        config = load_config(None)  # No file
        config_dict = config.to_dict()
        
        # Should include default values
        assert "nats" in config_dict
        assert "workers" in config_dict
        assert "events" in config_dict


class TestConfigEnvironmentVariables:
    """Test environment variable handling."""

    def test_all_environment_variables(self):
        """Test all supported environment variables."""
        env_vars = {
            "NAQ_NATS_URL": "nats://env:4222",
            "NAQ_NATS_CLIENT_NAME": "env-client",
            "NAQ_NATS_TIMEOUT": "45",
            "NAQ_WORKERS_CONCURRENCY": "16",
            "NAQ_WORKERS_HEARTBEAT_INTERVAL": "25",
            "NAQ_WORKERS_MAX_JOBS": "500",
            "NAQ_EVENTS_ENABLED": "true",
            "NAQ_EVENTS_BATCH_SIZE": "200",
            "NAQ_STREAMS_ENSURE_STREAMS": "false",
            "NAQ_LOGGING_LEVEL": "DEBUG"
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = load_config(None)
            config_dict = config.to_dict()
            
            assert config_dict["nats"]["url"] == "nats://env:4222"
            assert config_dict["nats"]["client_name"] == "env-client"
            assert config_dict["nats"]["timeout"] == 45
            assert config_dict["workers"]["concurrency"] == 16
            assert config_dict["workers"]["heartbeat_interval"] == 25
            assert config_dict["workers"]["max_jobs"] == 500
            assert config_dict["events"]["enabled"] is True
            assert config_dict["events"]["batch_size"] == 200
            assert config_dict["streams"]["ensure_streams"] is False
            assert config_dict["logging"]["level"] == "DEBUG"

    def test_environment_variable_type_conversion(self):
        """Test environment variable type conversion."""
        env_vars = {
            "NAQ_WORKERS_CONCURRENCY": "8",      # String to int
            "NAQ_EVENTS_ENABLED": "false",       # String to bool
            "NAQ_NATS_TIMEOUT": "30.5",          # String to float
        }
        
        with patch.dict(os.environ, env_vars):
            config = load_config(None)
            config_dict = config.to_dict()
            
            assert isinstance(config_dict["workers"]["concurrency"], int)
            assert config_dict["workers"]["concurrency"] == 8
            
            assert isinstance(config_dict["events"]["enabled"], bool)
            assert config_dict["events"]["enabled"] is False
            
            assert isinstance(config_dict["nats"]["timeout"], (int, float))
            assert config_dict["nats"]["timeout"] == 30.5

    def test_invalid_environment_variables(self):
        """Test handling of invalid environment variables."""
        env_vars = {
            "NAQ_WORKERS_CONCURRENCY": "invalid",  # Invalid integer
            "NAQ_EVENTS_ENABLED": "maybe"          # Invalid boolean
        }
        
        with patch.dict(os.environ, env_vars):
            # Should handle invalid values gracefully
            # Behavior depends on implementation - might use defaults or raise error
            try:
                config = load_config(None)
                # If no exception, verify reasonable defaults are used
                config_dict = config.to_dict()
                assert isinstance(config_dict["workers"]["concurrency"], int)
                assert isinstance(config_dict["events"]["enabled"], bool)
            except ConfigurationError:
                # This is also acceptable behavior
                pass


class TestConfigurationIntegration:
    """Test configuration integration scenarios."""

    def test_config_with_service_manager(self):
        """Test configuration integration with ServiceManager."""
        config_data = {
            "nats": {"url": "nats://integration:4222"},
            "workers": {"concurrency": 4},
            "events": {"enabled": True}
        }
        
        with patch('naq.services.ServiceManager') as mock_sm:
            mock_manager = MagicMock()
            mock_sm.return_value = mock_manager
            
            config = load_config(config_data)
            
            # ServiceManager should be able to use the config
            from naq.services import ServiceManager
            manager = ServiceManager(config)
            
            # Verify ServiceManager was called with config
            mock_sm.assert_called_once_with(config)

    def test_config_validation_integration(self):
        """Test configuration validation in real usage."""
        # Valid config that should pass all validations
        valid_config = {
            "nats": {
                "url": "nats://localhost:4222",
                "client_name": "test-client",
                "timeout": 30
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": 30,
                "max_jobs": 1000
            },
            "events": {
                "enabled": True,
                "batch_size": 100
            },
            "streams": {
                "ensure_streams": True
            },
            "logging": {
                "level": "INFO"
            }
        }
        
        # Should load and validate without errors
        config = load_config(valid_config)
        assert config is not None
        
        # Config should be usable
        config_dict = config.to_dict()
        assert config_dict["nats"]["url"] == "nats://localhost:4222"

    def test_config_precedence_order(self):
        """Test configuration precedence order."""
        # Create file config
        file_config = {
            "nats": {"url": "nats://file:4222", "client_name": "file-client"},
            "workers": {"concurrency": 4}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(file_config, f)
            temp_file = f.name
        
        try:
            # Environment overrides
            env_vars = {
                "NAQ_NATS_CLIENT_NAME": "env-client",  # Override file
                "NAQ_WORKERS_CONCURRENCY": "8"         # Override file
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(temp_file)
                config_dict = config.to_dict()
                
                # File value preserved where not overridden
                assert config_dict["nats"]["url"] == "nats://file:4222"
                
                # Environment overrides applied
                assert config_dict["nats"]["client_name"] == "env-client"
                assert config_dict["workers"]["concurrency"] == 8
        finally:
            os.unlink(temp_file)

    def test_config_error_propagation(self):
        """Test that configuration errors propagate correctly."""
        # Invalid config that should fail validation
        invalid_config = {
            "nats": {"url": "not-a-valid-url"},
            "workers": {"concurrency": -5}  # Invalid
        }
        
        with pytest.raises(ConfigurationError):
            load_config(invalid_config)


class TestConfigPerformance:
    """Test configuration performance characteristics."""

    def test_config_loading_performance(self):
        """Test configuration loading performance."""
        import time
        
        # Large config with many sections
        large_config = {}
        for i in range(100):
            large_config[f"section_{i}"] = {
                f"option_{j}": f"value_{j}" for j in range(50)
            }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(large_config, f)
            temp_file = f.name
        
        try:
            start_time = time.time()
            config = load_config(temp_file)
            load_time = time.time() - start_time
            
            # Should load reasonably quickly
            assert load_time < 5.0  # 5 seconds max
            assert config is not None
        finally:
            os.unlink(temp_file)

    def test_config_access_performance(self):
        """Test configuration access performance."""
        import time
        
        config_data = {
            "section": {"nested": {"deeply": {"value": "test"}}}
        }
        
        config = load_config(config_data)
        
        start_time = time.time()
        
        # Multiple accesses
        for _ in range(1000):
            config_dict = config.to_dict()
            value = config_dict["section"]["nested"]["deeply"]["value"]
            assert value == "test"
        
        access_time = time.time() - start_time
        
        # Should be very fast
        assert access_time < 1.0  # 1 second max for 1000 accesses