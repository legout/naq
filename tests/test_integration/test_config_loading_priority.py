"""Integration tests for configuration loading priority."""

import os
import tempfile
import pytest
from unittest.mock import patch
from typing import Any, Dict

import yaml

from naq.config import load_config, reload_config, get_config
from naq.exceptions import ConfigurationError


class TestConfigLoadingPriority:
    """Integration tests for configuration loading priority system."""

    def setup_method(self) -> None:
        """Reset global config instance before each test."""
        # Reset the global config instance
        import naq.config
        naq.config._config_instance = None

    def teardown_method(self) -> None:
        """Clean up after each test."""
        # Reset the global config instance
        import naq.config
        naq.config._config_instance = None
        
        # Clean up any environment variables we might have set
        env_vars_to_clean = [
            "NAQ_NATS__SERVERS",
            "NAQ_NATS__CLIENT_NAME",
            "NAQ_WORKERS__CONCURRENCY",
            "NAQ_WORKERS__HEARTBEAT_INTERVAL",
            "NAQ_EVENTS__ENABLED",
            "NAQ_EVENTS__BATCH_SIZE"
        ]
        for var in env_vars_to_clean:
            if var in os.environ:
                del os.environ[var]

    def test_priority_order_defaults_only(self) -> None:
        """Test configuration loading with only defaults."""
        # Mock default config paths to return no files
        with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", []):
            config = load_config(validate=False)
            
            # Should have default values
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "naq-client"
            assert config.workers.concurrency == 1
            assert config.events.enabled is False

    def test_priority_order_defaults_and_yaml(self) -> None:
        """Test configuration loading with defaults and YAML file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a YAML config file
            config_path = os.path.join(temp_dir, "naq.yaml")
            yaml_config = {
                "nats": {
                    "servers": ["nats://yaml:4222"],
                    "client_name": "yaml-client"
                },
                "workers": {
                    "concurrency": 4
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(yaml_config, f)
            
            # Mock default config paths to include our file
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                config = load_config(validate=False)
                
                # YAML should override defaults
                assert config.nats.servers == ["nats://yaml:4222"]
                assert config.nats.client_name == "yaml-client"
                assert config.workers.concurrency == 4
                
                # Default values should remain for unspecified fields
                assert config.nats.max_reconnect_attempts == 5
                assert config.workers.heartbeat_interval == 30.0
                assert config.events.enabled is False

    def test_priority_order_defaults_yaml_and_env(self) -> None:
        """Test configuration loading with defaults, YAML, and environment variables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a YAML config file
            config_path = os.path.join(temp_dir, "naq.yaml")
            yaml_config = {
                "nats": {
                    "servers": ["nats://yaml:4222"],
                    "client_name": "yaml-client"
                },
                "workers": {
                    "concurrency": 4
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(yaml_config, f)
            
            # Set environment variables
            env_vars = {
                "NAQ_NATS__CLIENT_NAME": "env-client",
                "NAQ_WORKERS__CONCURRENCY": "8",
                "NAQ_EVENTS__ENABLED": "true"
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                    config = load_config(validate=False)
                    
                    # Environment variables should override YAML and defaults
                    assert config.nats.servers == ["nats://yaml:4222"]  # From YAML
                    assert config.nats.client_name == "env-client"  # From env (overrides YAML)
                    assert config.workers.concurrency == 8  # From env (overrides YAML)
                    assert config.events.enabled is True  # From env (overrides default)
                    
                    # Default values should remain for unspecified fields
                    assert config.nats.max_reconnect_attempts == 5
                    assert config.workers.heartbeat_interval == 30.0

    def test_priority_order_explicit_path_highest_priority(self) -> None:
        """Test that explicit config path has highest priority."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create default YAML config file
            default_config_path = os.path.join(temp_dir, "naq.yaml")
            default_yaml_config = {
                "nats": {
                    "servers": ["nats://default:4222"],
                    "client_name": "default-client"
                },
                "workers": {
                    "concurrency": 1
                }
            }
            
            with open(default_config_path, "w") as f:
                yaml.dump(default_yaml_config, f)
            
            # Create explicit YAML config file
            explicit_config_path = os.path.join(temp_dir, "explicit.yaml")
            explicit_yaml_config = {
                "nats": {
                    "servers": ["nats://explicit:4222"],
                    "client_name": "explicit-client"
                },
                "workers": {
                    "concurrency": 4
                }
            }
            
            with open(explicit_config_path, "w") as f:
                yaml.dump(explicit_yaml_config, f)
            
            # Set environment variables
            env_vars = {
                "NAQ_WORKERS__CONCURRENCY": "8"
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [default_config_path]):
                    config = load_config(explicit_config_path, validate=False)
                    
                    # Explicit path should have highest priority
                    assert config.nats.servers == ["nats://explicit:4222"]
                    assert config.nats.client_name == "explicit-client"
                    assert config.workers.concurrency == 4  # From explicit (not env)
                    
                    # Environment should still override defaults
                    # but not explicit config
                    assert config.nats.max_reconnect_attempts == 5  # Default

    def test_priority_order_with_multiple_default_files(self) -> None:
        """Test that only the first found default config file is used."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple YAML config files
            config1_path = os.path.join(temp_dir, "naq.yaml")
            config1_content = {
                "nats": {
                    "servers": ["nats://first:4222"],
                    "client_name": "first-client"
                }
            }
            
            config2_path = os.path.join(temp_dir, "config", "naq.yaml")
            os.makedirs(os.path.join(temp_dir, "config"), exist_ok=True)
            config2_content = {
                "nats": {
                    "servers": ["nats://second:4222"],
                    "client_name": "second-client"
                }
            }
            
            with open(config1_path, "w") as f:
                yaml.dump(config1_content, f)
            with open(config2_path, "w") as f:
                yaml.dump(config2_content, f)
            
            # Mock default config paths with config1 first
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config1_path, config2_path]):
                config = load_config(validate=False)
                
                # Should use first config (config1)
                assert config.nats.servers == ["nats://first:4222"]
                assert config.nats.client_name == "first-client"

    def test_priority_order_with_nested_merging(self) -> None:
        """Test priority order with nested configuration merging."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create YAML config with nested structure
            config_path = os.path.join(temp_dir, "naq.yaml")
            yaml_config = {
                "nats": {
                    "servers": ["nats://yaml:4222"],
                    "client_name": "yaml-client",
                    "auth": {
                        "user": "yaml-user",
                        "password": "yaml-pass"
                    }
                },
                "workers": {
                    "concurrency": 4,
                    "pools": {
                        "default": {"size": 5},
                        "high_priority": {"size": 2}
                    }
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(yaml_config, f)
            
            # Set environment variables that override nested values
            env_vars = {
                "NAQ_NATS__AUTH__USER": "env-user",
                "NAQ_WORKERS__POOLS__DEFAULT__SIZE": "10"
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                    config = load_config(validate=False)
                    
                    # Environment should override nested YAML values
                    assert config.nats.servers == ["nats://yaml:4222"]  # From YAML
                    assert config.nats.client_name == "yaml-client"  # From YAML
                    assert config.nats.auth["user"] == "env-user"  # From env
                    assert config.nats.auth["password"] == "yaml-pass"  # From YAML
                    assert config.workers.pools["default"]["size"] == 10  # From env
                    assert config.workers.pools["high_priority"]["size"] == 2  # From YAML

    def test_priority_order_with_type_conversion(self) -> None:
        """Test priority order with proper type conversion from environment variables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create YAML config
            config_path = os.path.join(temp_dir, "naq.yaml")
            yaml_config = {
                "nats": {
                    "servers": ["nats://yaml:4222"],
                    "client_name": "yaml-client",
                    "max_reconnect_attempts": 5
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(yaml_config, f)
            
            # Set environment variables with various types
            env_vars = {
                "NAQ_NATS__MAX_RECONNECT_ATTEMPTS": "10",  # int
                "NAQ_WORKERS__HEARTBEAT_INTERVAL": "60.5",  # float
                "NAQ_EVENTS__ENABLED": "true",  # bool
                "NAQ_EVENTS__BATCH_SIZE": "200"  # int
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                    config = load_config(validate=False)
                    
                    # Environment variables should be properly converted
                    assert config.nats.max_reconnect_attempts == 10  # int
                    assert config.workers.heartbeat_interval == 60.5  # float
                    assert config.events.enabled is True  # bool
                    assert config.events.batch_size == 200  # int
                    
                    # YAML values should remain for non-overridden fields
                    assert config.nats.servers == ["nats://yaml:4222"]
                    assert config.workers.concurrency == 4

    def test_priority_order_with_reload_config(self) -> None:
        """Test that reload_config respects priority order."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create initial config
            config1_path = os.path.join(temp_dir, "config1.yaml")
            config1_content = {
                "nats": {
                    "servers": ["nats://initial:4222"],
                    "client_name": "initial-client"
                },
                "workers": {
                    "concurrency": 1
                }
            }
            
            with open(config1_path, "w") as f:
                yaml.dump(config1_content, f)
            
            # Load initial config
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config1_path]):
                config1 = load_config(validate=False)
                assert config1.nats.servers == ["nats://initial:4222"]
                assert config1.workers.concurrency == 1
            
            # Create new config
            config2_path = os.path.join(temp_dir, "config2.yaml")
            config2_content = {
                "nats": {
                    "servers": ["nats://reloaded:4222"],
                    "client_name": "reloaded-client"
                },
                "workers": {
                    "concurrency": 8
                }
            }
            
            with open(config2_path, "w") as f:
                yaml.dump(config2_content, f)
            
            # Set environment variables
            env_vars = {
                "NAQ_WORKERS__CONCURRENCY": "16"
            }
            
            with patch.dict(os.environ, env_vars):
                # Reload with new config
                config2 = reload_config(config2_path, validate=False)
                
                # Should respect priority: explicit > env > defaults
                assert config2.nats.servers == ["nats://reloaded:4222"]  # From explicit
                assert config2.nats.client_name == "reloaded-client"  # From explicit
                assert config2.workers.concurrency == 16  # From env (overrides explicit)

    def test_priority_order_with_get_config_lazy_loading(self) -> None:
        """Test that get_config lazy loading respects priority order."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config file
            config_path = os.path.join(temp_dir, "naq.yaml")
            yaml_config = {
                "nats": {
                    "servers": ["nats://lazy:4222"],
                    "client_name": "lazy-client"
                },
                "workers": {
                    "concurrency": 4
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(yaml_config, f)
            
            # Set environment variables
            env_vars = {
                "NAQ_WORKERS__CONCURRENCY": "8"
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                    # Reset global instance
                    import naq.config
                    naq.config._config_instance = None
                    
                    # Call get_config (should trigger lazy loading)
                    config = get_config()
                    
                    # Should respect priority order
                    assert config.nats.servers == ["nats://lazy:4222"]  # From YAML
                    assert config.nats.client_name == "lazy-client"  # From YAML
                    assert config.workers.concurrency == 8  # From env (overrides YAML)

    def test_priority_order_error_handling(self) -> None:
        """Test error handling in priority order."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create invalid YAML config
            config_path = os.path.join(temp_dir, "invalid.yaml")
            with open(config_path, "w") as f:
                f.write("invalid: yaml: [")
            
            # Should fall back to defaults if YAML is invalid
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                config = load_config(validate=False)
                
                # Should have default values
                assert config.nats.servers == ["nats://localhost:4222"]
                assert config.nats.client_name == "naq-client"
                assert config.workers.concurrency == 1