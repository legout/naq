"""Unit tests for configuration API (__init__.py)."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from typing import Any, Dict, Iterator

import yaml

from naq.config import (
    load_config,
    get_config,
    reload_config,
    temp_config,
    NAQConfig,
    NatsConfig,
    WorkerConfig,
    EventsConfig,
)
from naq.exceptions import ConfigurationError


class TestConfigAPI:
    """Test cases for configuration API functions."""

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
        if "NAQ_ENVIRONMENT" in os.environ:
            del os.environ["NAQ_ENVIRONMENT"]

    def test_load_config_without_validation(self) -> None:
        """Test loading configuration without validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            config = load_config(config_path, validate=False)
            
            assert isinstance(config, NAQConfig)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "test-client"
            assert config.workers.concurrency == 4
            assert config.events.enabled is True

    def test_load_config_with_validation(self) -> None:
        """Test loading configuration with validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            config = load_config(config_path, validate=True)
            
            assert isinstance(config, NAQConfig)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "test-client"
            assert config.workers.concurrency == 4
            assert config.events.enabled is True

    def test_load_config_with_invalid_config_validation_fails(self) -> None:
        """Test that loading invalid configuration with validation fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "invalid_config.yaml")
            # Invalid config - empty servers list
            config_content = {
                "nats": {
                    "servers": [],  # Invalid - should not be empty
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(config_path, validate=True)

    def test_load_config_without_path_uses_defaults(self) -> None:
        """Test loading configuration without path uses default locations."""
        # Mock the default config paths to point to our test config
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "naq.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://default:4222"],
                    "client_name": "default-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                config = load_config(validate=False)
                
                assert isinstance(config, NAQConfig)
                assert config.nats.servers == ["nats://default:4222"]
                assert config.nats.client_name == "default-client"

    def test_load_config_sets_global_instance(self) -> None:
        """Test that load_config sets the global configuration instance."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            # Load config
            config1 = load_config(config_path, validate=False)
            
            # Get global instance
            import naq.config
            config2 = naq.config._config_instance
            
            # Should be the same instance
            assert config1 is config2

    def test_get_config_when_loaded(self) -> None:
        """Test get_config when configuration is already loaded."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            # Load config first
            config1 = load_config(config_path, validate=False)
            
            # Get config
            config2 = get_config()
            
            # Should be the same instance
            assert config1 is config2

    def test_get_config_when_not_loaded(self) -> None:
        """Test get_config when configuration is not loaded yet."""
        # Mock default config paths to avoid file system dependency
        with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", []):
            config = get_config()
            
            # Should load default configuration
            assert isinstance(config, NAQConfig)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "naq-client"

    def test_reload_config(self) -> None:
        """Test reloading configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create first config
            config1_path = os.path.join(temp_dir, "config1.yaml")
            config1_content = {
                "nats": {
                    "servers": ["nats://server1:4222"],
                    "client_name": "client1",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config1_path, "w") as f:
                yaml.dump(config1_content, f)
            
            # Load first config
            config1 = load_config(config1_path, validate=False)
            assert config1.nats.servers == ["nats://server1:4222"]
            assert config1.nats.client_name == "client1"
            assert config1.workers.concurrency == 1
            
            # Create second config
            config2_path = os.path.join(temp_dir, "config2.yaml")
            config2_content = {
                "nats": {
                    "servers": ["nats://server2:4222"],
                    "client_name": "client2",
                    "max_reconnect_attempts": 10,
                    "reconnect_time_wait": 5.0,
                    "connection_timeout": 10.0,
                    "drain_timeout": 60.0
                },
                "workers": {
                    "concurrency": 8,
                    "heartbeat_interval": 60.0,
                    "ttl": 120.0,
                    "max_job_duration": 7200.0,
                    "shutdown_timeout": 20.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 200,
                    "flush_interval": 10.0,
                    "max_buffer_size": 2000,
                    "stream": "naq_events_prod"
                }
            }
            
            with open(config2_path, "w") as f:
                yaml.dump(config2_content, f)
            
            # Reload with second config
            config2 = reload_config(config2_path, validate=False)
            
            # Should have new values
            assert config2.nats.servers == ["nats://server2:4222"]
            assert config2.nats.client_name == "client2"
            assert config2.workers.concurrency == 8
            assert config2.events.enabled is True
            
            # Global instance should be updated
            config3 = get_config()
            assert config2 is config3

    def test_temp_config_with_custom_data(self) -> None:
        """Test temp_config context manager with custom data."""
        # Load initial config
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://original:4222"],
                    "client_name": "original-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            # Load original config
            original_config = load_config(config_path, validate=False)
            assert original_config.nats.servers == ["nats://original:4222"]
            assert original_config.nats.client_name == "original-client"
            
            # Use temp config
            temp_config_data = {
                "nats": {
                    "servers": ["nats://temp:4222"],
                    "client_name": "temp-client",
                    "max_reconnect_attempts": 10,
                    "reconnect_time_wait": 5.0,
                    "connection_timeout": 10.0,
                    "drain_timeout": 60.0
                },
                "workers": {
                    "concurrency": 8,
                    "heartbeat_interval": 60.0,
                    "ttl": 120.0,
                    "max_job_duration": 7200.0,
                    "shutdown_timeout": 20.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 200,
                    "flush_interval": 10.0,
                    "max_buffer_size": 2000,
                    "stream": "naq_events_temp"
                }
            }
            
            with temp_config(temp_config_data) as temp_config_instance:
                # Should have temp values
                assert temp_config_instance.nats.servers == ["nats://temp:4222"]
                assert temp_config_instance.nats.client_name == "temp-client"
                assert temp_config_instance.workers.concurrency == 8
                assert temp_config_instance.events.enabled is True
                
                # get_config should return temp config
                current_config = get_config()
                assert current_config is temp_config_instance
            
            # After context, should be back to original
            restored_config = get_config()
            assert restored_config is original_config
            assert restored_config.nats.servers == ["nats://original:4222"]
            assert restored_config.nats.client_name == "original-client"

    def test_temp_config_without_custom_data(self) -> None:
        """Test temp_config context manager without custom data."""
        # Load initial config
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://original:4222"],
                    "client_name": "original-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            # Load original config
            original_config = load_config(config_path, validate=False)
            assert original_config.nats.servers == ["nats://original:4222"]
            assert original_config.nats.client_name == "original-client"
            
            # Use temp config without custom data
            with temp_config() as temp_config_instance:
                # Should have default temp values
                assert temp_config_instance.nats.servers == ["nats://localhost:4222"]
                assert temp_config_instance.nats.client_name == "naq-test"
                assert temp_config_instance.workers.concurrency == 1
                assert temp_config_instance.events.enabled is False
                
                # get_config should return temp config
                current_config = get_config()
                assert current_config is temp_config_instance
            
            # After context, should be back to original
            restored_config = get_config()
            assert restored_config is original_config
            assert restored_config.nats.servers == ["nats://original:4222"]
            assert restored_config.nats.client_name == "original-client"

    def test_temp_config_with_exception(self) -> None:
        """Test temp_config restores original config even when exception occurs."""
        # Load initial config
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://original:4222"],
                    "client_name": "original-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 1,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": False,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            # Load original config
            original_config = load_config(config_path, validate=False)
            
            # Use temp config and raise exception
            temp_config_data = {
                "nats": {
                    "servers": ["nats://temp:4222"],
                    "client_name": "temp-client",
                    "max_reconnect_attempts": 10,
                    "reconnect_time_wait": 5.0,
                    "connection_timeout": 10.0,
                    "drain_timeout": 60.0
                },
                "workers": {
                    "concurrency": 8,
                    "heartbeat_interval": 60.0,
                    "ttl": 120.0,
                    "max_job_duration": 7200.0,
                    "shutdown_timeout": 20.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 200,
                    "flush_interval": 10.0,
                    "max_buffer_size": 2000,
                    "stream": "naq_events_temp"
                }
            }
            
            try:
                with temp_config(temp_config_data) as temp_config_instance:
                    assert temp_config_instance.nats.servers == ["nats://temp:4222"]
                    raise ValueError("Test exception")
            except ValueError:
                pass  # Expected
            
            # Should be back to original
            restored_config = get_config()
            assert restored_config is original_config
            assert restored_config.nats.servers == ["nats://original:4222"]

    def test_environment_property_access(self) -> None:
        """Test that environment property is accessible through loaded config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 4,
                    "heartbeat_interval": 30.0,
                    "ttl": 60.0,
                    "max_job_duration": 3600.0,
                    "shutdown_timeout": 10.0
                },
                "events": {
                    "enabled": True,
                    "batch_size": 100,
                    "flush_interval": 5.0,
                    "max_buffer_size": 1000,
                    "stream": "naq_events"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_content, f)
            
            config = load_config(config_path, validate=False)
            
            # Test without environment variable set
            if "NAQ_ENVIRONMENT" in os.environ:
                del os.environ["NAQ_ENVIRONMENT"]
            assert config.environment is None
            
            # Test with environment variable set
            os.environ["NAQ_ENVIRONMENT"] = "production"
            assert config.environment == "production"
            
            # Clean up
            del os.environ["NAQ_ENVIRONMENT"]