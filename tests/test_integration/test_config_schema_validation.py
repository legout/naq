"""Integration tests for schema validation across the configuration system."""

import os
import tempfile
import pytest
from unittest.mock import patch
from typing import Any, Dict

import yaml

from naq.config import load_config, reload_config, get_config
from naq.exceptions import ConfigurationError


class TestConfigSchemaValidation:
    """Integration tests for schema validation across the configuration system."""

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

    def test_valid_config_passes_validation(self) -> None:
        """Test that a valid configuration passes validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "valid_config.yaml")
            valid_config = {
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
                yaml.dump(valid_config, f)
            
            # Should not raise any exception
            config = load_config(config_path, validate=True)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.workers.concurrency == 4
            assert config.events.enabled is True

    def test_invalid_config_fails_validation(self) -> None:
        """Test that an invalid configuration fails validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "invalid_config.yaml")
            # Invalid config - empty servers list
            invalid_config = {
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
                yaml.dump(invalid_config, f)
            
            # Should raise ConfigurationError
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(config_path, validate=True)

    def test_validation_with_environment_variables(self) -> None:
        """Test validation works correctly with environment variable overrides."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            # Config with invalid concurrency (should be >= 1)
            base_config = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0
                },
                "workers": {
                    "concurrency": 0,  # Invalid - should be >= 1
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
                yaml.dump(base_config, f)
            
            # Set environment variable to fix the invalid value
            env_vars = {
                "NAQ_WORKERS__CONCURRENCY": "4"  # Valid value
            }
            
            with patch.dict(os.environ, env_vars):
                # Should pass validation after environment override
                config = load_config(config_path, validate=True)
                assert config.workers.concurrency == 4

    def test_validation_with_env_interpolation(self) -> None:
        """Test validation works with environment variable interpolation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
              max_reconnect_attempts: ${MAX_RECONNECT:5}
              reconnect_time_wait: ${RECONNECT_WAIT:2.0}
              connection_timeout: ${CONN_TIMEOUT:5.0}
              drain_timeout: ${DRAIN_TIMEOUT:30.0}
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
              heartbeat_interval: ${HEARTBEAT_INTERVAL:30.0}
              ttl: ${TTL:60.0}
              max_job_duration: ${MAX_JOB_DURATION:3600.0}
              shutdown_timeout: ${SHUTDOWN_TIMEOUT:10.0}
            events:
              enabled: ${EVENTS_ENABLED:false}
              batch_size: ${BATCH_SIZE:100}
              flush_interval: ${FLUSH_INTERVAL:5.0}
              max_buffer_size: ${MAX_BUFFER_SIZE:1000}
              stream: "${STREAM:naq_events}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set valid environment variables
            env_vars = {
                "NATS_SERVER": "nats://prod:4222",
                "CLIENT_NAME": "prod-client",
                "WORKER_CONCURRENCY": "4",
                "EVENTS_ENABLED": "true",
                "BATCH_SIZE": "200"
            }
            
            with patch.dict(os.environ, env_vars):
                # Should pass validation with interpolated values
                config = load_config(config_path, validate=True)
                assert config.nats.servers == ["nats://prod:4222"]
                assert config.workers.concurrency == 4
                assert config.events.enabled is True

    def test_validation_fails_with_invalid_env_interpolation(self) -> None:
        """Test validation fails with invalid environment variable interpolation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
            events:
              enabled: ${EVENTS_ENABLED:false}
              batch_size: ${BATCH_SIZE:100}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set invalid environment variable
            env_vars = {
                "BATCH_SIZE": "0"  # Invalid - should be >= 1
            }
            
            with patch.dict(os.environ, env_vars):
                # Should fail validation
                with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                    load_config(config_path, validate=True)

    def test_validation_with_optional_sections(self) -> None:
        """Test validation works correctly with optional sections."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            config_with_optional = {
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
                },
                "queues": {
                    "default": {"max_size": 1000}
                },
                "scheduler": {
                    "enabled": True
                },
                "results": {
                    "ttl": 86400
                },
                "serialization": {
                    "default": "pickle"
                },
                "logging": {
                    "level": "INFO"
                }
            }
            
            with open(config_path, "w") as f:
                yaml.dump(config_with_optional, f)
            
            # Should pass validation with optional sections
            config = load_config(config_path, validate=True)
            assert config.queues == {"default": {"max_size": 1000}}
            assert config.scheduler == {"enabled": True}
            assert config.results == {"ttl": 86400}
            assert config.serialization == {"default": "pickle"}
            assert config.logging == {"level": "INFO"}

    def test_validation_with_missing_required_section(self) -> None:
        """Test validation fails when required section is missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            # Missing required "events" section
            incomplete_config = {
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
                }
                # Missing "events" section
            }
            
            with open(config_path, "w") as f:
                yaml.dump(incomplete_config, f)
            
            # Should fail validation
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(config_path, validate=True)

    def test_validation_with_reload_config(self) -> None:
        """Test validation works with reload_config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create initial valid config
            config1_path = os.path.join(temp_dir, "config1.yaml")
            valid_config = {
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
            
            with open(config1_path, "w") as f:
                yaml.dump(valid_config, f)
            
            # Load initial config
            config1 = load_config(config1_path, validate=True)
            assert config1.nats.servers == ["nats://localhost:4222"]
            
            # Create invalid config for reload
            config2_path = os.path.join(temp_dir, "config2.yaml")
            invalid_config = {
                "nats": {
                    "servers": [],  # Invalid - empty list
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
            
            with open(config2_path, "w") as f:
                yaml.dump(invalid_config, f)
            
            # Reload should fail validation
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                reload_config(config2_path, validate=True)
            
            # Original config should remain unchanged
            config_current = get_config()
            assert config_current.nats.servers == ["nats://localhost:4222"]

    def test_validation_with_get_config_lazy_loading(self) -> None:
        """Test validation works with get_config lazy loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            valid_config = {
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
                yaml.dump(valid_config, f)
            
            # Reset global instance
            import naq.config
            naq.config._config_instance = None
            
            # Mock default config paths
            with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [config_path]):
                # get_config should trigger lazy loading with validation
                config = get_config()
                assert config.nats.servers == ["nats://localhost:4222"]
                assert config.workers.concurrency == 4

    def test_validation_without_validation_flag(self) -> None:
        """Test that validation can be disabled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            # Invalid config - empty servers list
            invalid_config = {
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
                yaml.dump(invalid_config, f)
            
            # Should not raise exception when validation is disabled
            config = load_config(config_path, validate=False)
            assert config.nats.servers == []  # Invalid value passes through

    def test_validation_with_additional_properties(self) -> None:
        """Test validation fails with additional properties not in schema."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            # Config with additional properties
            config_with_extra = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": 5,
                    "reconnect_time_wait": 2.0,
                    "connection_timeout": 5.0,
                    "drain_timeout": 30.0,
                    "invalid_property": "should_fail"  # Not in schema
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
                yaml.dump(config_with_extra, f)
            
            # Should fail validation
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(config_path, validate=True)

    def test_validation_with_wrong_data_types(self) -> None:
        """Test validation fails with wrong data types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.yaml")
            # Config with wrong data types
            config_with_wrong_types = {
                "nats": {
                    "servers": ["nats://localhost:4222"],
                    "client_name": "test-client",
                    "max_reconnect_attempts": "5",  # Should be int
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
                yaml.dump(config_with_wrong_types, f)
            
            # Should fail validation
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(config_path, validate=True)