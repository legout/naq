"""Integration tests for environment variable interpolation in configuration."""

import os
import tempfile
import pytest
from unittest.mock import patch
from typing import Any, Dict

import yaml

from naq.config import load_config
from naq.exceptions import ConfigurationError


class TestConfigEnvInterpolation:
    """Integration tests for environment variable interpolation."""

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
            "NATS_SERVER",
            "NATS_PORT",
            "CLIENT_NAME",
            "WORKER_CONCURRENCY",
            "EVENTS_ENABLED",
            "DB_HOST",
            "DB_PORT",
            "DB_NAME",
            "REDIS_URL",
            "LOG_LEVEL"
        ]
        for var in env_vars_to_clean:
            if var in os.environ:
                del os.environ[var]

    def test_basic_env_interpolation_with_defaults(self) -> None:
        """Test basic environment variable interpolation with default values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
              heartbeat_interval: ${HEARTBEAT_INTERVAL:30.0}
            events:
              enabled: ${EVENTS_ENABLED:false}
              batch_size: ${BATCH_SIZE:100}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Load without setting environment variables
            config = load_config(config_path, validate=False)
            
            # Should use default values
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "naq-client"
            assert config.workers.concurrency == 1
            assert config.workers.heartbeat_interval == 30.0
            assert config.events.enabled is False
            assert config.events.batch_size == 100

    def test_env_interpolation_with_set_values(self) -> None:
        """Test environment variable interpolation with set environment values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
            events:
              enabled: ${EVENTS_ENABLED:false}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables
            env_vars = {
                "NATS_SERVER": "nats://prod:4222",
                "CLIENT_NAME": "prod-client",
                "WORKER_CONCURRENCY": "8",
                "EVENTS_ENABLED": "true"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should use environment variable values
                assert config.nats.servers == ["nats://prod:4222"]
                assert config.nats.client_name == "prod-client"
                assert config.workers.concurrency == 8
                assert config.events.enabled is True

    def test_env_interpolation_without_defaults(self) -> None:
        """Test environment variable interpolation without default values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER}"]
              client_name: "${CLIENT_NAME}"
            workers:
              concurrency: ${WORKER_CONCURRENCY}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables
            env_vars = {
                "NATS_SERVER": "nats://prod:4222",
                "CLIENT_NAME": "prod-client",
                "WORKER_CONCURRENCY": "8"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should use environment variable values
                assert config.nats.servers == ["nats://prod:4222"]
                assert config.nats.client_name == "prod-client"
                assert config.workers.concurrency == 8

    def test_env_interpolation_unset_without_defaults(self) -> None:
        """Test environment variable interpolation with unset variables and no defaults."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${UNSET_VAR}"]
              client_name: "${CLIENT_NAME:default-client}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Ensure UNSET_VAR is not set
            with patch.dict(os.environ, {}, clear=True):
                config = load_config(config_path, validate=False)
                
                # Should replace unset var with empty string
                assert config.nats.servers == [""]
                # Should use default for CLIENT_NAME
                assert config.nats.client_name == "default-client"

    def test_env_interpolation_with_complex_values(self) -> None:
        """Test environment variable interpolation with complex values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER}:${NATS_PORT}"]
              client_name: "${CLIENT_NAME}"
            workers:
              pools:
                default:
                  size: ${POOL_SIZE:5}
                high_priority:
                  size: ${HIGH_PRIORITY_POOL_SIZE:2}
            events:
              filters: ${EVENT_FILTERS:[]}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables
            env_vars = {
                "NATS_SERVER": "nats://prod.example.com",
                "NATS_PORT": "4222",
                "CLIENT_NAME": "prod-client",
                "POOL_SIZE": "10",
                "HIGH_PRIORITY_POOL_SIZE": "3",
                "EVENT_FILTERS": '["job.started", "job.completed"]'
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should interpolate complex values
                assert config.nats.servers == ["nats://prod.example.com:4222"]
                assert config.nats.client_name == "prod-client"
                assert config.workers.pools["default"]["size"] == 10
                assert config.workers.pools["high_priority"]["size"] == 3
                # Note: EVENT_FILTERS would be a string that needs parsing in real implementation
                # This test shows the interpolation works, but actual parsing would need additional logic

    def test_env_interpolation_with_special_characters(self) -> None:
        """Test environment variable interpolation with special characters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_URL}"]
              auth:
                user: "${NATS_USER}"
                password: "${NATS_PASSWORD}"
            logging:
              file_path: "${LOG_PATH:/var/log/naq/app.log}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables with special characters
            env_vars = {
                "NATS_URL": "nats://user:pass@host:4222",
                "NATS_USER": "admin@example.com",
                "NATS_PASSWORD": "p@ssw0rd!@#$%",
                "LOG_PATH": "/tmp/naq-test.log"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should handle special characters correctly
                assert config.nats.servers == ["nats://user:pass@host:4222"]
                assert config.nats.auth["user"] == "admin@example.com"
                assert config.nats.auth["password"] == "p@ssw0rd!@#$%"
                assert config.logging["file_path"] == "/tmp/naq-test.log"

    def test_env_interpolation_multiple_occurrences(self) -> None:
        """Test environment variable interpolation with multiple occurrences of the same variable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: 
                - "${NATS_SERVER}:${NATS_PORT}"
                - "${NATS_SERVER}:${NATS_PORT}"
              client_name: "${CLIENT_NAME}-${CLIENT_NAME}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables
            env_vars = {
                "NATS_SERVER": "nats://cluster",
                "NATS_PORT": "4222",
                "CLIENT_NAME": "client"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should replace all occurrences
                assert config.nats.servers == [
                    "nats://cluster:4222",
                    "nats://cluster:4222"
                ]
                assert config.nats.client_name == "client-client"

    def test_env_interpolation_nested_variables(self) -> None:
        """Test environment variable interpolation with nested variable references."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            database:
              host: "${DB_HOST}"
              port: ${DB_PORT}
              name: "${DB_NAME}"
              url: "${DB_HOST}:${DB_PORT}/${DB_NAME}"
            redis:
              url: "${REDIS_URL}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables
            env_vars = {
                "DB_HOST": "localhost",
                "DB_PORT": "5432",
                "DB_NAME": "naq_prod",
                "REDIS_URL": "redis://localhost:6379/0"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should interpolate nested references correctly
                assert config.database["host"] == "localhost"
                assert config.database["port"] == 5432
                assert config.database["name"] == "naq_prod"
                assert config.database["url"] == "localhost:5432/naq_prod"
                assert config.redis["url"] == "redis://localhost:6379/0"

    def test_env_interpolation_with_validation(self) -> None:
        """Test environment variable interpolation works with validation enabled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
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
            
            # Set environment variables
            env_vars = {
                "NATS_SERVER": "nats://prod:4222",
                "CLIENT_NAME": "prod-client",
                "WORKER_CONCURRENCY": "4",
                "EVENTS_ENABLED": "true",
                "BATCH_SIZE": "200"
            }
            
            with patch.dict(os.environ, env_vars):
                # Should work with validation enabled
                config = load_config(config_path, validate=True)
                
                # Should use environment variable values
                assert config.nats.servers == ["nats://prod:4222"]
                assert config.nats.client_name == "prod-client"
                assert config.workers.concurrency == 4
                assert config.events.enabled is True
                assert config.events.batch_size == 200
                
                # Should use defaults for unset variables
                assert config.nats.max_reconnect_attempts == 5
                assert config.workers.heartbeat_interval == 30.0
                assert config.events.flush_interval == 5.0

    def test_env_interpolation_error_cases(self) -> None:
        """Test environment variable interpolation error cases."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Test with invalid numeric values
            env_vars = {
                "WORKER_CONCURRENCY": "not_a_number"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should handle invalid conversion gracefully
                # In the actual implementation, this might be a string or cause validation to fail
                assert isinstance(config.workers.concurrency, str)

    def test_env_interpolation_with_empty_values(self) -> None:
        """Test environment variable interpolation with empty values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            events:
              enabled: ${EVENTS_ENABLED:false}
              stream: "${STREAM:naq_events}"
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set empty environment variables
            env_vars = {
                "NATS_SERVER": "",
                "CLIENT_NAME": "",
                "EVENTS_ENABLED": "",
                "STREAM": ""
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Should handle empty values
                assert config.nats.servers == [""]
                assert config.nats.client_name == ""
                # Empty string for boolean should be treated as string, not bool
                assert isinstance(config.events.enabled, str)
                assert config.events.stream == ""

    def test_env_interpolation_priority_with_direct_env_vars(self) -> None:
        """Test that environment variable interpolation works alongside direct environment variable overrides."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            config_content = """
            nats:
              servers: ["${NATS_SERVER:nats://localhost:4222}"]
              client_name: "${CLIENT_NAME:naq-client}"
            workers:
              concurrency: ${WORKER_CONCURRENCY:1}
            """
            
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Set environment variables for both interpolation and direct override
            env_vars = {
                # For interpolation
                "NATS_SERVER": "nats://interpolated:4222",
                "CLIENT_NAME": "interpolated-client",
                "WORKER_CONCURRENCY": "4",
                # For direct override (higher priority)
                "NAQ_WORKERS__CONCURRENCY": "8"
            }
            
            with patch.dict(os.environ, env_vars):
                config = load_config(config_path, validate=False)
                
                # Interpolated values should be used
                assert config.nats.servers == ["nats://interpolated:4222"]
                assert config.nats.client_name == "interpolated-client"
                
                # Direct environment override should take precedence over interpolated value
                assert config.workers.concurrency == 8