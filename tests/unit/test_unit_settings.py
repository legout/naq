"""
Tests for the settings module.
"""

import os
import pytest
from unittest.mock import patch

from naq.settings import (
    NATSConnectionConfig,
    Config,
    get_global_config,
    set_global_config,
    reset_global_config,
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
)


class TestNATSConnectionConfig:
    """Test cases for NATSConnectionConfig."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = NATSConnectionConfig()
        
        assert config.servers == [DEFAULT_NATS_URL]
        assert config.max_reconnect_attempts == 5
        assert config.reconnect_time_wait == 2.0
        assert config.connection_timeout == 10.0
        assert config.ping_interval == 30.0
        assert config.max_outstanding_pings == 3
        assert config.prefer_thread_local is False
        assert config.name == "naq_client"
        assert config.no_randomize is False
        assert config.tls is None
        assert config.user is None
        assert config.password is None
        assert config.token is None
        assert config.nkey is None
        assert config.credentials is None

    def test_environment_variables(self) -> None:
        """Test that environment variables are respected."""
        with patch.dict(os.environ, {
            "NAQ_MAX_RECONNECT_ATTEMPTS": "10",
            "NAQ_RECONNECT_TIME_WAIT": "5.5",
            "NAQ_CONNECTION_TIMEOUT": "20.0",
            "NAQ_PING_INTERVAL": "60.0",
            "NAQ_MAX_OUTSTANDING_PINGS": "5",
            "NAQ_PREFER_THREAD_LOCAL": "true",
            "NAQ_CLIENT_NAME": "test_client",
            "NAQ_NO_RANDOMIZE": "true",
            "NAQ_USER": "test_user",
            "NAQ_PASSWORD": "test_password",
            "NAQ_TOKEN": "test_token",
            "NAQ_NKEY": "test_nkey",
            "NAQ_CREDENTIALS": "/path/to/creds",
        }):
            config = NATSConnectionConfig()
            
            assert config.max_reconnect_attempts == 10
            assert config.reconnect_time_wait == 5.5
            assert config.connection_timeout == 20.0
            assert config.ping_interval == 60.0
            assert config.max_outstanding_pings == 5
            assert config.prefer_thread_local is True
            assert config.name == "test_client"
            assert config.no_randomize is True
            assert config.user == "test_user"
            assert config.password == "test_password"
            assert config.token == "test_token"
            assert config.nkey == "test_nkey"
            assert config.credentials == "/path/to/creds"

    def test_custom_servers(self) -> None:
        """Test that custom servers can be provided."""
        config = NATSConnectionConfig(servers=["nats://server1:4222", "nats://server2:4222"])
        assert config.servers == ["nats://server1:4222", "nats://server2:4222"]

    def test_validation_negative_values(self) -> None:
        """Test that negative values raise validation errors."""
        with pytest.raises(ValueError, match="max_reconnect_attempts must be non-negative"):
            NATSConnectionConfig(max_reconnect_attempts=-1)
        
        with pytest.raises(ValueError, match="reconnect_time_wait must be non-negative"):
            NATSConnectionConfig(reconnect_time_wait=-1.0)
        
        with pytest.raises(ValueError, match="connection_timeout must be non-negative"):
            NATSConnectionConfig(connection_timeout=-1.0)
        
        with pytest.raises(ValueError, match="ping_interval must be non-negative"):
            NATSConnectionConfig(ping_interval=-1.0)
        
        with pytest.raises(ValueError, match="max_outstanding_pings must be non-negative"):
            NATSConnectionConfig(max_outstanding_pings=-1)

    def test_empty_servers_fallback(self) -> None:
        """Test that empty servers list falls back to default."""
        config = NATSConnectionConfig(servers=[])
        assert config.servers == [DEFAULT_NATS_URL]


class TestConfig:
    """Test cases for Config."""

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        config = Config()
        
        assert config.nats_connection.servers == [DEFAULT_NATS_URL]
        assert config.queue_name == DEFAULT_QUEUE_NAME
        assert config.job_serializer == "pickle"
        assert config.json_encoder == "json.JSONEncoder"
        assert config.json_decoder == "json.JSONDecoder"
        assert config.scheduler_lock_ttl_seconds == 30
        assert config.scheduler_lock_renew_interval_seconds == 15
        assert config.max_schedule_failures == 5
        assert config.job_status_ttl_seconds == 86400
        assert config.default_result_ttl_seconds == 604800
        assert config.worker_ttl_seconds == 60
        assert config.worker_heartbeat_interval_seconds == 15
        assert config.default_ack_wait_seconds == 60
        assert config.ack_wait_per_queue == {}
        assert config.dependency_check_delay_seconds == 5
        assert config.log_level == "CRITICAL"
        assert config.log_to_file_enabled is False
        assert config.log_file_path == "naq_{time}.log"

    def test_custom_nats_connection(self) -> None:
        """Test that custom NATS connection config is used."""
        nats_config = NATSConnectionConfig(servers=["nats://custom:4222"])
        config = Config(nats_connection=nats_config)
        
        assert config.nats_connection.servers == ["nats://custom:4222"]

    def test_from_env(self) -> None:
        """Test that Config.from_env() works correctly."""
        with patch.dict(os.environ, {
            "NAQ_DEFAULT_QUEUE": "test_queue",
            "NAQ_JOB_SERIALIZER": "json",
            "NAQ_JSON_ENCODER": "custom.JSONEncoder",
            "NAQ_JSON_DECODER": "custom.JSONDecoder",
            "NAQ_SCHEDULER_LOCK_TTL": "60",
            "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL": "30",
            "NAQ_MAX_SCHEDULE_FAILURES": "10",
            "NAQ_JOB_STATUS_TTL": "172800",
            "NAQ_DEFAULT_RESULT_TTL": "1209600",
            "NAQ_WORKER_TTL": "120",
            "NAQ_WORKER_HEARTBEAT_INTERVAL": "30",
            "NAQ_DEFAULT_ACK_WAIT": "120",
            "NAQ_LOG_LEVEL": "INFO",
            "NAQ_LOG_TO_FILE_ENABLED": "true",
            "NAQ_LOG_FILE_PATH": "custom.log",
        }):
            config = Config.from_env()
            
            assert config.queue_name == "test_queue"
            assert config.job_serializer == "json"
            assert config.json_encoder == "custom.JSONEncoder"
            assert config.json_decoder == "custom.JSONDecoder"
            assert config.scheduler_lock_ttl_seconds == 60
            assert config.scheduler_lock_renew_interval_seconds == 30
            assert config.max_schedule_failures == 10
            assert config.job_status_ttl_seconds == 172800
            assert config.default_result_ttl_seconds == 1209600
            assert config.worker_ttl_seconds == 120
            assert config.worker_heartbeat_interval_seconds == 30
            assert config.default_ack_wait_seconds == 120
            assert config.log_level == "INFO"
            assert config.log_to_file_enabled is True
            assert config.log_file_path == "custom.log"

    def test_from_dict(self) -> None:
        """Test that Config.from_dict() works correctly."""
        config_dict = {
            "queue_name": "dict_queue",
            "job_serializer": "json",
            "nats_connection": {
                "servers": ["nats://dict:4222"],
                "max_reconnect_attempts": 3,
            },
            "ack_wait_per_queue": {"email": 120, "reports": 300},
        }
        
        config = Config.from_dict(config_dict)
        
        assert config.queue_name == "dict_queue"
        assert config.job_serializer == "json"
        assert config.nats_connection.servers == ["nats://dict:4222"]
        assert config.nats_connection.max_reconnect_attempts == 3
        assert config.ack_wait_per_queue == {"email": 120, "reports": 300}

    def test_to_dict(self) -> None:
        """Test that Config.to_dict() works correctly."""
        nats_config = NATSConnectionConfig(servers=["nats://test:4222"])
        config = Config(
            queue_name="test_queue",
            nats_connection=nats_config,
            ack_wait_per_queue={"email": 120},
        )
        
        config_dict = config.to_dict()
        
        assert config_dict["queue_name"] == "test_queue"
        assert config_dict["nats_connection"]["servers"] == ["nats://test:4222"]
        assert config_dict["ack_wait_per_queue"] == {"email": 120}

    def test_validation_errors(self) -> None:
        """Test that validation errors are raised for invalid values."""
        with pytest.raises(ValueError, match="scheduler_lock_ttl_seconds must be positive"):
            Config(scheduler_lock_ttl_seconds=0)
        
        with pytest.raises(ValueError, match="scheduler_lock_renew_interval_seconds must be positive"):
            Config(scheduler_lock_renew_interval_seconds=0)
        
        with pytest.raises(ValueError, match="worker_ttl_seconds must be positive"):
            Config(worker_ttl_seconds=0)
        
        with pytest.raises(ValueError, match="worker_heartbeat_interval_seconds must be positive"):
            Config(worker_heartbeat_interval_seconds=0)
        
        with pytest.raises(ValueError, match="default_ack_wait_seconds must be positive"):
            Config(default_ack_wait_seconds=0)
        
        with pytest.raises(ValueError, match="ack_wait for queue 'email' must be positive"):
            Config(ack_wait_per_queue={"email": 0})


class TestGlobalConfig:
    """Test cases for global configuration functions."""

    def test_get_global_config_creates_instance(self) -> None:
        """Test that get_global_config() creates an instance if none exists."""
        reset_global_config()
        config = get_global_config()
        
        assert isinstance(config, Config)
        assert config.nats_connection.servers == [DEFAULT_NATS_URL]

    def test_set_global_config(self) -> None:
        """Test that set_global_config() sets the global instance."""
        reset_global_config()
        
        custom_config = Config(queue_name="global_test")
        set_global_config(custom_config)
        
        retrieved_config = get_global_config()
        assert retrieved_config.queue_name == "global_test"
        assert retrieved_config is custom_config

    def test_reset_global_config(self) -> None:
        """Test that reset_global_config() clears the global instance."""
        reset_global_config()
        
        # Set a custom config
        custom_config = Config(queue_name="reset_test")
        set_global_config(custom_config)
        
        # Reset
        reset_global_config()
        
        # Get should create a new instance with default values
        new_config = get_global_config()
        assert new_config.queue_name == DEFAULT_QUEUE_NAME
        assert new_config is not custom_config