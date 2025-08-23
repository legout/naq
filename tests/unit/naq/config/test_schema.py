"""Unit tests for configuration schema validation."""

import pytest
from typing import Optional

from naq.config.schema import ConfigValidator, CONFIG_SCHEMA
from naq.exceptions import ConfigurationError


class TestConfigValidator:
    """Test cases for ConfigValidator class."""

    def test_init_with_default_schema(self) -> None:
        """Test ConfigValidator initialization with default schema."""
        validator = ConfigValidator()
        assert validator.schema == CONFIG_SCHEMA

    def test_init_with_custom_schema(self) -> None:
        """Test ConfigValidator initialization with custom schema."""
        custom_schema = {"type": "object", "properties": {"test": {"type": "string"}}}
        validator = ConfigValidator(custom_schema)
        assert validator.schema == custom_schema

    def test_validate_valid_config(self) -> None:
        """Test validation of a valid configuration."""
        config = {
            "nats": {
                "servers": ["nats://localhost:4222"],
                "client_name": "test-client",
                "max_reconnect_attempts": 5,
                "reconnect_time_wait": 2.0,
                "connection_timeout": 5.0,
                "drain_timeout": 30.0,
                "auth": None,
                "tls": None
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": 30.0,
                "ttl": 60.0,
                "max_job_duration": 3600.0,
                "shutdown_timeout": 10.0,
                "pools": None
            },
            "events": {
                "enabled": True,
                "batch_size": 100,
                "flush_interval": 5.0,
                "max_buffer_size": 1000,
                "stream": "naq_events",
                "filters": None
            },
            "queues": {},
            "scheduler": {},
            "results": {},
            "serialization": {},
            "logging": {}
        }
        
        validator = ConfigValidator()
        # Should not raise any exception
        validator.validate(config)

    def test_validate_valid_config_with_optional_sections(self) -> None:
        """Test validation of a valid configuration with optional sections populated."""
        config = {
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
        
        validator = ConfigValidator()
        # Should not raise any exception
        validator.validate(config)

    def test_validate_missing_required_section(self) -> None:
        """Test validation fails when required section is missing."""
        config = {
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
            # Missing required "events" section
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_invalid_nats_config(self) -> None:
        """Test validation fails with invalid NATS configuration."""
        config = {
            "nats": {
                "servers": [],  # Empty servers list should fail
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
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_invalid_workers_config(self) -> None:
        """Test validation fails with invalid workers configuration."""
        config = {
            "nats": {
                "servers": ["nats://localhost:4222"],
                "client_name": "test-client",
                "max_reconnect_attempts": 5,
                "reconnect_time_wait": 2.0,
                "connection_timeout": 5.0,
                "drain_timeout": 30.0
            },
            "workers": {
                "concurrency": 0,  # Should be >= 1
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
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_invalid_events_config(self) -> None:
        """Test validation fails with invalid events configuration."""
        config = {
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
                "batch_size": 0,  # Should be >= 1
                "flush_interval": 5.0,
                "max_buffer_size": 1000,
                "stream": ""  # Should not be empty
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_wrong_data_type(self) -> None:
        """Test validation fails with wrong data types."""
        config = {
            "nats": {
                "servers": ["nats://localhost:4222"],
                "client_name": "test-client",
                "max_reconnect_attempts": "5",  # Should be integer
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
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_negative_values(self) -> None:
        """Test validation fails with negative values where not allowed."""
        config = {
            "nats": {
                "servers": ["nats://localhost:4222"],
                "client_name": "test-client",
                "max_reconnect_attempts": -1,  # Should be >= 0
                "reconnect_time_wait": 2.0,
                "connection_timeout": 5.0,
                "drain_timeout": 30.0
            },
            "workers": {
                "concurrency": 4,
                "heartbeat_interval": -1.0,  # Should be >= 0
                "ttl": 60.0,
                "max_job_duration": 3600.0,
                "shutdown_timeout": 10.0
            },
            "events": {
                "enabled": True,
                "batch_size": 100,
                "flush_interval": -1.0,  # Should be >= 0
                "max_buffer_size": 1000,
                "stream": "naq_events"
            }
        }
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_additional_properties(self) -> None:
        """Test validation fails with additional properties not in schema."""
        config = {
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
        
        validator = ConfigValidator()
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(config)

    def test_validate_nats_servers_valid_urls(self) -> None:
        """Test validation of valid NATS server URLs."""
        validator = ConfigValidator()
        
        valid_servers = [
            "nats://localhost:4222",
            "nats://127.0.0.1:4222",
            "nats://example.com:4222",
            "nats://sub.example.com:4222",
            "nats://user:pass@localhost:4222",
            "nats://user:password@example.com:4222",
            "nats://user:p_ss_w0rd@localhost:4222",
            "nats://localhost:8080",
            "nats://nats.example.com:4222"
        ]
        
        # Should not raise any exception
        validator.validate_nats_servers(valid_servers)

    def test_validate_nats_servers_invalid_urls(self) -> None:
        """Test validation of invalid NATS server URLs."""
        validator = ConfigValidator()
        
        invalid_servers = [
            "invalid://localhost:4222",  # Wrong protocol
            "nats://localhost",  # Missing port
            "nats://localhost:",  # Empty port
            "nats://:4222",  # Missing host
            "nats://localhost:abc",  # Non-numeric port
            "nats://localhost:4222/extra",  # Extra path
            "localhost:4222",  # Missing protocol
            "nats://localhost:4222?query=param",  # Query string
            "nats://localhost:4222#fragment",  # Fragment
            "",  # Empty string
            "nats://",  # Only protocol
        ]
        
        for invalid_server in invalid_servers:
            with pytest.raises(ConfigurationError, match="Invalid NATS server URL"):
                validator.validate_nats_servers([invalid_server])

    def test_validate_nats_servers_empty_list(self) -> None:
        """Test validation fails with empty servers list."""
        validator = ConfigValidator()
        
        with pytest.raises(ConfigurationError, match="NATS servers list cannot be empty"):
            validator.validate_nats_servers([])

    def test_validate_nats_servers_non_string(self) -> None:
        """Test validation fails with non-string server URLs."""
        validator = ConfigValidator()
        
        invalid_servers = [
            ["nats://localhost:4222", 123],  # Integer instead of string
            ["nats://localhost:4222", None],  # None instead of string
            ["nats://localhost:4222", {}],  # Dict instead of string
            ["nats://localhost:4222", []],  # List instead of string
        ]
        
        for invalid_server_list in invalid_servers:
            with pytest.raises(ConfigurationError, match="NATS server URL must be a string"):
                validator.validate_nats_servers(invalid_server_list)

    def test_validate_nats_servers_mixed_valid_invalid(self) -> None:
        """Test validation fails when at least one server URL is invalid."""
        validator = ConfigValidator()
        
        mixed_servers = [
            "nats://localhost:4222",  # Valid
            "invalid://localhost:4222",  # Invalid
            "nats://example.com:4222"  # Valid
        ]
        
        with pytest.raises(ConfigurationError, match="Invalid NATS server URL"):
            validator.validate_nats_servers(mixed_servers)

    def test_validate_with_custom_schema(self) -> None:
        """Test validation with custom schema."""
        custom_schema = {
            "type": "object",
            "properties": {
                "test_field": {"type": "string", "minLength": 3}
            },
            "required": ["test_field"]
        }
        
        validator = ConfigValidator(custom_schema)
        
        # Valid config
        valid_config = {"test_field": "hello"}
        validator.validate(valid_config)  # Should not raise
        
        # Invalid config - too short
        invalid_config = {"test_field": "hi"}
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(invalid_config)
        
        # Invalid config - missing required field
        missing_config = {}
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            validator.validate(missing_config)