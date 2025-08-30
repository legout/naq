"""
Tests for configuration validation functionality.
"""

import pytest
from typing import Dict, Any, List

from naq.config.validation import (
    validate_nats_url,
    validate_http_url,
    validate_positive_integer,
    validate_non_negative_integer,
    validate_positive_float,
    validate_string_length,
    validate_string_choice,
    validate_boolean,
    validate_list_of_strings,
    validate_dict_string_keys,
    validate_connection_config,
    validate_job_service_config,
    validate_worker_service_config,
    validate_scheduler_service_config,
    validate_stream_service_config,
    validate_kv_store_config,
    validate_event_service_config,
    validate_naq_config,
)
from naq.exceptions import ValidationError


class TestValidateNatsUrl:
    """Test NATS URL validation."""

    def test_valid_nats_url(self):
        """Test valid NATS URLs."""
        valid_urls = [
            "nats://localhost:4222",
            "nats://127.0.0.1:4222",
            "nats://user:pass@localhost:4222",
            "nats://example.com:4222",
        ]
        
        for url in valid_urls:
            validate_nats_url(url)  # Should not raise

    def test_empty_nats_url(self):
        """Test empty NATS URL."""
        with pytest.raises(ValidationError, match="NATS URL cannot be empty"):
            validate_nats_url("")

    def test_non_string_nats_url(self):
        """Test non-string NATS URL."""
        with pytest.raises(ValidationError, match="NATS URL must be a string"):
            validate_nats_url(123)

    def test_invalid_nats_url_format(self):
        """Test invalid NATS URL format."""
        invalid_urls = [
            "http://localhost:4222",
            "nats://localhost",
            "nats://localhost:abc",
            "nats://:4222",
            "localhost:4222",
        ]
        
        for url in invalid_urls:
            with pytest.raises(ValidationError, match="Invalid NATS URL"):
                validate_nats_url(url)


class TestValidateHttpUrl:
    """Test HTTP URL validation."""

    def test_valid_http_url(self):
        """Test valid HTTP URLs."""
        valid_urls = [
            "http://example.com",
            "https://example.com",
            "http://example.com/path",
            "https://user:pass@example.com",
        ]
        
        for url in valid_urls:
            validate_http_url(url)  # Should not raise

    def test_empty_http_url(self):
        """Test empty HTTP URL."""
        with pytest.raises(ValidationError, match="URL cannot be empty"):
            validate_http_url("")

    def test_non_string_http_url(self):
        """Test non-string HTTP URL."""
        with pytest.raises(ValidationError, match="URL must be a string"):
            validate_http_url(123)

    def test_invalid_http_url_format(self):
        """Test invalid HTTP URL format."""
        invalid_urls = [
            "ftp://example.com",
            "example.com",
            "http://",
            "https://",
        ]
        
        for url in invalid_urls:
            with pytest.raises(ValidationError, match="Invalid URL"):
                validate_http_url(url)

    def test_custom_field_name(self):
        """Test custom field name in error message."""
        with pytest.raises(ValidationError, match="CustomField cannot be empty"):
            validate_http_url("", "CustomField")


class TestValidatePositiveInteger:
    """Test positive integer validation."""

    def test_valid_positive_integer(self):
        """Test valid positive integers."""
        validate_positive_integer(1, "test")
        validate_positive_integer(100, "test")

    def test_non_integer_value(self):
        """Test non-integer value."""
        with pytest.raises(ValidationError, match="test must be an integer"):
            validate_positive_integer("not an int", "test")

    def test_zero_value(self):
        """Test zero value."""
        with pytest.raises(ValidationError, match="test must be at least 1"):
            validate_positive_integer(0, "test")

    def test_negative_value(self):
        """Test negative value."""
        with pytest.raises(ValidationError, match="test must be at least 1"):
            validate_positive_integer(-1, "test")

    def test_max_value(self):
        """Test maximum value constraint."""
        validate_positive_integer(5, "test", max_value=10)
        with pytest.raises(ValidationError, match="test must be at most 10"):
            validate_positive_integer(11, "test", max_value=10)

    def test_custom_min_value(self):
        """Test custom minimum value."""
        validate_positive_integer(10, "test", min_value=10)
        with pytest.raises(ValidationError, match="test must be at least 10"):
            validate_positive_integer(9, "test", min_value=10)


class TestValidateNonNegativeInteger:
    """Test non-negative integer validation."""

    def test_valid_non_negative_integer(self):
        """Test valid non-negative integers."""
        validate_non_negative_integer(0, "test")
        validate_non_negative_integer(1, "test")
        validate_non_negative_integer(100, "test")

    def test_non_integer_value(self):
        """Test non-integer value."""
        with pytest.raises(ValidationError, match="test must be an integer"):
            validate_non_negative_integer("not an int", "test")

    def test_negative_value(self):
        """Test negative value."""
        with pytest.raises(ValidationError, match="test must be non-negative"):
            validate_non_negative_integer(-1, "test")

    def test_max_value(self):
        """Test maximum value constraint."""
        validate_non_negative_integer(5, "test", max_value=10)
        with pytest.raises(ValidationError, match="test must be at most 10"):
            validate_non_negative_integer(11, "test", max_value=10)


class TestValidatePositiveFloat:
    """Test positive float validation."""

    def test_valid_positive_float(self):
        """Test valid positive floats."""
        validate_positive_float(0.0, "test")
        validate_positive_float(1.0, "test")
        validate_positive_float(1.5, "test")
        validate_positive_float(100, "test")  # Integer should work too

    def test_non_numeric_value(self):
        """Test non-numeric value."""
        with pytest.raises(ValidationError, match="test must be a number"):
            validate_positive_float("not a number", "test")

    def test_negative_value(self):
        """Test negative value."""
        with pytest.raises(ValidationError, match="test must be at least 0.0"):
            validate_positive_float(-1.0, "test")

    def test_custom_min_value(self):
        """Test custom minimum value."""
        validate_positive_float(10.0, "test", min_value=10.0)
        with pytest.raises(ValidationError, match="test must be at least 10.0"):
            validate_positive_float(9.9, "test", min_value=10.0)

    def test_max_value(self):
        """Test maximum value constraint."""
        validate_positive_float(5.0, "test", max_value=10.0)
        with pytest.raises(ValidationError, match="test must be at most 10.0"):
            validate_positive_float(10.1, "test", max_value=10.0)


class TestValidateStringLength:
    """Test string length validation."""

    def test_valid_string_length(self):
        """Test valid string lengths."""
        validate_string_length("test", "test")
        validate_string_length("", "test", min_length=0)
        validate_string_length("a", "test", min_length=1)
        validate_string_length("a" * 10, "test", max_length=10)

    def test_non_string_value(self):
        """Test non-string value."""
        with pytest.raises(ValidationError, match="test must be a string"):
            validate_string_length(123, "test")

    def test_string_too_short(self):
        """Test string too short."""
        with pytest.raises(ValidationError, match="test must be at least 5 characters"):
            validate_string_length("abc", "test", min_length=5)

    def test_string_too_long(self):
        """Test string too long."""
        with pytest.raises(ValidationError, match="test must be at most 5 characters"):
            validate_string_length("abcdef", "test", max_length=5)


class TestValidateStringChoice:
    """Test string choice validation."""

    def test_valid_choice(self):
        """Test valid choice."""
        validate_string_choice("option1", "test", ["option1", "option2", "option3"])

    def test_non_string_value(self):
        """Test non-string value."""
        with pytest.raises(ValidationError, match="test must be a string"):
            validate_string_choice(123, "test", ["option1", "option2"])

    def test_invalid_choice(self):
        """Test invalid choice."""
        with pytest.raises(ValidationError, match="Invalid test: invalid. Must be one of"):
            validate_string_choice("invalid", "test", ["option1", "option2"])


class TestValidateBoolean:
    """Test boolean validation."""

    def test_valid_boolean(self):
        """Test valid boolean values."""
        validate_boolean(True, "test")
        validate_boolean(False, "test")

    def test_non_boolean_value(self):
        """Test non-boolean value."""
        with pytest.raises(ValidationError, match="test must be a boolean"):
            validate_boolean("true", "test")
        with pytest.raises(ValidationError, match="test must be a boolean"):
            validate_boolean(1, "test")
        with pytest.raises(ValidationError, match="test must be a boolean"):
            validate_boolean(None, "test")


class TestValidateListOfStrings:
    """Test list of strings validation."""

    def test_valid_list_of_strings(self):
        """Test valid list of strings."""
        validate_list_of_strings(["a", "b", "c"], "test")
        validate_list_of_strings([], "test")  # Empty list should be allowed by default

    def test_non_list_value(self):
        """Test non-list value."""
        with pytest.raises(ValidationError, match="test must be a list"):
            validate_list_of_strings("not a list", "test")

    def test_empty_list_not_allowed(self):
        """Test empty list when not allowed."""
        with pytest.raises(ValidationError, match="test cannot be empty"):
            validate_list_of_strings([], "test", allow_empty=False)

    def test_list_with_non_string_elements(self):
        """Test list with non-string elements."""
        with pytest.raises(ValidationError, match="test\\[1\\] must be a string"):
            validate_list_of_strings(["a", 123, "c"], "test")


class TestValidateDictStringKeys:
    """Test dictionary with string keys validation."""

    def test_valid_dict_string_keys(self):
        """Test valid dictionary with string keys."""
        validate_dict_string_keys({"a": 1, "b": 2}, "test")
        validate_dict_string_keys({}, "test")  # Empty dict should be allowed by default

    def test_non_dict_value(self):
        """Test non-dictionary value."""
        with pytest.raises(ValidationError, match="test must be a dictionary"):
            validate_dict_string_keys("not a dict", "test")

    def test_empty_dict_not_allowed(self):
        """Test empty dictionary when not allowed."""
        with pytest.raises(ValidationError, match="test cannot be empty"):
            validate_dict_string_keys({}, "test", allow_empty=False)

    def test_dict_with_non_string_keys(self):
        """Test dictionary with non-string keys."""
        with pytest.raises(ValidationError, match="test keys must be strings"):
            validate_dict_string_keys({"a": 1, 2: "b"}, "test")


class TestValidateConnectionConfig:
    """Test connection configuration validation."""

    def test_valid_connection_config(self):
        """Test valid connection configuration."""
        config = {
            "servers": ["nats://localhost:4222"],
            "max_reconnect_attempts": 5,
            "reconnect_time_wait": 2.0,
            "connection_timeout": 30.0,
            "drain_timeout": 10.0,
        }
        validate_connection_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Connection config must be a dictionary"):
            validate_connection_config("not a dict")

    def test_invalid_servers(self):
        """Test invalid servers configuration."""
        # Non-list servers
        with pytest.raises(ValidationError, match="connection.servers must be a list"):
            validate_connection_config({"servers": "not a list"})

        # Empty servers when not allowed
        with pytest.raises(ValidationError, match="connection.servers cannot be empty"):
            validate_connection_config({"servers": []})

        # Invalid server URL
        with pytest.raises(ValidationError, match="Invalid NATS URL"):
            validate_connection_config({"servers": ["invalid-url"]})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Negative max_reconnect_attempts
        with pytest.raises(ValidationError, match="connection.max_reconnect_attempts must be non-negative"):
            validate_connection_config({"max_reconnect_attempts": -1})

        # Negative reconnect_time_wait
        with pytest.raises(ValidationError, match="connection.reconnect_time_wait must be at least 0.0"):
            validate_connection_config({"reconnect_time_wait": -1.0})

        # Negative connection_timeout
        with pytest.raises(ValidationError, match="connection.connection_timeout must be at least 0.0"):
            validate_connection_config({"connection_timeout": -1.0})

        # Negative drain_timeout
        with pytest.raises(ValidationError, match="connection.drain_timeout must be at least 0.0"):
            validate_connection_config({"drain_timeout": -1.0})


class TestValidateJobServiceConfig:
    """Test job service configuration validation."""

    def test_valid_job_service_config(self):
        """Test valid job service configuration."""
        config = {
            "queue_name": "test_queue",
            "default_job_ttl": 3600,
            "max_retries": 3,
            "result_expiry": 86400,
            "enable_result_backend": True,
            "enable_dead_letter_queue": False,
        }
        validate_job_service_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Job service config must be a dictionary"):
            validate_job_service_config("not a dict")

    def test_invalid_queue_name(self):
        """Test invalid queue name."""
        # Empty queue name
        with pytest.raises(ValidationError, match="job_service.queue_name must be at least 1 characters"):
            validate_job_service_config({"queue_name": ""})

        # Too long queue name
        with pytest.raises(ValidationError, match="job_service.queue_name must be at most 100 characters"):
            validate_job_service_config({"queue_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Negative default_job_ttl
        with pytest.raises(ValidationError, match="job_service.default_job_ttl must be non-negative"):
            validate_job_service_config({"default_job_ttl": -1})

        # Negative max_retries
        with pytest.raises(ValidationError, match="job_service.max_retries must be non-negative"):
            validate_job_service_config({"max_retries": -1})

        # Negative result_expiry
        with pytest.raises(ValidationError, match="job_service.result_expiry must be non-negative"):
            validate_job_service_config({"result_expiry": -1})

    def test_invalid_boolean_fields(self):
        """Test invalid boolean fields."""
        # Non-boolean enable_result_backend
        with pytest.raises(ValidationError, match="job_service.enable_result_backend must be a boolean"):
            validate_job_service_config({"enable_result_backend": "true"})

        # Non-boolean enable_dead_letter_queue
        with pytest.raises(ValidationError, match="job_service.enable_dead_letter_queue must be a boolean"):
            validate_job_service_config({"enable_dead_letter_queue": "false"})


class TestValidateWorkerServiceConfig:
    """Test worker service configuration validation."""

    def test_valid_worker_service_config(self):
        """Test valid worker service configuration."""
        config = {
            "workers_bucket_name": "test_workers",
            "default_worker_ttl": 300,
            "heartbeat_interval": 30.0,
            "enable_worker_registration": True,
            "enable_event_logging": False,
            "auto_create_buckets": True,
        }
        validate_worker_service_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Worker service config must be a dictionary"):
            validate_worker_service_config("not a dict")

    def test_invalid_workers_bucket_name(self):
        """Test invalid workers bucket name."""
        # Empty bucket name
        with pytest.raises(ValidationError, match="worker_service.workers_bucket_name must be at least 1 characters"):
            validate_worker_service_config({"workers_bucket_name": ""})

        # Too long bucket name
        with pytest.raises(ValidationError, match="worker_service.workers_bucket_name must be at most 100 characters"):
            validate_worker_service_config({"workers_bucket_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Zero default_worker_ttl
        with pytest.raises(ValidationError, match="worker_service.default_worker_ttl must be at least 1"):
            validate_worker_service_config({"default_worker_ttl": 0})

        # Negative heartbeat_interval
        with pytest.raises(ValidationError, match="worker_service.heartbeat_interval must be at least 0.0"):
            validate_worker_service_config({"heartbeat_interval": -1.0})

    def test_invalid_boolean_fields(self):
        """Test invalid boolean fields."""
        # Non-boolean enable_worker_registration
        with pytest.raises(ValidationError, match="worker_service.enable_worker_registration must be a boolean"):
            validate_worker_service_config({"enable_worker_registration": "true"})

        # Non-boolean enable_event_logging
        with pytest.raises(ValidationError, match="worker_service.enable_event_logging must be a boolean"):
            validate_worker_service_config({"enable_event_logging": "false"})

        # Non-boolean auto_create_buckets
        with pytest.raises(ValidationError, match="worker_service.auto_create_buckets must be a boolean"):
            validate_worker_service_config({"auto_create_buckets": "true"})


class TestValidateSchedulerServiceConfig:
    """Test scheduler service configuration validation."""

    def test_valid_scheduler_service_config(self):
        """Test valid scheduler service configuration."""
        config = {
            "lock_bucket_name": "test_locks",
            "lock_ttl": 60.0,
            "lock_renewal_interval": 30.0,
            "max_schedule_failures": 5,
            "enable_leader_election": True,
            "auto_create_buckets": False,
        }
        validate_scheduler_service_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Scheduler service config must be a dictionary"):
            validate_scheduler_service_config("not a dict")

    def test_invalid_lock_bucket_name(self):
        """Test invalid lock bucket name."""
        # Empty bucket name
        with pytest.raises(ValidationError, match="scheduler_service.lock_bucket_name must be at least 1 characters"):
            validate_scheduler_service_config({"lock_bucket_name": ""})

        # Too long bucket name
        with pytest.raises(ValidationError, match="scheduler_service.lock_bucket_name must be at most 100 characters"):
            validate_scheduler_service_config({"lock_bucket_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Negative lock_ttl
        with pytest.raises(ValidationError, match="scheduler_service.lock_ttl must be at least 0.0"):
            validate_scheduler_service_config({"lock_ttl": -1.0})

        # Negative lock_renewal_interval
        with pytest.raises(ValidationError, match="scheduler_service.lock_renewal_interval must be at least 0.0"):
            validate_scheduler_service_config({"lock_renewal_interval": -1.0})

        # Negative max_schedule_failures
        with pytest.raises(ValidationError, match="scheduler_service.max_schedule_failures must be non-negative"):
            validate_scheduler_service_config({"max_schedule_failures": -1})

    def test_invalid_boolean_fields(self):
        """Test invalid boolean fields."""
        # Non-boolean enable_leader_election
        with pytest.raises(ValidationError, match="scheduler_service.enable_leader_election must be a boolean"):
            validate_scheduler_service_config({"enable_leader_election": "true"})

        # Non-boolean auto_create_buckets
        with pytest.raises(ValidationError, match="scheduler_service.auto_create_buckets must be a boolean"):
            validate_scheduler_service_config({"auto_create_buckets": "false"})


class TestValidateStreamServiceConfig:
    """Test stream service configuration validation."""

    def test_valid_stream_service_config(self):
        """Test valid stream service configuration."""
        config = {
            "stream_name": "test_stream",
            "max_msgs": 1000,
            "max_bytes": 1048576,
            "max_age": 3600.0,
            "retention": "limits",
            "auto_create_stream": True,
        }
        validate_stream_service_config(config)  # Should not raise

    def test_valid_stream_service_config_with_none_values(self):
        """Test valid stream service configuration with None values."""
        config = {
            "stream_name": "test_stream",
            "max_msgs": None,
            "max_bytes": None,
            "max_age": None,
            "retention": None,
            "auto_create_stream": True,
        }
        validate_stream_service_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Stream service config must be a dictionary"):
            validate_stream_service_config("not a dict")

    def test_invalid_stream_name(self):
        """Test invalid stream name."""
        # Empty stream name
        with pytest.raises(ValidationError, match="stream_service.stream_name must be at least 1 characters"):
            validate_stream_service_config({"stream_name": ""})

        # Too long stream name
        with pytest.raises(ValidationError, match="stream_service.stream_name must be at most 100 characters"):
            validate_stream_service_config({"stream_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Zero max_msgs
        with pytest.raises(ValidationError, match="stream_service.max_msgs must be at least 1"):
            validate_stream_service_config({"max_msgs": 0})

        # Zero max_bytes
        with pytest.raises(ValidationError, match="stream_service.max_bytes must be at least 1"):
            validate_stream_service_config({"max_bytes": 0})

        # Negative max_age
        with pytest.raises(ValidationError, match="stream_service.max_age must be at least 0.0"):
            validate_stream_service_config({"max_age": -1.0})

    def test_invalid_retention(self):
        """Test invalid retention value."""
        with pytest.raises(ValidationError, match="Invalid stream_service.retention: invalid. Must be one of"):
            validate_stream_service_config({"retention": "invalid"})

    def test_invalid_boolean_fields(self):
        """Test invalid boolean fields."""
        # Non-boolean auto_create_stream
        with pytest.raises(ValidationError, match="stream_service.auto_create_stream must be a boolean"):
            validate_stream_service_config({"auto_create_stream": "true"})


class TestValidateKVStoreConfig:
    """Test KV store configuration validation."""

    def test_valid_kv_store_config(self):
        """Test valid KV store configuration."""
        config = {
            "bucket_name": "test_kv",
            "ttl": 3600,
            "history": 5,
            "replicas": 1,
        }
        validate_kv_store_config(config)  # Should not raise

    def test_valid_kv_store_config_with_none_values(self):
        """Test valid KV store configuration with None values."""
        config = {
            "bucket_name": "test_kv",
            "ttl": None,
            "history": None,
            "replicas": None,
        }
        validate_kv_store_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="KV store config must be a dictionary"):
            validate_kv_store_config("not a dict")

    def test_invalid_bucket_name(self):
        """Test invalid bucket name."""
        # Empty bucket name
        with pytest.raises(ValidationError, match="kv_store.bucket_name must be at least 1 characters"):
            validate_kv_store_config({"bucket_name": ""})

        # Too long bucket name
        with pytest.raises(ValidationError, match="kv_store.bucket_name must be at most 100 characters"):
            validate_kv_store_config({"bucket_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Negative ttl
        with pytest.raises(ValidationError, match="kv_store.ttl must be non-negative"):
            validate_kv_store_config({"ttl": -1})

        # Zero history
        with pytest.raises(ValidationError, match="kv_store.history must be at least 1"):
            validate_kv_store_config({"history": 0})

        # Zero replicas
        with pytest.raises(ValidationError, match="kv_store.replicas must be at least 1"):
            validate_kv_store_config({"replicas": 0})


class TestValidateEventServiceConfig:
    """Test event service configuration validation."""

    def test_valid_event_service_config(self):
        """Test valid event service configuration."""
        config = {
            "event_bucket_name": "test_events",
            "max_events": 1000,
            "event_ttl": 86400,
            "enable_event_logging": True,
            "auto_create_bucket": False,
        }
        validate_event_service_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="Event service config must be a dictionary"):
            validate_event_service_config("not a dict")

    def test_invalid_event_bucket_name(self):
        """Test invalid event bucket name."""
        # Empty bucket name
        with pytest.raises(ValidationError, match="event_service.event_bucket_name must be at least 1 characters"):
            validate_event_service_config({"event_bucket_name": ""})

        # Too long bucket name
        with pytest.raises(ValidationError, match="event_service.event_bucket_name must be at most 100 characters"):
            validate_event_service_config({"event_bucket_name": "a" * 101})

    def test_invalid_numeric_fields(self):
        """Test invalid numeric fields."""
        # Zero max_events
        with pytest.raises(ValidationError, match="event_service.max_events must be at least 1"):
            validate_event_service_config({"max_events": 0})

        # Negative event_ttl
        with pytest.raises(ValidationError, match="event_service.event_ttl must be non-negative"):
            validate_event_service_config({"event_ttl": -1})

    def test_invalid_boolean_fields(self):
        """Test invalid boolean fields."""
        # Non-boolean enable_event_logging
        with pytest.raises(ValidationError, match="event_service.enable_event_logging must be a boolean"):
            validate_event_service_config({"enable_event_logging": "true"})

        # Non-boolean auto_create_bucket
        with pytest.raises(ValidationError, match="event_service.auto_create_bucket must be a boolean"):
            validate_event_service_config({"auto_create_bucket": "false"})


class TestValidateNaqConfig:
    """Test complete NAQ configuration validation."""

    def test_valid_naq_config(self):
        """Test valid NAQ configuration."""
        config = {
            "nats": {
                "servers": ["nats://localhost:4222"],
            },
            "connection": {
                "servers": ["nats://localhost:4222"],
                "max_reconnect_attempts": 5,
                "reconnect_time_wait": 2.0,
                "connection_timeout": 30.0,
                "drain_timeout": 10.0,
            },
            "job_service": {
                "queue_name": "test_queue",
                "default_job_ttl": 3600,
                "max_retries": 3,
                "result_expiry": 86400,
                "enable_result_backend": True,
                "enable_dead_letter_queue": False,
            },
            "worker_service": {
                "workers_bucket_name": "test_workers",
                "default_worker_ttl": 300,
                "heartbeat_interval": 30.0,
                "enable_worker_registration": True,
                "enable_event_logging": False,
                "auto_create_buckets": True,
            },
            "scheduler_service": {
                "lock_bucket_name": "test_locks",
                "lock_ttl": 60.0,
                "lock_renewal_interval": 30.0,
                "max_schedule_failures": 5,
                "enable_leader_election": True,
                "auto_create_buckets": False,
            },
            "streams": {
                "stream_name": "test_stream",
                "max_msgs": 1000,
                "max_bytes": 1048576,
                "max_age": 3600.0,
                "retention": "limits",
                "auto_create_stream": True,
            },
            "kv_store": {
                "bucket_name": "test_kv",
                "ttl": 3600,
                "history": 5,
                "replicas": 1,
            },
            "event_service": {
                "event_bucket_name": "test_events",
                "max_events": 1000,
                "event_ttl": 86400,
                "enable_event_logging": True,
                "auto_create_bucket": False,
            },
        }
        validate_naq_config(config)  # Should not raise

    def test_non_dict_config(self):
        """Test non-dictionary configuration."""
        with pytest.raises(ValidationError, match="NAQ config must be a dictionary"):
            validate_naq_config("not a dict")

    def test_empty_nats_config(self):
        """Test empty NATS configuration."""
        with pytest.raises(ValidationError, match="nats cannot be empty"):
            validate_naq_config({"nats": {}})

    def test_invalid_nats_servers(self):
        """Test invalid NATS servers."""
        # Non-list servers
        with pytest.raises(ValidationError, match="nats.servers must be a list"):
            validate_naq_config({"nats": {"servers": "not a list"}})

        # Empty servers
        with pytest.raises(ValidationError, match="nats.servers cannot be empty"):
            validate_naq_config({"nats": {"servers": []}})

        # Invalid server URL
        with pytest.raises(ValidationError, match="Invalid NATS URL"):
            validate_naq_config({"nats": {"servers": ["invalid-url"]}})

    def test_invalid_sub_config(self):
        """Test invalid sub-configuration."""
        # Invalid connection config
        with pytest.raises(ValidationError, match="Connection config must be a dictionary"):
            validate_naq_config({"connection": "not a dict"})

        # Invalid job_service config
        with pytest.raises(ValidationError, match="Job service config must be a dictionary"):
            validate_naq_config({"job_service": "not a dict"})

        # Invalid worker_service config
        with pytest.raises(ValidationError, match="Worker service config must be a dictionary"):
            validate_naq_config({"worker_service": "not a dict"})

        # Invalid scheduler_service config
        with pytest.raises(ValidationError, match="Scheduler service config must be a dictionary"):
            validate_naq_config({"scheduler_service": "not a dict"})

        # Invalid streams config
        with pytest.raises(ValidationError, match="Stream service config must be a dictionary"):
            validate_naq_config({"streams": "not a dict"})

        # Invalid kv_store config
        with pytest.raises(ValidationError, match="KV store config must be a dictionary"):
            validate_naq_config({"kv_store": "not a dict"})

        # Invalid event_service config
        with pytest.raises(ValidationError, match="Event service config must be a dictionary"):
            validate_naq_config({"event_service": "not a dict"})