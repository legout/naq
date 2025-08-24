"""
Configuration validation module for NAQ

This module provides validation functions for the NAQ configuration system,
ensuring that all configuration values are valid and consistent.
"""

import re
from typing import Any, Dict, List, Optional

from ..exceptions import ValidationError

# URL validation patterns
NATS_URL_PATTERN = re.compile(r'^nats://(?:[^\s:@]+:[^\s@]*@)?[^\s:]+:[0-9]+$')
HTTP_URL_PATTERN = re.compile(r'^https?://(?:[^\s:@]+:[^\s@]*@)?[^\s:]+(?:/[^\s]*)?$')


def validate_nats_url(url: str) -> None:
    """
    Validate a NATS server URL.

    Args:
        url: The NATS URL to validate.

    Raises:
        ValidationError: If the URL is invalid.
    """
    if not url:
        raise ValidationError("NATS URL cannot be empty")
    
    if not isinstance(url, str):
        raise ValidationError(f"NATS URL must be a string, got {type(url).__name__}")
    
    if not NATS_URL_PATTERN.match(url):
        raise ValidationError(
            f"Invalid NATS URL: {url}. Expected format: nats://host:port or nats://user:pass@host:port"
        )


def validate_http_url(url: str, field_name: str = "URL") -> None:
    """
    Validate an HTTP/HTTPS URL.

    Args:
        url: The URL to validate.
        field_name: The name of the field being validated for error messages.

    Raises:
        ValidationError: If the URL is invalid.
    """
    if not url:
        raise ValidationError(f"{field_name} cannot be empty")
    
    if not isinstance(url, str):
        raise ValidationError(f"{field_name} must be a string, got {type(url).__name__}")
    
    if not HTTP_URL_PATTERN.match(url):
        raise ValidationError(
            f"Invalid {field_name}: {url}. Expected format: http://host/path or https://host/path"
        )


def validate_positive_integer(
    value: int, 
    field_name: str, 
    min_value: int = 1, 
    max_value: Optional[int] = None
) -> None:
    """
    Validate that a value is a positive integer within optional bounds.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.
        min_value: Minimum allowed value (inclusive).
        max_value: Maximum allowed value (inclusive).

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, int):
        raise ValidationError(f"{field_name} must be an integer, got {type(value).__name__}")
    
    if value < min_value:
        raise ValidationError(f"{field_name} must be at least {min_value}, got {value}")
    
    if max_value is not None and value > max_value:
        raise ValidationError(f"{field_name} must be at most {max_value}, got {value}")


def validate_non_negative_integer(
    value: int, 
    field_name: str, 
    max_value: Optional[int] = None
) -> None:
    """
    Validate that a value is a non-negative integer within optional bounds.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.
        max_value: Maximum allowed value (inclusive).

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, int):
        raise ValidationError(f"{field_name} must be an integer, got {type(value).__name__}")
    
    if value < 0:
        raise ValidationError(f"{field_name} must be non-negative, got {value}")
    
    if max_value is not None and value > max_value:
        raise ValidationError(f"{field_name} must be at most {max_value}, got {value}")


def validate_positive_float(
    value: float, 
    field_name: str, 
    min_value: float = 0.0, 
    max_value: Optional[float] = None
) -> None:
    """
    Validate that a value is a positive float within optional bounds.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.
        min_value: Minimum allowed value (inclusive).
        max_value: Maximum allowed value (inclusive).

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, (int, float)):
        raise ValidationError(f"{field_name} must be a number, got {type(value).__name__}")
    
    value_float = float(value)
    
    if value_float < min_value:
        raise ValidationError(f"{field_name} must be at least {min_value}, got {value_float}")
    
    if max_value is not None and value_float > max_value:
        raise ValidationError(f"{field_name} must be at most {max_value}, got {value_float}")


def validate_string_length(
    value: str, 
    field_name: str, 
    min_length: int = 0, 
    max_length: Optional[int] = None
) -> None:
    """
    Validate that a string has a valid length.

    Args:
        value: The string to validate.
        field_name: The name of the field being validated for error messages.
        min_length: Minimum allowed length (inclusive).
        max_length: Maximum allowed length (inclusive).

    Raises:
        ValidationError: If the string is invalid.
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string, got {type(value).__name__}")
    
    length = len(value)
    
    if length < min_length:
        raise ValidationError(f"{field_name} must be at least {min_length} characters, got {length}")
    
    if max_length is not None and length > max_length:
        raise ValidationError(f"{field_name} must be at most {max_length} characters, got {length}")


def validate_string_choice(
    value: str, 
    field_name: str, 
    choices: List[str]
) -> None:
    """
    Validate that a string is one of the allowed choices.

    Args:
        value: The string to validate.
        field_name: The name of the field being validated for error messages.
        choices: List of allowed values.

    Raises:
        ValidationError: If the string is invalid.
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string, got {type(value).__name__}")
    
    if value not in choices:
        raise ValidationError(
            f"Invalid {field_name}: {value}. Must be one of: {', '.join(choices)}"
        )


def validate_boolean(value: Any, field_name: str) -> None:
    """
    Validate that a value is a boolean.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, bool):
        raise ValidationError(f"{field_name} must be a boolean, got {type(value).__name__}")


def validate_list_of_strings(
    value: List[Any], 
    field_name: str, 
    allow_empty: bool = True
) -> None:
    """
    Validate that a value is a list of strings.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.
        allow_empty: Whether an empty list is allowed.

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, list):
        raise ValidationError(f"{field_name} must be a list, got {type(value).__name__}")
    
    if not allow_empty and not value:
        raise ValidationError(f"{field_name} cannot be empty")
    
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise ValidationError(
                f"{field_name}[{i}] must be a string, got {type(item).__name__}"
            )


def validate_dict_string_keys(
    value: Dict[Any, Any], 
    field_name: str, 
    allow_empty: bool = True
) -> None:
    """
    Validate that a value is a dictionary with string keys.

    Args:
        value: The value to validate.
        field_name: The name of the field being validated for error messages.
        allow_empty: Whether an empty dictionary is allowed.

    Raises:
        ValidationError: If the value is invalid.
    """
    if not isinstance(value, dict):
        raise ValidationError(f"{field_name} must be a dictionary, got {type(value).__name__}")
    
    if not allow_empty and not value:
        raise ValidationError(f"{field_name} cannot be empty")
    
    for key in value.keys():
        if not isinstance(key, str):
            raise ValidationError(
                f"{field_name} keys must be strings, got {type(key).__name__}"
            )


def validate_connection_config(config: Dict[str, Any]) -> None:
    """
    Validate the connection configuration.

    Args:
        config: The connection configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Connection config must be a dictionary")
    
    # Validate servers
    if "servers" in config:
        validate_list_of_strings(config["servers"], "connection.servers", allow_empty=False)
        for server in config["servers"]:
            validate_nats_url(server)
    
    # Validate numeric fields
    if "max_reconnect_attempts" in config:
        validate_non_negative_integer(
            config["max_reconnect_attempts"], 
            "connection.max_reconnect_attempts"
        )
    
    if "reconnect_time_wait" in config:
        validate_positive_float(
            config["reconnect_time_wait"], 
            "connection.reconnect_time_wait"
        )
    
    if "connection_timeout" in config:
        validate_positive_float(
            config["connection_timeout"], 
            "connection.connection_timeout"
        )
    
    if "drain_timeout" in config:
        validate_positive_float(
            config["drain_timeout"], 
            "connection.drain_timeout"
        )


def validate_job_service_config(config: Dict[str, Any]) -> None:
    """
    Validate the job service configuration.

    Args:
        config: The job service configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Job service config must be a dictionary")
    
    # Validate queue_name
    if "queue_name" in config:
        validate_string_length(
            config["queue_name"], 
            "job_service.queue_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate default_job_ttl
    if "default_job_ttl" in config:
        validate_non_negative_integer(
            config["default_job_ttl"], 
            "job_service.default_job_ttl"
        )
    
    # Validate max_retries
    if "max_retries" in config:
        validate_non_negative_integer(
            config["max_retries"], 
            "job_service.max_retries"
        )
    
    # Validate result_expiry
    if "result_expiry" in config:
        validate_non_negative_integer(
            config["result_expiry"], 
            "job_service.result_expiry"
        )
    
    # Validate boolean fields
    if "enable_result_backend" in config:
        validate_boolean(config["enable_result_backend"], "job_service.enable_result_backend")
    
    if "enable_dead_letter_queue" in config:
        validate_boolean(config["enable_dead_letter_queue"], "job_service.enable_dead_letter_queue")


def validate_worker_service_config(config: Dict[str, Any]) -> None:
    """
    Validate the worker service configuration.

    Args:
        config: The worker service configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Worker service config must be a dictionary")
    
    # Validate workers_bucket_name
    if "workers_bucket_name" in config:
        validate_string_length(
            config["workers_bucket_name"], 
            "worker_service.workers_bucket_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate numeric fields
    if "default_worker_ttl" in config:
        validate_positive_integer(
            config["default_worker_ttl"], 
            "worker_service.default_worker_ttl"
        )
    
    if "heartbeat_interval" in config:
        validate_positive_float(
            config["heartbeat_interval"], 
            "worker_service.heartbeat_interval"
        )
    
    # Validate boolean fields
    if "enable_worker_registration" in config:
        validate_boolean(config["enable_worker_registration"], "worker_service.enable_worker_registration")
    
    if "enable_event_logging" in config:
        validate_boolean(config["enable_event_logging"], "worker_service.enable_event_logging")
    
    if "auto_create_buckets" in config:
        validate_boolean(config["auto_create_buckets"], "worker_service.auto_create_buckets")


def validate_scheduler_service_config(config: Dict[str, Any]) -> None:
    """
    Validate the scheduler service configuration.

    Args:
        config: The scheduler service configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Scheduler service config must be a dictionary")
    
    # Validate lock_bucket_name
    if "lock_bucket_name" in config:
        validate_string_length(
            config["lock_bucket_name"], 
            "scheduler_service.lock_bucket_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate numeric fields
    if "lock_ttl" in config:
        validate_positive_float(
            config["lock_ttl"],
            "scheduler_service.lock_ttl"
        )
    
    if "lock_renewal_interval" in config:
        validate_positive_float(
            config["lock_renewal_interval"], 
            "scheduler_service.lock_renewal_interval"
        )
    
    if "max_schedule_failures" in config:
        validate_non_negative_integer(
            config["max_schedule_failures"], 
            "scheduler_service.max_schedule_failures"
        )
    
    # Validate boolean fields
    if "enable_leader_election" in config:
        validate_boolean(config["enable_leader_election"], "scheduler_service.enable_leader_election")
    
    if "auto_create_buckets" in config:
        validate_boolean(config["auto_create_buckets"], "scheduler_service.auto_create_buckets")


def validate_stream_service_config(config: Dict[str, Any]) -> None:
    """
    Validate the stream service configuration.

    Args:
        config: The stream service configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Stream service config must be a dictionary")
    
    # Validate stream_name
    if "stream_name" in config:
        validate_string_length(
            config["stream_name"], 
            "stream_service.stream_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate numeric fields
    if "max_msgs" in config and config["max_msgs"] is not None:
        validate_positive_integer(
            config["max_msgs"],
            "stream_service.max_msgs"
        )
    
    if "max_bytes" in config and config["max_bytes"] is not None:
        validate_positive_integer(
            config["max_bytes"],
            "stream_service.max_bytes"
        )
    
    if "max_age" in config and config["max_age"] is not None:
        validate_positive_float(
            config["max_age"],
            "stream_service.max_age"
        )
    
    if "retention" in config and config["retention"] is not None:
        validate_string_choice(
            config["retention"],
            "stream_service.retention",
            ["limits", "interest", "workqueue"]
        )
    
    # Validate boolean fields
    if "auto_create_stream" in config:
        validate_boolean(config["auto_create_stream"], "stream_service.auto_create_stream")


def validate_kv_store_config(config: Dict[str, Any]) -> None:
    """
    Validate the KV store configuration.

    Args:
        config: The KV store configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("KV store config must be a dictionary")
    
    # Validate bucket_name
    if "bucket_name" in config:
        validate_string_length(
            config["bucket_name"], 
            "kv_store.bucket_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate numeric fields
    if "ttl" in config and config["ttl"] is not None:
        validate_non_negative_integer(
            config["ttl"],
            "kv_store.ttl"
        )
    
    if "history" in config and config["history"] is not None:
        validate_positive_integer(
            config["history"],
            "kv_store.history"
        )
    
    if "replicas" in config and config["replicas"] is not None:
        validate_positive_integer(
            config["replicas"],
            "kv_store.replicas"
        )


def validate_event_service_config(config: Dict[str, Any]) -> None:
    """
    Validate the event service configuration.

    Args:
        config: The event service configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("Event service config must be a dictionary")
    
    # Validate event_bucket_name
    if "event_bucket_name" in config:
        validate_string_length(
            config["event_bucket_name"], 
            "event_service.event_bucket_name", 
            min_length=1, 
            max_length=100
        )
    
    # Validate numeric fields
    if "max_events" in config:
        validate_positive_integer(
            config["max_events"], 
            "event_service.max_events"
        )
    
    if "event_ttl" in config:
        validate_non_negative_integer(
            config["event_ttl"], 
            "event_service.event_ttl"
        )
    
    # Validate boolean fields
    if "enable_event_logging" in config:
        validate_boolean(config["enable_event_logging"], "event_service.enable_event_logging")
    
    if "auto_create_bucket" in config:
        validate_boolean(config["auto_create_bucket"], "event_service.auto_create_bucket")


def validate_naq_config(config: Dict[str, Any]) -> None:
    """
    Validate the entire NAQ configuration.

    Args:
        config: The NAQ configuration to validate.

    Raises:
        ValidationError: If the configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValidationError("NAQ config must be a dictionary")
    
    # Validate top-level fields
    if "nats" in config:
        validate_dict_string_keys(config["nats"], "nats", allow_empty=False)
        if "servers" in config["nats"]:
            validate_list_of_strings(config["nats"]["servers"], "nats.servers", allow_empty=False)
            for server in config["nats"]["servers"]:
                validate_nats_url(server)
    
    # Validate service configurations
    if "connection" in config:
        validate_connection_config(config["connection"])
    
    if "job_service" in config:
        validate_job_service_config(config["job_service"])
    
    if "worker_service" in config:
        validate_worker_service_config(config["worker_service"])
    
    if "scheduler_service" in config:
        validate_scheduler_service_config(config["scheduler_service"])
    
    if "streams" in config:
        validate_stream_service_config(config["streams"])
    
    if "kv_store" in config:
        validate_kv_store_config(config["kv_store"])
    
    if "event_service" in config:
        validate_event_service_config(config["event_service"])