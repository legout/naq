"""Configuration schema and validation for NAQ."""

import re
from typing import Optional

import jsonschema

from ..exceptions import ConfigurationError


# JSON Schema for NAQ configuration
CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "nats": {
            "type": "object",
            "properties": {
                "servers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "List of NATS server URLs",
                },
                "client_name": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Name to identify this client connection",
                },
                "max_reconnect_attempts": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Maximum number of reconnection attempts",
                },
                "reconnect_time_wait": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Time to wait between reconnection attempts"
                    " (seconds)",
                },
                "connection_timeout": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Timeout for establishing a connection (in seconds)",
                },
                "drain_timeout": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Timeout for draining connections (in seconds)",
                },
                "auth": {
                    "type": ["object", "null"],
                    "description": "Authentication configuration",
                },
                "tls": {
                    "type": ["object", "null"],
                    "description": "TLS configuration for secure connections",
                },
            },
            "required": [
                "servers",
                "client_name",
                "max_reconnect_attempts",
                "reconnect_time_wait",
                "connection_timeout",
                "drain_timeout",
            ],
            "additionalProperties": False,
        },
        "workers": {
            "type": "object",
            "properties": {
                "concurrency": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum number of concurrent jobs to process",
                },
                "heartbeat_interval": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Interval for sending heartbeat messages (seconds)",
                },
                "ttl": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Time-to-live for worker entries (in seconds)",
                },
                "max_job_duration": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Maximum duration for a single job (in seconds)",
                },
                "shutdown_timeout": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Timeout for graceful shutdown (in seconds)",
                },
                "pools": {
                    "type": ["object", "null"],
                    "description": "Worker pool configuration",
                },
            },
            "required": [
                "concurrency",
                "heartbeat_interval",
                "ttl",
                "max_job_duration",
                "shutdown_timeout",
            ],
            "additionalProperties": False,
        },
        "events": {
            "type": "object",
            "properties": {
                "enabled": {
                    "type": "boolean",
                    "description": "Whether event processing is enabled",
                },
                "batch_size": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Number of events to batch together",
                },
                "flush_interval": {
                    "type": "number",
                    "minimum": 0,
                    "description": "Interval for flushing events (in seconds)",
                },
                "max_buffer_size": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum size of the event buffer",
                },
                "stream": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Name of the event stream",
                },
                "filters": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "List of event filters to apply",
                },
            },
            "required": [
                "enabled",
                "batch_size",
                "flush_interval",
                "max_buffer_size",
                "stream",
            ],
            "additionalProperties": False,
        },
        "queues": {
            "type": ["object", "null"],
            "description": "Queue configuration",
            "properties": {
                "default_name": {"type": "string", "description": "Default queue name"},
                "ack_wait": {"type": "integer", "description": "Default ack wait time"}
            }
        },
        "scheduler": {
            "type": ["object", "null"],
            "description": "Scheduler configuration",
            "properties": {
                "lock_ttl": {"type": "number", "description": "Scheduler lock TTL"},
                "lock_renew_interval": {"type": "number", "description": "Lock renew interval"},
                "job_status_ttl": {"type": "number", "description": "Job status TTL"},
                "max_failures": {"type": "integer", "description": "Max schedule failures"}
            }
        },
        "results": {
            "type": ["object", "null"],
            "description": "Results configuration",
            "properties": {
                "ttl": {"type": "integer", "description": "Result TTL"}
            }
        },
        "serialization": {
            "type": ["object", "null"],
            "description": "Serialization configuration",
            "properties": {
                "method": {"type": "string", "description": "Serialization method"},
                "json_encoder": {"type": "string", "description": "JSON encoder class"},
                "json_decoder": {"type": "string", "description": "JSON decoder class"}
            }
        },
        "logging": {
            "type": ["object", "null"],
            "description": "Logging configuration",
            "properties": {
                "level": {"type": "string", "description": "Log level"},
                "to_file_enabled": {"type": "boolean", "description": "Enable file logging"},
                "file_path": {"type": "string", "description": "Log file path"}
            }
        },
        "database": {
            "type": ["object", "null"],
            "description": "Database configuration"
        },
    },
    "required": ["nats", "workers", "events"],
    "additionalProperties": False,
}

# Schema for service configurations
SERVICE_CONFIG_SCHEMAS = {
    "connection": {
        "type": "object",
        "properties": {
            "servers": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "description": "List of NATS server URLs",
            },
            "client_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name to identify this client connection",
            },
            "max_reconnect_attempts": {
                "type": "integer",
                "minimum": 0,
                "description": "Maximum number of reconnection attempts",
            },
            "reconnect_time_wait": {
                "type": "number",
                "minimum": 0,
                "description": "Time to wait between reconnection attempts (seconds)",
            },
            "connection_timeout": {
                "type": "number",
                "minimum": 0,
                "description": "Timeout for establishing a connection (in seconds)",
            },
            "drain_timeout": {
                "type": "number",
                "minimum": 0,
                "description": "Timeout for draining connections (in seconds)",
            },
            "auth": {
                "type": ["object", "null"],
                "description": "Authentication configuration",
            },
            "tls": {
                "type": ["object", "null"],
                "description": "TLS configuration for secure connections",
            },
        },
        "required": [
            "servers",
            "client_name",
            "max_reconnect_attempts",
            "reconnect_time_wait",
            "connection_timeout",
            "drain_timeout",
        ],
        "additionalProperties": False,
    },
    "kv_store": {
        "type": "object",
        "properties": {
            "bucket_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the KV bucket",
            },
            "history": {
                "type": "integer",
                "minimum": 1,
                "description": "Number of historical values to keep",
            },
            "stream_name": {
                "type": ["string", "null"],
                "description": "Name of the underlying stream",
            },
            "ttl": {
                "type": ["number", "null"],
                "minimum": 0,
                "description": "Time-to-live for keys in seconds",
            },
            "replicas": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Number of replicas for the bucket",
            },
        },
        "required": ["bucket_name"],
        "additionalProperties": False,
    },
    "streams": {
        "type": "object",
        "properties": {
            "stream_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the stream",
            },
            "subjects": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "description": "List of subjects for the stream",
            },
            "retention_limit": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Maximum number of messages to retain",
            },
            "max_age": {
                "type": ["number", "null"],
                "minimum": 0,
                "description": "Maximum age of messages in seconds",
            },
            "max_msgs": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Maximum number of messages",
            },
            "max_bytes": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Maximum size of messages in bytes",
            },
            "replicas": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Number of replicas for the stream",
            },
            "storage": {
                "type": ["string", "null"],
                "description": "Storage type for the stream",
            },
        },
        "required": ["stream_name", "subjects"],
        "additionalProperties": False,
    },
    "job_service": {
        "type": "object",
        "properties": {
            "enable_job_execution": {
                "type": "boolean",
                "description": "Whether job execution is enabled",
            },
            "enable_result_storage": {
                "type": "boolean",
                "description": "Whether result storage is enabled",
            },
            "enable_event_logging": {
                "type": "boolean",
                "description": "Whether event logging is enabled",
            },
            "max_job_execution_time": {
                "type": "number",
                "minimum": 0,
                "description": "Maximum time for job execution in seconds",
            },
            "default_result_ttl": {
                "type": "number",
                "minimum": 0,
                "description": "Default TTL for job results in seconds",
            },
            "results_bucket_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the results bucket",
            },
            "auto_create_buckets": {
                "type": "boolean",
                "description": "Whether to automatically create buckets",
            },
            "default_queue": {
                "type": "string",
                "minLength": 1,
                "description": "Default queue name",
            },
            "default_max_retries": {
                "type": "integer",
                "minimum": 0,
                "description": "Default maximum number of retries",
            },
            "default_retry_delay": {
                "type": "number",
                "minimum": 0,
                "description": "Default retry delay in seconds",
            },
        },
        "required": [
            "enable_job_execution",
            "enable_result_storage",
            "enable_event_logging",
            "max_job_execution_time",
            "default_result_ttl",
            "results_bucket_name",
            "auto_create_buckets",
            "default_queue",
            "default_max_retries",
            "default_retry_delay",
        ],
        "additionalProperties": False,
    },
    "worker_service": {
        "type": "object",
        "properties": {
            "worker_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the worker",
            },
            "queues": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "description": "List of queues the worker processes",
            },
            "max_concurrent_jobs": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of concurrent jobs",
            },
            "heartbeat_interval": {
                "type": "number",
                "minimum": 0,
                "description": "Heartbeat interval in seconds",
            },
            "ttl": {
                "type": "number",
                "minimum": 0,
                "description": "Time-to-live for worker status in seconds",
            },
            "max_job_duration": {
                "type": "number",
                "minimum": 0,
                "description": "Maximum job duration in seconds",
            },
            "shutdown_timeout": {
                "type": "number",
                "minimum": 0,
                "description": "Shutdown timeout in seconds",
            },
            "status_bucket_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the status bucket",
            },
            "auto_create_buckets": {
                "type": "boolean",
                "description": "Whether to automatically create buckets",
            },
        },
        "required": [
            "worker_name",
            "queues",
            "max_concurrent_jobs",
            "heartbeat_interval",
            "ttl",
            "max_job_duration",
            "shutdown_timeout",
            "status_bucket_name",
            "auto_create_buckets",
        ],
        "additionalProperties": False,
    },
    "scheduler_service": {
        "type": "object",
        "properties": {
            "scheduler_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the scheduler",
            },
            "check_interval": {
                "type": "number",
                "minimum": 0.1,
                "description": "Interval for checking scheduled jobs in seconds",
            },
            "max_concurrent_schedules": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of concurrent schedules",
            },
            "schedules_bucket_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the schedules bucket",
            },
            "lock_bucket_name": {
                "type": "string",
                "minLength": 1,
                "description": "Name of the locks bucket",
            },
            "lock_ttl": {
                "type": "number",
                "minimum": 0,
                "description": "Time-to-live for locks in seconds",
            },
            "lock_renew_interval": {
                "type": "number",
                "minimum": 0,
                "description": "Interval for renewing locks in seconds",
            },
            "auto_create_buckets": {
                "type": "boolean",
                "description": "Whether to automatically create buckets",
            },
        },
        "required": [
            "scheduler_name",
            "check_interval",
            "max_concurrent_schedules",
            "schedules_bucket_name",
            "lock_bucket_name",
            "lock_ttl",
            "lock_renew_interval",
            "auto_create_buckets",
        ],
        "additionalProperties": False,
    },
}

# Update the main schema to include service configurations
for service_name, service_schema in SERVICE_CONFIG_SCHEMAS.items():
    CONFIG_SCHEMA["properties"][service_name] = service_schema


class ConfigValidator:
    """Validates NAQ configuration against a defined schema.

    This class provides methods to validate configuration dictionaries
    against a JSON Schema and perform specific validations for NATS server URLs.
    """

    def __init__(self, schema: Optional[dict] = None) -> None:
        """Initialize the validator with an optional schema.

        Args:
            schema: Optional schema dictionary to use for validation.
                   If not provided, uses the default CONFIG_SCHEMA.
        """
        self.schema = schema or CONFIG_SCHEMA

    def validate(self, config: dict) -> None:
        """Validate a configuration dictionary against the schema.

        Args:
            config: The configuration dictionary to validate.

        Raises:
            ConfigurationError: If the configuration fails validation.
        """
        try:
            jsonschema.validate(config, self.schema)
        except jsonschema.ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e.message}")

    def validate_nats_servers(self, servers: list[str]) -> None:
        """Validate a list of NATS server URLs.

        Args:
            servers: List of NATS server URLs to validate.

        Raises:
            ConfigurationError: If any server URL is invalid.
        """
        if not servers:
            raise ConfigurationError("NATS servers list cannot be empty")

        # NATS URL pattern: nats://host:port or nats://user:pass@host:port
        nats_url_pattern = re.compile(r"^nats://(?:[^:@]+(?::[^@]*)?@)?[^:]+:\d+$")

        for server_url in servers:
            if not isinstance(server_url, str):
                raise ConfigurationError(
                    f"NATS server URL must be a string, got {type(server_url).__name__}"
                )

            if not nats_url_pattern.match(server_url):
                raise ConfigurationError(
                    f"Invalid NATS server URL: {server_url}. "
                    "Expected format: nats://host:port or nats://user:pass@host:port"
                )
