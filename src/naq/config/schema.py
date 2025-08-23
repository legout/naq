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
                "lock_ttl": {"type": "integer", "description": "Scheduler lock TTL"},
                "lock_renew_interval": {"type": "integer", "description": "Lock renew interval"},
                "job_status_ttl": {"type": "integer", "description": "Job status TTL"},
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
