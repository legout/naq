# src/naq/config/schema.py
"""
JSON Schema definitions for NAQ configuration validation.

This module defines comprehensive JSON schemas for validating NAQ configuration
files and provides validation utilities to ensure configuration integrity.
"""

from typing import Dict, Any


# NATS configuration schema
NATS_SCHEMA = {
    "type": "object",
    "properties": {
        "servers": {
            "type": "array",
            "items": {
                "type": "string",
                "pattern": r"^nats(-tls)?://[a-zA-Z0-9.-]+(?::\d+)?$"
            },
            "minItems": 1,
            "description": "List of NATS server URLs"
        },
        "client_name": {
            "type": "string",
            "minLength": 1,
            "description": "NATS client name identifier"
        },
        "max_reconnect_attempts": {
            "type": "integer",
            "minimum": 0,
            "description": "Maximum number of reconnection attempts"
        },
        "reconnect_time_wait": {
            "type": "number",
            "minimum": 0,
            "description": "Time to wait between reconnection attempts (seconds)"
        },
        "connection_timeout": {
            "type": "number",
            "minimum": 0,
            "description": "Connection timeout (seconds)"
        },
        "drain_timeout": {
            "type": "number",
            "minimum": 0,
            "description": "Connection drain timeout (seconds)"
        },
        "flush_timeout": {
            "type": "number",
            "minimum": 0,
            "description": "Flush timeout (seconds)"
        },
        "ping_interval": {
            "type": "number",
            "minimum": 1,
            "description": "Ping interval (seconds)"
        },
        "max_outstanding_pings": {
            "type": "integer",
            "minimum": 1,
            "description": "Maximum outstanding pings"
        },
        "auth": {
            "type": ["object", "null"],
            "properties": {
                "username": {"type": ["string", "null"]},
                "password": {"type": ["string", "null"]},
                "token": {"type": ["string", "null"]}
            },
            "additionalProperties": False,
            "description": "NATS authentication configuration"
        },
        "tls": {
            "type": ["object", "null"],
            "properties": {
                "enabled": {"type": "boolean"},
                "cert_file": {"type": ["string", "null"]},
                "key_file": {"type": ["string", "null"]},
                "ca_file": {"type": ["string", "null"]}
            },
            "additionalProperties": False,
            "description": "NATS TLS configuration"
        }
    },
    "required": ["servers"],
    "additionalProperties": False
}


# Queue configuration schema
QUEUE_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "ack_wait": {
            "type": "integer",
            "minimum": 1,
            "description": "Message acknowledgment wait time (seconds)"
        },
        "max_deliver": {
            "type": "integer",
            "minimum": 1,
            "description": "Maximum delivery attempts"
        },
        "max_ack_pending": {
            "type": ["integer", "null"],
            "minimum": 1,
            "description": "Maximum pending acknowledgments"
        }
    },
    "additionalProperties": False
}


QUEUES_SCHEMA = {
    "type": "object",
    "properties": {
        "default": {
            "type": "string",
            "minLength": 1,
            "pattern": r"^[a-zA-Z0-9_.-]+$",
            "description": "Default queue name"
        },
        "prefix": {
            "type": "string",
            "minLength": 1,
            "pattern": r"^[a-zA-Z0-9_.-]+$",
            "description": "Queue name prefix"
        },
        "configs": {
            "type": "object",
            "patternProperties": {
                r"^[a-zA-Z0-9_.-]+$": QUEUE_CONFIG_SCHEMA
            },
            "additionalProperties": False,
            "description": "Queue-specific configurations"
        }
    },
    "required": ["default", "prefix"],
    "additionalProperties": False
}


# Worker configuration schema
WORKER_POOL_SCHEMA = {
    "type": "object",
    "properties": {
        "concurrency": {
            "type": "integer",
            "minimum": 1,
            "description": "Pool concurrency level"
        },
        "queues": {
            "type": "array",
            "items": {
                "type": "string",
                "minLength": 1
            },
            "description": "Queues handled by this pool"
        }
    },
    "required": ["concurrency"],
    "additionalProperties": False
}


WORKERS_SCHEMA = {
    "type": "object",
    "properties": {
        "concurrency": {
            "type": "integer",
            "minimum": 1,
            "description": "Default worker concurrency"
        },
        "heartbeat_interval": {
            "type": "integer",
            "minimum": 1,
            "description": "Worker heartbeat interval (seconds)"
        },
        "ttl": {
            "type": "integer",
            "minimum": 1,
            "description": "Worker TTL (seconds)"
        },
        "max_job_duration": {
            "type": "integer",
            "minimum": 1,
            "description": "Maximum job execution time (seconds)"
        },
        "shutdown_timeout": {
            "type": "integer",
            "minimum": 1,
            "description": "Graceful shutdown timeout (seconds)"
        },
        "pools": {
            "type": "object",
            "patternProperties": {
                r"^[a-zA-Z0-9_.-]+$": WORKER_POOL_SCHEMA
            },
            "additionalProperties": False,
            "description": "Named worker pools"
        }
    },
    "required": ["concurrency"],
    "additionalProperties": False
}


# Scheduler configuration schema
SCHEDULER_SCHEMA = {
    "type": "object",
    "properties": {
        "enabled": {
            "type": "boolean",
            "description": "Whether scheduler is enabled"
        },
        "lock_ttl": {
            "type": "integer",
            "minimum": 1,
            "description": "Scheduler lock TTL (seconds)"
        },
        "lock_renew_interval": {
            "type": "integer",
            "minimum": 1,
            "description": "Lock renewal interval (seconds)"
        },
        "max_failures": {
            "type": "integer",
            "minimum": 0,
            "description": "Maximum scheduling failures before giving up"
        },
        "scan_interval": {
            "type": "number",
            "minimum": 0.1,
            "description": "Job scanning interval (seconds)"
        }
    },
    "required": ["enabled"],
    "additionalProperties": False
}


# Events configuration schema
EVENT_STREAM_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "minLength": 1,
            "description": "JetStream stream name"
        },
        "max_age": {
            "type": "string",
            "pattern": r"^\d+[smhd]$",
            "description": "Maximum age for events (e.g., '168h', '7d')"
        },
        "max_bytes": {
            "type": "string",
            "pattern": r"^\d+[KMGT]?B$",
            "description": "Maximum stream size (e.g., '1GB', '500MB')"
        },
        "replicas": {
            "type": "integer",
            "minimum": 1,
            "maximum": 5,
            "description": "Number of stream replicas"
        }
    },
    "required": ["name"],
    "additionalProperties": False
}


EVENT_FILTERS_SCHEMA = {
    "type": "object",
    "properties": {
        "exclude_heartbeats": {
            "type": "boolean",
            "description": "Whether to exclude heartbeat events"
        },
        "min_job_duration": {
            "type": "integer",
            "minimum": 0,
            "description": "Minimum job duration to log (milliseconds)"
        }
    },
    "additionalProperties": False
}


EVENTS_SCHEMA = {
    "type": "object",
    "properties": {
        "enabled": {
            "type": "boolean",
            "description": "Whether event logging is enabled"
        },
        "batch_size": {
            "type": "integer",
            "minimum": 1,
            "maximum": 10000,
            "description": "Event batching size"
        },
        "flush_interval": {
            "type": "number",
            "minimum": 0.1,
            "description": "Event flush interval (seconds)"
        },
        "max_buffer_size": {
            "type": "integer",
            "minimum": 1,
            "description": "Maximum event buffer size"
        },
        "storage_url": {
            "type": ["string", "null"],
            "pattern": r"^nats(-tls)?://[a-zA-Z0-9.-]+(?::\d+)?$",
            "description": "Event storage NATS URL"
        },
        "subject_prefix": {
            "type": "string",
            "minLength": 1,
            "description": "Event subject prefix"
        },
        "stream": EVENT_STREAM_SCHEMA,
        "filters": EVENT_FILTERS_SCHEMA
    },
    "required": ["enabled"],
    "additionalProperties": False
}


# Results configuration schema
RESULTS_SCHEMA = {
    "type": "object",
    "properties": {
        "ttl": {
            "type": "integer",
            "minimum": 60,
            "description": "Result TTL (seconds)"
        },
        "cleanup_interval": {
            "type": "integer",
            "minimum": 60,
            "description": "Cleanup interval (seconds)"
        },
        "kv_bucket": {
            "type": "string",
            "minLength": 1,
            "description": "KV bucket name for results"
        }
    },
    "required": ["ttl"],
    "additionalProperties": False
}


# Serialization configuration schema
JSON_SERIALIZATION_SCHEMA = {
    "type": "object",
    "properties": {
        "encoder": {
            "type": "string",
            "minLength": 1,
            "description": "JSON encoder class path"
        },
        "decoder": {
            "type": "string",
            "minLength": 1,
            "description": "JSON decoder class path"
        }
    },
    "additionalProperties": False
}


SERIALIZATION_SCHEMA = {
    "type": "object",
    "properties": {
        "job_serializer": {
            "type": "string",
            "enum": ["pickle", "json"],
            "description": "Job serialization format"
        },
        "compression": {
            "type": "boolean",
            "description": "Whether to compress serialized data"
        },
        "json": JSON_SERIALIZATION_SCHEMA
    },
    "required": ["job_serializer"],
    "additionalProperties": False
}


# Logging configuration schema
LOGGING_SCHEMA = {
    "type": "object",
    "properties": {
        "level": {
            "type": "string",
            "enum": ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "description": "Logging level"
        },
        "format": {
            "type": "string",
            "enum": ["text", "json"],
            "description": "Log format"
        },
        "file": {
            "type": ["string", "null"],
            "description": "Log file path (null for stdout)"
        },
        "max_size": {
            "type": "string",
            "pattern": r"^\d+[KMGT]?B$",
            "description": "Maximum log file size"
        },
        "backup_count": {
            "type": "integer",
            "minimum": 0,
            "description": "Number of backup log files"
        },
        "to_file_enabled": {
            "type": "boolean",
            "description": "Whether file logging is enabled"
        }
    },
    "required": ["level"],
    "additionalProperties": False
}


# Environment override schema
ENVIRONMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "minLength": 1,
            "description": "Environment name"
        },
        "overrides": {
            "type": "object",
            "patternProperties": {
                r"^[a-zA-Z0-9_.[\]]+$": {}  # Allow any configuration path
            },
            "additionalProperties": False,
            "description": "Configuration overrides for this environment"
        }
    },
    "required": ["name"],
    "additionalProperties": False
}


# Complete NAQ configuration schema
NAQ_CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "NAQ Configuration",
    "description": "Complete NAQ configuration schema",
    "type": "object",
    "properties": {
        "nats": NATS_SCHEMA,
        "queues": QUEUES_SCHEMA,
        "workers": WORKERS_SCHEMA,
        "scheduler": SCHEDULER_SCHEMA,
        "events": EVENTS_SCHEMA,
        "results": RESULTS_SCHEMA,
        "serialization": SERIALIZATION_SCHEMA,
        "logging": LOGGING_SCHEMA,
        "environments": {
            "type": "object",
            "patternProperties": {
                r"^[a-zA-Z0-9_.-]+$": ENVIRONMENT_SCHEMA
            },
            "additionalProperties": False,
            "description": "Environment-specific configuration overrides"
        }
    },
    "additionalProperties": False
}


def get_schema() -> Dict[str, Any]:
    """
    Get the complete NAQ configuration JSON schema.
    
    Returns:
        Complete JSON schema for NAQ configuration
    """
    return NAQ_CONFIG_SCHEMA


def get_nats_schema() -> Dict[str, Any]:
    """Get NATS configuration schema."""
    return NATS_SCHEMA


def get_workers_schema() -> Dict[str, Any]:
    """Get workers configuration schema."""
    return WORKERS_SCHEMA


def get_events_schema() -> Dict[str, Any]:
    """Get events configuration schema."""
    return EVENTS_SCHEMA


def get_scheduler_schema() -> Dict[str, Any]:
    """Get scheduler configuration schema."""
    return SCHEDULER_SCHEMA


def get_logging_schema() -> Dict[str, Any]:
    """Get logging configuration schema."""
    return LOGGING_SCHEMA


# Schema validation error messages
VALIDATION_ERROR_MESSAGES = {
    "required": "Required field '{field}' is missing",
    "type": "Field '{field}' must be of type {expected_type}",
    "minimum": "Field '{field}' must be at least {minimum}",
    "maximum": "Field '{field}' must be at most {maximum}",
    "minLength": "Field '{field}' must be at least {minLength} characters long",
    "maxLength": "Field '{field}' must be at most {maxLength} characters long",
    "pattern": "Field '{field}' does not match required pattern",
    "enum": "Field '{field}' must be one of: {enum_values}",
    "minItems": "Field '{field}' must have at least {minItems} items",
    "maxItems": "Field '{field}' must have at most {maxItems} items",
}


def format_validation_error(error_msg: str, context: Dict[str, Any] = None) -> str:
    """
    Format validation error message with context.
    
    Args:
        error_msg: Raw validation error message
        context: Additional context for formatting
        
    Returns:
        Formatted error message
    """
    if not context:
        return error_msg
        
    try:
        return error_msg.format(**context)
    except (KeyError, ValueError):
        return error_msg