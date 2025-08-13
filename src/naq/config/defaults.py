# src/naq/config/defaults.py
"""
Default configuration values for NAQ.

This module provides the complete default configuration that serves as
the base for all configuration loading. These values are used when no
other configuration source (YAML files, environment variables) provides
a value for a specific setting.
"""

from typing import Dict, Any


def get_default_config() -> Dict[str, Any]:
    """
    Get the complete default configuration dictionary.
    
    This represents the baseline configuration that will work for most
    development scenarios and provides sensible defaults for production use.
    
    Returns:
        Complete default configuration dictionary
    """
    return {
        "nats": {
            "servers": ["nats://localhost:4222"],
            "client_name": "naq-client",
            "max_reconnect_attempts": 5,
            "reconnect_time_wait": 2.0,
            "connection_timeout": 10.0,
            "drain_timeout": 30.0,
            "flush_timeout": 5.0,
            "ping_interval": 120.0,
            "max_outstanding_pings": 2,
            "auth": None,
            "tls": {
                "enabled": False,
                "cert_file": None,
                "key_file": None,
                "ca_file": None,
            }
        },
        
        "queues": {
            "default": "naq_default_queue",
            "prefix": "naq",
            "configs": {}  # Queue-specific configurations
        },
        
        "workers": {
            "concurrency": 10,
            "heartbeat_interval": 15,
            "ttl": 60,
            "max_job_duration": 3600,  # 1 hour
            "shutdown_timeout": 30,
            "pools": {}  # Worker pool configurations
        },
        
        "scheduler": {
            "enabled": True,
            "lock_ttl": 30,
            "lock_renew_interval": 15,
            "max_failures": 5,
            "scan_interval": 1.0
        },
        
        "events": {
            "enabled": True,
            "batch_size": 100,
            "flush_interval": 5.0,
            "max_buffer_size": 10000,
            "storage_url": None,  # Uses main NATS URL
            "subject_prefix": "naq.jobs.events",
            "stream": {
                "name": "NAQ_JOB_EVENTS",
                "max_age": "168h",  # 7 days
                "max_bytes": "1GB",
                "replicas": 1
            },
            "filters": {
                "exclude_heartbeats": True,
                "min_job_duration": 0  # ms
            }
        },
        
        "results": {
            "ttl": 604800,  # 7 days
            "cleanup_interval": 3600,  # 1 hour
            "kv_bucket": "naq_results"
        },
        
        "serialization": {
            "job_serializer": "pickle",
            "compression": False,
            "json": {
                "encoder": "json.JSONEncoder",
                "decoder": "json.JSONDecoder"
            }
        },
        
        "logging": {
            "level": "CRITICAL",  # Matches current default
            "format": "text",
            "file": None,  # stdout
            "max_size": "100MB",
            "backup_count": 5,
            "to_file_enabled": False
        },
        
        # Environment-specific overrides
        "environments": {
            "development": {
                "name": "development",
                "overrides": {
                    "logging.level": "DEBUG",
                    "events.batch_size": 10,
                    "workers.concurrency": 2
                }
            },
            
            "production": {
                "name": "production", 
                "overrides": {
                    "logging.level": "INFO",
                    "events.batch_size": 500,
                    "events.flush_interval": 1.0,
                    "workers.concurrency": 50,
                    "workers.heartbeat_interval": 30,
                    "scheduler.scan_interval": 5.0
                }
            },
            
            "testing": {
                "name": "testing",
                "overrides": {
                    "logging.level": "WARNING",
                    "events.enabled": False,
                    "scheduler.enabled": False,
                    "workers.concurrency": 1
                }
            }
        }
    }


def get_development_config() -> Dict[str, Any]:
    """
    Get configuration optimized for development.
    
    Returns:
        Development-optimized configuration dictionary
    """
    config = get_default_config()
    
    # Apply development-specific settings
    config["logging"]["level"] = "DEBUG"
    config["events"]["batch_size"] = 10
    config["workers"]["concurrency"] = 2
    config["nats"]["servers"] = ["nats://localhost:4222"]
    
    return config


def get_production_config() -> Dict[str, Any]:
    """
    Get configuration optimized for production.
    
    Returns:
        Production-optimized configuration dictionary
    """
    config = get_default_config()
    
    # Apply production-specific settings
    config["logging"]["level"] = "INFO"
    config["logging"]["format"] = "json"
    config["events"]["batch_size"] = 500
    config["events"]["flush_interval"] = 1.0
    config["workers"]["concurrency"] = 50
    config["workers"]["heartbeat_interval"] = 30
    config["scheduler"]["scan_interval"] = 5.0
    
    # Production typically uses multiple NATS servers
    config["nats"]["servers"] = [
        "nats://nats-1:4222",
        "nats://nats-2:4222", 
        "nats://nats-3:4222"
    ]
    
    return config


def get_testing_config() -> Dict[str, Any]:
    """
    Get configuration optimized for testing.
    
    Returns:
        Testing-optimized configuration dictionary
    """
    config = get_default_config()
    
    # Apply testing-specific settings
    config["logging"]["level"] = "WARNING"
    config["events"]["enabled"] = False
    config["scheduler"]["enabled"] = False
    config["workers"]["concurrency"] = 1
    config["workers"]["heartbeat_interval"] = 5
    config["results"]["ttl"] = 60  # Short TTL for tests
    
    return config


def get_minimal_config() -> Dict[str, Any]:
    """
    Get minimal configuration for basic operation.
    
    This configuration includes only the essential settings needed
    for NAQ to function, useful for embedded use cases or when
    minimal resource usage is required.
    
    Returns:
        Minimal configuration dictionary
    """
    return {
        "nats": {
            "servers": ["nats://localhost:4222"],
            "client_name": "naq-client"
        },
        "queues": {
            "default": "naq_default_queue",
            "prefix": "naq"
        },
        "workers": {
            "concurrency": 1
        },
        "scheduler": {
            "enabled": False
        },
        "events": {
            "enabled": False
        },
        "serialization": {
            "job_serializer": "pickle"
        },
        "logging": {
            "level": "ERROR"
        }
    }


# Environment variable to configuration path mappings
ENVIRONMENT_VARIABLE_MAPPINGS = {
    # NATS configuration
    "NAQ_NATS_URL": "nats.servers[0]",
    "NAQ_CLIENT_NAME": "nats.client_name",
    "NAQ_MAX_RECONNECT_ATTEMPTS": "nats.max_reconnect_attempts",
    "NAQ_RECONNECT_TIME_WAIT": "nats.reconnect_time_wait",
    "NAQ_CONNECTION_TIMEOUT": "nats.connection_timeout",
    "NAQ_DRAIN_TIMEOUT": "nats.drain_timeout",
    
    # Queue configuration
    "NAQ_DEFAULT_QUEUE": "queues.default",
    "NAQ_QUEUE_PREFIX": "queues.prefix",
    
    # Worker configuration
    "NAQ_WORKER_CONCURRENCY": "workers.concurrency",
    "NAQ_WORKER_HEARTBEAT_INTERVAL": "workers.heartbeat_interval",
    "NAQ_WORKER_TTL": "workers.ttl",
    "NAQ_MAX_JOB_DURATION": "workers.max_job_duration",
    "NAQ_WORKER_SHUTDOWN_TIMEOUT": "workers.shutdown_timeout",
    
    # Scheduler configuration
    "NAQ_SCHEDULER_ENABLED": "scheduler.enabled",
    "NAQ_SCHEDULER_LOCK_TTL": "scheduler.lock_ttl",
    "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL": "scheduler.lock_renew_interval",
    "NAQ_MAX_SCHEDULE_FAILURES": "scheduler.max_failures",
    "NAQ_SCHEDULER_SCAN_INTERVAL": "scheduler.scan_interval",
    
    # Events configuration
    "NAQ_EVENTS_ENABLED": "events.enabled",
    "NAQ_EVENT_LOGGER_BATCH_SIZE": "events.batch_size",
    "NAQ_EVENT_LOGGER_FLUSH_INTERVAL": "events.flush_interval",
    "NAQ_EVENT_LOGGER_MAX_BUFFER_SIZE": "events.max_buffer_size",
    "NAQ_EVENT_STORAGE_URL": "events.storage_url",
    "NAQ_EVENT_STREAM_NAME": "events.stream.name",
    "NAQ_EVENT_SUBJECT_PREFIX": "events.subject_prefix",
    
    # Results configuration
    "NAQ_DEFAULT_RESULT_TTL": "results.ttl",
    "NAQ_RESULT_CLEANUP_INTERVAL": "results.cleanup_interval",
    "NAQ_RESULT_KV_BUCKET": "results.kv_bucket",
    
    # Serialization configuration
    "NAQ_JOB_SERIALIZER": "serialization.job_serializer",
    "NAQ_JSON_ENCODER": "serialization.json.encoder",
    "NAQ_JSON_DECODER": "serialization.json.decoder",
    
    # Logging configuration
    "NAQ_LOG_LEVEL": "logging.level",
    "NAQ_LOG_FORMAT": "logging.format",
    "NAQ_LOG_FILE_PATH": "logging.file",
    "NAQ_LOG_TO_FILE_ENABLED": "logging.to_file_enabled",
    "NAQ_LOG_MAX_SIZE": "logging.max_size",
    "NAQ_LOG_BACKUP_COUNT": "logging.backup_count",
    
    # Legacy mappings for backward compatibility
    "NAQ_DEFAULT_ACK_WAIT": "queues.configs.default.ack_wait",
    "NAQ_JOB_STATUS_TTL": "results.ttl",  # Maps to same as NAQ_DEFAULT_RESULT_TTL
}


def get_environment_mapping() -> Dict[str, str]:
    """
    Get mapping of environment variables to configuration paths.
    
    Returns:
        Dictionary mapping environment variable names to config paths
    """
    return ENVIRONMENT_VARIABLE_MAPPINGS.copy()


def get_config_template(environment: str = "development") -> Dict[str, Any]:
    """
    Get configuration template for a specific environment.
    
    Args:
        environment: Target environment (development, production, testing)
        
    Returns:
        Configuration template dictionary
    """
    if environment == "production":
        return get_production_config()
    elif environment == "testing":
        return get_testing_config()
    elif environment == "minimal":
        return get_minimal_config()
    else:
        return get_development_config()


# Backward compatibility alias for tests
DEFAULT_CONFIG = get_default_config()