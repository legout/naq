"""
Default Configuration Values

This module defines the default configuration values for NAQ.
"""

from typing import Dict, Any
from .types import ConfigDict


def get_default_config() -> ConfigDict:
    """
    Get the default configuration for NAQ
    
    Returns:
        ConfigDict: Default configuration dictionary
    """
    return ConfigDict({
        # NATS configuration
        "nats": {
            "servers": ["nats://localhost:4222"],
            "client_name": "naq-client",
            "max_reconnect_attempts": 5,
            "reconnect_time_wait": 2.0,
            "connection_timeout": 10.0,
            "drain_timeout": 30.0,
            "auth": {
                "username": "",
                "password": "",
                "token": ""
            },
            "tls": {
                "enabled": False,
                "cert_file": "",
                "key_file": "",
                "ca_file": ""
            }
        },
        
        # Queues configuration
        "queues": {
            "default": "naq_default_queue",
            "prefix": "naq",
            "configs": {
                "high_priority": {
                    "ack_wait": 30,
                    "max_deliver": 3
                },
                "low_priority": {
                    "ack_wait": 60,
                    "max_deliver": 5
                }
            }
        },
        
        # Workers configuration
        "workers": {
            "concurrency": 10,
            "heartbeat_interval": 15,
            "ttl": 60,
            "max_job_duration": 3600,  # 1 hour
            "shutdown_timeout": 30,
            "pools": {
                "cpu_intensive": {
                    "concurrency": 2,
                    "queues": ["compute"]
                },
                "io_intensive": {
                    "concurrency": 20,
                    "queues": ["io", "network"]
                }
            }
        },
        
        # Scheduler configuration
        "scheduler": {
            "enabled": True,
            "lock_ttl": 30,
            "lock_renew_interval": 15,
            "max_failures": 5,
            "scan_interval": 1.0
        },
        
        # Events configuration
        "events": {
            "enabled": True,
            "batch_size": 100,
            "flush_interval": 5.0,
            "max_buffer_size": 10000,
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
        
        # Results configuration
        "results": {
            "ttl": 604800,  # 7 days
            "cleanup_interval": 3600  # 1 hour
        },
        
        # Serialization configuration
        "serialization": {
            "job_serializer": "pickle",  # or "json"
            "compression": False,
            "json": {
                "encoder": "json.JSONEncoder",
                "decoder": "json.JSONDecoder"
            }
        },
        
        # Logging configuration
        "logging": {
            "level": "INFO",
            "format": "json",  # or "text"
            "file": "",  # empty = stdout
            "max_size": "100MB",
            "backup_count": 5
        },
        
        # Environment-specific overrides
        "environments": {
            "development": {
                "logging": {
                    "level": "DEBUG"
                },
                "events": {
                    "batch_size": 10
                }
            },
            "production": {
                "nats": {
                    "servers": [
                        "nats://nats-1.prod:4222",
                        "nats://nats-2.prod:4222",
                        "nats://nats-3.prod:4222"
                    ]
                },
                "workers": {
                    "concurrency": 50
                },
                "events": {
                    "batch_size": 500,
                    "flush_interval": 1.0
                }
            },
            "testing": {
                "nats": {
                    "servers": ["nats://localhost:4222"]
                },
                "events": {
                    "enabled": False
                }
            }
        }
    })


def get_environment_mapping() -> Dict[str, str]:
    """
    Get mapping from environment variables to configuration paths
    
    Returns:
        Dict[str, str]: Mapping of environment variable names to config paths
    """
    return {
        # NATS
        "NAQ_NATS_URL": "nats.servers[0]",
        "NAQ_NATS_CLIENT_NAME": "nats.client_name",
        "NAQ_NATS_MAX_RECONNECT_ATTEMPTS": "nats.max_reconnect_attempts",
        "NAQ_NATS_RECONNECT_TIME_WAIT": "nats.reconnect_time_wait",
        "NAQ_NATS_CONNECTION_TIMEOUT": "nats.connection_timeout",
        "NAQ_NATS_DRAIN_TIMEOUT": "nats.drain_timeout",
        "NAQ_NATS_AUTH_USERNAME": "nats.auth.username",
        "NAQ_NATS_AUTH_PASSWORD": "nats.auth.password",
        "NAQ_NATS_AUTH_TOKEN": "nats.auth.token",
        "NAQ_NATS_TLS_ENABLED": "nats.tls.enabled",
        "NAQ_NATS_TLS_CERT_FILE": "nats.tls.cert_file",
        "NAQ_NATS_TLS_KEY_FILE": "nats.tls.key_file",
        "NAQ_NATS_TLS_CA_FILE": "nats.tls.ca_file",
        
        # Queues
        "NAQ_DEFAULT_QUEUE": "queues.default",
        "NAQ_QUEUE_PREFIX": "queues.prefix",
        
        # Workers
        "NAQ_WORKER_CONCURRENCY": "workers.concurrency",
        "NAQ_WORKER_HEARTBEAT_INTERVAL": "workers.heartbeat_interval",
        "NAQ_WORKER_TTL": "workers.ttl",
        "NAQ_WORKER_MAX_JOB_DURATION": "workers.max_job_duration",
        "NAQ_WORKER_SHUTDOWN_TIMEOUT": "workers.shutdown_timeout",
        
        # Scheduler
        "NAQ_SCHEDULER_ENABLED": "scheduler.enabled",
        "NAQ_SCHEDULER_LOCK_TTL": "scheduler.lock_ttl",
        "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL": "scheduler.lock_renew_interval",
        "NAQ_SCHEDULER_MAX_FAILURES": "scheduler.max_failures",
        "NAQ_SCHEDULER_SCAN_INTERVAL": "scheduler.scan_interval",
        
        # Events
        "NAQ_EVENTS_ENABLED": "events.enabled",
        "NAQ_EVENTS_BATCH_SIZE": "events.batch_size",
        "NAQ_EVENTS_FLUSH_INTERVAL": "events.flush_interval",
        "NAQ_EVENTS_MAX_BUFFER_SIZE": "events.max_buffer_size",
        "NAQ_EVENTS_STREAM_NAME": "events.stream.name",
        "NAQ_EVENTS_STREAM_MAX_AGE": "events.stream.max_age",
        "NAQ_EVENTS_STREAM_MAX_BYTES": "events.stream.max_bytes",
        "NAQ_EVENTS_STREAM_REPLICAS": "events.stream.replicas",
        "NAQ_EVENTS_FILTERS_EXCLUDE_HEARTBEATS": "events.filters.exclude_heartbeats",
        "NAQ_EVENTS_FILTERS_MIN_JOB_DURATION": "events.filters.min_job_duration",
        
        # Results
        "NAQ_RESULTS_TTL": "results.ttl",
        "NAQ_RESULTS_CLEANUP_INTERVAL": "results.cleanup_interval",
        
        # Serialization
        "NAQ_JOB_SERIALIZER": "serialization.job_serializer",
        "NAQ_SERIALIZATION_COMPRESSION": "serialization.compression",
        "NAQ_JSON_ENCODER": "serialization.json.encoder",
        "NAQ_JSON_DECODER": "serialization.json.decoder",
        
        # Logging
        "NAQ_LOG_LEVEL": "logging.level",
        "NAQ_LOG_FORMAT": "logging.format",
        "NAQ_LOG_FILE": "logging.file",
        "NAQ_LOG_MAX_SIZE": "logging.max_size",
        "NAQ_LOG_BACKUP_COUNT": "logging.backup_count",
        
        # Environment
        "NAQ_ENVIRONMENT": "environments.current",
    }