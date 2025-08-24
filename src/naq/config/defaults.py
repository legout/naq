"""Default configuration values for NAQ.

This module provides sensible default values for all NAQ configuration options.
These defaults are used as the baseline configuration when no other sources
are provided, and have the lowest priority in the configuration loading order.
"""

from typing import Any, Dict

# Default configuration values for NAQ
DEFAULT_CONFIG: Dict[str, Any] = {
    "nats": {
        "servers": ["nats://localhost:4222"],
        "client_name": "naq-client",
        "max_reconnect_attempts": 5,
        "reconnect_time_wait": 2.0,
        "connection_timeout": 5.0,
        "drain_timeout": 30.0,
        "auth": None,
        "tls": None,
    },
    "workers": {
        "concurrency": 1,
        "heartbeat_interval": 30.0,
        "ttl": 60.0,
        "max_job_duration": 3600.0,
        "shutdown_timeout": 10.0,
        "pools": None,
    },
    "events": {
        "enabled": False,
        "batch_size": 100,
        "flush_interval": 5.0,
        "max_buffer_size": 1000,
        "stream": "naq_events",
        "filters": None,
    },
    "connection": {
        "servers": ["nats://localhost:4222"],
        "client_name": "naq-connection-service",
        "max_reconnect_attempts": 5,
        "reconnect_time_wait": 2.0,
        "connection_timeout": 5.0,
        "drain_timeout": 30.0,
        "auth": None,
        "tls": None,
    },
    "kv_store": {
        "bucket_name": "naq_kv_store",
        "history": 10,
        "stream_name": None,
        "ttl": None,
        "replicas": None,
    },
    "streams": {
        "stream_name": "naq_stream",
        "subjects": ["naq.>"],
        "retention_limit": None,
        "max_age": None,
        "max_msgs": None,
        "max_bytes": None,
        "replicas": None,
        "storage": None,
    },
    "job_service": {
        "enable_job_execution": True,
        "enable_result_storage": True,
        "enable_event_logging": True,
        "max_job_execution_time": 3600.0,
        "default_result_ttl": 3600.0,
        "results_bucket_name": "job_results",
        "auto_create_buckets": True,
        "default_queue": "default",
        "default_max_retries": 3,
        "default_retry_delay": 5.0,
    },
    "worker_service": {
        "worker_name": "naq-worker",
        "queues": ["default"],
        "max_concurrent_jobs": 1,
        "heartbeat_interval": 30.0,
        "ttl": 60.0,
        "max_job_duration": 3600.0,
        "shutdown_timeout": 10.0,
        "status_bucket_name": "worker_status",
        "auto_create_buckets": True,
    },
    "scheduler_service": {
        "scheduler_name": "naq-scheduler",
        "check_interval": 5.0,
        "max_concurrent_schedules": 10,
        "schedules_bucket_name": "scheduled_jobs",
        "lock_bucket_name": "scheduler_locks",
        "lock_ttl": 30.0,
        "lock_renew_interval": 10.0,
        "auto_create_buckets": True,
    },
    "queues": {},
    "scheduler": {},
    "results": {},
    "serialization": {},
    "logging": {},
}
