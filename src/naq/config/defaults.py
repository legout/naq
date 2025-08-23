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
    "queues": {},
    "scheduler": {},
    "results": {},
    "serialization": {},
    "logging": {},
}
