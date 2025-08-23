"""Configuration data types for NAQ."""

import os
from typing import Optional

from msgspec import Struct


class NatsConfig(Struct):
    """Configuration for NATS connection settings.

    This class defines the configuration parameters for connecting to NATS,
    including server addresses, authentication, and connection behavior.
    """

    servers: list[str]
    client_name: str
    max_reconnect_attempts: int
    reconnect_time_wait: float
    connection_timeout: float
    drain_timeout: float
    auth: Optional[dict] = None
    tls: Optional[dict] = None


class WorkerConfig(Struct):
    """Configuration for worker settings.

    This class defines the configuration parameters for worker behavior,
    including concurrency, heartbeat intervals, and timeouts.
    """

    concurrency: int
    heartbeat_interval: float
    ttl: float
    max_job_duration: float
    shutdown_timeout: float
    pools: Optional[dict] = None


class EventsConfig(Struct):
    """Configuration for event processing.

    This class defines the configuration parameters for event processing,
    including batching, flushing, and filtering.
    """

    enabled: bool
    batch_size: int
    flush_interval: float
    max_buffer_size: int
    stream: str
    filters: Optional[list[str]] = None


class NAQConfig(Struct):
    """Main configuration class for NAQ.

    This class aggregates all configuration sections for the NAQ system,
    including NATS connection, worker settings, event processing, and
    other configurable components.
    """

    nats: NatsConfig
    workers: WorkerConfig
    events: EventsConfig
    queues: Optional[dict] = None
    scheduler: Optional[dict] = None
    results: Optional[dict] = None
    serialization: Optional[dict] = None
    logging: Optional[dict] = None
    database: Optional[dict] = None

    @property
    def environment(self) -> Optional[str]:
        """Get the current environment from the NAQ_ENVIRONMENT environment variable.

        Returns:
            The environment name (e.g., 'development', 'production') or None if not set.
        """
        return os.getenv("NAQ_ENVIRONMENT")
