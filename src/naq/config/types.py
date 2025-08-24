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


class ConnectionServiceConfig(Struct):
    """Configuration for the ConnectionService.

    This class defines the configuration parameters for the ConnectionService,
    which manages NATS connections and provides connectivity to other services.
    """

    servers: list[str]
    client_name: str
    max_reconnect_attempts: int
    reconnect_time_wait: float
    connection_timeout: float
    drain_timeout: float
    auth: Optional[dict] = None
    tls: Optional[dict] = None


class KVStoreServiceConfig(Struct):
    """Configuration for the KVStoreService.

    This class defines the configuration parameters for the KVStoreService,
    which provides key-value storage functionality using NATS KV.
    """

    bucket_name: str
    history: int = 10
    stream_name: Optional[str] = None
    ttl: Optional[float] = None
    replicas: Optional[int] = None


class StreamServiceConfig(Struct):
    """Configuration for the StreamService.

    This class defines the configuration parameters for the StreamService,
    which manages NATS JetStream streams.
    """

    stream_name: str
    subjects: list[str]
    retention_limit: Optional[int] = None
    max_age: Optional[float] = None
    max_msgs: Optional[int] = None
    max_bytes: Optional[int] = None
    replicas: Optional[int] = None
    storage: Optional[str] = None


class EventServiceConfig(Struct):
    """Configuration for the EventService.

    This class defines the configuration parameters for the EventService,
    which handles event logging and retrieval.
    """

    event_bucket_name: str
    event_ttl: float = 86400.0
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 1000
    stream: str = "naq_events"
    filters: Optional[list[str]] = None


class JobServiceConfig(Struct):
    """Configuration for the JobService.

    This class defines the configuration parameters for the JobService,
    which handles job execution, result storage, and lifecycle events.
    """

    enable_job_execution: bool = True
    enable_result_storage: bool = True
    enable_event_logging: bool = True
    max_job_execution_time: float = 3600.0
    default_result_ttl: float = 3600.0
    results_bucket_name: str = "job_results"
    auto_create_buckets: bool = True
    default_queue: str = "default"
    default_max_retries: int = 3
    default_retry_delay: float = 5.0


class WorkerServiceConfig(Struct):
    """Configuration for the WorkerService.

    This class defines the configuration parameters for the WorkerService,
    which manages worker registration, status, and job processing.
    """

    worker_name: str
    queues: list[str]
    max_concurrent_jobs: int = 1
    heartbeat_interval: float = 30.0
    ttl: float = 60.0
    max_job_duration: float = 3600.0
    shutdown_timeout: float = 10.0
    status_bucket_name: str = "worker_status"
    auto_create_buckets: bool = True


class SchedulerServiceConfig(Struct):
    """Configuration for the SchedulerService.

    This class defines the configuration parameters for the SchedulerService,
    which handles job scheduling and triggering.
    """

    scheduler_name: str
    check_interval: float = 5.0
    max_concurrent_schedules: int = 10
    schedules_bucket_name: str = "scheduled_jobs"
    lock_bucket_name: str = "scheduler_locks"
    lock_ttl: float = 30.0
    lock_renew_interval: float = 10.0
    auto_create_buckets: bool = True


class NAQConfig(Struct):
    """Main configuration class for NAQ.

    This class aggregates all configuration sections for the NAQ system,
    including NATS connection, worker settings, event processing, and
    other configurable components.
    """

    nats: NatsConfig
    workers: WorkerConfig
    events: EventsConfig
    connection: Optional[ConnectionServiceConfig] = None
    kv_store: Optional[KVStoreServiceConfig] = None
    streams: Optional[StreamServiceConfig] = None
    job_service: Optional[JobServiceConfig] = None
    worker_service: Optional[WorkerServiceConfig] = None
    scheduler_service: Optional[SchedulerServiceConfig] = None
    queues: Optional[dict] = None
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
