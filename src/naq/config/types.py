# src/naq/config/types.py
"""
Configuration type definitions for NAQ.

This module defines typed dataclasses for all configuration sections,
providing strong typing, validation, and easy access to configuration values.
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Union
from pathlib import Path


@dataclass
class NatsAuthConfig:
    """NATS authentication configuration."""
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    

@dataclass 
class NatsTLSConfig:
    """NATS TLS configuration."""
    enabled: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None


@dataclass
class NatsConfig:
    """NATS server connection configuration."""
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    client_name: str = "naq-client"
    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    connection_timeout: float = 10.0
    drain_timeout: float = 30.0
    flush_timeout: float = 5.0
    ping_interval: float = 120.0
    max_outstanding_pings: int = 2
    auth: Optional[NatsAuthConfig] = None
    tls: Optional[NatsTLSConfig] = None

    def get_primary_server(self) -> str:
        """Get the primary NATS server URL."""
        return self.servers[0] if self.servers else "nats://localhost:4222"


@dataclass
class QueueConfig:
    """Queue-specific configuration."""
    ack_wait: int = 60
    max_deliver: int = 3
    max_ack_pending: Optional[int] = None


@dataclass
class QueuesConfig:
    """Queue system configuration."""
    default: str = "naq_default_queue"
    prefix: str = "naq"
    configs: Dict[str, QueueConfig] = field(default_factory=dict)

    def get_queue_config(self, queue_name: str) -> QueueConfig:
        """Get configuration for a specific queue."""
        return self.configs.get(queue_name, QueueConfig())


@dataclass
class WorkerPoolConfig:
    """Worker pool configuration."""
    concurrency: int = 10
    queues: List[str] = field(default_factory=list)


@dataclass
class WorkersConfig:
    """Worker system configuration."""
    concurrency: int = 10
    heartbeat_interval: int = 15
    ttl: int = 60
    max_job_duration: int = 3600  # 1 hour
    shutdown_timeout: int = 30
    pools: Dict[str, WorkerPoolConfig] = field(default_factory=dict)

    def get_pool_config(self, pool_name: str) -> Optional[WorkerPoolConfig]:
        """Get configuration for a specific worker pool."""
        return self.pools.get(pool_name)


@dataclass
class SchedulerConfig:
    """Scheduler system configuration."""
    enabled: bool = True
    lock_ttl: int = 30
    lock_renew_interval: int = 15
    max_failures: int = 5
    scan_interval: float = 1.0


@dataclass
class EventStreamConfig:
    """Event stream configuration."""
    name: str = "NAQ_JOB_EVENTS"
    max_age: str = "168h"  # 7 days
    max_bytes: str = "1GB"
    replicas: int = 1


@dataclass
class EventFiltersConfig:
    """Event filtering configuration."""
    exclude_heartbeats: bool = True
    min_job_duration: int = 0  # ms


@dataclass
class EventsConfig:
    """Event system configuration."""
    enabled: bool = True
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 10000
    storage_url: Optional[str] = None  # Defaults to main NATS URL
    stream: EventStreamConfig = field(default_factory=EventStreamConfig)
    filters: EventFiltersConfig = field(default_factory=EventFiltersConfig)
    subject_prefix: str = "naq.jobs.events"

    def get_storage_url(self, default_nats_url: str) -> str:
        """Get the storage URL for events, falling back to default NATS URL."""
        return self.storage_url or default_nats_url


@dataclass
class ResultsConfig:
    """Results storage configuration."""
    ttl: int = 604800  # 7 days
    cleanup_interval: int = 3600  # 1 hour
    kv_bucket: str = "naq_results"


@dataclass
class JsonSerializationConfig:
    """JSON serialization configuration."""
    encoder: str = "json.JSONEncoder"
    decoder: str = "json.JSONDecoder"


@dataclass
class SerializationConfig:
    """Serialization configuration."""
    job_serializer: str = "pickle"  # or "json"
    compression: bool = False
    json: JsonSerializationConfig = field(default_factory=JsonSerializationConfig)

    def is_json_serializer(self) -> bool:
        """Check if JSON serialization is enabled."""
        return self.job_serializer.lower() == "json"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "text"  # or "json"
    file: Optional[str] = None  # None = stdout
    max_size: str = "100MB"
    backup_count: int = 5
    to_file_enabled: bool = False

    def get_effective_level(self) -> str:
        """Get the effective log level, ensuring it's uppercase."""
        return self.level.upper()

    def should_log_to_file(self) -> bool:
        """Check if logging to file is enabled."""
        return self.to_file_enabled and self.file is not None


@dataclass
class EnvironmentConfig:
    """Environment-specific configuration overrides."""
    name: str
    overrides: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NAQConfig:
    """
    Complete NAQ configuration.
    
    This is the root configuration object that contains all configuration
    sections and provides methods for accessing and manipulating configuration.
    """
    nats: NatsConfig = field(default_factory=NatsConfig)
    queues: QueuesConfig = field(default_factory=QueuesConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    events: EventsConfig = field(default_factory=EventsConfig)
    results: ResultsConfig = field(default_factory=ResultsConfig)
    serialization: SerializationConfig = field(default_factory=SerializationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    environments: Dict[str, EnvironmentConfig] = field(default_factory=dict)

    @property
    def environment(self) -> Optional[str]:
        """Get the current environment from NAQ_ENVIRONMENT."""
        return os.getenv('NAQ_ENVIRONMENT')

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NAQConfig':
        """
        Create NAQConfig from dictionary.
        
        Args:
            data: Configuration dictionary
            
        Returns:
            NAQConfig instance with values from dictionary
        """
        # Helper function to create dataclass from dict
        def create_dataclass(dataclass_type, data_dict):
            if not isinstance(data_dict, dict):
                return data_dict
                
            field_types = {f.name: f.type for f in dataclass_type.__dataclass_fields__.values()}
            kwargs = {}
            
            for key, value in data_dict.items():
                if key in field_types:
                    field_type = field_types[key]
                    
                    # Handle Optional types
                    if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
                        args = field_type.__args__
                        if len(args) == 2 and type(None) in args:
                            # This is Optional[T]
                            field_type = args[0] if args[1] is type(None) else args[1]
                    
                    # Handle nested dataclasses
                    if hasattr(field_type, '__dataclass_fields__'):
                        kwargs[key] = create_dataclass(field_type, value)
                    # Handle Dict types with dataclass values
                    elif hasattr(field_type, '__origin__') and field_type.__origin__ is dict:
                        if hasattr(field_type, '__args__') and len(field_type.__args__) == 2:
                            value_type = field_type.__args__[1]
                            if hasattr(value_type, '__dataclass_fields__'):
                                kwargs[key] = {k: create_dataclass(value_type, v) 
                                             for k, v in value.items()}
                            else:
                                kwargs[key] = value
                        else:
                            kwargs[key] = value
                    else:
                        kwargs[key] = value
                        
            return dataclass_type(**kwargs)

        return create_dataclass(cls, data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Configuration as nested dictionary
        """
        def convert_dataclass(obj):
            if hasattr(obj, '__dataclass_fields__'):
                result = {}
                for field_name in obj.__dataclass_fields__:
                    value = getattr(obj, field_name)
                    result[field_name] = convert_dataclass(value)
                return result
            elif isinstance(obj, dict):
                return {k: convert_dataclass(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_dataclass(item) for item in obj]
            else:
                return obj
                
        return convert_dataclass(self)

    def get_nested(self, path: str, default: Any = None) -> Any:
        """
        Get nested configuration value using dot notation.
        
        Args:
            path: Dot-separated path (e.g., "nats.servers[0]", "workers.concurrency")
            default: Default value if path not found
            
        Returns:
            Configuration value or default
            
        Examples:
            config.get_nested("nats.servers[0]")
            config.get_nested("workers.concurrency")
            config.get_nested("events.stream.name")
        """
        try:
            obj = self
            parts = path.split('.')
            
            for part in parts:
                # Handle array access like "servers[0]"
                if '[' in part and part.endswith(']'):
                    attr_name = part[:part.index('[')]
                    index_str = part[part.index('[') + 1:-1]
                    
                    try:
                        index = int(index_str)
                        obj = getattr(obj, attr_name)[index]
                    except (ValueError, IndexError, TypeError):
                        return default
                else:
                    obj = getattr(obj, part)
                    
            return obj
        except AttributeError:
            return default

    def set_nested(self, path: str, value: Any) -> None:
        """
        Set nested configuration value using dot notation.
        
        Args:
            path: Dot-separated path
            value: Value to set
            
        Examples:
            config.set_nested("nats.client_name", "my-client")
            config.set_nested("workers.concurrency", 20)
        """
        obj = self
        parts = path.split('.')
        
        # Navigate to parent object
        for part in parts[:-1]:
            obj = getattr(obj, part)
        
        # Set the final value
        final_part = parts[-1]
        if '[' in final_part and final_part.endswith(']'):
            attr_name = final_part[:final_part.index('[')]
            index_str = final_part[final_part.index('[') + 1:-1]
            index = int(index_str)
            
            attr_list = getattr(obj, attr_name)
            # Extend list if necessary
            while len(attr_list) <= index:
                attr_list.append(None)
            attr_list[index] = value
        else:
            setattr(obj, final_part, value)

    def apply_environment_overrides(self) -> None:
        """
        Apply environment-specific configuration overrides.
        
        If NAQ_ENVIRONMENT is set and matches a configured environment,
        applies the overrides for that environment.
        """
        env_name = self.environment
        if env_name and env_name in self.environments:
            env_config = self.environments[env_name]
            for path, value in env_config.overrides.items():
                self.set_nested(path, value)

    def validate_configuration(self) -> List[str]:
        """
        Validate configuration and return list of issues.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        issues = []
        
        # Validate NATS configuration
        if not self.nats.servers:
            issues.append("NATS servers list cannot be empty")
        
        for server in self.nats.servers:
            if not server.startswith(('nats://', 'nats-tls://')):
                issues.append(f"Invalid NATS server URL: {server}")
        
        # Validate worker configuration
        if self.workers.concurrency < 1:
            issues.append("Worker concurrency must be at least 1")
            
        if self.workers.heartbeat_interval < 1:
            issues.append("Worker heartbeat interval must be at least 1 second")
            
        # Validate events configuration
        if self.events.batch_size < 1:
            issues.append("Events batch size must be at least 1")
            
        if self.events.flush_interval < 0.1:
            issues.append("Events flush interval must be at least 0.1 seconds")
            
        # Validate serialization
        if self.serialization.job_serializer not in ('pickle', 'json'):
            issues.append(f"Invalid job serializer: {self.serialization.job_serializer}")
            
        return issues