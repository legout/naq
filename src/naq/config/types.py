"""
Configuration Type Definitions

This module defines the types used throughout the configuration system.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Union, List
from enum import Enum
from pathlib import Path


class ConfigSource(Enum):
    """Enumeration of configuration sources"""
    DEFAULT = "default"
    YAML_FILE = "yaml_file"
    ENVIRONMENT = "environment"
    COMMAND_LINE = "command_line"


class ConfigDict(Dict[str, Any]):
    """
    Type alias for configuration dictionary
    
    This represents the configuration data structure with proper typing.
    """
    
    def get_nested(self, key_path: str, default: Any = None) -> Any:
        """
        Get a nested value using dot notation
        
        Args:
            key_path: Dot-separated path to the nested key (e.g., 'server.port')
            default: Default value if key not found
            
        Returns:
            The value at the nested key path or default
        """
        keys = key_path.split('.')
        value = self
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
                
        return value
    
    def set_nested(self, key_path: str, value: Any) -> None:
        """
        Set a nested value using dot notation
        
        Args:
            key_path: Dot-separated path to the nested key (e.g., 'server.port' or 'servers[0]')
            value: Value to set
        """
        keys = key_path.split('.')
        config = self
        
        for key in keys[:-1]:
            # Handle array indices like [0]
            if '[' in key and key.endswith(']'):
                key_name = key.split('[')[0]
                index = int(key.split('[')[1].rstrip(']'))
                
                if key_name not in config:
                    config[key_name] = []
                
                # Ensure list is long enough
                while len(config[key_name]) <= index:
                    config[key_name].append({})
                
                config = config[key_name][index]
            else:
                if key not in config:
                    config[key] = {}
                config = config[key]
        
        # Handle the last key (which might have an array index)
        last_key = keys[-1]
        if '[' in last_key and last_key.endswith(']'):
            key_name = last_key.split('[')[0]
            index = int(last_key.split('[')[1].rstrip(']'))
            
            if key_name not in config:
                config[key_name] = []
            
            # Ensure list is long enough
            while len(config[key_name]) <= index:
                config[key_name].append(None)
            
            config[key_name][index] = value
        else:
            config[last_key] = value


class ConfigLocation:
    """Represents a configuration file location"""
    
    def __init__(self, path: Union[str, Path], priority: int = 0):
        """
        Initialize configuration location
        
        Args:
            path: Path to the configuration file
            priority: Priority level (higher numbers = higher priority)
        """
        self.path = Path(path)
        self.priority = priority
    
    def __lt__(self, other: 'ConfigLocation') -> bool:
        """Compare locations by priority"""
        return self.priority < other.priority
    
    def exists(self) -> bool:
        """Check if the configuration file exists"""
        return self.path.exists()
    
    def __str__(self) -> str:
        return str(self.path)


class ConfigChange:
    """Represents a configuration change event"""
    
    def __init__(self, key_path: str, old_value: Any, new_value: Any, source: ConfigSource):
        """
        Initialize configuration change
        
        Args:
            key_path: Dot-separated path to the changed key
            old_value: Previous value
            new_value: New value
            source: Source of the change
        """
        self.key_path = key_path
        self.old_value = old_value
        self.new_value = new_value
        self.source = source
    
    def __str__(self) -> str:
        return f"ConfigChange({self.key_path}: {self.old_value} -> {self.new_value} [{self.source.value}])"


@dataclass
class NatsAuthConfig:
    """NATS authentication configuration"""
    username: str = ""
    password: str = ""
    token: str = ""


@dataclass
class NatsTLSConfig:
    """NATS TLS configuration"""
    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""
    ca_file: str = ""


@dataclass
class NatsConfig:
    """NATS connection configuration"""
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    client_name: str = "naq-client"
    max_reconnect_attempts: int = 5
    reconnect_time_wait: float = 2.0
    connection_timeout: float = 10.0
    drain_timeout: float = 30.0
    auth: Optional[NatsAuthConfig] = None
    tls: Optional[NatsTLSConfig] = None
    
    def __post_init__(self):
        if self.auth is None:
            self.auth = NatsAuthConfig()
        if self.tls is None:
            self.tls = NatsTLSConfig()


@dataclass
class WorkerPoolConfig:
    """Worker pool configuration"""
    concurrency: int = 10
    queues: List[str] = field(default_factory=list)


@dataclass
class WorkerConfig:
    """Worker behavior configuration"""
    concurrency: int = 10
    heartbeat_interval: int = 15
    ttl: int = 60
    max_job_duration: int = 3600  # 1 hour
    shutdown_timeout: int = 30
    pools: Optional[Dict[str, WorkerPoolConfig]] = None
    
    def __post_init__(self):
        if self.pools is None:
            self.pools = {}


@dataclass
class EventStreamConfig:
    """Event stream configuration"""
    name: str = "NAQ_JOB_EVENTS"
    max_age: str = "168h"  # 7 days
    max_bytes: str = "1GB"
    replicas: int = 1


@dataclass
class EventFilterConfig:
    """Event filtering configuration"""
    exclude_heartbeats: bool = True
    min_job_duration: int = 0  # ms


@dataclass
class EventsConfig:
    """Event logging configuration"""
    enabled: bool = True
    batch_size: int = 100
    flush_interval: float = 5.0
    max_buffer_size: int = 10000
    stream: EventStreamConfig = field(default_factory=EventStreamConfig)
    filters: EventFilterConfig = field(default_factory=EventFilterConfig)


@dataclass
class QueueConfig:
    """Queue configuration"""
    default: str = "naq_default_queue"
    prefix: str = "naq"
    configs: Optional[Dict[str, Dict[str, Any]]] = None
    
    def __post_init__(self):
        if self.configs is None:
            self.configs = {}


@dataclass
class SchedulerConfig:
    """Scheduler configuration"""
    enabled: bool = True
    lock_ttl: int = 30
    lock_renew_interval: int = 15
    max_failures: int = 5
    scan_interval: float = 1.0


@dataclass
class ResultsConfig:
    """Results configuration"""
    ttl: int = 604800  # 7 days
    cleanup_interval: int = 3600  # 1 hour


@dataclass
class JSONSerializationConfig:
    """JSON-specific serialization configuration"""
    encoder: str = "json.JSONEncoder"
    decoder: str = "json.JSONDecoder"


@dataclass
class SerializationConfig:
    """Serialization configuration"""
    job_serializer: str = "pickle"  # or "json"
    compression: bool = False
    json: JSONSerializationConfig = field(default_factory=JSONSerializationConfig)


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "json"  # or "text"
    file: str = ""  # empty = stdout
    max_size: str = "100MB"
    backup_count: int = 5


@dataclass
class EnvironmentConfig:
    """Environment-specific configuration"""
    development: Optional[Dict[str, Any]] = None
    production: Optional[Dict[str, Any]] = None
    testing: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.development is None:
            self.development = {}
        if self.production is None:
            self.production = {}
        if self.testing is None:
            self.testing = {}


@dataclass
class NAQConfig:
    """Main configuration container class"""
    nats: NatsConfig = field(default_factory=NatsConfig)
    workers: WorkerConfig = field(default_factory=WorkerConfig)
    events: EventsConfig = field(default_factory=EventsConfig)
    queues: QueueConfig = field(default_factory=QueueConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    results: ResultsConfig = field(default_factory=ResultsConfig)
    serialization: SerializationConfig = field(default_factory=SerializationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    environments: EnvironmentConfig = field(default_factory=EnvironmentConfig)
    
    @property
    def environment(self) -> Optional[str]:
        """Get the current environment from environment variable"""
        return os.getenv('NAQ_ENVIRONMENT')
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'NAQConfig':
        """
        Create configuration from dictionary
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            NAQConfig: Configuration instance
        """
        # Extract nested configurations
        nats_dict = config_dict.get('nats', {})
        workers_dict = config_dict.get('workers', {})
        events_dict = config_dict.get('events', {})
        queues_dict = config_dict.get('queues', {})
        scheduler_dict = config_dict.get('scheduler', {})
        results_dict = config_dict.get('results', {})
        serialization_dict = config_dict.get('serialization', {})
        logging_dict = config_dict.get('logging', {})
        environments_dict = config_dict.get('environments', {})
        
        # Create NATS config
        nats_auth_dict = nats_dict.get('auth', {})
        nats_tls_dict = nats_dict.get('tls', {})
        nats_config = NatsConfig(
            servers=nats_dict.get('servers', ["nats://localhost:4222"]),
            client_name=nats_dict.get('client_name', "naq-client"),
            max_reconnect_attempts=nats_dict.get('max_reconnect_attempts', 5),
            reconnect_time_wait=nats_dict.get('reconnect_time_wait', 2.0),
            connection_timeout=nats_dict.get('connection_timeout', 10.0),
            drain_timeout=nats_dict.get('drain_timeout', 30.0),
            auth=NatsAuthConfig(
                username=nats_auth_dict.get('username', ""),
                password=nats_auth_dict.get('password', ""),
                token=nats_auth_dict.get('token', "")
            ),
            tls=NatsTLSConfig(
                enabled=nats_tls_dict.get('enabled', False),
                cert_file=nats_tls_dict.get('cert_file', ""),
                key_file=nats_tls_dict.get('key_file', ""),
                ca_file=nats_tls_dict.get('ca_file', "")
            )
        )
        
        # Create worker config
        worker_pools_dict = workers_dict.get('pools', {})
        worker_pools = {}
        for pool_name, pool_dict in worker_pools_dict.items():
            worker_pools[pool_name] = WorkerPoolConfig(
                concurrency=pool_dict.get('concurrency', 10),
                queues=pool_dict.get('queues', [])
            )
        
        workers_config = WorkerConfig(
            concurrency=workers_dict.get('concurrency', 10),
            heartbeat_interval=workers_dict.get('heartbeat_interval', 15),
            ttl=workers_dict.get('ttl', 60),
            max_job_duration=workers_dict.get('max_job_duration', 3600),
            shutdown_timeout=workers_dict.get('shutdown_timeout', 30),
            pools=worker_pools
        )
        
        # Create events config
        events_stream_dict = events_dict.get('stream', {})
        events_filters_dict = events_dict.get('filters', {})
        events_config = EventsConfig(
            enabled=events_dict.get('enabled', True),
            batch_size=events_dict.get('batch_size', 100),
            flush_interval=events_dict.get('flush_interval', 5.0),
            max_buffer_size=events_dict.get('max_buffer_size', 10000),
            stream=EventStreamConfig(
                name=events_stream_dict.get('name', "NAQ_JOB_EVENTS"),
                max_age=events_stream_dict.get('max_age', "168h"),
                max_bytes=events_stream_dict.get('max_bytes', "1GB"),
                replicas=events_stream_dict.get('replicas', 1)
            ),
            filters=EventFilterConfig(
                exclude_heartbeats=events_filters_dict.get('exclude_heartbeats', True),
                min_job_duration=events_filters_dict.get('min_job_duration', 0)
            )
        )
        
        # Create queue config
        queues_config = QueueConfig(
            default=queues_dict.get('default', "naq_default_queue"),
            prefix=queues_dict.get('prefix', "naq"),
            configs=queues_dict.get('configs', {})
        )
        
        # Create scheduler config
        scheduler_config = SchedulerConfig(
            enabled=scheduler_dict.get('enabled', True),
            lock_ttl=scheduler_dict.get('lock_ttl', 30),
            lock_renew_interval=scheduler_dict.get('lock_renew_interval', 15),
            max_failures=scheduler_dict.get('max_failures', 5),
            scan_interval=scheduler_dict.get('scan_interval', 1.0)
        )
        
        # Create results config
        results_config = ResultsConfig(
            ttl=results_dict.get('ttl', 604800),
            cleanup_interval=results_dict.get('cleanup_interval', 3600)
        )
        
        # Create serialization config
        json_serialization_dict = serialization_dict.get('json', {})
        serialization_config = SerializationConfig(
            job_serializer=serialization_dict.get('job_serializer', "pickle"),
            compression=serialization_dict.get('compression', False),
            json=JSONSerializationConfig(
                encoder=json_serialization_dict.get('encoder', "json.JSONEncoder"),
                decoder=json_serialization_dict.get('decoder', "json.JSONDecoder")
            )
        )
        
        # Create logging config
        logging_config = LoggingConfig(
            level=logging_dict.get('level', "INFO"),
            format=logging_dict.get('format', "json"),
            file=logging_dict.get('file', ""),
            max_size=logging_dict.get('max_size', "100MB"),
            backup_count=logging_dict.get('backup_count', 5)
        )
        
        # Create environment config
        environment_config = EnvironmentConfig(
            development=environments_dict.get('development', {}),
            production=environments_dict.get('production', {}),
            testing=environments_dict.get('testing', {})
        )
        
        return cls(
            nats=nats_config,
            workers=workers_config,
            events=events_config,
            queues=queues_config,
            scheduler=scheduler_config,
            results=results_config,
            serialization=serialization_config,
            logging=logging_config,
            environments=environment_config
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        return {
            'nats': {
                'servers': self.nats.servers,
                'client_name': self.nats.client_name,
                'max_reconnect_attempts': self.nats.max_reconnect_attempts,
                'reconnect_time_wait': self.nats.reconnect_time_wait,
                'connection_timeout': self.nats.connection_timeout,
                'drain_timeout': self.nats.drain_timeout,
                'auth': {
                    'username': self.nats.auth.username if self.nats.auth else "",
                    'password': self.nats.auth.password if self.nats.auth else "",
                    'token': self.nats.auth.token if self.nats.auth else ""
                },
                'tls': {
                    'enabled': self.nats.tls.enabled if self.nats.tls else False,
                    'cert_file': self.nats.tls.cert_file if self.nats.tls else "",
                    'key_file': self.nats.tls.key_file if self.nats.tls else "",
                    'ca_file': self.nats.tls.ca_file if self.nats.tls else ""
                }
            },
            'workers': {
                'concurrency': self.workers.concurrency,
                'heartbeat_interval': self.workers.heartbeat_interval,
                'ttl': self.workers.ttl,
                'max_job_duration': self.workers.max_job_duration,
                'shutdown_timeout': self.workers.shutdown_timeout,
                'pools': {
                    pool_name: {
                        'concurrency': pool.concurrency,
                        'queues': pool.queues
                    }
                    for pool_name, pool in (self.workers.pools or {}).items()
                }
            },
            'events': {
                'enabled': self.events.enabled,
                'batch_size': self.events.batch_size,
                'flush_interval': self.events.flush_interval,
                'max_buffer_size': self.events.max_buffer_size,
                'stream': {
                    'name': self.events.stream.name,
                    'max_age': self.events.stream.max_age,
                    'max_bytes': self.events.stream.max_bytes,
                    'replicas': self.events.stream.replicas
                },
                'filters': {
                    'exclude_heartbeats': self.events.filters.exclude_heartbeats,
                    'min_job_duration': self.events.filters.min_job_duration
                }
            },
            'queues': {
                'default': self.queues.default,
                'prefix': self.queues.prefix,
                'configs': self.queues.configs
            },
            'scheduler': {
                'enabled': self.scheduler.enabled,
                'lock_ttl': self.scheduler.lock_ttl,
                'lock_renew_interval': self.scheduler.lock_renew_interval,
                'max_failures': self.scheduler.max_failures,
                'scan_interval': self.scheduler.scan_interval
            },
            'results': {
                'ttl': self.results.ttl,
                'cleanup_interval': self.results.cleanup_interval
            },
            'serialization': {
                'job_serializer': self.serialization.job_serializer,
                'compression': self.serialization.compression,
                'json': {
                    'encoder': self.serialization.json.encoder,
                    'decoder': self.serialization.json.decoder
                }
            },
            'logging': {
                'level': self.logging.level,
                'format': self.logging.format,
                'file': self.logging.file,
                'max_size': self.logging.max_size,
                'backup_count': self.logging.backup_count
            },
            'environments': {
                'development': self.environments.development,
                'production': self.environments.production,
                'testing': self.environments.testing
            }
        }