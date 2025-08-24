"""
Configuration Integration for NAQ Services

This module provides centralized configuration management for all NAQ services
using the unified NAQConfig system. All services now use the same configuration
structure for consistency and maintainability.
"""

from typing import Optional, Any, Dict

from ..config import get_config
from ..config.types import NAQConfig
from .base import ServiceManager


class GlobalServiceConfig:
    """
    Global configuration for all NAQ services.

    This class provides access to the global NAQConfig and converts it
    to service-compatible format. It serves as the base configuration
    that can be overridden by service-specific configurations.
    """

    def __init__(self, nats_url: Optional[str] = None, queue_name: Optional[str] = None, log_level: Optional[str] = None, service_manager: Optional[ServiceManager] = None, **kwargs):
        """Initialize global service configuration."""
        self.nats_url = nats_url
        self.queue_name = queue_name
        self.log_level = log_level
        self.service_manager = service_manager
        self.custom_settings: Dict[str, Any] = {}
        
        # Add any additional kwargs to custom_settings
        for key, value in kwargs.items():
            self.custom_settings[key] = value

        # Get the global NAQConfig
        config = get_config()

        # Set defaults from NAQConfig if not provided
        if self.nats_url is None and config.nats.servers:
            self.nats_url = config.nats.servers[0]
        if self.queue_name is None and config.queues and 'default_name' in config.queues:
            self.queue_name = config.queues['default_name']
        if self.log_level is None and config.logging and 'level' in config.logging:
            self.log_level = config.logging['level']

        # Add settings from NAQConfig to custom_settings
        self._populate_custom_settings(config)

    def _populate_custom_settings(self, config: NAQConfig) -> None:
        """Populate custom settings from NAQConfig."""
        # NATS settings
        if config.nats.servers:
            self.custom_settings['nats_servers'] = config.nats.servers
        self.custom_settings['nats_client_name'] = config.nats.client_name

        # Worker settings
        self.custom_settings['worker_concurrency'] = config.workers.concurrency
        self.custom_settings['worker_heartbeat_interval'] = config.workers.heartbeat_interval
        self.custom_settings['worker_ttl'] = config.workers.ttl

        # Queue settings
        if config.queues:
            if 'ack_wait' in config.queues:
                self.custom_settings['default_ack_wait_seconds'] = config.queues['ack_wait']
            if 'default_name' in config.queues:
                self.custom_settings['default_queue_name'] = config.queues['default_name']

        # Scheduler settings
        if config.scheduler_service:
            self.custom_settings['scheduler_lock_ttl_seconds'] = config.scheduler_service.lock_ttl
            self.custom_settings['scheduler_lock_renew_interval_seconds'] = config.scheduler_service.lock_renew_interval
            # Note: max_failures and job_status_ttl are not in the new SchedulerServiceConfig
            # We'll set default values if needed
            self.custom_settings.setdefault('max_schedule_failures', 3)
            self.custom_settings.setdefault('job_status_ttl_seconds', 86400)

        # Results settings
        if config.results and 'ttl' in config.results:
            self.custom_settings['default_result_ttl_seconds'] = config.results['ttl']

        # Serialization settings
        if config.serialization:
            if 'method' in config.serialization:
                self.custom_settings['job_serializer'] = config.serialization['method']
            if 'json_encoder' in config.serialization:
                self.custom_settings['json_encoder'] = config.serialization['json_encoder']
            if 'json_decoder' in config.serialization:
                self.custom_settings['json_decoder'] = config.serialization['json_decoder']

        # Logging settings
        if config.logging:
            if 'level' in config.logging:
                self.custom_settings['log_level'] = config.logging['level']
            if 'to_file_enabled' in config.logging:
                self.custom_settings['log_to_file_enabled'] = config.logging['to_file_enabled']
            if 'file_path' in config.logging:
                self.custom_settings['log_file_path'] = config.logging['file_path']

        # Event settings
        self.custom_settings['events_enabled'] = config.events.enabled
        self.custom_settings['events_batch_size'] = config.events.batch_size
        self.custom_settings['events_flush_interval'] = config.events.flush_interval

        # Add default values for missing settings
        self.custom_settings.setdefault('naq_prefix', 'naq')
        self.custom_settings.setdefault('dependency_check_delay_seconds', 5)


# Service-specific configurations that extend GlobalServiceConfig
class ConnectionServiceConfig(GlobalServiceConfig):
    """Configuration for ConnectionService."""
    pass


class JobServiceConfig(GlobalServiceConfig):
    """Configuration for JobService."""
    pass


class WorkerServiceConfig(GlobalServiceConfig):
    """Configuration for WorkerService."""
    pass


class SchedulerServiceConfig(GlobalServiceConfig):
    """Configuration for SchedulerService."""
    pass


class StreamServiceConfig(GlobalServiceConfig):
    """Configuration for StreamService."""
    pass


class KVStoreServiceConfig(GlobalServiceConfig):
    """Configuration for KVStoreService."""
    pass


class EventServiceConfig(GlobalServiceConfig):
    """Configuration for EventService."""
    pass


def create_global_config() -> GlobalServiceConfig:
    """
    Create a global service configuration with default values.

    Returns:
        A GlobalServiceConfig instance with default values from NAQConfig.
    """
    return GlobalServiceConfig()


def create_config_from_env(service_type: str) -> GlobalServiceConfig:
    """
    Create a service configuration from environment variables.

    Args:
        service_type: The type of service to create configuration for.
                      (Currently ignored - all services use same config structure)

    Returns:
        A GlobalServiceConfig instance.
    """
    return GlobalServiceConfig()


def merge_configs(
    base_config: GlobalServiceConfig, override_config: Optional[GlobalServiceConfig] = None
) -> GlobalServiceConfig:
    """
    Merge two service configurations.

    The override_config values take precedence over base_config values.

    Args:
        base_config: The base configuration.
        override_config: The configuration to override with.

    Returns:
        A new GlobalServiceConfig instance with merged values.
    """
    if override_config is None:
        return base_config

    # Create new config with merged values
    merged_config = GlobalServiceConfig(
        nats_url=override_config.nats_url or base_config.nats_url,
        queue_name=override_config.queue_name or base_config.queue_name,
        log_level=override_config.log_level or base_config.log_level,
        service_manager=override_config.service_manager or base_config.service_manager,
    )

    # Merge custom settings
    merged_custom_settings = base_config.custom_settings.copy()
    merged_custom_settings.update(override_config.custom_settings)
    merged_config.custom_settings = merged_custom_settings

    return merged_config
