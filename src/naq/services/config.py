"""
Configuration Integration for NAQ Services

This module provides centralized configuration management for all NAQ services,
integrating with the global settings and providing service-specific configuration
classes that inherit from the base ServiceConfig.
"""

import os
from typing import Any, Dict, Optional, Union

import msgspec
from loguru import logger

from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_QUEUE_NAME,
    LOG_LEVEL,
    DEFAULT_ACK_WAIT_SECONDS,
    ACK_WAIT_PER_QUEUE,
    DEFAULT_WORKER_TTL_SECONDS,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_RESULT_TTL_SECONDS,
    JOB_STATUS_TTL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    MAX_SCHEDULE_FAILURES,
    JOB_SERIALIZER,
    JSON_ENCODER,
    JSON_DECODER,
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_KEY,
    JOB_STATUS_KV_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
    FAILED_JOB_STREAM_NAME,
    RESULT_KV_NAME,
    WORKER_KV_NAME,
    NAQ_PREFIX,
    DEPENDENCY_CHECK_DELAY_SECONDS,
)
from .base import ServiceConfig


class GlobalServiceConfig(ServiceConfig):
    """
    Global configuration for all NAQ services.
    
    This configuration class integrates with the global settings and provides
    default values for all services. It serves as the base configuration
    that can be overridden by service-specific configurations.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up global settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.queue_name is None:
            self.queue_name = DEFAULT_QUEUE_NAME
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add global settings to custom_settings for easy access
        self.custom_settings.update({
            "default_ack_wait_seconds": DEFAULT_ACK_WAIT_SECONDS,
            "ack_wait_per_queue": ACK_WAIT_PER_QUEUE,
            "default_worker_ttl_seconds": DEFAULT_WORKER_TTL_SECONDS,
            "default_worker_heartbeat_interval_seconds": DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
            "default_result_ttl_seconds": DEFAULT_RESULT_TTL_SECONDS,
            "job_status_ttl_seconds": JOB_STATUS_TTL_SECONDS,
            "scheduler_lock_ttl_seconds": SCHEDULER_LOCK_TTL_SECONDS,
            "scheduler_lock_renew_interval_seconds": SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
            "max_schedule_failures": MAX_SCHEDULE_FAILURES,
            "job_serializer": JOB_SERIALIZER,
            "json_encoder": JSON_ENCODER,
            "json_decoder": JSON_DECODER,
            "scheduled_jobs_kv_name": SCHEDULED_JOBS_KV_NAME,
            "scheduler_lock_kv_name": SCHEDULER_LOCK_KV_NAME,
            "scheduler_lock_key": SCHEDULER_LOCK_KEY,
            "job_status_kv_name": JOB_STATUS_KV_NAME,
            "failed_job_subject_prefix": FAILED_JOB_SUBJECT_PREFIX,
            "failed_job_stream_name": FAILED_JOB_STREAM_NAME,
            "result_kv_name": RESULT_KV_NAME,
            "worker_kv_name": WORKER_KV_NAME,
            "naq_prefix": NAQ_PREFIX,
            "dependency_check_delay_seconds": DEPENDENCY_CHECK_DELAY_SECONDS,
        })


class ConnectionServiceConfig(ServiceConfig):
    """
    Configuration for the ConnectionService.
    
    Extends the base ServiceConfig with connection-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up connection-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.log_level is None:
            self.log_level = LOG_LEVEL


class JobServiceConfig(ServiceConfig):
    """
    Configuration for the JobService.
    
    Extends the base ServiceConfig with job-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up job-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.queue_name is None:
            self.queue_name = DEFAULT_QUEUE_NAME
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add job-specific settings
        self.custom_settings.update({
            "job_serializer": JOB_SERIALIZER,
            "json_encoder": JSON_ENCODER,
            "json_decoder": JSON_DECODER,
            "default_ack_wait_seconds": DEFAULT_ACK_WAIT_SECONDS,
            "ack_wait_per_queue": ACK_WAIT_PER_QUEUE,
            "result_ttl_seconds": DEFAULT_RESULT_TTL_SECONDS,
            "job_status_ttl_seconds": JOB_STATUS_TTL_SECONDS,
            "failed_job_subject_prefix": FAILED_JOB_SUBJECT_PREFIX,
            "failed_job_stream_name": FAILED_JOB_STREAM_NAME,
            "result_kv_name": RESULT_KV_NAME,
            "job_status_kv_name": JOB_STATUS_KV_NAME,
            "naq_prefix": NAQ_PREFIX,
        })


class WorkerServiceConfig(ServiceConfig):
    """
    Configuration for the WorkerService.
    
    Extends the base ServiceConfig with worker-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up worker-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.queue_name is None:
            self.queue_name = DEFAULT_QUEUE_NAME
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add worker-specific settings
        self.custom_settings.update({
            "default_ack_wait_seconds": DEFAULT_ACK_WAIT_SECONDS,
            "ack_wait_per_queue": ACK_WAIT_PER_QUEUE,
            "worker_ttl_seconds": DEFAULT_WORKER_TTL_SECONDS,
            "worker_heartbeat_interval_seconds": DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
            "worker_kv_name": WORKER_KV_NAME,
            "job_serializer": JOB_SERIALIZER,
            "json_encoder": JSON_ENCODER,
            "json_decoder": JSON_DECODER,
            "naq_prefix": NAQ_PREFIX,
        })


class SchedulerServiceConfig(ServiceConfig):
    """
    Configuration for the SchedulerService.
    
    Extends the base ServiceConfig with scheduler-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up scheduler-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.queue_name is None:
            self.queue_name = DEFAULT_QUEUE_NAME
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add scheduler-specific settings
        self.custom_settings.update({
            "scheduled_jobs_kv_name": SCHEDULED_JOBS_KV_NAME,
            "scheduler_lock_kv_name": SCHEDULER_LOCK_KV_NAME,
            "scheduler_lock_key": SCHEDULER_LOCK_KEY,
            "scheduler_lock_ttl_seconds": SCHEDULER_LOCK_TTL_SECONDS,
            "scheduler_lock_renew_interval_seconds": SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
            "max_schedule_failures": MAX_SCHEDULE_FAILURES,
            "job_status_kv_name": JOB_STATUS_KV_NAME,
            "job_status_ttl_seconds": JOB_STATUS_TTL_SECONDS,
            "dependency_check_delay_seconds": DEPENDENCY_CHECK_DELAY_SECONDS,
            "job_serializer": JOB_SERIALIZER,
            "json_encoder": JSON_ENCODER,
            "json_decoder": JSON_DECODER,
            "naq_prefix": NAQ_PREFIX,
        })


class StreamServiceConfig(ServiceConfig):
    """
    Configuration for the StreamService.
    
    Extends the base ServiceConfig with stream-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up stream-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add stream-specific settings
        self.custom_settings.update({
            "naq_prefix": NAQ_PREFIX,
            "failed_job_subject_prefix": FAILED_JOB_SUBJECT_PREFIX,
            "failed_job_stream_name": FAILED_JOB_STREAM_NAME,
        })


class KVStoreServiceConfig(ServiceConfig):
    """
    Configuration for the KVStoreService.
    
    Extends the base ServiceConfig with KV store-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up KV store-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add KV store-specific settings
        self.custom_settings.update({
            "scheduled_jobs_kv_name": SCHEDULED_JOBS_KV_NAME,
            "scheduler_lock_kv_name": SCHEDULER_LOCK_KV_NAME,
            "job_status_kv_name": JOB_STATUS_KV_NAME,
            "result_kv_name": RESULT_KV_NAME,
            "worker_kv_name": WORKER_KV_NAME,
            "job_status_ttl_seconds": JOB_STATUS_TTL_SECONDS,
            "result_ttl_seconds": DEFAULT_RESULT_TTL_SECONDS,
            "worker_ttl_seconds": DEFAULT_WORKER_TTL_SECONDS,
            "naq_prefix": NAQ_PREFIX,
        })


class EventServiceConfig(ServiceConfig):
    """
    Configuration for the EventService.
    
    Extends the base ServiceConfig with event-specific settings.
    """
    
    def __post_init__(self) -> None:
        """
        Post-initialization hook to set up event-specific settings.
        """
        # Set defaults from global settings if not provided
        if self.nats_url is None:
            self.nats_url = DEFAULT_NATS_URL
        if self.log_level is None:
            self.log_level = LOG_LEVEL
            
        # Add event-specific settings
        self.custom_settings.update({
            "naq_prefix": NAQ_PREFIX,
            "enable_event_logging": os.getenv("NAQ_ENABLE_EVENT_LOGGING", "false").lower() == "true",
        })


def create_global_config() -> GlobalServiceConfig:
    """
    Create a global service configuration with default values.
    
    Returns:
        A GlobalServiceConfig instance with default values from settings.
    """
    return GlobalServiceConfig()


def create_config_from_env(service_type: str) -> ServiceConfig:
    """
    Create a service configuration from environment variables.
    
    Args:
        service_type: The type of service to create configuration for.
                     Should be one of: 'connection', 'job', 'worker', 
                     'scheduler', 'stream', 'kv', 'event'.
    
    Returns:
        A ServiceConfig instance appropriate for the service type.
    
    Raises:
        ValueError: If the service_type is not recognized.
    """
    # Get common settings from environment
    nats_url = os.getenv("NAQ_NATS_URL", DEFAULT_NATS_URL)
    queue_name = os.getenv("NAQ_DEFAULT_QUEUE", DEFAULT_QUEUE_NAME)
    log_level = os.getenv("NAQ_LOG_LEVEL", LOG_LEVEL)
    
    # Create service-specific configuration
    if service_type == "connection":
        return ConnectionServiceConfig(
            nats_url=nats_url,
            log_level=log_level,
        )
    elif service_type == "job":
        return JobServiceConfig(
            nats_url=nats_url,
            queue_name=queue_name,
            log_level=log_level,
        )
    elif service_type == "worker":
        return WorkerServiceConfig(
            nats_url=nats_url,
            queue_name=queue_name,
            log_level=log_level,
        )
    elif service_type == "scheduler":
        return SchedulerServiceConfig(
            nats_url=nats_url,
            queue_name=queue_name,
            log_level=log_level,
        )
    elif service_type == "stream":
        return StreamServiceConfig(
            nats_url=nats_url,
            log_level=log_level,
        )
    elif service_type == "kv":
        return KVStoreServiceConfig(
            nats_url=nats_url,
            log_level=log_level,
        )
    elif service_type == "event":
        return EventServiceConfig(
            nats_url=nats_url,
            log_level=log_level,
        )
    else:
        raise ValueError(f"Unknown service type: {service_type}")


def merge_configs(
    base_config: ServiceConfig, 
    override_config: Optional[ServiceConfig] = None
) -> ServiceConfig:
    """
    Merge two service configurations.
    
    The override_config values take precedence over base_config values.
    
    Args:
        base_config: The base configuration.
        override_config: The configuration to override with.
    
    Returns:
        A new ServiceConfig instance with merged values.
    """
    if override_config is None:
        return base_config
    
    # Create new config with base values
    merged_config = ServiceConfig(
        nats_url=override_config.nats_url or base_config.nats_url,
        queue_name=override_config.queue_name or base_config.queue_name,
        log_level=override_config.log_level or base_config.log_level,
    )
    
    # Merge custom settings
    merged_custom_settings = base_config.custom_settings.copy()
    merged_custom_settings.update(override_config.custom_settings)
    merged_config.custom_settings = merged_custom_settings
    
    return merged_config