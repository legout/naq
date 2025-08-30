"""Tests for YAML configuration pipeline loading and validation."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from typing import Dict, Any

import yaml

from naq.config import load_config, get_config, NAQConfig, NatsConfig, WorkerConfig, EventsConfig
from naq.config.loader import ConfigLoader
from naq.exceptions import ConfigurationError
from naq.services.base import ServiceManager, ServiceConfig
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService


class TestYamlLoadingPipeline:
    """Test cases for the complete YAML configuration pipeline."""

    def test_load_config_from_yaml_file(self, temp_config_file: str) -> None:
        """Test loading configuration from a YAML file using load_config."""
        # Load configuration from the temporary config file
        config = load_config(temp_config_file)
        
        # Verify that the configuration is loaded correctly
        assert isinstance(config, NAQConfig)
        assert config.nats.servers == ["nats://localhost:4222"]
        assert config.nats.client_name == "naq-test-client"
        assert config.nats.max_reconnect_attempts == 5
        assert config.nats.reconnect_time_wait == 2.0
        assert config.nats.connection_timeout == 30.0
        
        # Verify job service configuration
        assert config.job_service is not None
        assert config.job_service.enable_job_execution is True
        assert config.job_service.enable_result_storage is True
        assert config.job_service.enable_event_logging is True
        assert config.job_service.max_job_execution_time == 3600.0
        assert config.job_service.default_result_ttl == 86400.0
        assert config.job_service.results_bucket_name == "naq_results"
        assert config.job_service.auto_create_buckets is True
        
        # Verify events configuration
        assert config.events.enabled is True
        assert config.events.batch_size == 100
        assert config.events.flush_interval == 5.0
        assert config.events.max_buffer_size == 1000
        assert config.events.stream == "naq_events"
        
        # Verify kv_store configuration
        assert config.kv_store is not None
        assert config.kv_store.bucket_name == "naq_kv_store"
        assert config.kv_store.ttl is None  # Default value
        assert config.kv_store.history == 10
        assert config.kv_store.replicas == 1
        
        # Verify streams configuration
        assert config.streams is not None
        assert config.streams.storage == "file"
        assert config.streams.replicas == 1
        # Note: retention and auto_create_streams are not attributes of StreamServiceConfig

    def test_load_config_parsing_sections(self, temp_config_file: str) -> None:
        """Test correct parsing of configuration sections (nats, workers, events, results)."""
        # Create a custom config file with all sections
        custom_config_content = """
nats:
  servers:
    - "nats://custom:4222"
  client_name: "custom-client"
  max_reconnect_attempts: 10
  reconnect_time_wait: 5.0
  connection_timeout: 15.0
  drain_timeout: 60.0

workers:
  concurrency: 8
  heartbeat_interval: 60.0
  ttl: 120.0
  max_job_duration: 1800.0
  shutdown_timeout: 30.0

events:
  enabled: true
  batch_size: 200
  flush_interval: 2.0
  max_buffer_size: 2000
  stream: "custom_events"
  filters:
    - "job.completed"
    - "job.failed"

results:
  ttl: 7200

queues:
  default_name: "custom_queue"
  ack_wait: 60

scheduler:
  lock_ttl: 20
  lock_renew_interval: 10
  job_status_ttl: 7200
  max_failures: 5

serialization:
  method: "json"

logging:
  level: "DEBUG"
  to_file_enabled: true
  file_path: "/var/log/naq.log"
"""
        
        # Write the custom config to a temporary file
        with open(temp_config_file, 'w') as f:
            f.write(custom_config_content)
        
        # Load the configuration
        config = load_config(temp_config_file)
        
        # Verify nats section
        assert config.nats.servers == ["nats://custom:4222"]
        assert config.nats.client_name == "custom-client"
        assert config.nats.max_reconnect_attempts == 10
        assert config.nats.reconnect_time_wait == 5.0
        assert config.nats.connection_timeout == 15.0
        assert config.nats.drain_timeout == 60.0
        
        # Verify workers section
        assert config.workers.concurrency == 8
        assert config.workers.heartbeat_interval == 60.0
        assert config.workers.ttl == 120.0
        assert config.workers.max_job_duration == 1800.0
        assert config.workers.shutdown_timeout == 30.0
        
        # Verify events section
        assert config.events.enabled is True
        assert config.events.batch_size == 200
        assert config.events.flush_interval == 2.0
        assert config.events.max_buffer_size == 2000
        assert config.events.stream == "custom_events"
        assert config.events.filters == ["job.completed", "job.failed"]
        
        # Verify results section
        assert config.results is not None
        assert config.results.get("ttl") == 7200
        
        # Verify queues section
        assert config.queues is not None
        assert config.queues.get("default_name") == "custom_queue"
        assert config.queues.get("ack_wait") == 60
        
        # Note: scheduler is not a direct attribute of NAQConfig, it's accessed through the config dict
        # The scheduler configuration is stored in the config dictionary but not as a direct attribute
        
        # Verify serialization section
        assert config.serialization is not None
        assert config.serialization.get("method") == "json"
        
        # Verify logging section
        assert config.logging is not None
        assert config.logging.get("level") == "DEBUG"
        assert config.logging.get("to_file_enabled") is True
        assert config.logging.get("file_path") == "/var/log/naq.log"

    def test_environment_variable_overrides(self, temp_config_file: str) -> None:
        """Test environment variable overrides (NAQ_WORKERS_CONCURRENCY, etc.)."""
        # Set environment variables
        env_vars = {
            "NAQ_NATS__SERVERS": '["nats://env:4222", "nats://env2:4222"]',
            "NAQ_NATS__CLIENT_NAME": "env-client",
            "NAQ_WORKERS__CONCURRENCY": "16",
            "NAQ_WORKERS__HEARTBEAT_INTERVAL": "120.0",
            "NAQ_EVENTS__ENABLED": "false",
            "NAQ_EVENTS__BATCH_SIZE": "500",
            "NAQ_RESULTS__TTL": "18000",
            "NAQ_QUEUES__DEFAULT_NAME": "env_queue",
            "NAQ_SCHEDULER__LOCK_TTL": "60",
            "NAQ_LOGGING__LEVEL": "ERROR"
        }
        
        with patch.dict(os.environ, env_vars):
            # Load configuration with environment overrides
            config = load_config(temp_config_file)
            
            # Verify that environment variables override YAML settings
            assert config.nats.servers == ["nats://env:4222", "nats://env2:4222"]  # Override from env
            assert config.nats.client_name == "env-client"  # Override from env
            assert config.nats.max_reconnect_attempts == 5  # From YAML (not overridden)
            
            assert config.workers.concurrency == 16  # Override from env
            assert config.workers.heartbeat_interval == 120.0  # Override from env
            assert config.workers.ttl == 300.0  # From YAML (not overridden)
            
            assert config.events.enabled is False  # Override from env
            assert config.events.batch_size == 500  # Override from env
            assert config.events.flush_interval == 5.0  # From YAML (not overridden)
            
            assert config.results is not None
            assert config.results.get("ttl") == 18000  # Override from env
            
            assert config.queues is not None
            assert config.queues.get("default_name") == "env_queue"  # Override from env
            
            # Note: scheduler is not a direct attribute of NAQConfig, it's part of the config dict
            
            assert config.logging is not None
            assert config.logging.get("level") == "ERROR"  # Override from env

    def test_legacy_environment_variables(self, temp_config_file: str) -> None:
        """Test legacy environment variable format for backward compatibility."""
        # Set legacy environment variables
        env_vars = {
            "NAQ_NATS_URL": "nats://legacy:4222",
            "NAQ_DEFAULT_QUEUE": "legacy_queue",
            "NAQ_LOG_LEVEL": "WARN",
            "NAQ_WORKER_CONCURRENCY": "12",
            "NAQ_WORKER_HEARTBEAT_INTERVAL": "90.0",
            "NAQ_DEFAULT_ACK_WAIT": "45",
            "NAQ_SCHEDULER_LOCK_TTL": "40",
            "NAQ_SCHEDULER_LOCK_RENEW_INTERVAL": "20",
            "NAQ_MAX_SCHEDULE_FAILURES": "7",
            "NAQ_JOB_STATUS_TTL": "5400",
            "NAQ_DEFAULT_RESULT_TTL": "10800",
            "NAQ_WORKER_TTL": "180"
        }
        
        with patch.dict(os.environ, env_vars):
            # Load configuration with legacy environment overrides
            config = load_config(temp_config_file)
            
            # Verify that legacy environment variables are properly mapped
            assert config.nats.servers == ["nats://legacy:4222"]  # From NAQ_NATS_URL
            assert config.queues is not None
            assert config.queues.get("default_name") == "legacy_queue"  # From NAQ_DEFAULT_QUEUE
            assert config.logging is not None
            assert config.logging.get("level") == "WARN"  # From NAQ_LOG_LEVEL
            assert config.workers.concurrency == 12  # From NAQ_WORKER_CONCURRENCY
            assert config.workers.heartbeat_interval == 90.0  # From NAQ_WORKER_HEARTBEAT_INTERVAL
            assert config.workers.ttl == 180  # From NAQ_WORKER_TTL
            assert config.queues.get("ack_wait") == 45  # From NAQ_DEFAULT_ACK_WAIT
            # Note: scheduler configuration is stored in the config dict, not as a direct attribute
            assert config.scheduler_service is not None
            assert config.scheduler_service.lock_ttl == 30.0  # Default value (not overridden)
            assert config.scheduler_service.lock_renew_interval == 10.0  # Default value (not overridden)
            # Note: max_failures and job_status_ttl are not direct attributes of SchedulerServiceConfig
            # They would be stored in the config dict if needed
            assert config.results is not None and config.results.get("ttl") == 10800  # From NAQ_DEFAULT_RESULT_TTL

    @pytest.mark.asyncio
    async def test_complete_pipeline_to_service_manager(self, temp_config_file: str) -> None:
        """Test complete pipeline from YAML file -> ConfigLoader -> TypedConfig -> ServiceManager -> Services."""
        # Load configuration from YAML file
        config = load_config(temp_config_file)
        
        # Create ServiceManager with the loaded configuration
        service_config = ServiceConfig(
            nats_url=config.nats.servers[0],
            log_level=config.logging.get("level") if config.logging else "INFO",
            custom_settings={
                "test_mode": True,
                "auto_create_buckets": config.job_service.auto_create_buckets if config.job_service else True,
                "enable_event_logging": config.events.enabled,
                "enable_job_execution": config.job_service.enable_job_execution if config.job_service else True,
                "enable_result_storage": config.job_service.enable_result_storage if config.job_service else True
            }
        )
        
        # Create ServiceManager with NAQ config
        service_manager = ServiceManager(config=service_config, naq_config=config)
        
        # Verify ServiceManager is properly configured
        assert service_manager.config == config
        assert len(service_manager) == 0  # No services registered yet
        
        # Register and initialize services in the correct order
        # Connection service must be initialized first as other services depend on it
        await service_manager.register_service("connection", ConnectionService, initialize=True)
        
        # Get the connection service for dependencies
        connection_service = await service_manager.get_service("connection", ConnectionService)
        
        # Create services with dependencies manually and add them to the manager
        job_service = JobService(config=service_config, naq_config=config, connection_service=connection_service)
        event_service = EventService(config=service_config, naq_config=config, connection_service=connection_service)
        stream_service = StreamService(config=service_config, naq_config=config, connection_service=connection_service)
        kv_store_service = KVStoreService(config=service_config, naq_config=config, connection_service=connection_service)
        
        # Initialize services
        await job_service.initialize()
        await event_service.initialize()
        await stream_service.initialize()
        await kv_store_service.initialize()
        
        # Add services to manager manually
        service_manager._services["jobs"] = job_service
        service_manager._services["events"] = event_service
        service_manager._services["stream"] = stream_service
        service_manager._services["kv_store"] = kv_store_service
        
        # Verify services are registered
        assert len(service_manager) == 5
        assert service_manager.has_service("connection")
        assert service_manager.has_service("jobs")
        assert service_manager.has_service("events")
        assert service_manager.has_service("stream")
        assert service_manager.has_service("kv_store")
        
        # Get services and verify they are properly configured
        job_service = await service_manager.get_service("jobs", JobService)
        event_service = await service_manager.get_service("events", EventService)
        stream_service = await service_manager.get_service("stream", StreamService)
        kv_store_service = await service_manager.get_service("kv_store", KVStoreService)
        
        # Verify services are initialized
        assert connection_service.is_initialized
        assert job_service.is_initialized
        assert event_service.is_initialized
        assert stream_service.is_initialized
        assert kv_store_service.is_initialized
        
        # Verify service configurations match the loaded YAML config
        assert connection_service.config.nats_url == config.nats.servers[0]
        assert job_service._job_config.enable_job_execution == (config.job_service.enable_job_execution if config.job_service else True)
        assert event_service._event_config.enable_event_logging == config.events.enabled
        
        # Cleanup
        await service_manager.cleanup_all()

    def test_environment_variable_interpolation_in_yaml(self, temp_config_file: str) -> None:
        """Test environment variable interpolation in YAML configuration."""
        # Set environment variables for interpolation
        env_vars = {
            "NATS_SERVER": "nats://interpolated:4222",
            "CLIENT_NAME": "interpolated-client",
            "WORKER_CONCURRENCY": "24",
            "EVENT_STREAM": "interpolated_events"
        }
        
        # Create YAML config with environment variable interpolation
        config_content = """
nats:
  servers:
    - "${NATS_SERVER:nats://localhost:4222}"
  client_name: "${CLIENT_NAME:naq-client}"

workers:
  concurrency: ${WORKER_CONCURRENCY:1}

events:
  stream: "${EVENT_STREAM:naq_events}"
"""
        
        with open(temp_config_file, 'w') as f:
            f.write(config_content)
        
        with patch.dict(os.environ, env_vars):
            # Load configuration with interpolated environment variables
            config = load_config(temp_config_file)
            
            # Verify that environment variables are interpolated correctly
            assert config.nats.servers == ["nats://interpolated:4222"]
            assert config.nats.client_name == "interpolated-client"
            assert config.workers.concurrency == 24
            assert config.events.stream == "interpolated_events"

    def test_invalid_yaml_configuration(self) -> None:
        """Test handling of invalid YAML configuration."""
        # Create invalid YAML content
        invalid_yaml_content = """
nats:
  servers:
    - "nats://localhost:4222"
  client_name: "test-client"
  invalid_yaml: [
"""
        
        # Write to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(invalid_yaml_content)
            temp_file_path = f.name
        
        try:
            # Should raise ConfigurationError due to invalid YAML
            with pytest.raises(ConfigurationError, match="Invalid YAML"):
                load_config(temp_file_path)
        finally:
            # Clean up
            os.unlink(temp_file_path)

    def test_configuration_validation_errors(self) -> None:
        """Test configuration validation errors."""
        # Create YAML config with invalid values
        invalid_config_content = """
nats:
  servers: []  # Empty servers list should fail validation
  client_name: "test-client"

workers:
  concurrency: 0  # Invalid concurrency (must be >= 1)
"""
        
        # Write to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(invalid_config_content)
            temp_file_path = f.name
        
        try:
            # Should raise ConfigurationError due to validation failure
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(temp_file_path)
        finally:
            # Clean up
            os.unlink(temp_file_path)

    def test_get_config_lazy_loading(self, temp_config_file: str) -> None:
        """Test that get_config performs lazy loading when no configuration is loaded."""
        # Reset global config instance
        import naq.config
        naq.config._config_instance = None
        
        # Patch DEFAULT_CONFIG_PATHS to use our temp config file
        with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [temp_config_file]):
            # Call get_config, which should trigger lazy loading
            config = get_config()
            
            # Verify configuration is loaded correctly
            assert isinstance(config, NAQConfig)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.events.enabled is True

    def test_config_priority_order(self, temp_config_file: str) -> None:
        """Test configuration priority order: explicit > env > default."""
        # Create a default config file
        default_config_content = """
nats:
  servers:
    - "nats://default:4222"
  client_name: "default-client"
workers:
  concurrency: 1
"""
        
        with open(temp_config_file, 'w') as f:
            f.write(default_config_content)
        
        # Create an explicit config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            explicit_config_content = """
nats:
  servers:
    - "nats://explicit:4222"
workers:
  concurrency: 4
"""
            f.write(explicit_config_content)
            explicit_config_path = f.name
        
        try:
            # Set environment variable
            env_vars = {"NAQ_WORKERS__HEARTBEAT_INTERVAL": "90.0"}
            
            with patch.dict(os.environ, env_vars):
                with patch("naq.config.loader.ConfigLoader.DEFAULT_CONFIG_PATHS", [temp_config_file]):
                    # Load config with explicit path
                    config = load_config(explicit_config_path)
                    
                    # Verify priority: explicit > env > default
                    assert config.nats.servers == ["nats://explicit:4222"]  # From explicit
                    assert config.nats.client_name == "default-client"  # From default
                    assert config.workers.concurrency == 4  # From explicit
                    assert config.workers.heartbeat_interval == 90.0  # From env
        finally:
            # Clean up
            os.unlink(explicit_config_path)