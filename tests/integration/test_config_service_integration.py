"""Integration tests for service classes consuming configuration."""

import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from naq.config import load_config, get_config, reload_config
from naq.services.base import ServiceConfig, ServiceManager
from naq.services.config import (
    GlobalServiceConfig,
    ConnectionServiceConfig,
    JobServiceConfig,
    WorkerServiceConfig,
    SchedulerServiceConfig,
    StreamServiceConfig,
    KVStoreServiceConfig,
    EventServiceConfig,
    create_config_from_env,
    merge_configs,
)
from naq.services.jobs import JobService, JobServiceConfig as JobServiceSpecificConfig


class TestConfigServiceIntegration:
    """Test integration between configuration system and service classes."""

    def test_global_service_config_uses_settings(self):
        """Test that GlobalServiceConfig uses values from settings."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_DEFAULT_QUEUE"] = "env_queue"
        os.environ["NAQ_LOG_LEVEL"] = "DEBUG"
        
        try:
            # Create a config file with different values
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file = Path(temp_dir) / "config.yaml"
                config_file.write_text("""
nats:
  servers: nats://config-server:4222
queues:
  default_name: config_queue
logging:
  level: INFO
""")
                
                # Load config
                load_config(str(config_file))
                
                # Create GlobalServiceConfig
                global_config = GlobalServiceConfig()
                
                # Should use environment variables (higher priority)
                assert global_config.nats_url == "nats://env-server:4222"
                assert global_config.queue_name == "env_queue"
                assert global_config.log_level == "DEBUG"
                
                # Check that custom_settings are populated
                assert "default_ack_wait_seconds" in global_config.custom_settings
                assert "job_serializer" in global_config.custom_settings
                assert "naq_prefix" in global_config.custom_settings
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
                if key in os.environ:
                    del os.environ[key]
            reload_config()

    def test_service_configs_use_settings(self):
        """Test that all service configs use values from settings."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_DEFAULT_QUEUE"] = "env_queue"
        os.environ["NAQ_LOG_LEVEL"] = "DEBUG"
        
        try:
            # Load default config
            reload_config()
            
            # Test all service configs
            configs = [
                ConnectionServiceConfig(),
                JobServiceConfig(),
                WorkerServiceConfig(),
                SchedulerServiceConfig(),
                StreamServiceConfig(),
                KVStoreServiceConfig(),
                EventServiceConfig(),
            ]
            
            for config in configs:
                # Should use environment variables
                assert config.nats_url == "nats://env-server:4222"
                assert config.log_level == "DEBUG"
                
                # Queue-specific configs should have queue name
                if hasattr(config, 'queue_name'):
                    assert config.queue_name == "env_queue"
                
                # Check that custom_settings are populated
                assert len(config.custom_settings) > 0
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
                if key in os.environ:
                    del os.environ[key]
            reload_config()

    def test_create_config_from_env(self):
        """Test create_config_from_env function."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_DEFAULT_QUEUE"] = "env_queue"
        os.environ["NAQ_LOG_LEVEL"] = "DEBUG"
        
        try:
            # Test all service types
            service_types = [
                "connection",
                "job",
                "worker",
                "scheduler",
                "stream",
                "kv",
                "event",
            ]
            
            for service_type in service_types:
                config = create_config_from_env(service_type)
                
                # Should use environment variables
                assert config.nats_url == "nats://env-server:4222"
                assert config.log_level == "DEBUG"
                
                # Queue-specific configs should have queue name
                if service_type in ["job", "worker", "scheduler"]:
                    assert config.queue_name == "env_queue"
                
                # Check that custom_settings are populated
                assert len(config.custom_settings) > 0
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_DEFAULT_QUEUE", "NAQ_LOG_LEVEL"]:
                if key in os.environ:
                    del os.environ[key]

    def test_create_config_from_env_invalid_type(self):
        """Test create_config_from_env with invalid service type."""
        with pytest.raises(ValueError, match="Unknown service type: invalid"):
            create_config_from_env("invalid")

    def test_merge_configs(self):
        """Test merge_configs function."""
        # Create base config
        base_config = ServiceConfig(
            nats_url="nats://base-server:4222",
            queue_name="base_queue",
            log_level="INFO"
        )
        base_config.custom_settings = {"key1": "value1", "key2": "value2"}
        
        # Create override config
        override_config = ServiceConfig(
            nats_url="nats://override-server:4222",
            log_level="DEBUG"
        )
        override_config.custom_settings = {"key2": "override_value2", "key3": "value3"}
        
        # Merge configs
        merged_config = merge_configs(base_config, override_config)
        
        # Should use override values where provided
        assert merged_config.nats_url == "nats://override-server:4222"
        assert merged_config.queue_name == "base_queue"  # From base
        assert merged_config.log_level == "DEBUG"
        
        # Custom settings should be merged
        assert merged_config.custom_settings["key1"] == "value1"  # From base
        assert merged_config.custom_settings["key2"] == "override_value2"  # From override
        assert merged_config.custom_settings["key3"] == "value3"  # From override

    def test_merge_configs_none_override(self):
        """Test merge_configs with None override."""
        # Create base config
        base_config = ServiceConfig(
            nats_url="nats://base-server:4222",
            queue_name="base_queue",
            log_level="INFO"
        )
        base_config.custom_settings = {"key1": "value1"}
        
        # Merge with None override
        merged_config = merge_configs(base_config, None)
        
        # Should be the same as base config
        assert merged_config.nats_url == "nats://base-server:4222"
        assert merged_config.queue_name == "base_queue"
        assert merged_config.log_level == "INFO"
        assert merged_config.custom_settings["key1"] == "value1"

    @pytest.mark.asyncio
    async def test_job_service_uses_config(self):
        """Test that JobService correctly uses configuration."""
        # Create a custom config
        service_config = ServiceConfig(
            nats_url="nats://test-server:4222",
            queue_name="test_queue",
            log_level="DEBUG"
        )
        service_config.custom_settings = {
            "results_bucket_name": "test_results",
            "default_result_ttl": 3600,
            "enable_job_execution": True,
            "enable_result_storage": True,
            "enable_event_logging": False,
            "auto_create_buckets": True,
            "max_job_execution_time": 300,
        }
        
        # Create JobService with config
        job_service = JobService(config=service_config)
        
        # Check that the service extracted the config correctly
        job_config = job_service.job_config
        assert job_config.results_bucket_name == "test_results"
        assert job_config.default_result_ttl == 3600
        assert job_config.enable_job_execution is True
        assert job_config.enable_result_storage is True
        assert job_config.enable_event_logging is False
        assert job_config.auto_create_buckets is True
        assert job_config.max_job_execution_time == 300

    @pytest.mark.asyncio
    async def test_service_manager_with_config(self):
        """Test that ServiceManager correctly handles configuration."""
        # Create a config
        manager_config = ServiceConfig(
            nats_url="nats://manager-server:4222",
            log_level="INFO"
        )
        
        # Create ServiceManager with config
        manager = ServiceManager(config=manager_config)
        
        # Create a mock service class
        class MockService:
            def __init__(self, config: ServiceConfig):
                self.config = config
                self._is_initialized = False
            
            async def initialize(self):
                self._is_initialized = True
            
            @property
            def is_initialized(self):
                return self._is_initialized
        
        # Register service with custom config
        service_config = ServiceConfig(
            nats_url="nats://service-server:4222",
            log_level="DEBUG"
        )
        
        service = await manager.register_service(
            "mock_service", MockService, config=service_config
        )
        
        # Service should use its own config, not the manager's default
        assert service.config.nats_url == "nats://service-server:4222"
        assert service.config.log_level == "DEBUG"
        
        # Register another service without custom config
        service2 = await manager.register_service(
            "mock_service2", MockService
        )
        
        # Service should use manager's default config
        assert service2.config.nats_url == "nats://manager-server:4222"
        assert service2.config.log_level == "INFO"

    @pytest.mark.asyncio
    async def test_service_config_priority(self):
        """Test configuration priority across different sources."""
        # Set environment variables (highest priority)
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_LOG_LEVEL"] = "DEBUG"
        
        try:
            # Create a config file (medium priority)
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file = Path(temp_dir) / "config.yaml"
                config_file.write_text("""
nats:
  servers: nats://config-server:4222
logging:
  level: INFO
""")
                
                # Load config
                load_config(str(config_file))
                
                # Create a service config with explicit values (lowest priority)
                service_config = ServiceConfig(
                    nats_url="nats://explicit-server:4222",
                    log_level="WARNING"
                )
                
                # Create service config class that uses _get_env_or_config
                connection_config = ConnectionServiceConfig()
                
                # Should use environment variables (highest priority)
                assert connection_config.nats_url == "nats://env-server:4222"
                assert connection_config.log_level == "DEBUG"
                
                # Now test with explicit service config
                explicit_connection_config = ConnectionServiceConfig()
                explicit_connection_config.nats_url = "nats://explicit-server:4222"
                explicit_connection_config.log_level = "WARNING"
                
                # Should use explicit values
                assert explicit_connection_config.nats_url == "nats://explicit-server:4222"
                assert explicit_connection_config.log_level == "WARNING"
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_LOG_LEVEL"]:
                if key in os.environ:
                    del os.environ[key]
            reload_config()

    def test_service_config_custom_settings(self):
        """Test that service configs correctly populate custom settings."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        
        try:
            # Load default config
            reload_config()
            
            # Test JobServiceConfig custom settings
            job_config = JobServiceConfig()
            assert "job_serializer" in job_config.custom_settings
            assert "json_encoder" in job_config.custom_settings
            assert "default_ack_wait_seconds" in job_config.custom_settings
            assert "result_ttl_seconds" in job_config.custom_settings
            assert "naq_prefix" in job_config.custom_settings
            
            # Test WorkerServiceConfig custom settings
            worker_config = WorkerServiceConfig()
            assert "worker_ttl_seconds" in worker_config.custom_settings
            assert "worker_heartbeat_interval_seconds" in worker_config.custom_settings
            assert "worker_kv_name" in worker_config.custom_settings
            
            # Test SchedulerServiceConfig custom settings
            scheduler_config = SchedulerServiceConfig()
            assert "scheduled_jobs_kv_name" in scheduler_config.custom_settings
            assert "scheduler_lock_ttl_seconds" in scheduler_config.custom_settings
            assert "max_schedule_failures" in scheduler_config.custom_settings
        finally:
            # Clean up
            if "NAQ_NATS_URL" in os.environ:
                del os.environ["NAQ_NATS_URL"]
            reload_config()