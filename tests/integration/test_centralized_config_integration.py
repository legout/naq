"""Integration tests for centralized configuration system with services."""

import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from naq.config import load_config, get_config, reload_config, NAQConfig, NatsConfig, WorkerConfig, EventsConfig
from naq.services.base import ServiceConfig, ServiceManager
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService
from naq.services.events import EventService
from naq.services.jobs import JobService
from naq.services.streams import StreamService
from naq.services.worker import WorkerService
from naq.services.scheduler import SchedulerService
from naq.exceptions import ConfigurationError


class TestCentralizedConfigIntegration:
    """Test integration between centralized configuration system and service classes."""

    def setup_method(self) -> None:
        """Reset global config instance before each test."""
        # Reset the global config instance
        import naq.config
        naq.config._config_instance = None

    def teardown_method(self) -> None:
        """Clean up after each test."""
        # Reset the global config instance
        import naq.config
        naq.config._config_instance = None

    def test_load_centralized_config(self) -> None:
        """Test loading centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://localhost:4222
  client_name: naq-client
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
  connection_timeout: 5.0
  drain_timeout: 30.0

workers:
  concurrency: 1
  heartbeat_interval: 30.0
  ttl: 60.0
  max_job_duration: 3600.0
  shutdown_timeout: 10.0

events:
  enabled: true
  batch_size: 100
  flush_interval: 5.0
  max_buffer_size: 1000
  stream: naq_events

queues:
  default:
    max_size: 1000
    ack_wait: 60

scheduler:
  enabled: true
  lock_ttl: 30
  lock_renew_interval: 15

results:
  ttl: 86400

serialization:
  default: pickle

logging:
  level: INFO
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Verify it's a NAQConfig instance
            assert isinstance(config, NAQConfig)
            assert config.nats.servers == ["nats://localhost:4222"]
            assert config.nats.client_name == "naq-client"
            assert config.workers.concurrency == 1
            assert config.events.enabled is True
            assert config.queues == {"default": {"max_size": 1000, "ack_wait": 60}}
            assert config.scheduler_service.enabled is True
            assert config.scheduler_service.lock_ttl == 30
            assert config.scheduler_service.lock_renew_interval == 15
            assert config.results == {"ttl": 86400}
            assert config.serialization == {"default": "pickle"}
            assert config.logging == {"level": "INFO"}

    def test_get_config_returns_centralized_config(self) -> None:
        """Test that get_config returns the centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://localhost:4222
  client_name: test-client

workers:
  concurrency: 4

events:
  enabled: true
"""
            config_path.write_text(config_content)

            # Load config
            config1 = load_config(str(config_path))

            # Get config
            config2 = get_config()

            # Should be the same instance
            assert config1 is config2
            assert isinstance(config2, NAQConfig)
            assert config2.nats.client_name == "test-client"
            assert config2.workers.concurrency == 4
            assert config2.events.enabled is True

    @pytest.mark.asyncio
    async def test_connection_service_uses_centralized_config(self) -> None:
        """Test that ConnectionService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222
  client_name: test-client
  max_reconnect_attempts: 10
  reconnect_time_wait: 5.0
  connection_timeout: 15.0
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create ConnectionService with centralized config
            connection_service = ConnectionService(config=config)

            # Verify the service extracted the config correctly
            assert connection_service.connection_config.servers == ["nats://test-server:4222"]
            assert connection_service.connection_config.client_name == "test-client"
            assert connection_service.connection_config.max_reconnect_attempts == 10
            assert connection_service.connection_config.reconnect_time_wait == 5.0
            assert connection_service.connection_config.connection_timeout == 15.0

    @pytest.mark.asyncio
    async def test_kv_store_service_uses_centralized_config(self) -> None:
        """Test that KVStoreService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

kv_store:
  bucket_name: test_kv_bucket
  history: 10
  ttl: 3600
  replicas: 1
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create KVStoreService with centralized config
            kv_store_service = KVStoreService(config=config)

            # Verify the service extracted the config correctly
            assert kv_store_service.kv_config.bucket_name == "test_kv_bucket"
            assert kv_store_service.kv_config.history == 10
            assert kv_store_service.kv_config.ttl == 3600
            assert kv_store_service.kv_config.replicas == 1

    @pytest.mark.asyncio
    async def test_event_service_uses_centralized_config(self) -> None:
        """Test that EventService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

events:
  enabled: true
  batch_size: 50
  flush_interval: 2.0
  max_buffer_size: 500
  stream: test_events
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create mock KVStoreService
            mock_kv_store = AsyncMock()

            # Create EventService with centralized config
            event_service = EventService(config=config, kv_store_service=mock_kv_store)

            # Verify the service extracted the config correctly
            assert event_service.event_config.enabled is True
            assert event_service.event_config.batch_size == 50
            assert event_service.event_config.flush_interval == 2.0
            assert event_service.event_config.max_buffer_size == 500
            assert event_service.event_config.stream == "test_events"

    @pytest.mark.asyncio
    async def test_job_service_uses_centralized_config(self) -> None:
        """Test that JobService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

job_service:
  results_bucket_name: test_results
  default_result_ttl: 7200
  enable_job_execution: true
  enable_result_storage: true
  enable_event_logging: false
  auto_create_buckets: true
  max_job_execution_time: 600
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create mock services
            mock_connection = AsyncMock()
            mock_kv_store = AsyncMock()
            mock_event = AsyncMock()

            # Create JobService with centralized config
            job_service = JobService(
                config=config,
                connection_service=mock_connection,
                kv_store_service=mock_kv_store,
                event_service=mock_event
            )

            # Verify the service extracted the config correctly
            assert job_service.job_config.results_bucket_name == "test_results"
            assert job_service.job_config.default_result_ttl == 7200
            assert job_service.job_config.enable_job_execution is True
            assert job_service.job_config.enable_result_storage is True
            assert job_service.job_config.enable_event_logging is False
            assert job_service.job_config.auto_create_buckets is True
            assert job_service.job_config.max_job_execution_time == 600

    @pytest.mark.asyncio
    async def test_stream_service_uses_centralized_config(self) -> None:
        """Test that StreamService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

streams:
  stream_name: test_stream
  subjects:
    - naq.>
  max_msgs: 10000
  max_bytes: 104857600
  max_age: 86400
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create mock connection service
            mock_connection = AsyncMock()

            # Create StreamService with centralized config
            stream_service = StreamService(config=config, connection_service=mock_connection)

            # Verify the service extracted the config correctly
            assert stream_service.stream_config.stream_name == "test_stream"
            assert stream_service.stream_config.max_msgs == 10000
            assert stream_service.stream_config.max_bytes == 104857600
            assert stream_service.stream_config.max_age == 86400

    @pytest.mark.asyncio
    async def test_worker_service_uses_centralized_config(self) -> None:
        """Test that WorkerService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

workers:
  concurrency: 4
  heartbeat_interval: 60.0
  ttl: 120.0
  max_job_duration: 7200.0
  shutdown_timeout: 30.0
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create mock services
            mock_connection = AsyncMock()
            mock_kv_store = AsyncMock()
            mock_job = AsyncMock()

            # Create WorkerService with centralized config
            worker_service = WorkerService(
                config=config,
                connection_service=mock_connection,
                kv_store_service=mock_kv_store,
                event_service=mock_job
            )

            # Verify the service extracted the config correctly
            assert worker_service.worker_config.concurrency == 4
            assert worker_service.worker_config.heartbeat_interval == 60.0
            assert worker_service.worker_config.ttl == 120.0
            assert worker_service.worker_config.max_job_duration == 7200.0
            assert worker_service.worker_config.shutdown_timeout == 30.0

    @pytest.mark.asyncio
    async def test_scheduler_service_uses_centralized_config(self) -> None:
        """Test that SchedulerService uses centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://test-server:4222

scheduler_service:
  scheduler_name: test_scheduler
  check_interval: 30
  lock_ttl: 60
  lock_renew_interval: 30
  max_concurrent_schedules: 10
  schedules_bucket_name: test_scheduled_jobs
  lock_bucket_name: scheduler_locks
  auto_create_buckets: true
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create mock services
            mock_connection = AsyncMock()
            mock_kv_store = AsyncMock()

            # Create SchedulerService with centralized config
            scheduler_service = SchedulerService(
                config=config,
                connection_service=mock_connection,
                kv_store_service=mock_kv_store
            )

            # Verify the service extracted the config correctly
            assert scheduler_service.scheduler_config.scheduler_name == "test_scheduler"
            assert scheduler_service.scheduler_config.check_interval == 30
            assert scheduler_service.scheduler_config.lock_ttl == 60
            assert scheduler_service.scheduler_config.lock_renew_interval == 30
            assert scheduler_service.scheduler_config.max_concurrent_schedules == 10
            assert scheduler_service.scheduler_config.schedules_bucket_name == "test_scheduled_jobs"
            assert scheduler_service.scheduler_config.lock_bucket_name == "scheduler_locks"
            assert scheduler_service.scheduler_config.auto_create_buckets is True

    @pytest.mark.asyncio
    async def test_service_manager_with_centralized_config(self) -> None:
        """Test that ServiceManager works with centralized configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://manager-server:4222
  client_name: manager-client

workers:
  concurrency: 2

events:
  enabled: true
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create ServiceManager with centralized config
            service_manager = ServiceManager(config=config)

            # Verify the manager has the config
            # ServiceManager doesn't have a direct config attribute, get the connection service config
            connection_service = await service_manager.get_service("connection")
            assert connection_service.connection_config.servers == ["nats://manager-server:4222"]
            assert service_manager.config.nats.client_name == "manager-client"

            # Mock the connection service to avoid actual NATS connection
            with patch('naq.services.connection.NATSConnection'):
                # Initialize all services
                await service_manager.initialize_all()

                # Verify services were created with the centralized config
                connection_service = await service_manager.get_service("connection")
                assert connection_service.connection_config.servers == ["nats://manager-server:4222"]
                assert connection_service.connection_config.client_name == "manager-client"

                worker_service = await service_manager.get_service("worker")
                assert worker_service.worker_config.concurrency == 2

                event_service = await service_manager.get_service("events")
                assert event_service.event_config.enabled is True

                # Cleanup
                await service_manager.cleanup_all()

    @pytest.mark.asyncio
    async def test_centralized_config_with_service_overrides(self) -> None:
        """Test that services can override centralized configuration values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            config_content = """
nats:
  servers:
    - nats://central-server:4222
  client_name: central-client

workers:
  concurrency: 2
"""
            config_path.write_text(config_content)

            # Load config
            config = load_config(str(config_path))

            # Create a service config with overrides
            service_config = ServiceConfig()
            service_config.nats_url = "nats://override-server:4222"
            service_config.custom_settings = {
                "concurrency": 4,  # Override the centralized value
            }

            # Create WorkerService with both centralized config and overrides
            mock_connection = AsyncMock()
            mock_kv_store = AsyncMock()
            mock_job = AsyncMock()

            worker_service = WorkerService(
                naq_config=config,
                connection_service=mock_connection,
                kv_store_service=mock_kv_store,
                event_service=mock_job
            )

            # Verify the service used the overrides
            assert worker_service.worker_config.concurrency == 4  # From service_config override
            # Note: The nats_url override would be handled by the connection service, not the worker service

    @pytest.mark.asyncio
    async def test_centralized_config_environment_overrides(self) -> None:
        """Test that environment variables override centralized configuration."""
        # Set environment variables
        os.environ["NAQ_NATS_URL"] = "nats://env-server:4222"
        os.environ["NAQ_WORKERS_CONCURRENCY"] = "8"

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                config_path = Path(temp_dir) / "naq.yaml"
                config_content = """
nats:
  servers:
    - nats://config-server:4222

workers:
  concurrency: 2
"""
                config_path.write_text(config_content)

                # Load config
                config = load_config(str(config_path))

                # Create WorkerService with centralized config
                mock_connection = AsyncMock()
                mock_kv_store = AsyncMock()
                mock_job = AsyncMock()

                worker_service = WorkerService(
                    config=config,
                    connection_service=mock_connection,
                    kv_store_service=mock_kv_store,
                    event_service=mock_job
                )

                # Verify the service used the environment overrides
                # The connection service would use the env override for NATS URL
                # The worker service would use the env override for concurrency
                assert worker_service.worker_config.concurrency == 8
        finally:
            # Clean up
            for key in ["NAQ_NATS_URL", "NAQ_WORKERS_CONCURRENCY"]:
                if key in os.environ:
                    del os.environ[key]
            reload_config()

    @pytest.mark.asyncio
    async def test_centralized_config_validation(self) -> None:
        """Test that centralized configuration validation works."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "naq.yaml"
            # Invalid config - negative concurrency
            config_content = """
nats:
  servers:
    - nats://localhost:4222

workers:
  concurrency: -1
"""
            config_path.write_text(config_content)

            # Should raise ConfigurationError when validating
            with pytest.raises(ConfigurationError, match="Configuration validation failed"):
                load_config(str(config_path), validate=True)

    @pytest.mark.asyncio
    async def test_centralized_config_reload(self) -> None:
        """Test that centralized configuration can be reloaded."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create first config
            config1_path = Path(temp_dir) / "config1.yaml"
            config1_content = """
nats:
  servers:
    - nats://server1:4222
  client_name: client1

workers:
  concurrency: 2
"""
            config1_path.write_text(config1_content)

            # Load first config
            config1 = load_config(str(config1_path), validate=False)
            assert config1.nats.servers == ["nats://server1:4222"]
            assert config1.nats.client_name == "client1"
            assert config1.workers.concurrency == 2

            # Create second config
            config2_path = Path(temp_dir) / "config2.yaml"
            config2_content = """
nats:
  servers:
    - nats://server2:4222
  client_name: client2

workers:
  concurrency: 4
"""
            config2_path.write_text(config2_content)

            # Reload with second config
            config2 = reload_config(str(config2_path), validate=False)

            # Should have new values
            assert config2.nats.servers == ["nats://server2:4222"]
            assert config2.nats.client_name == "client2"
            assert config2.workers.concurrency == 4

            # Global instance should be updated
            config3 = get_config()
            assert config2 is config3