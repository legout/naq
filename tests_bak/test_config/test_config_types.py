"""Unit tests for configuration data types."""

import os
import pytest
from typing import Optional

from naq.config.types import NatsConfig, WorkerConfig, EventsConfig, NAQConfig, SchedulerServiceConfig


class TestNatsConfig:
    """Test cases for NatsConfig dataclass."""

    def test_nats_config_creation(self) -> None:
        """Test that NatsConfig can be created with valid parameters."""
        config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        assert config.servers == ["nats://localhost:4222"]
        assert config.client_name == "test-client"
        assert config.max_reconnect_attempts == 5
        assert config.reconnect_time_wait == 2.0
        assert config.connection_timeout == 5.0
        assert config.drain_timeout == 30.0
        assert config.auth is None
        assert config.tls is None

    def test_nats_config_with_auth(self) -> None:
        """Test that NatsConfig can be created with auth configuration."""
        auth_config = {"user": "testuser", "password": "testpass"}
        config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
            auth=auth_config,
        )
        
        assert config.auth == auth_config

    def test_nats_config_with_tls(self) -> None:
        """Test that NatsConfig can be created with TLS configuration."""
        tls_config = {"cert_file": "/path/to/cert.pem", "key_file": "/path/to/key.pem"}
        config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
            tls=tls_config,
        )
        
        assert config.tls == tls_config

    def test_nats_config_multiple_servers(self) -> None:
        """Test that NatsConfig can handle multiple server URLs."""
        servers = [
            "nats://server1:4222",
            "nats://server2:4222",
            "nats://server3:4222"
        ]
        config = NatsConfig(
            servers=servers,
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        assert config.servers == servers


class TestWorkerConfig:
    """Test cases for WorkerConfig dataclass."""

    def test_worker_config_creation(self) -> None:
        """Test that WorkerConfig can be created with valid parameters."""
        config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
        )
        
        assert config.concurrency == 4
        assert config.heartbeat_interval == 30.0
        assert config.ttl == 60.0
        assert config.max_job_duration == 3600.0
        assert config.shutdown_timeout == 10.0
        assert config.pools is None

    def test_worker_config_with_pools(self) -> None:
        """Test that WorkerConfig can be created with pools configuration."""
        pools_config = {"default": {"size": 5}, "high_priority": {"size": 2}}
        config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
            pools=pools_config,
        )
        
        assert config.pools == pools_config


class TestEventsConfig:
    """Test cases for EventsConfig dataclass."""

    def test_events_config_creation(self) -> None:
        """Test that EventsConfig can be created with valid parameters."""
        config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        assert config.enabled is True
        assert config.batch_size == 100
        assert config.flush_interval == 5.0
        assert config.max_buffer_size == 1000
        assert config.stream == "naq_events"
        assert config.filters is None

    def test_events_config_with_filters(self) -> None:
        """Test that EventsConfig can be created with filters."""
        filters = ["job.started", "job.completed", "job.failed"]
        config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
            filters=filters,
        )
        
        assert config.filters == filters

    def test_events_config_disabled(self) -> None:
        """Test that EventsConfig can be disabled."""
        config = EventsConfig(
            enabled=False,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        assert config.enabled is False


class TestNAQConfig:
    """Test cases for NAQConfig dataclass."""

    def test_naq_config_creation(self) -> None:
        """Test that NAQConfig can be created with valid parameters."""
        nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        worker_config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
        )
        
        events_config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        config = NAQConfig(
            nats=nats_config,
            workers=worker_config,
            events=events_config,
        )
        
        assert config.nats == nats_config
        assert config.workers == worker_config
        assert config.events == events_config
        assert config.queues is None
        assert config.scheduler_service is None
        assert config.results is None
        assert config.serialization is None
        assert config.logging is None

    def test_naq_config_with_optional_sections(self) -> None:
        """Test that NAQConfig can be created with all optional sections."""
        nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        worker_config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
        )
        
        events_config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        queues_config = {"default": {"max_size": 1000}}
        scheduler_service_config = SchedulerServiceConfig(scheduler_name="test_scheduler")
        results_config = {"ttl": 86400}
        serialization_config = {"default": "pickle"}
        logging_config = {"level": "INFO"}
        
        config = NAQConfig(
            nats=nats_config,
            workers=worker_config,
            events=events_config,
            queues=queues_config,
            scheduler_service=scheduler_service_config,
            results=results_config,
            serialization=serialization_config,
            logging=logging_config,
        )
        
        assert config.queues == queues_config
        assert config.scheduler_service == scheduler_service_config
        assert config.results == results_config
        assert config.serialization == serialization_config
        assert config.logging == logging_config

    def test_environment_property_no_env_var(self) -> None:
        """Test that environment property returns None when NAQ_ENVIRONMENT is not set."""
        nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        worker_config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
        )
        
        events_config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        config = NAQConfig(
            nats=nats_config,
            workers=worker_config,
            events=events_config,
        )
        
        # Ensure NAQ_ENVIRONMENT is not set
        if "NAQ_ENVIRONMENT" in os.environ:
            del os.environ["NAQ_ENVIRONMENT"]
        
        assert config.environment is None

    def test_environment_property_with_env_var(self) -> None:
        """Test that environment property returns the correct value when NAQ_ENVIRONMENT is set."""
        nats_config = NatsConfig(
            servers=["nats://localhost:4222"],
            client_name="test-client",
            max_reconnect_attempts=5,
            reconnect_time_wait=2.0,
            connection_timeout=5.0,
            drain_timeout=30.0,
        )
        
        worker_config = WorkerConfig(
            concurrency=4,
            heartbeat_interval=30.0,
            ttl=60.0,
            max_job_duration=3600.0,
            shutdown_timeout=10.0,
        )
        
        events_config = EventsConfig(
            enabled=True,
            batch_size=100,
            flush_interval=5.0,
            max_buffer_size=1000,
            stream="naq_events",
        )
        
        config = NAQConfig(
            nats=nats_config,
            workers=worker_config,
            events=events_config,
        )
        
        # Set NAQ_ENVIRONMENT
        os.environ["NAQ_ENVIRONMENT"] = "production"
        
        try:
            assert config.environment == "production"
        finally:
            # Clean up
            if "NAQ_ENVIRONMENT" in os.environ:
                del os.environ["NAQ_ENVIRONMENT"]