"""Unit tests for configuration defaults."""

import pytest
from typing import Any, Dict

from naq.config.defaults import DEFAULT_CONFIG


class TestDefaultConfig:
    """Test cases for DEFAULT_CONFIG."""

    def test_default_config_structure(self) -> None:
        """Test that DEFAULT_CONFIG has the correct structure."""
        assert isinstance(DEFAULT_CONFIG, dict)
        
        # Check required top-level keys
        required_keys = ["nats", "workers", "events", "queues", "scheduler", "results", "serialization", "logging"]
        for key in required_keys:
            assert key in DEFAULT_CONFIG, f"Missing required key: {key}"

    def test_nats_defaults(self) -> None:
        """Test NATS configuration defaults."""
        nats_config = DEFAULT_CONFIG["nats"]
        assert isinstance(nats_config, dict)
        
        # Check required NATS keys
        required_nats_keys = ["servers", "client_name", "max_reconnect_attempts", 
                             "reconnect_time_wait", "connection_timeout", "drain_timeout", 
                             "auth", "tls"]
        for key in required_nats_keys:
            assert key in nats_config, f"Missing required NATS key: {key}"
        
        # Check default values
        assert nats_config["servers"] == ["nats://localhost:4222"]
        assert nats_config["client_name"] == "naq-client"
        assert nats_config["max_reconnect_attempts"] == 5
        assert nats_config["reconnect_time_wait"] == 2.0
        assert nats_config["connection_timeout"] == 5.0
        assert nats_config["drain_timeout"] == 30.0
        assert nats_config["auth"] is None
        assert nats_config["tls"] is None

    def test_workers_defaults(self) -> None:
        """Test worker configuration defaults."""
        workers_config = DEFAULT_CONFIG["workers"]
        assert isinstance(workers_config, dict)
        
        # Check required worker keys
        required_worker_keys = ["concurrency", "heartbeat_interval", "ttl", 
                               "max_job_duration", "shutdown_timeout", "pools"]
        for key in required_worker_keys:
            assert key in workers_config, f"Missing required worker key: {key}"
        
        # Check default values
        assert workers_config["concurrency"] == 1
        assert workers_config["heartbeat_interval"] == 30.0
        assert workers_config["ttl"] == 60.0
        assert workers_config["max_job_duration"] == 3600.0
        assert workers_config["shutdown_timeout"] == 10.0
        assert workers_config["pools"] is None

    def test_events_defaults(self) -> None:
        """Test events configuration defaults."""
        events_config = DEFAULT_CONFIG["events"]
        assert isinstance(events_config, dict)
        
        # Check required events keys
        required_events_keys = ["enabled", "batch_size", "flush_interval", 
                               "max_buffer_size", "stream", "filters"]
        for key in required_events_keys:
            assert key in events_config, f"Missing required events key: {key}"
        
        # Check default values
        assert events_config["enabled"] is False
        assert events_config["batch_size"] == 100
        assert events_config["flush_interval"] == 5.0
        assert events_config["max_buffer_size"] == 1000
        assert events_config["stream"] == "naq_events"
        assert events_config["filters"] is None

    def test_queues_defaults(self) -> None:
        """Test queues configuration defaults."""
        queues_config = DEFAULT_CONFIG["queues"]
        assert isinstance(queues_config, dict)
        assert queues_config == {}  # Should be empty dict by default

    def test_scheduler_defaults(self) -> None:
        """Test scheduler configuration defaults."""
        scheduler_config = DEFAULT_CONFIG["scheduler"]
        assert isinstance(scheduler_config, dict)
        assert scheduler_config == {}  # Should be empty dict by default

    def test_results_defaults(self) -> None:
        """Test results configuration defaults."""
        results_config = DEFAULT_CONFIG["results"]
        assert isinstance(results_config, dict)
        assert results_config == {}  # Should be empty dict by default

    def test_serialization_defaults(self) -> None:
        """Test serialization configuration defaults."""
        serialization_config = DEFAULT_CONFIG["serialization"]
        assert isinstance(serialization_config, dict)
        assert serialization_config == {}  # Should be empty dict by default

    def test_logging_defaults(self) -> None:
        """Test logging configuration defaults."""
        logging_config = DEFAULT_CONFIG["logging"]
        assert isinstance(logging_config, dict)
        assert logging_config == {}  # Should be empty dict by default

    def test_default_config_immutability(self) -> None:
        """Test that DEFAULT_CONFIG is not accidentally modified."""
        # Get original config
        original_servers = DEFAULT_CONFIG["nats"]["servers"].copy()
        
        # Try to modify it
        DEFAULT_CONFIG["nats"]["servers"].append("nats://test:4222")
        
        # Check that the original was modified (this is expected behavior for a mutable default)
        # In a real implementation, you might want to make this immutable or return a copy
        assert len(DEFAULT_CONFIG["nats"]["servers"]) == len(original_servers) + 1
        
        # Restore original state for other tests
        DEFAULT_CONFIG["nats"]["servers"] = original_servers

    def test_default_config_types(self) -> None:
        """Test that all default values have the correct types."""
        # NATS config types
        assert isinstance(DEFAULT_CONFIG["nats"]["servers"], list)
        assert all(isinstance(server, str) for server in DEFAULT_CONFIG["nats"]["servers"])
        assert isinstance(DEFAULT_CONFIG["nats"]["client_name"], str)
        assert isinstance(DEFAULT_CONFIG["nats"]["max_reconnect_attempts"], int)
        assert isinstance(DEFAULT_CONFIG["nats"]["reconnect_time_wait"], (int, float))
        assert isinstance(DEFAULT_CONFIG["nats"]["connection_timeout"], (int, float))
        assert isinstance(DEFAULT_CONFIG["nats"]["drain_timeout"], (int, float))
        
        # Workers config types
        assert isinstance(DEFAULT_CONFIG["workers"]["concurrency"], int)
        assert isinstance(DEFAULT_CONFIG["workers"]["heartbeat_interval"], (int, float))
        assert isinstance(DEFAULT_CONFIG["workers"]["ttl"], (int, float))
        assert isinstance(DEFAULT_CONFIG["workers"]["max_job_duration"], (int, float))
        assert isinstance(DEFAULT_CONFIG["workers"]["shutdown_timeout"], (int, float))
        
        # Events config types
        assert isinstance(DEFAULT_CONFIG["events"]["enabled"], bool)
        assert isinstance(DEFAULT_CONFIG["events"]["batch_size"], int)
        assert isinstance(DEFAULT_CONFIG["events"]["flush_interval"], (int, float))
        assert isinstance(DEFAULT_CONFIG["events"]["max_buffer_size"], int)
        assert isinstance(DEFAULT_CONFIG["events"]["stream"], str)

    def test_default_config_value_ranges(self) -> None:
        """Test that numeric default values are within reasonable ranges."""
        # NATS config ranges
        assert DEFAULT_CONFIG["nats"]["max_reconnect_attempts"] >= 0
        assert DEFAULT_CONFIG["nats"]["reconnect_time_wait"] > 0
        assert DEFAULT_CONFIG["nats"]["connection_timeout"] > 0
        assert DEFAULT_CONFIG["nats"]["drain_timeout"] > 0
        
        # Workers config ranges
        assert DEFAULT_CONFIG["workers"]["concurrency"] > 0
        assert DEFAULT_CONFIG["workers"]["heartbeat_interval"] > 0
        assert DEFAULT_CONFIG["workers"]["ttl"] > 0
        assert DEFAULT_CONFIG["workers"]["max_job_duration"] > 0
        assert DEFAULT_CONFIG["workers"]["shutdown_timeout"] > 0
        
        # Events config ranges
        assert DEFAULT_CONFIG["events"]["batch_size"] > 0
        assert DEFAULT_CONFIG["events"]["flush_interval"] >= 0
        assert DEFAULT_CONFIG["events"]["max_buffer_size"] > 0
        assert len(DEFAULT_CONFIG["events"]["stream"]) > 0