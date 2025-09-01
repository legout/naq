"""
Tests for the metrics collection implementation.
"""

import time
from unittest.mock import patch

import pytest

from naq.metrics import (
    EventType,
    LeaderElectionMetrics,
    get_all_stats,
    get_metrics,
    record_event,
    reset_metrics,
)


class TestLeaderElectionMetrics:
    """Test cases for the LeaderElectionMetrics class."""

    @pytest.fixture
    def metrics(self):
        """Create a metrics instance for testing."""
        return LeaderElectionMetrics(max_events=10)

    def test_initial_state(self, metrics):
        """Test that metrics start in the correct initial state."""
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 0
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_SUCCESS) == 0
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_FAILURE) == 0
        assert metrics.get_leadership_stats()["current_leader"] is None
        assert metrics.get_leadership_stats()["leadership_transitions"] == 0

    def test_record_event(self, metrics):
        """Test recording events."""
        metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_FAILURE, "instance-2")

        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 1
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_SUCCESS) == 1
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_FAILURE) == 1

    def test_record_event_with_duration(self, metrics):
        """Test recording events with duration."""
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1", duration=1.5)
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1", duration=2.0)
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1", duration=0.5)

        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_SUCCESS) == 3
        assert metrics.get_average_duration(EventType.LOCK_ACQUISITION_SUCCESS) == 1.3333333333333333
        assert metrics.get_percentile_duration(EventType.LOCK_ACQUISITION_SUCCESS, 50.0) == 1.5
        assert metrics.get_percentile_duration(EventType.LOCK_ACQUISITION_SUCCESS, 100.0) == 2.0

    def test_record_event_with_metadata(self, metrics):
        """Test recording events with metadata."""
        metadata = {"error": "timeout", "retry_count": 3}
        metrics.record_event(
            EventType.LOCK_ACQUISITION_FAILURE, "instance-1", metadata=metadata
        )

        events = metrics.get_recent_events(EventType.LOCK_ACQUISITION_FAILURE)
        assert len(events) == 1
        assert events[0].metadata == metadata

    def test_get_event_rate(self, metrics):
        """Test calculating event rate."""
        # Record events at different times
        with patch("time.time", return_value=1000.0):
            metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
            metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")

        with patch("time.time", return_value=1050.0):
            metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")

        # Current time is 1050, so window from 990 to 1050 should include all 3 events
        with patch("time.time", return_value=1050.0):
            rate = metrics.get_event_rate(EventType.LOCK_ACQUISITION_ATTEMPT, 60.0)
            assert rate == 3.0 / 60.0

        # Window from 1000 to 1060 should include only 1 event
        with patch("time.time", return_value=1060.0):
            rate = metrics.get_event_rate(EventType.LOCK_ACQUISITION_ATTEMPT, 60.0)
            assert rate == 1.0 / 60.0

    def test_get_recent_events(self, metrics):
        """Test getting recent events."""
        # Record events with different timestamps
        with patch("time.time", return_value=1000.0):
            metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")

        with patch("time.time", return_value=1001.0):
            metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")

        with patch("time.time", return_value=1002.0):
            metrics.record_event(EventType.LOCK_ACQUISITION_FAILURE, "instance-2")

        # Get all recent events
        events = metrics.get_recent_events(limit=10)
        assert len(events) == 3
        assert events[0].event_type == EventType.LOCK_ACQUISITION_FAILURE  # Most recent
        assert events[1].event_type == EventType.LOCK_ACQUISITION_SUCCESS
        assert events[2].event_type == EventType.LOCK_ACQUISITION_ATTEMPT  # Oldest

        # Get events filtered by type
        success_events = metrics.get_recent_events(EventType.LOCK_ACQUISITION_SUCCESS)
        assert len(success_events) == 1
        assert success_events[0].event_type == EventType.LOCK_ACQUISITION_SUCCESS

    def test_leadership_tracking(self, metrics):
        """Test leadership tracking."""
        # Gain leadership
        with patch("time.time", return_value=1000.0):
            metrics.record_event(EventType.LEADERSHIP_GAINED, "instance-1")

        stats = metrics.get_leadership_stats()
        assert stats["current_leader"] == "instance-1"
        assert stats["leadership_start_time"] == 1000.0
        assert stats["leadership_transitions"] == 0

        # Lose leadership
        with patch("time.time", return_value=1010.0):
            metrics.record_event(EventType.LEADERSHIP_LOST, "instance-1")

        stats = metrics.get_leadership_stats()
        assert stats["current_leader"] is None
        assert stats["total_leadership_time"] == 10.0  # 1010 - 1000
        assert stats["leadership_transitions"] == 0

        # Different instance gains leadership
        with patch("time.time", return_value=1020.0):
            metrics.record_event(EventType.LEADERSHIP_GAINED, "instance-2")

        stats = metrics.get_leadership_stats()
        assert stats["current_leader"] == "instance-2"
        assert stats["leadership_transitions"] == 1

    def test_lock_stats(self, metrics):
        """Test lock statistics."""
        # Record some lock events
        metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1", duration=1.0)
        metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_FAILURE, "instance-1")

        metrics.record_event(EventType.LOCK_RENEWAL_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_RENEWAL_SUCCESS, "instance-1", duration=0.5)
        metrics.record_event(EventType.LOCK_RENEWAL_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_RENEWAL_SUCCESS, "instance-1", duration=0.7)

        stats = metrics.get_lock_stats()

        # Check acquisition stats
        assert stats["acquisition"]["attempts"] == 2
        assert stats["acquisition"]["successes"] == 1
        assert stats["acquisition"]["failures"] == 1
        assert stats["acquisition"]["success_rate"] == 0.5
        assert stats["acquisition"]["avg_duration"] == 1.0

        # Check renewal stats
        assert stats["renewal"]["attempts"] == 2
        assert stats["renewal"]["successes"] == 2
        assert stats["renewal"]["failures"] == 0
        assert stats["renewal"]["success_rate"] == 1.0
        assert stats["renewal"]["avg_duration"] == 0.6

    def test_circuit_breaker_stats(self, metrics):
        """Test circuit breaker statistics."""
        # Record circuit breaker events
        metrics.record_event(EventType.CIRCUIT_BREAKER_OPEN, "instance-1")
        metrics.record_event(EventType.CIRCUIT_BREAKER_CLOSED, "instance-1")
        metrics.record_event(EventType.CIRCUIT_BREAKER_OPEN, "instance-1")
        metrics.record_event(EventType.CIRCUIT_BREAKER_HALF_OPEN, "instance-1")

        stats = metrics.get_circuit_breaker_stats()
        assert stats["open_events"] == 2
        assert stats["closed_events"] == 1
        assert stats["half_open_events"] == 1

    def test_get_all_stats(self, metrics):
        """Test getting all statistics."""
        # Record some events
        metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")
        metrics.record_event(EventType.LEADERSHIP_GAINED, "instance-1")

        stats = metrics.get_all_stats()
        assert "leadership" in stats
        assert "lock" in stats
        assert "circuit_breaker" in stats
        assert "event_counts" in stats
        assert "total_events" in stats

        # Check specific values
        assert stats["leadership"]["current_leader"] == "instance-1"
        assert stats["lock"]["acquisition"]["attempts"] == 1
        assert stats["lock"]["acquisition"]["successes"] == 1
        assert stats["total_events"] == 3

    def test_max_events_limit(self, metrics):
        """Test that the events list respects the max_events limit."""
        # Record more events than the limit
        for i in range(15):
            metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, f"instance-{i}")

        # Should only keep the most recent events
        events = metrics.get_recent_events(limit=20)
        assert len(events) == 10  # max_events was set to 10

        # Check that we have the most recent events
        for i, event in enumerate(events):
            assert event.instance_id == f"instance-{i + 5}"  # Should have instances 5-14

    def test_reset(self, metrics):
        """Test resetting metrics."""
        # Record some events
        metrics.record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        metrics.record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")
        metrics.record_event(EventType.LEADERSHIP_GAINED, "instance-1")

        # Verify events were recorded
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 1
        assert metrics.get_leadership_stats()["current_leader"] == "instance-1"

        # Reset metrics
        metrics.reset()

        # Verify everything was reset
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 0
        assert metrics.get_leadership_stats()["current_leader"] is None
        assert metrics.get_leadership_stats()["leadership_transitions"] == 0
        assert len(metrics.get_recent_events()) == 0


class TestGlobalMetrics:
    """Test cases for the global metrics functions."""

    def test_get_metrics(self):
        """Test getting the global metrics instance."""
        metrics1 = get_metrics()
        metrics2 = get_metrics()
        assert metrics1 is metrics2  # Should be the same instance

    def test_record_event_global(self):
        """Test recording events using the global function."""
        reset_metrics()  # Start with a clean slate

        record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")

        metrics = get_metrics()
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 1
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_SUCCESS) == 1

    def test_get_all_stats_global(self):
        """Test getting all stats using the global function."""
        reset_metrics()  # Start with a clean slate

        record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")

        stats = get_all_stats()
        assert stats["lock"]["acquisition"]["attempts"] == 1
        assert stats["lock"]["acquisition"]["successes"] == 1
        assert stats["total_events"] == 2

    def test_reset_metrics_global(self):
        """Test resetting metrics using the global function."""
        record_event(EventType.LOCK_ACQUISITION_ATTEMPT, "instance-1")
        record_event(EventType.LOCK_ACQUISITION_SUCCESS, "instance-1")

        # Verify events were recorded
        metrics = get_metrics()
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 1

        # Reset using global function
        reset_metrics()

        # Verify everything was reset
        assert metrics.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT) == 0
        assert len(metrics.get_recent_events()) == 0