import pytest

from naq.connection.utils import ConnectionMonitor, ConnectionMetrics


class TestConnectionMonitor:
    """Test cases for the ConnectionMonitor class."""

    def test_init_default_parameters(self):
        """Test ConnectionMonitor initialization with default parameters."""
        monitor = ConnectionMonitor()

        assert isinstance(monitor.metrics, ConnectionMetrics)
        assert monitor.metrics.total_connections == 0
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 0
        assert monitor.metrics.average_connection_time == 0.0
        assert monitor._connection_durations == []

    def test_record_connection_start(self):
        """Test recording connection start increments metrics correctly."""
        monitor = ConnectionMonitor()

        # Record first connection start
        monitor.record_connection_start()
        assert monitor.metrics.total_connections == 1
        assert monitor.metrics.active_connections == 1
        assert monitor.metrics.failed_connections == 0
        assert monitor.metrics.average_connection_time == 0.0

        # Record second connection start
        monitor.record_connection_start()
        assert monitor.metrics.total_connections == 2
        assert monitor.metrics.active_connections == 2
        assert monitor.metrics.failed_connections == 0
        assert monitor.metrics.average_connection_time == 0.0

    def test_record_connection_end_single_connection(self):
        """Test recording connection end with a single connection."""
        monitor = ConnectionMonitor()
        duration = 1.5

        # Start and end a connection
        monitor.record_connection_start()
        monitor.record_connection_end(duration)

        assert monitor.metrics.total_connections == 1
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 0
        assert monitor.metrics.average_connection_time == duration
        assert monitor._connection_durations == [duration]

    def test_record_connection_end_multiple_connections(self):
        """Test recording connection end with multiple connections."""
        monitor = ConnectionMonitor()
        durations = [1.0, 2.0, 3.0]

        # Start and end multiple connections
        for duration in durations:
            monitor.record_connection_start()
            monitor.record_connection_end(duration)

        assert monitor.metrics.total_connections == 3
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 0
        expected_average = sum(durations) / len(durations)
        assert monitor.metrics.average_connection_time == expected_average
        assert monitor._connection_durations == durations

    def test_record_connection_failure(self):
        """Test recording connection failure increments failed_connections."""
        monitor = ConnectionMonitor()

        # Record first connection failure
        monitor.record_connection_failure()
        assert monitor.metrics.total_connections == 0
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 1
        assert monitor.metrics.average_connection_time == 0.0

        # Record second connection failure
        monitor.record_connection_failure()
        assert monitor.metrics.total_connections == 0
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 2
        assert monitor.metrics.average_connection_time == 0.0

    def test_mixed_operations(self):
        """Test mixed connection operations."""
        monitor = ConnectionMonitor()

        # Start two connections
        monitor.record_connection_start()
        monitor.record_connection_start()
        assert monitor.metrics.total_connections == 2
        assert monitor.metrics.active_connections == 2

        # End one connection
        monitor.record_connection_end(1.0)
        assert monitor.metrics.total_connections == 2
        assert monitor.metrics.active_connections == 1
        assert monitor.metrics.average_connection_time == 1.0

        # Record a failure
        monitor.record_connection_failure()
        assert monitor.metrics.total_connections == 2
        assert monitor.metrics.active_connections == 1
        assert monitor.metrics.failed_connections == 1
        assert monitor.metrics.average_connection_time == 1.0

        # End the second connection
        monitor.record_connection_end(3.0)
        assert monitor.metrics.total_connections == 2
        assert monitor.metrics.active_connections == 0
        assert monitor.metrics.failed_connections == 1
        assert monitor.metrics.average_connection_time == 2.0  # (1.0 + 3.0) / 2

    def test_zero_connections_average_time(self):
        """Test that average connection time remains 0 with no connections."""
        monitor = ConnectionMonitor()

        # No connections recorded
        assert monitor.metrics.average_connection_time == 0.0

        # Only failures recorded
        monitor.record_connection_failure()
        assert monitor.metrics.average_connection_time == 0.0

    def test_connection_durations_accumulation(self):
        """Test that connection durations are accumulated correctly."""
        monitor = ConnectionMonitor()
        durations = [0.5, 1.5, 2.5]

        for duration in durations:
            monitor.record_connection_start()
            monitor.record_connection_end(duration)

        assert monitor._connection_durations == durations
        assert len(monitor._connection_durations) == 3