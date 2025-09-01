"""
Metrics Collection for Leader Election Events

This module provides metrics collection functionality for leader election events,
including lock acquisition, renewal, and release events. It supports both
in-memory metrics and optional export to external monitoring systems.
"""

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from loguru import logger


class EventType(Enum):
    """Types of leader election events to track."""

    LOCK_ACQUISITION_ATTEMPT = "lock_acquisition_attempt"
    LOCK_ACQUISITION_SUCCESS = "lock_acquisition_success"
    LOCK_ACQUISITION_FAILURE = "lock_acquisition_failure"
    LOCK_RENEWAL_ATTEMPT = "lock_renewal_attempt"
    LOCK_RENEWAL_SUCCESS = "lock_renewal_success"
    LOCK_RENEWAL_FAILURE = "lock_renewal_failure"
    LOCK_RELEASE_ATTEMPT = "lock_release_attempt"
    LOCK_RELEASE_SUCCESS = "lock_release_success"
    LOCK_RELEASE_FAILURE = "lock_release_failure"
    LEADERSHIP_GAINED = "leadership_gained"
    LEADERSHIP_LOST = "leadership_lost"
    LEADERSHIP_VERIFICATION_SUCCESS = "leadership_verification_success"
    LEADERSHIP_VERIFICATION_FAILURE = "leadership_verification_failure"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    CIRCUIT_BREAKER_CLOSED = "circuit_breaker_closed"
    CIRCUIT_BREAKER_HALF_OPEN = "circuit_breaker_half_open"


@dataclass
class MetricEvent:
    """Represents a single metric event."""

    event_type: EventType
    instance_id: str
    timestamp: float = field(default_factory=time.time)
    duration: Optional[float] = None  # For timing events
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a dictionary."""
        return {
            "event_type": self.event_type.value,
            "instance_id": self.instance_id,
            "timestamp": self.timestamp,
            "duration": self.duration,
            "metadata": self.metadata,
        }


class LeaderElectionMetrics:
    """
    Metrics collector for leader election events.

    This class collects and aggregates metrics for leader election events,
    providing both real-time statistics and historical data.
    """

    def __init__(self, max_events: int = 1000) -> None:
        """
        Initialize the metrics collector.

        Args:
            max_events: Maximum number of events to keep in memory
        """
        self.max_events = max_events
        self._events: deque[MetricEvent] = deque(maxlen=max_events)
        self._counters: Dict[EventType, int] = defaultdict(int)
        self._timings: Dict[EventType, List[float]] = defaultdict(list)
        self._last_event_time: Dict[EventType, float] = {}
        self._current_leader: Optional[str] = None
        self._leadership_start_time: Optional[float] = None
        self._total_leadership_time: float = 0.0
        self._leadership_transitions: int = 0

    def record_event(
        self,
        event_type: EventType,
        instance_id: str,
        duration: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a leader election event.

        Args:
            event_type: Type of event
            instance_id: ID of the instance generating the event
            duration: Optional duration of the event in seconds
            metadata: Optional additional metadata for the event
        """
        event = MetricEvent(
            event_type=event_type,
            instance_id=instance_id,
            duration=duration,
            metadata=metadata or {},
        )

        self._events.append(event)
        self._counters[event_type] += 1
        self._last_event_time[event_type] = event.timestamp

        if duration is not None:
            self._timings[event_type].append(duration)
            # Keep only the last 100 timings to prevent memory growth
            if len(self._timings[event_type]) > 100:
                self._timings[event_type] = self._timings[event_type][-100:]

        # Track leadership-specific metrics
        if event_type == EventType.LEADERSHIP_GAINED:
            if self._current_leader is not None:
                # Leadership transition
                self._leadership_transitions += 1
                if self._leadership_start_time is not None:
                    self._total_leadership_time += (
                        event.timestamp - self._leadership_start_time
                    )

            self._current_leader = instance_id
            self._leadership_start_time = event.timestamp

        elif (
            event_type == EventType.LEADERSHIP_LOST
            and self._current_leader == instance_id
        ):
            if self._leadership_start_time is not None:
                self._total_leadership_time += (
                    event.timestamp - self._leadership_start_time
                )
            self._current_leader = None
            self._leadership_start_time = None

    def get_event_count(self, event_type: EventType) -> int:
        """
        Get the count of a specific event type.

        Args:
            event_type: Type of event to count

        Returns:
            Number of times this event has occurred
        """
        return self._counters.get(event_type, 0)

    def get_event_rate(
        self, event_type: EventType, window_seconds: float = 60.0
    ) -> float:
        """
        Calculate the rate of events per second for a given time window.

        Args:
            event_type: Type of event to calculate rate for
            window_seconds: Time window in seconds to consider

        Returns:
            Events per second in the given window
        """
        if not self._events:
            return 0.0

        now = time.time()
        cutoff = now - window_seconds

        count = sum(
            1
            for event in self._events
            if event.event_type == event_type and event.timestamp >= cutoff
        )

        return count / window_seconds

    def get_average_duration(self, event_type: EventType) -> Optional[float]:
        """
        Get the average duration for a specific event type.

        Args:
            event_type: Type of event to get duration for

        Returns:
            Average duration in seconds, or None if no timing data available
        """
        timings = self._timings.get(event_type, [])
        if not timings:
            return None
        return sum(timings) / len(timings)

    def get_percentile_duration(
        self, event_type: EventType, percentile: float = 95.0
    ) -> Optional[float]:
        """
        Get the percentile duration for a specific event type.

        Args:
            event_type: Type of event to get duration for
            percentile: Percentile to calculate (0-100)

        Returns:
            Percentile duration in seconds, or None if no timing data available
        """
        timings = self._timings.get(event_type, [])
        if not timings:
            return None

        sorted_timings = sorted(timings)
        index = int(len(sorted_timings) * percentile / 100)
        index = min(index, len(sorted_timings) - 1)
        return sorted_timings[index]

    def get_recent_events(
        self, event_type: Optional[EventType] = None, limit: int = 10
    ) -> List[MetricEvent]:
        """
        Get recent events, optionally filtered by type.

        Args:
            event_type: Optional event type to filter by
            limit: Maximum number of events to return

        Returns:
            List of recent events, most recent first
        """
        events = list(self._events)
        events.reverse()  # Most recent first

        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]

        return events[:limit]

    def get_leadership_stats(self) -> Dict[str, Any]:
        """
        Get leadership-related statistics.

        Returns:
            Dictionary containing leadership statistics
        """
        current_leadership_duration = 0.0
        if self._current_leader is not None and self._leadership_start_time is not None:
            current_leadership_duration = time.time() - self._leadership_start_time

        return {
            "current_leader": self._current_leader,
            "leadership_start_time": self._leadership_start_time,
            "current_leadership_duration": current_leadership_duration,
            "total_leadership_time": self._total_leadership_time,
            "leadership_transitions": self._leadership_transitions,
            "leadership_gained_count": self.get_event_count(
                EventType.LEADERSHIP_GAINED
            ),
            "leadership_lost_count": self.get_event_count(EventType.LEADERSHIP_LOST),
        }

    def get_lock_stats(self) -> Dict[str, Any]:
        """
        Get lock-related statistics.

        Returns:
            Dictionary containing lock statistics
        """
        acquisition_attempts = self.get_event_count(EventType.LOCK_ACQUISITION_ATTEMPT)
        acquisition_successes = self.get_event_count(EventType.LOCK_ACQUISITION_SUCCESS)
        acquisition_failures = self.get_event_count(EventType.LOCK_ACQUISITION_FAILURE)

        renewal_attempts = self.get_event_count(EventType.LOCK_RENEWAL_ATTEMPT)
        renewal_successes = self.get_event_count(EventType.LOCK_RENEWAL_SUCCESS)
        renewal_failures = self.get_event_count(EventType.LOCK_RENEWAL_FAILURE)

        release_attempts = self.get_event_count(EventType.LOCK_RELEASE_ATTEMPT)
        release_successes = self.get_event_count(EventType.LOCK_RELEASE_SUCCESS)
        release_failures = self.get_event_count(EventType.LOCK_RELEASE_FAILURE)

        acquisition_success_rate = (
            acquisition_successes / acquisition_attempts
            if acquisition_attempts > 0
            else 0.0
        )

        renewal_success_rate = (
            renewal_successes / renewal_attempts if renewal_attempts > 0 else 0.0
        )

        release_success_rate = (
            release_successes / release_attempts if release_attempts > 0 else 0.0
        )

        return {
            "acquisition": {
                "attempts": acquisition_attempts,
                "successes": acquisition_successes,
                "failures": acquisition_failures,
                "success_rate": acquisition_success_rate,
                "avg_duration": self.get_average_duration(
                    EventType.LOCK_ACQUISITION_SUCCESS
                ),
            },
            "renewal": {
                "attempts": renewal_attempts,
                "successes": renewal_successes,
                "failures": renewal_failures,
                "success_rate": renewal_success_rate,
                "avg_duration": self.get_average_duration(
                    EventType.LOCK_RENEWAL_SUCCESS
                ),
            },
            "release": {
                "attempts": release_attempts,
                "successes": release_successes,
                "failures": release_failures,
                "success_rate": release_success_rate,
                "avg_duration": self.get_average_duration(
                    EventType.LOCK_RELEASE_SUCCESS
                ),
            },
        }

    def get_circuit_breaker_stats(self) -> Dict[str, Any]:
        """
        Get circuit breaker statistics.

        Returns:
            Dictionary containing circuit breaker statistics
        """
        return {
            "open_events": self.get_event_count(EventType.CIRCUIT_BREAKER_OPEN),
            "closed_events": self.get_event_count(EventType.CIRCUIT_BREAKER_CLOSED),
            "half_open_events": self.get_event_count(
                EventType.CIRCUIT_BREAKER_HALF_OPEN
            ),
            "open_rate": self.get_event_rate(EventType.CIRCUIT_BREAKER_OPEN),
        }

    def get_all_stats(self) -> Dict[str, Any]:
        """
        Get all statistics.

        Returns:
            Dictionary containing all statistics
        """
        return {
            "leadership": self.get_leadership_stats(),
            "lock": self.get_lock_stats(),
            "circuit_breaker": self.get_circuit_breaker_stats(),
            "event_counts": {et.value: count for et, count in self._counters.items()},
            "total_events": len(self._events),
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self._events.clear()
        self._counters.clear()
        self._timings.clear()
        self._last_event_time.clear()
        self._current_leader = None
        self._leadership_start_time = None
        self._total_leadership_time = 0.0
        self._leadership_transitions = 0


# Global metrics instance
_metrics = LeaderElectionMetrics()


def get_metrics() -> LeaderElectionMetrics:
    """Get the global metrics instance."""
    return _metrics


def record_event(
    event_type: EventType,
    instance_id: str,
    duration: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record an event using the global metrics instance.

    Args:
        event_type: Type of event
        instance_id: ID of the instance generating the event
        duration: Optional duration of the event in seconds
        metadata: Optional additional metadata for the event
    """
    _metrics.record_event(event_type, instance_id, duration, metadata)


def get_all_stats() -> Dict[str, Any]:
    """
    Get all statistics from the global metrics instance.

    Returns:
        Dictionary containing all statistics
    """
    return _metrics.get_all_stats()


def reset_metrics() -> None:
    """Reset the global metrics instance."""
    _metrics.reset()
