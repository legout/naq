"""
Circuit Breaker Implementation

This module provides a circuit breaker pattern implementation for handling
connection failures to external services like NATS. The circuit breaker
prevents cascading failures by temporarily blocking requests to a failing
service and allowing it to recover.
"""

import asyncio
import time
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar

from loguru import logger

from .exceptions import NaqConnectionError
from .metrics import EventType, record_event

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = auto()  # Normal operation, requests pass through
    OPEN = auto()  # Circuit is open, requests are blocked
    HALF_OPEN = auto()  # Testing if service has recovered


class CircuitBreaker:
    """
    Circuit breaker implementation for handling connection failures.

    This circuit breaker prevents cascading failures by temporarily blocking
    requests to a failing service and allowing it to recover. It implements
    the standard circuit breaker pattern with three states: CLOSED, OPEN, and HALF_OPEN.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = NaqConnectionError,
        name: str = "default",
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            failure_threshold: Number of failures before opening the circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type that indicates a failure
            name: Name of the circuit breaker for logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._successful_calls = 0
        self._total_calls = 0
        self._lock = asyncio.Lock()

        # Record initial circuit breaker state
        record_event(EventType.CIRCUIT_BREAKER_CLOSED, self.name)

    @property
    def state(self) -> CircuitState:
        """Get the current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get the current failure count."""
        return self._failure_count

    @property
    def last_failure_time(self) -> Optional[float]:
        """Get the last failure time."""
        return self._last_failure_time

    @property
    def success_rate(self) -> float:
        """Get the success rate (0.0 to 1.0)."""
        if self._total_calls == 0:
            return 1.0
        return self._successful_calls / self._total_calls

    async def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Call a function with circuit breaker protection.

        Args:
            func: Function to call
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Result of the function call

        Raises:
            NaqConnectionError: If circuit is open or function call fails
        """
        async with self._lock:
            self._total_calls += 1

            # Check if circuit is open and if we should attempt recovery
            if self._state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    logger.info(f"Circuit breaker '{self.name}' attempting recovery")
                    self._state = CircuitState.HALF_OPEN
                    record_event(EventType.CIRCUIT_BREAKER_HALF_OPEN, self.name)
                else:
                    logger.warning(
                        f"Circuit breaker '{self.name}' is open, blocking call"
                    )
                    raise NaqConnectionError(f"Circuit breaker '{self.name}' is open")

        try:
            # Call the function
            result = (
                await func(*args, **kwargs)
                if asyncio.iscoroutinefunction(func)
                else func(*args, **kwargs)
            )

            # Record success
            async with self._lock:
                self._successful_calls += 1
                if self._state == CircuitState.HALF_OPEN:
                    logger.info(
                        f"Circuit breaker '{self.name}' recovered, closing circuit"
                    )
                    self._reset()
                    record_event(EventType.CIRCUIT_BREAKER_CLOSED, self.name)

            return result

        except self.expected_exception as e:
            # Record failure
            await self._record_failure()
            raise
        except Exception as e:
            # For unexpected exceptions, record failure but re-raise
            await self._record_failure()
            raise

    def _should_attempt_recovery(self) -> bool:
        """
        Check if we should attempt recovery from OPEN state.

        Returns:
            True if recovery should be attempted, False otherwise
        """
        if self._last_failure_time is None:
            return True

        return (time.time() - self._last_failure_time) >= self.recovery_timeout

    async def _record_failure(self) -> None:
        """Record a failure and potentially open the circuit."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(
                    f"Circuit breaker '{self.name}' failed in HALF_OPEN state, opening circuit"
                )
                self._state = CircuitState.OPEN
                record_event(EventType.CIRCUIT_BREAKER_OPEN, self.name)
            elif (
                self._failure_count >= self.failure_threshold
                and self._state == CircuitState.CLOSED
            ):
                logger.warning(
                    f"Circuit breaker '{self.name}' reached failure threshold "
                    f"({self.failure_count}/{self.failure_threshold}), opening circuit"
                )
                self._state = CircuitState.OPEN
                record_event(EventType.CIRCUIT_BREAKER_OPEN, self.name)

    def _reset(self) -> None:
        """Reset the circuit breaker to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None

    def force_open(self) -> None:
        """Force the circuit breaker to OPEN state."""
        logger.info(f"Circuit breaker '{self.name}' forced open")
        self._state = CircuitState.OPEN
        self._last_failure_time = time.time()
        record_event(EventType.CIRCUIT_BREAKER_OPEN, self.name)

    def force_close(self) -> None:
        """Force the circuit breaker to CLOSED state."""
        logger.info(f"Circuit breaker '{self.name}' forced closed")
        self._reset()
        record_event(EventType.CIRCUIT_BREAKER_CLOSED, self.name)

    def get_stats(self) -> dict[str, Any]:
        """
        Get circuit breaker statistics.

        Returns:
            Dictionary containing circuit breaker statistics
        """
        return {
            "name": self.name,
            "state": self._state.name,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure_time": self._last_failure_time,
            "recovery_timeout": self.recovery_timeout,
            "successful_calls": self._successful_calls,
            "total_calls": self._total_calls,
            "success_rate": self.success_rate,
        }


class CircuitBreakerManager:
    """
    Manager for multiple circuit breakers.

    This class provides a centralized way to manage multiple circuit breakers
    for different services or endpoints.
    """

    def __init__(self) -> None:
        """Initialize the circuit breaker manager."""
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def get_circuit_breaker(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = NaqConnectionError,
    ) -> CircuitBreaker:
        """
        Get or create a circuit breaker.

        Args:
            name: Name of the circuit breaker
            failure_threshold: Number of failures before opening the circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type that indicates a failure

        Returns:
            CircuitBreaker instance
        """
        async with self._lock:
            if name not in self._circuit_breakers:
                self._circuit_breakers[name] = CircuitBreaker(
                    failure_threshold=failure_threshold,
                    recovery_timeout=recovery_timeout,
                    expected_exception=expected_exception,
                    name=name,
                )
                logger.debug(f"Created circuit breaker '{name}'")

            return self._circuit_breakers[name]

    async def remove_circuit_breaker(self, name: str) -> None:
        """
        Remove a circuit breaker.

        Args:
            name: Name of the circuit breaker to remove
        """
        async with self._lock:
            if name in self._circuit_breakers:
                del self._circuit_breakers[name]
                logger.debug(f"Removed circuit breaker '{name}'")

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """
        Get statistics for all circuit breakers.

        Returns:
            Dictionary mapping circuit breaker names to their statistics
        """
        return {name: cb.get_stats() for name, cb in self._circuit_breakers.items()}

    def get_circuit_breaker_names(self) -> list[str]:
        """
        Get all circuit breaker names.

        Returns:
            List of circuit breaker names
        """
        return list(self._circuit_breakers.keys())


# Global circuit breaker manager instance
_circuit_breaker_manager = CircuitBreakerManager()


async def get_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: type = NaqConnectionError,
) -> CircuitBreaker:
    """
    Get or create a circuit breaker using the global manager.

    Args:
        name: Name of the circuit breaker
        failure_threshold: Number of failures before opening the circuit
        recovery_timeout: Seconds to wait before attempting recovery
        expected_exception: Exception type that indicates a failure

    Returns:
        CircuitBreaker instance
    """
    return await _circuit_breaker_manager.get_circuit_breaker(
        name=name,
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        expected_exception=expected_exception,
    )
