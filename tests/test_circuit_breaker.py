"""
Tests for the circuit breaker implementation.
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock

from naq.circuit_breaker import CircuitBreaker, CircuitState, get_circuit_breaker
from naq.exceptions import NaqConnectionError


class TestCircuitBreaker:
    """Test cases for the CircuitBreaker class."""

    @pytest.fixture
    def circuit_breaker(self):
        """Create a circuit breaker for testing."""
        return CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=1.0,
            expected_exception=NaqConnectionError,
            name="test-circuit"
        )

    @pytest.mark.asyncio
    async def test_initial_state(self, circuit_breaker):
        """Test that the circuit breaker starts in CLOSED state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.last_failure_time is None
        assert circuit_breaker.success_rate == 1.0

    @pytest.mark.asyncio
    async def test_successful_call(self, circuit_breaker):
        """Test that successful calls work normally."""
        mock_func = AsyncMock(return_value="success")
        
        result = await circuit_breaker.call(mock_func, "arg1", kwarg1="value1")
        
        assert result == "success"
        mock_func.assert_called_once_with("arg1", kwarg1="value1")
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.success_rate == 1.0

    @pytest.mark.asyncio
    async def test_failure_count_increments(self, circuit_breaker):
        """Test that failure count increments on expected exceptions."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        with pytest.raises(NaqConnectionError):
            await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.last_failure_time is not None
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_circuit_opens_on_threshold(self, circuit_breaker):
        """Test that the circuit opens after reaching the failure threshold."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        # Fail enough times to open the circuit
        for i in range(3):
            with pytest.raises(NaqConnectionError):
                await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_calls_blocked_when_open(self, circuit_breaker):
        """Test that calls are blocked when the circuit is open."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        # Fail enough times to open the circuit
        for i in range(3):
            with pytest.raises(NaqConnectionError):
                await circuit_breaker.call(mock_func)
        
        # Circuit should now be open
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Next call should be blocked immediately
        mock_func.reset_mock()
        with pytest.raises(NaqConnectionError, match="Circuit breaker 'test-circuit' is open"):
            await circuit_breaker.call(mock_func)
        
        # Original function should not have been called
        mock_func.assert_not_called()

    @pytest.mark.asyncio
    async def test_half_open_state_after_timeout(self, circuit_breaker):
        """Test that the circuit transitions to HALF_OPEN after the recovery timeout."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        # Fail enough times to open the circuit
        for i in range(3):
            with pytest.raises(NaqConnectionError):
                await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        await asyncio.sleep(1.1)
        
        # Next call should transition to HALF_OPEN
        mock_func.reset_mock()
        mock_func.side_effect = NaqConnectionError("Still failed")
        with pytest.raises(NaqConnectionError):
            await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_circuit_closes_on_successful_call(self, circuit_breaker):
        """Test that the circuit closes on a successful call in HALF_OPEN state."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        # Fail enough times to open the circuit
        for i in range(3):
            with pytest.raises(NaqConnectionError):
                await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        await asyncio.sleep(1.1)
        
        # Make a successful call
        mock_func.reset_mock()
        mock_func.side_effect = None
        mock_func.return_value = "success"
        
        result = await circuit_breaker.call(mock_func)
        
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_reopens_on_failure_in_half_open(self, circuit_breaker):
        """Test that the circuit reopens on a failure in HALF_OPEN state."""
        mock_func = AsyncMock(side_effect=NaqConnectionError("Connection failed"))
        
        # Fail enough times to open the circuit
        for i in range(3):
            with pytest.raises(NaqConnectionError):
                await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        await asyncio.sleep(1.1)
        
        # Fail again in HALF_OPEN state
        mock_func.reset_mock()
        mock_func.side_effect = NaqConnectionError("Still failed")
        
        with pytest.raises(NaqConnectionError):
            await circuit_breaker.call(mock_func)
        
        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_force_open(self, circuit_breaker):
        """Test forcing the circuit open."""
        circuit_breaker.force_open()
        
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.last_failure_time is not None
        
        # Calls should be blocked
        mock_func = AsyncMock()
        with pytest.raises(NaqConnectionError, match="Circuit breaker 'test-circuit' is open"):
            await circuit_breaker.call(mock_func)
        
        mock_func.assert_not_called()

    @pytest.mark.asyncio
    async def test_force_close(self, circuit_breaker):
        """Test forcing the circuit closed."""
        # First open the circuit
        circuit_breaker.force_open()
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Then force it closed
        circuit_breaker.force_close()
        
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.last_failure_time is None
        
        # Calls should work normally
        mock_func = AsyncMock(return_value="success")
        result = await circuit_breaker.call(mock_func)
        
        assert result == "success"
        mock_func.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_stats(self, circuit_breaker):
        """Test getting circuit breaker statistics."""
        # Initially
        stats = circuit_breaker.get_stats()
        assert stats["name"] == "test-circuit"
        assert stats["state"] == "CLOSED"
        assert stats["failure_count"] == 0
        assert stats["failure_threshold"] == 3
        assert stats["last_failure_time"] is None
        assert stats["recovery_timeout"] == 1.0
        assert stats["successful_calls"] == 0
        assert stats["total_calls"] == 0
        assert stats["success_rate"] == 1.0
        
        # After a successful call
        mock_func = AsyncMock(return_value="success")
        await circuit_breaker.call(mock_func)
        
        stats = circuit_breaker.get_stats()
        assert stats["successful_calls"] == 1
        assert stats["total_calls"] == 1
        assert stats["success_rate"] == 1.0
        
        # After a failed call
        mock_func.side_effect = NaqConnectionError("Failed")
        with pytest.raises(NaqConnectionError):
            await circuit_breaker.call(mock_func)
        
        stats = circuit_breaker.get_stats()
        assert stats["successful_calls"] == 1
        assert stats["total_calls"] == 2
        assert stats["success_rate"] == 0.5
        assert stats["failure_count"] == 1
        assert stats["last_failure_time"] is not None

    @pytest.mark.asyncio
    async def test_synchronous_function(self, circuit_breaker):
        """Test that the circuit breaker works with synchronous functions."""
        mock_func = MagicMock(return_value="sync_success")
        
        result = await circuit_breaker.call(mock_func, "arg1", kwarg1="value1")
        
        assert result == "sync_success"
        mock_func.assert_called_once_with("arg1", kwarg1="value1")

    @pytest.mark.asyncio
    async def test_unexpected_exception(self, circuit_breaker):
        """Test that unexpected exceptions are handled properly."""
        mock_func = AsyncMock(side_effect=ValueError("Unexpected error"))
        
        with pytest.raises(ValueError):
            await circuit_breaker.call(mock_func)
        
        # Should still count as a failure
        assert circuit_breaker.failure_count == 1


class TestGlobalCircuitBreaker:
    """Test cases for the global circuit breaker manager."""

    @pytest.mark.asyncio
    async def test_get_circuit_breaker(self):
        """Test getting a circuit breaker from the global manager."""
        cb1 = await get_circuit_breaker("test-1")
        cb2 = await get_circuit_breaker("test-1")
        cb3 = await get_circuit_breaker("test-2")
        
        # Should return the same instance for the same name
        assert cb1 is cb2
        assert cb1 is not cb3
        
        # Should have the correct configuration
        assert cb1.name == "test-1"
        assert cb3.name == "test-2"

    @pytest.mark.asyncio
    async def test_get_circuit_breaker_with_custom_config(self):
        """Test getting a circuit breaker with custom configuration."""
        cb = await get_circuit_breaker(
            "custom-test",
            failure_threshold=10,
            recovery_timeout=30.0,
            expected_exception=ValueError,
        )
        
        assert cb.name == "custom-test"
        assert cb.failure_threshold == 10
        assert cb.recovery_timeout == 30.0
        assert cb.expected_exception == ValueError