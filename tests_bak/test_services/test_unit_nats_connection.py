import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from naq.connection import utils
from naq.exceptions import NaqConnectionError


class TestNatsConnection:
    """Test cases for the test_nats_connection function."""

    @pytest.mark.asyncio
    async def test_nats_connection_success(self):
        """Test that test_nats_connection returns True for a successful connection."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.flush = AsyncMock()
        
        with patch('naq.connection.utils.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is True
            mock_nc.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_nats_connection_failure_flush_error(self):
        """Test that test_nats_connection returns False when flush operation fails."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.flush = AsyncMock(side_effect=Exception("Flush failed"))
        
        with patch('naq.connection.utils.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is False
            mock_nc.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_nats_connection_failure_connection_error(self):
        """Test that test_nats_connection returns False when connection establishment fails."""
        # Mock the nats_connection context manager to raise an exception
        with patch('naq.connection.utils.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.side_effect = NaqConnectionError("Connection failed")
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_nats_connection_failure_generic_exception(self):
        """Test that test_nats_connection returns False when a generic exception occurs."""
        # Mock the nats_connection context manager to raise an exception
        with patch('naq.connection.utils.nats_connection') as mock_context:
            mock_context.return_value.__aenter__.side_effect = Exception("Generic error")
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is False

    @pytest.mark.asyncio
    async def test_nats_connection_logs_success(self):
        """Test that test_nats_connection logs success message."""
        # Mock the nats_connection context manager and its return value
        mock_nc = AsyncMock()
        mock_nc.flush = AsyncMock()
        
        with patch('naq.connection.utils.nats_connection') as mock_context, \
             patch('naq.connection.utils.logger') as mock_logger:
            mock_context.return_value.__aenter__.return_value = mock_nc
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is True
            mock_logger.debug.assert_called_with("NATS connection test successful")

    @pytest.mark.asyncio
    async def test_nats_connection_logs_error(self):
        """Test that test_nats_connection logs error message on failure."""
        # Mock the nats_connection context manager to raise an exception
        error_message = "Connection failed"
        with patch('naq.connection.utils.nats_connection') as mock_context, \
             patch('naq.connection.utils.logger') as mock_logger:
            mock_context.return_value.__aenter__.side_effect = NaqConnectionError(error_message)
            mock_context.return_value.__aexit__.return_value = None
            
            result = await utils.test_nats_connection()
            
            assert result is False
            mock_logger.error.assert_called_with(f"NATS connection test failed: {error_message}")

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_immediate_success(self):
        """Test that wait_for_nats_connection returns True when connection is immediately available."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test:
            mock_test.return_value = True
            
            result = await utils.wait_for_nats_connection(timeout=5.0, retry_delay=0.1)
            
            assert result is True
            mock_test.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_success_after_retries(self):
        """Test that wait_for_nats_connection returns True after a few retries."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep') as mock_sleep:
            
            # Fail first two attempts, succeed on third
            mock_test.side_effect = [False, False, True]
            
            result = await utils.wait_for_nats_connection(timeout=5.0, retry_delay=0.1)
            
            assert result is True
            assert mock_test.call_count == 3
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(0.1)

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_timeout(self):
        """Test that wait_for_nats_connection returns False when timeout is reached."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep') as mock_sleep, \
             patch('naq.connection.utils.logger') as mock_logger:
            
            # Always fail connection test
            mock_test.return_value = False
            
            result = await utils.wait_for_nats_connection(timeout=0.5, retry_delay=0.1)
            
            assert result is False
            # Should be called multiple times until timeout
            assert mock_test.call_count >= 1
            mock_logger.warning.assert_called_with("NATS connection not established within 0.5s timeout")

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_respects_timeout(self):
        """Test that wait_for_nats_connection respects the timeout parameter."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep') as mock_sleep, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Mock time to simulate timeout
            mock_loop.return_value.time.side_effect = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
            
            # Always fail connection test
            mock_test.return_value = False
            
            result = await utils.wait_for_nats_connection(timeout=0.5, retry_delay=0.1)
            
            assert result is False
            # Should stop trying after timeout
            assert mock_test.call_count >= 1

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_custom_retry_delay(self):
        """Test that wait_for_nats_connection uses custom retry delay."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep') as mock_sleep:
            
            # Fail first attempt, succeed on second
            mock_test.side_effect = [False, True]
            
            result = await utils.wait_for_nats_connection(timeout=5.0, retry_delay=0.5)
            
            assert result is True
            assert mock_test.call_count == 2
            mock_sleep.assert_called_once_with(0.5)

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_logs_debug_messages(self):
        """Test that wait_for_nats_connection logs appropriate debug messages."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep'), \
             patch('naq.connection.utils.logger') as mock_logger:
            
            # Fail first attempt, succeed on second
            mock_test.side_effect = [False, True]
            
            result = await utils.wait_for_nats_connection(timeout=5.0, retry_delay=0.1)
            
            assert result is True
            # Check that debug messages were logged
            mock_logger.debug.assert_any_call("Waiting for NATS connection (timeout: 5.0s, retry_delay: 0.1s)")
            mock_logger.debug.assert_any_call("NATS connection test failed, retrying in 0.1s")
            mock_logger.debug.assert_any_call("NATS connection established successfully")

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_default_parameters(self):
        """Test that wait_for_nats_connection works with default parameters."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test:
            mock_test.return_value = True
            
            result = await utils.wait_for_nats_connection()
            
            assert result is True
            mock_test.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_for_nats_connection_adjusts_sleep_time_to_timeout(self):
        """Test that wait_for_nats_connection adjusts sleep time to not exceed timeout."""
        with patch('naq.connection.utils.test_nats_connection') as mock_test, \
             patch('asyncio.sleep') as mock_sleep, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Mock time to simulate timeout
            mock_loop.return_value.time.side_effect = [0.0, 0.4, 0.5]
            
            # Always fail connection test
            mock_test.return_value = False
            
            result = await utils.wait_for_nats_connection(timeout=0.5, retry_delay=0.2)
            
            assert result is False
            # Should sleep for approximately 0.1s on second attempt (remaining time) instead of 0.2s
            # Use pytest.approx to handle floating-point precision issues
            mock_sleep.assert_called_once()
            sleep_arg = mock_sleep.call_args[0][0]
            assert sleep_arg == pytest.approx(0.1)