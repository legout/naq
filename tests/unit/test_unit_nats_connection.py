import pytest
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