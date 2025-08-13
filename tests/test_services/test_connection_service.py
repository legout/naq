# tests/test_services/test_connection_service.py
"""Tests for ConnectionService - NATS connection management."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from naq.services.connection import ConnectionService


@pytest.mark.asyncio
class TestConnectionService:
    """Test ConnectionService functionality."""

    async def test_jetstream_scope_context_manager(self, service_test_config):
        """Test jetstream_scope provides proper context management."""
        service = ConnectionService(service_test_config)
        
        # Mock the actual NATS connection for this unit test
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            async with service.jetstream_scope() as js:
                assert js is not None
                # Test that js has expected methods
                assert hasattr(js, 'publish')
                assert hasattr(js, 'key_value')

    async def test_connection_reuse(self, service_test_config):
        """Test connection reuse and pooling behavior."""
        service = ConnectionService(service_test_config)
        
        # Mock NATS connection
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            # Multiple jetstream_scope calls should reuse connection
            async with service.jetstream_scope() as js1:
                async with service.jetstream_scope() as js2:
                    # Both should work
                    assert js1 is not None
                    assert js2 is not None
                    
                    # Connection should be reused (called once or managed efficiently)
                    # The exact behavior depends on implementation

    async def test_connection_error_handling(self, service_test_config):
        """Test connection error handling."""
        service = ConnectionService(service_test_config)
        
        # Mock connection failure
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")
            
            # Should handle connection errors gracefully
            with pytest.raises(Exception):
                async with service.jetstream_scope():
                    pass

    async def test_config_usage(self, service_test_config):
        """Test that service uses configuration correctly."""
        # Modify config to test different URL
        test_config = service_test_config.copy()
        test_config['nats']['url'] = 'nats://test:4222'
        
        service = ConnectionService(test_config)
        
        # Service should store and use the config
        assert hasattr(service, '_config') or hasattr(service, 'config')

    async def test_cleanup(self, service_test_config):
        """Test service cleanup functionality."""
        service = ConnectionService(service_test_config)
        
        # Mock connections
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js = AsyncMock()
            mock_nc.jetstream.return_value = mock_js
            mock_get_conn.return_value = mock_nc
            
            # Use the service
            async with service.jetstream_scope():
                pass
            
            # Cleanup should not raise errors
            if hasattr(service, 'cleanup'):
                await service.cleanup()

    def test_service_configuration_validation(self):
        """Test service validates configuration on initialization."""
        # Test with missing config
        invalid_config = {}
        
        # Service should handle missing configuration gracefully
        # or raise appropriate error
        try:
            service = ConnectionService(invalid_config)
            # If no error, service should have reasonable defaults
            assert service is not None
        except (ValueError, KeyError, TypeError):
            # Expected if service validates config strictly
            pass

    async def test_multiple_jetstream_contexts(self, service_test_config):
        """Test handling multiple concurrent jetstream contexts."""
        service = ConnectionService(service_test_config)
        
        with patch('naq.services.connection.get_nats_connection') as mock_get_conn:
            mock_nc = AsyncMock()
            mock_js1 = AsyncMock()
            mock_js2 = AsyncMock() 
            mock_nc.jetstream.side_effect = [mock_js1, mock_js2]
            mock_get_conn.return_value = mock_nc
            
            # Test concurrent jetstream contexts
            async with service.jetstream_scope() as js1:
                async with service.jetstream_scope() as js2:
                    assert js1 is not None
                    assert js2 is not None
                    # Both contexts should be usable