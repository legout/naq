# tests/test_shared_event_logger_manager.py
"""
Comprehensive tests for the shared event logger manager.

This test file covers:
1. Tests for the shared event logger manager:
   - Test singleton behavior
   - Test initialization and configuration
   - Test cleanup functionality
   - Test both sync and async logger access
"""

import asyncio
import pytest
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

from naq.events.shared_logger import (
    SharedEventLoggerManager,
    get_shared_event_logger_manager,
    get_shared_sync_logger,
    get_shared_async_logger,
    configure_shared_logger,
)
from naq.events.logger import JobEventLogger, AsyncJobEventLogger
from naq.events.storage import NATSJobEventStorage
from naq.models import JobEvent, JobEventType


class TestSharedEventLoggerManager:
    """Test cases for SharedEventLoggerManager class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Reset the shared logger manager before each test
        self.manager = get_shared_event_logger_manager()
        self.manager.reset()

    def teardown_method(self):
        """Clean up after each test method."""
        # Reset the shared logger manager after each test
        asyncio.run(self.manager.async_reset())

    def test_singleton_behavior(self):
        """Test that the shared logger manager is a singleton."""
        manager1 = get_shared_event_logger_manager()
        manager2 = get_shared_event_logger_manager()
        
        # Should be the same instance
        assert manager1 is manager2
        
        # Direct instantiation should also return the same instance
        manager3 = SharedEventLoggerManager()
        assert manager1 is manager3

    def test_initialization_state(self):
        """Test the initial state of the manager."""
        assert self.manager._initialized is True
        assert self.manager._sync_logger is None
        assert self.manager._async_logger is None
        assert self.manager._storage is None
        assert self.manager._config == {}
        assert isinstance(self.manager._lock, threading.Lock)
        assert isinstance(self.manager._async_lock, asyncio.Lock)

    def test_configuration_with_defaults(self):
        """Test that configuration works with default values."""
        self.manager.configure()
        
        config = self.manager.get_config()
        
        # Should have default values
        assert 'enabled' in config
        assert 'storage_type' in config
        assert 'storage_url' in config
        assert 'stream_name' in config
        assert 'subject_prefix' in config

    def test_configuration_with_custom_values(self):
        """Test that configuration works with custom values."""
        custom_config = {
            'enabled': False,
            'storage_type': 'custom',
            'storage_url': 'nats://custom:4222',
            'stream_name': 'CUSTOM_STREAM',
            'subject_prefix': 'custom.subject',
            'custom_option': 'custom_value'
        }
        
        self.manager.configure(**custom_config)
        
        config = self.manager.get_config()
        
        for key, value in custom_config.items():
            assert config[key] == value

    def test_configuration_update_partial(self):
        """Test that partial configuration updates only specified values."""
        # First configure with some values
        initial_config = {
            'enabled': True,
            'storage_url': 'nats://initial:4222',
            'stream_name': 'INITIAL_STREAM'
        }
        self.manager.configure(**initial_config)
        
        # Then update with partial config
        update_config = {
            'enabled': False,
            'storage_type': 'updated'
        }
        self.manager.configure(**update_config)
        
        config = self.manager.get_config()
        
        # Check that updated values changed
        assert config['enabled'] is False
        assert config['storage_type'] == 'updated'
        
        # Check that other values remained
        assert config['storage_url'] == 'nats://initial:4222'
        assert config['stream_name'] == 'INITIAL_STREAM'

    def test_is_enabled_function(self):
        """Test that is_enabled function works correctly."""
        # Test with enabled=True
        self.manager.configure(enabled=True)
        assert self.manager.is_enabled() is True
        
        # Test with enabled=False
        self.manager.configure(enabled=False)
        assert self.manager.is_enabled() is False

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    @patch('naq.events.shared_logger.JobEventLogger')
    def test_get_sync_logger_when_enabled(self, mock_logger_class, mock_storage_class):
        """Test getting sync logger when enabled."""
        self.manager.configure(enabled=True, storage_url='nats://test:4222')
        
        # Mock the storage and logger
        mock_storage = MagicMock()
        mock_logger = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_logger_class.return_value = mock_logger
        
        # Get the sync logger
        sync_logger = self.manager.get_sync_logger()
        
        # Verify the storage was created with the correct URL
        mock_storage_class.assert_called_once_with(nats_url='nats://test:4222')
        
        # Verify the logger was created with the correct storage
        mock_logger_class.assert_called_once_with(storage=mock_storage)
        
        # Verify the returned logger is the mocked one
        assert sync_logger is mock_logger
        
        # Verify the logger is cached
        assert self.manager._sync_logger is mock_logger
        assert self.manager._storage is mock_storage

    def test_get_sync_logger_when_disabled(self):
        """Test getting sync logger when disabled."""
        self.manager.configure(enabled=False)
        
        sync_logger = self.manager.get_sync_logger()
        
        # Should return None when disabled
        assert sync_logger is None

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    @patch('naq.events.shared_logger.JobEventLogger')
    def test_sync_logger_reuse(self, mock_logger_class, mock_storage_class):
        """Test that the same sync logger instance is reused."""
        self.manager.configure(enabled=True)
        
        # Mock the storage
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        
        # Get the sync logger twice
        sync_logger1 = self.manager.get_sync_logger()
        sync_logger2 = self.manager.get_sync_logger()
        
        # Verify the storage was created only once
        mock_storage_class.assert_called_once()
        
        # Verify the same logger instance is returned
        assert sync_logger1 is sync_logger2

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    @patch('naq.events.shared_logger.JobEventLogger')
    def test_sync_logger_thread_safety(self, mock_logger_class, mock_storage_class):
        """Test that sync logger access is thread-safe."""
        self.manager.configure(enabled=True)
        
        # Mock the storage and logger
        mock_storage = MagicMock()
        mock_logger = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_logger_class.return_value = mock_logger
        
        loggers = []
        exceptions = []
        
        def get_logger():
            try:
                logger = self.manager.get_sync_logger()
                loggers.append(logger)
            except Exception as e:
                exceptions.append(e)
        
        # Create multiple threads to access the logger simultaneously
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=get_logger)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify no exceptions occurred
        assert len(exceptions) == 0
        
        # Verify all threads got the same logger instance
        assert len(set(loggers)) == 1
        assert all(logger is mock_logger for logger in loggers)
        
        # Verify storage was created only once
        mock_storage_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_async_logger_when_enabled(self):
        """Test getting async logger when enabled."""
        self.manager.configure(enabled=True, storage_url='nats://test:4222')
        
        with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            # Mock the storage and logger
            mock_storage = MagicMock()
            mock_logger = MagicMock()
            mock_storage_class.return_value = mock_storage
            mock_logger_class.return_value = mock_logger
            
            # Get the async logger
            async_logger = await self.manager.get_async_logger()
            
            # Verify the storage was created with the correct URL
            mock_storage_class.assert_called_once_with(nats_url='nats://test:4222')
            
            # Verify the logger was created with the correct storage
            mock_logger_class.assert_called_once_with(storage=mock_storage)
            
            # Verify the returned logger is the mocked one
            assert async_logger is mock_logger
            
            # Verify the logger is cached
            assert self.manager._async_logger is mock_logger
            assert self.manager._storage is mock_storage

    @pytest.mark.asyncio
    async def test_get_async_logger_when_disabled(self):
        """Test getting async logger when disabled."""
        self.manager.configure(enabled=False)
        
        async_logger = await self.manager.get_async_logger()
        
        # Should return None when disabled
        assert async_logger is None

    @pytest.mark.asyncio
    async def test_async_logger_reuse(self):
        """Test that the same async logger instance is reused."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            # Mock the storage
            mock_storage = MagicMock()
            mock_storage_class.return_value = mock_storage
            
            # Get the async logger twice
            async_logger1 = await self.manager.get_async_logger()
            async_logger2 = await self.manager.get_async_logger()
            
            # Verify the storage was created only once
            mock_storage_class.assert_called_once()
            
            # Verify the same logger instance is returned
            assert async_logger1 is async_logger2

    @pytest.mark.asyncio
    async def test_async_logger_concurrency_safety(self):
        """Test that async logger access is concurrency-safe."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            # Mock the storage and logger
            mock_storage = MagicMock()
            mock_logger = MagicMock()
            mock_storage_class.return_value = mock_storage
            mock_logger_class.return_value = mock_logger
            
            loggers = []
            exceptions = []
            
            async def get_logger():
                try:
                    logger = await self.manager.get_async_logger()
                    loggers.append(logger)
                except Exception as e:
                    exceptions.append(e)
            
            # Create multiple tasks to access the logger simultaneously
            tasks = []
            for _ in range(10):
                task = asyncio.create_task(get_logger())
                tasks.append(task)
            
            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Verify no exceptions occurred
            assert len(exceptions) == 0
            
            # Verify all tasks got the same logger instance
            assert len(set(loggers)) == 1
            assert all(logger is mock_logger for logger in loggers)
            
            # Verify storage was created only once
            mock_storage_class.assert_called_once()

    @patch('naq.events.shared_logger.logger')
    @patch('naq.events.shared_logger.NATSJobEventStorage')
    def test_sync_logger_creation_error_handling(self, mock_storage_class, mock_logger):
        """Test that errors during sync logger creation are handled gracefully."""
        self.manager.configure(enabled=True)
        
        # Make the storage constructor raise an exception
        mock_storage_class.side_effect = Exception("Connection failed")
        
        # Get the sync logger
        sync_logger = self.manager.get_sync_logger()
        
        # Verify None is returned when there's an error
        assert sync_logger is None
        
        # Verify the error was logged
        mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_logger_creation_error_handling(self):
        """Test that errors during async logger creation are handled gracefully."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.logger') as mock_logger, \
             patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class:
            
            # Make the storage constructor raise an exception
            mock_storage_class.side_effect = Exception("Connection failed")
            
            # Get the async logger
            async_logger = await self.manager.get_async_logger()
            
            # Verify None is returned when there's an error
            assert async_logger is None
            
            # Verify the error was logged
            mock_logger.error.assert_called_once()

    def test_reset_functionality(self):
        """Test that reset functionality works correctly."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage'), \
             patch('naq.events.shared_logger.JobEventLogger') as mock_logger_class:
            
            # Create a new mock for each call
            mock_logger1 = MagicMock()
            mock_logger2 = MagicMock()
            
            # Set up the mock to return different instances
            mock_logger_class.side_effect = [mock_logger1, mock_logger2]
            
            # Get the sync logger
            sync_logger = self.manager.get_sync_logger()
            assert sync_logger is not None
            assert sync_logger is mock_logger1
            
            # Reset the manager
            self.manager.reset()
            
            # Verify the logger was cleared
            assert self.manager._sync_logger is None
            assert self.manager._storage is None
            
            # Get the sync logger again
            sync_logger2 = self.manager.get_sync_logger()
            
            # Verify a new logger was created
            assert sync_logger2 is not None
            assert sync_logger2 is mock_logger2
            assert sync_logger is not sync_logger2

    @pytest.mark.asyncio
    async def test_async_reset_functionality(self):
        """Test that async reset functionality works correctly."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage'), \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            mock_logger = MagicMock()
            mock_logger_class.return_value = mock_logger
            
            # Get the async logger
            async_logger = await self.manager.get_async_logger()
            assert async_logger is not None
            
            # Reset the manager
            await self.manager.async_reset()
            
            # Verify the logger was cleared
            assert self.manager._async_logger is None
            assert self.manager._storage is None
            
            # Get the async logger again
            async_logger2 = await self.manager.get_async_logger()
            
            # Verify a new logger was created
            assert async_logger2 is not None
            assert async_logger is not async_logger2

    def test_convenience_functions(self):
        """Test that convenience functions work correctly."""
        # Test configure_shared_logger
        configure_shared_logger(enabled=False, storage_url='nats://test:4222')
        
        manager = get_shared_event_logger_manager()
        assert manager.is_enabled() is False
        assert manager.get_config()['storage_url'] == 'nats://test:4222'
        
        # Test get_shared_sync_logger when disabled
        sync_logger = get_shared_sync_logger()
        assert sync_logger is None
        
        # Test get_shared_async_logger when disabled
        async_logger = asyncio.run(get_shared_async_logger())
        assert async_logger is None

    def test_mixed_sync_and_async_access(self):
        """Test that sync and async logger access work together correctly."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
             patch('naq.events.shared_logger.JobEventLogger') as mock_sync_logger_class, \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_async_logger_class:
            
            # Mock the storage and loggers
            mock_storage = MagicMock()
            mock_sync_logger = MagicMock()
            mock_async_logger = MagicMock()
            
            mock_storage_class.return_value = mock_storage
            mock_sync_logger_class.return_value = mock_sync_logger
            mock_async_logger_class.return_value = mock_async_logger
            
            # Get sync logger first
            sync_logger = self.manager.get_sync_logger()
            assert sync_logger is mock_sync_logger
            
            # Verify storage is shared
            assert self.manager._storage is mock_storage
            
            # Get async logger
            async_logger = asyncio.run(self.manager.get_async_logger())
            assert async_logger is mock_async_logger
            
            # Verify storage is still the same instance
            assert self.manager._storage is mock_storage
            
            # Verify storage was created only once
            mock_storage_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_reset_while_logger_in_use(self):
        """Test async reset behavior when logger might be in use."""
        self.manager.configure(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage') as mock_storage_class, \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            # Mock the storage and logger
            mock_storage = MagicMock()
            mock_logger = MagicMock()
            mock_storage_class.return_value = mock_storage
            mock_logger_class.return_value = mock_logger
            
            # Get the async logger
            async_logger = await self.manager.get_async_logger()
            assert async_logger is not None
            
            # Simulate logger in use by holding a reference
            logger_ref = async_logger
            
            # Reset the manager
            await self.manager.async_reset()
            
            # Verify the manager's reference was cleared
            assert self.manager._async_logger is None
            assert self.manager._storage is None
            
            # Our reference should still be valid (not affected by reset)
            assert logger_ref is mock_logger

    def test_multiple_configure_calls(self):
        """Test that multiple configure calls work correctly."""
        # Initial configuration
        self.manager.configure(
            enabled=True,
            storage_url='nats://initial:4222',
            stream_name='INITIAL_STREAM'
        )
        
        # Update configuration
        self.manager.configure(
            enabled=False,
            storage_type='updated',
            subject_prefix='updated.subject'
        )
        
        # Verify final configuration
        config = self.manager.get_config()
        assert config['enabled'] is False
        assert config['storage_type'] == 'updated'
        assert config['subject_prefix'] == 'updated.subject'
        assert config['storage_url'] == 'nats://initial:4222'  # Unchanged
        assert config['stream_name'] == 'INITIAL_STREAM'  # Unchanged