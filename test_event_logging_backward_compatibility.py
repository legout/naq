#!/usr/bin/env python3
"""
Test script to verify backward compatibility of the shared event logger implementation.

This script tests that the new shared logger implementation works correctly
with existing code and maintains the same behavior as before.
"""

import asyncio
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import naq modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from naq.events.shared_logger import (
    SharedEventLoggerManager,
    get_shared_event_logger_manager,
    get_shared_sync_logger,
    get_shared_async_logger,
    configure_shared_logger,
)
from naq.events.logger import JobEventLogger, AsyncJobEventLogger
from naq.events.storage import NATSJobEventStorage
from naq.settings import (
    NAQ_EVENTS_ENABLED,
    NAQ_EVENT_STORAGE_TYPE,
    NAQ_EVENT_STORAGE_URL,
    NAQ_EVENT_STREAM_NAME,
    NAQ_EVENT_SUBJECT_PREFIX,
)


class TestSharedLoggerBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility of the shared event logger."""

    def setUp(self):
        """Set up test fixtures."""
        # Reset the shared logger manager before each test
        manager = get_shared_event_logger_manager()
        manager.reset()
        
        # Store original settings
        self.original_settings = {
            'NAQ_EVENTS_ENABLED': NAQ_EVENTS_ENABLED,
            'NAQ_EVENT_STORAGE_TYPE': NAQ_EVENT_STORAGE_TYPE,
            'NAQ_EVENT_STORAGE_URL': NAQ_EVENT_STORAGE_URL,
            'NAQ_EVENT_STREAM_NAME': NAQ_EVENT_STREAM_NAME,
            'NAQ_EVENT_SUBJECT_PREFIX': NAQ_EVENT_SUBJECT_PREFIX,
        }
        
        # Reset the manager's config to ensure clean state
        manager._config = {}

    def tearDown(self):
        """Clean up after tests."""
        # Reset the shared logger manager after each test
        manager = get_shared_event_logger_manager()
        asyncio.run(manager.async_reset())
        
        # Restore original settings
        for key, value in self.original_settings.items():
            setattr(sys.modules['naq.settings'], key, value)

    def test_singleton_pattern(self):
        """Test that the shared logger manager is a singleton."""
        manager1 = get_shared_event_logger_manager()
        manager2 = get_shared_event_logger_manager()
        
        self.assertIs(manager1, manager2)
        
        # Also test direct instantiation
        manager3 = SharedEventLoggerManager()
        self.assertIs(manager1, manager3)

    def test_configuration_with_defaults(self):
        """Test that configuration works with default values."""
        manager = get_shared_event_logger_manager()
        
        # Configure with no arguments (should use defaults)
        manager.configure()
        
        config = manager.get_config()
        
        self.assertEqual(config['enabled'], self.original_settings['NAQ_EVENTS_ENABLED'])
        self.assertEqual(config['storage_type'], self.original_settings['NAQ_EVENT_STORAGE_TYPE'])
        self.assertEqual(config['storage_url'], self.original_settings['NAQ_EVENT_STORAGE_URL'])
        self.assertEqual(config['stream_name'], self.original_settings['NAQ_EVENT_STREAM_NAME'])
        self.assertEqual(config['subject_prefix'], self.original_settings['NAQ_EVENT_SUBJECT_PREFIX'])

    def test_configuration_with_custom_values(self):
        """Test that configuration works with custom values."""
        manager = get_shared_event_logger_manager()
        
        custom_config = {
            'enabled': False,
            'storage_type': 'custom',
            'storage_url': 'nats://custom:4222',
            'stream_name': 'CUSTOM_STREAM',
            'subject_prefix': 'custom.subject',
        }
        
        manager.configure(**custom_config)
        
        config = manager.get_config()
        
        for key, value in custom_config.items():
            self.assertEqual(config[key], value)

    def test_convenience_functions(self):
        """Test that convenience functions work correctly."""
        # Test configure_shared_logger
        configure_shared_logger(enabled=False)
        
        manager = get_shared_event_logger_manager()
        self.assertFalse(manager.is_enabled())
        
        # Test get_shared_sync_logger when disabled
        sync_logger = get_shared_sync_logger()
        self.assertIsNone(sync_logger)
        
        # Test get_shared_async_logger when disabled
        async_logger = asyncio.run(get_shared_async_logger())
        self.assertIsNone(async_logger)

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    @patch('naq.events.shared_logger.JobEventLogger')
    def test_sync_logger_creation(self, mock_logger_class, mock_storage_class):
        """Test that sync logger is created correctly."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True, storage_url='nats://test:4222')
        
        # Mock the storage and logger
        mock_storage = MagicMock()
        mock_logger = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_logger_class.return_value = mock_logger
        
        # Get the sync logger
        sync_logger = get_shared_sync_logger()
        
        # Verify the storage was created with the correct URL
        mock_storage_class.assert_called_once_with(nats_url='nats://test:4222')
        
        # Verify the logger was created with the correct storage
        mock_logger_class.assert_called_once_with(storage=mock_storage)
        
        # Verify the returned logger is the mocked one
        self.assertIs(sync_logger, mock_logger)

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    @patch('naq.events.shared_logger.AsyncJobEventLogger')
    async def test_async_logger_creation(self, mock_logger_class, mock_storage_class):
        """Test that async logger is created correctly."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True, storage_url='nats://test:4222')
        
        # Mock the storage and logger
        mock_storage = MagicMock()
        mock_logger = MagicMock()
        mock_storage_class.return_value = mock_storage
        mock_logger_class.return_value = mock_logger
        
        # Get the async logger
        async_logger = await get_shared_async_logger()
        
        # Verify the storage was created with the correct URL
        mock_storage_class.assert_called_once_with(nats_url='nats://test:4222')
        
        # Verify the logger was created with the correct storage
        mock_logger_class.assert_called_once_with(storage=mock_storage)
        
        # Verify the returned logger is the mocked one
        self.assertIs(async_logger, mock_logger)

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    def test_logger_reuse(self, mock_storage_class):
        """Test that the same logger instance is reused."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True)
        
        # Mock the storage
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        
        # Get the sync logger twice
        sync_logger1 = get_shared_sync_logger()
        sync_logger2 = get_shared_sync_logger()
        
        # Verify the storage was created only once
        mock_storage_class.assert_called_once()
        
        # Verify the same logger instance is returned
        self.assertIs(sync_logger1, sync_logger2)

    @patch('naq.events.shared_logger.NATSJobEventStorage')
    async def test_async_logger_reuse(self, mock_storage_class):
        """Test that the same async logger instance is reused."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True)
        
        # Mock the storage
        mock_storage = MagicMock()
        mock_storage_class.return_value = mock_storage
        
        # Get the async logger twice
        async_logger1 = await get_shared_async_logger()
        async_logger2 = await get_shared_async_logger()
        
        # Verify the storage was created only once
        mock_storage_class.assert_called_once()
        
        # Verify the same logger instance is returned
        self.assertIs(async_logger1, async_logger2)

    @patch('naq.events.shared_logger.logger')
    @patch('naq.events.shared_logger.NATSJobEventStorage')
    def test_logger_creation_error_handling(self, mock_storage_class, mock_logger):
        """Test that errors during logger creation are handled gracefully."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True)
        
        # Make the storage constructor raise an exception
        mock_storage_class.side_effect = Exception("Connection failed")
        
        # Get the sync logger
        sync_logger = get_shared_sync_logger()
        
        # Verify None is returned when there's an error
        self.assertIsNone(sync_logger)
        
        # Verify the error was logged
        mock_logger.error.assert_called_once()

    @patch('naq.events.shared_logger.logger')
    @patch('naq.events.shared_logger.NATSJobEventStorage')
    async def test_async_logger_creation_error_handling(self, mock_storage_class, mock_logger):
        """Test that errors during async logger creation are handled gracefully."""
        # Configure with enabled=True
        configure_shared_logger(enabled=True)
        
        # Make the storage constructor raise an exception
        mock_storage_class.side_effect = Exception("Connection failed")
        
        # Get the async logger
        async_logger = await get_shared_async_logger()
        
        # Verify None is returned when there's an error
        self.assertIsNone(async_logger)
        
        # Verify the error was logged
        mock_logger.error.assert_called_once()

    def test_reset_functionality(self):
        """Test that reset functionality works correctly."""
        manager = get_shared_event_logger_manager()
        
        # Configure and get a logger
        configure_shared_logger(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage'), \
             patch('naq.events.shared_logger.JobEventLogger') as mock_logger_class:
            
            # Create a new mock for each call
            mock_logger1 = MagicMock()
            mock_logger2 = MagicMock()
            
            # Set up the mock to return different instances
            mock_logger_class.side_effect = [mock_logger1, mock_logger2]
            
            # Get the sync logger
            sync_logger = get_shared_sync_logger()
            self.assertIsNotNone(sync_logger)
            self.assertIs(sync_logger, mock_logger1)
            
            # Reset the manager
            manager.reset()
            
            # Get the sync logger again
            sync_logger2 = get_shared_sync_logger()
            
            # Verify a new logger was created
            self.assertIsNotNone(sync_logger2)
            self.assertIs(sync_logger2, mock_logger2)
            self.assertNotEqual(sync_logger, sync_logger2)

    async def test_async_reset_functionality(self):
        """Test that async reset functionality works correctly."""
        manager = get_shared_event_logger_manager()
        
        # Configure and get a logger
        configure_shared_logger(enabled=True)
        
        with patch('naq.events.shared_logger.NATSJobEventStorage'), \
             patch('naq.events.shared_logger.AsyncJobEventLogger') as mock_logger_class:
            
            mock_logger = MagicMock()
            mock_logger_class.return_value = mock_logger
            
            # Get the async logger
            async_logger = await get_shared_async_logger()
            self.assertIsNotNone(async_logger)
            
            # Reset the manager
            await manager.async_reset()
            
            # Get the async logger again
            async_logger2 = await get_shared_async_logger()
            
            # Verify a new logger was created
            self.assertIsNotNone(async_logger2)
            self.assertNotEqual(async_logger, async_logger2)

    def test_is_enabled_function(self):
        """Test that is_enabled function works correctly."""
        manager = get_shared_event_logger_manager()
        
        # Test with default settings (should be True from settings)
        self.assertTrue(manager.is_enabled())
        
        # Test with enabled=False
        manager.configure(enabled=False)
        self.assertFalse(manager.is_enabled())
        
        # Test with enabled=True
        manager.configure(enabled=True)
        self.assertTrue(manager.is_enabled())


if __name__ == '__main__':
    unittest.main()