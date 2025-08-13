# src/naq/events/shared_logger.py
"""
Shared event logger manager for NAQ components.

This module provides a centralized way to manage and share event logger instances
across Worker, Queue, and Scheduler components.
"""

import asyncio
import threading
from typing import Optional, Dict, Any
from loguru import logger

from .logger import JobEventLogger, AsyncJobEventLogger
from .storage import NATSJobEventStorage, BaseEventStorage
from ..settings import (
    NAQ_EVENTS_ENABLED,
    NAQ_EVENT_STORAGE_TYPE,
    NAQ_EVENT_STORAGE_URL,
    NAQ_EVENT_STREAM_NAME,
    NAQ_EVENT_SUBJECT_PREFIX,
)


class SharedEventLoggerManager:
    """
    A singleton manager for shared event logger instances.
    
    This class provides a centralized way to create, manage, and share
    event logger instances across different components in the NAQ system.
    It supports both sync and async usage patterns and ensures proper
    initialization and cleanup.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Implement singleton pattern with thread safety."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the shared event logger manager."""
        if self._initialized:
            return
            
        self._sync_logger: Optional[JobEventLogger] = None
        self._async_logger: Optional[AsyncJobEventLogger] = None
        self._storage: Optional[BaseEventStorage] = None
        self._config: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        self._initialized = True
        
        logger.debug("SharedEventLoggerManager initialized")
    
    def configure(
        self,
        enabled: Optional[bool] = None,
        storage_type: Optional[str] = None,
        storage_url: Optional[str] = None,
        stream_name: Optional[str] = None,
        subject_prefix: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Configure the shared event logger manager.
        
        Args:
            enabled: Whether event logging is enabled
            storage_type: Type of storage backend ('nats', etc.)
            storage_url: URL for the storage backend
            stream_name: Name of the stream for events
            subject_prefix: Base subject for events
            **kwargs: Additional configuration options
        """
        with self._lock:
            self._config.update({
                'enabled': enabled if enabled is not None else NAQ_EVENTS_ENABLED,
                'storage_type': storage_type if storage_type is not None else NAQ_EVENT_STORAGE_TYPE,
                'storage_url': storage_url if storage_url is not None else NAQ_EVENT_STORAGE_URL,
                'stream_name': stream_name if stream_name is not None else NAQ_EVENT_STREAM_NAME,
                'subject_prefix': subject_prefix if subject_prefix is not None else NAQ_EVENT_SUBJECT_PREFIX,
                **kwargs
            })
            
            logger.debug(f"SharedEventLoggerManager configured: {self._config}")
    
    def get_sync_logger(self) -> Optional[JobEventLogger]:
        """
        Get the shared synchronous event logger instance.
        
        Returns:
            The shared JobEventLogger instance or None if disabled
        """
        if not self._config.get('enabled', NAQ_EVENTS_ENABLED):
            return None
            
        with self._lock:
            if self._sync_logger is None:
                try:
                    storage_instance = self._config.get('storage_instance')
                    if storage_instance and isinstance(storage_instance, BaseEventStorage):
                        self._storage = storage_instance
                        logger.debug("Using provided storage_instance for synchronous event logger")
                    else:
                        self._storage = NATSJobEventStorage(
                            nats_url=self._config.get('storage_url', NAQ_EVENT_STORAGE_URL)
                        )
                        logger.debug("Created NATSJobEventStorage for synchronous event logger")
                    
                    self._sync_logger = JobEventLogger(storage=self._storage)
                    logger.debug("Created shared synchronous event logger")
                except Exception as e:
                    logger.error(f"Failed to create shared synchronous event logger: {e}")
                    return None
            
            return self._sync_logger
    
    async def get_async_logger(self) -> Optional[AsyncJobEventLogger]:
        """
        Get the shared asynchronous event logger instance.
        
        Returns:
            The shared AsyncJobEventLogger instance or None if disabled
        """
        if not self._config.get('enabled', NAQ_EVENTS_ENABLED):
            return None
            
        async with self._async_lock:
            if self._async_logger is None:
                try:
                    storage_instance = self._config.get('storage_instance')
                    if storage_instance and isinstance(storage_instance, BaseEventStorage):
                        self._storage = storage_instance
                        logger.debug("Using provided storage_instance for asynchronous event logger")
                    else:
                        self._storage = NATSJobEventStorage(
                            nats_url=self._config.get('storage_url', NAQ_EVENT_STORAGE_URL)
                        )
                        logger.debug("Created NATSJobEventStorage for asynchronous event logger")
                    
                    self._async_logger = AsyncJobEventLogger(storage=self._storage)
                    logger.debug("Created shared asynchronous event logger")
                except Exception as e:
                    logger.error(f"Failed to create shared asynchronous event logger: {e}")
                    return None
            
            return self._async_logger
    
    def reset(self) -> None:
        """
        Reset the shared event logger manager.
        
        This clears all cached logger instances and forces reinitialization
        on next access. Useful for testing or when configuration changes.
        """
        with self._lock:
            if self._sync_logger:
                try:
                    # Cleanup sync logger using the stop method
                    if hasattr(self._sync_logger, 'stop'):
                        self._sync_logger.stop()
                except Exception as e:
                    logger.error(f"Error during sync logger cleanup: {e}")
                finally:
                    self._sync_logger = None
            
            if self._storage:
                try:
                    # For sync cleanup, we'll just set to None
                    # Async cleanup will be handled in async_reset
                    pass
                except Exception as e:
                    logger.error(f"Error during storage cleanup: {e}")
                finally:
                    self._storage = None
            
            # Note: Async logger cleanup needs to be done asynchronously
            # This will be handled by the async_reset method
            
        logger.debug("SharedEventLoggerManager reset")
    
    async def async_reset(self) -> None:
        """
        Asynchronously reset the shared event logger manager.
        
        This clears all cached logger instances including async ones
        and forces reinitialization on next access.
        """
        # First do the synchronous part
        self.reset()
        
        # Then handle async-specific cleanup
        async with self._async_lock:
            if self._async_logger:
                try:
                    # Cleanup async logger if it has a cleanup method
                    if hasattr(self._async_logger, 'stop'):
                        await self._async_logger.stop()
                except Exception as e:
                    logger.error(f"Error during async logger cleanup: {e}")
                finally:
                    self._async_logger = None
            
            if self._storage:
                try:
                    # Cleanup storage using the close method if it's a NATSJobEventStorage
                    if isinstance(self._storage, NATSJobEventStorage) and hasattr(self._storage, 'close'):
                        await self._storage.close()
                except Exception as e:
                    logger.error(f"Error during storage cleanup: {e}")
                finally:
                    self._storage = None
            
        logger.debug("SharedEventLoggerManager async reset complete")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration.
        
        Returns:
            The current configuration dictionary
        """
        with self._lock:
            return self._config.copy()
    
    def is_enabled(self) -> bool:
        """
        Check if event logging is enabled.
        
        Returns:
            True if event logging is enabled, False otherwise
        """
        return self._config.get('enabled', NAQ_EVENTS_ENABLED)


# Global instance for easy access
_shared_logger_manager = SharedEventLoggerManager()


def get_shared_event_logger_manager() -> SharedEventLoggerManager:
    """
    Get the global shared event logger manager instance.
    
    Returns:
        The global SharedEventLoggerManager instance
    """
    return _shared_logger_manager


def get_shared_sync_logger() -> Optional[JobEventLogger]:
    """
    Get the shared synchronous event logger instance.
    
    This is a convenience function for quick access to the sync logger.
    
    Returns:
        The shared JobEventLogger instance or None if disabled
    """
    return _shared_logger_manager.get_sync_logger()


async def get_shared_async_logger() -> Optional[AsyncJobEventLogger]:
    """
    Get the shared asynchronous event logger instance.
    
    This is a convenience function for quick access to the async logger.
    
    Returns:
        The shared AsyncJobEventLogger instance or None if disabled
    """
    return await _shared_logger_manager.get_async_logger()


def configure_shared_logger(**kwargs: Any) -> None:
    """
    Configure the shared event logger manager.
    
    This is a convenience function for quick configuration.
    
    Args:
        **kwargs: Configuration options to pass to the manager
    """
    _shared_logger_manager.configure(**kwargs)