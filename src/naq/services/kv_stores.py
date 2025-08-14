"""KeyValue Store Service for NAQ.

This module provides a centralized service for NATS KeyValue store operations,
including connection management, pooling, and transaction support.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

from nats.aio.client import Client as NATS
from nats.js.kv import KeyValue

from naq.exceptions import NaqException
from naq.services.base import BaseService

logger = logging.getLogger(__name__)


class KVStoreService(BaseService):
    """Centralized service for KeyValue store operations and management.
    
    This service provides methods for interacting with NATS KeyValue stores,
    including connection management, pooling, and transaction support.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, service_manager=None) -> None:
        """Initialize the KVStoreService.
        
        Args:
            config: Configuration dictionary for the service
            service_manager: The service manager that created this service
        """
        super().__init__(config)
        self._connection_service = None
        self._kv_stores: Dict[str, KeyValue] = {}
        self._service_manager = service_manager

    async def _do_initialize(self) -> None:
        """Initialize the KV store service."""
        # KV stores are lazily initialized when requested
        pass

    async def cleanup(self) -> None:
        """Clean up the KV store service resources."""
        self._kv_stores.clear()
        await super().cleanup()

    async def get_kv_store(self, bucket: str, create_if_missing: bool = True, service_manager=None) -> KeyValue:
        """Get a KeyValue store by bucket name.
        
        Args:
            bucket: The name of the bucket
            create_if_missing: Whether to create the bucket if it doesn't exist
            service_manager: The service manager to use for getting the connection service
            
        Returns:
            The KeyValue store instance
            
        Raises:
            NAQError: If there's an error getting or creating the KV store
        """
        if bucket in self._kv_stores:
            return self._kv_stores[bucket]

        try:
            # Get connection through the connection service
            # Use the service manager passed as parameter, or fallback to the one from this service
            if service_manager is None:
                service_manager = self._service_manager
                
            if service_manager is None:
                raise NaqException("No service manager available to get connection service")
                
            connection_service = await service_manager.get_service("connection")
            connection = await connection_service.get_connection()
            js = connection.jetstream()
            
            if create_if_missing:
                # Try to get existing bucket first
                try:
                    kv = await js.key_value(bucket)
                    self._kv_stores[bucket] = kv
                    return kv
                except Exception:
                    # If it doesn't exist, create it
                    kv = await js.create_key_value(bucket=bucket)
                    self._kv_stores[bucket] = kv
                    return kv
            else:
                kv = await js.key_value(bucket)
                self._kv_stores[bucket] = kv
                return kv
        except Exception as e:
            raise NaqException(f"Failed to get KV store '{bucket}': {e}") from e

    async def put(self, bucket: str, key: str, value: Any) -> None:
        """Put a value in the KeyValue store.
        
        Args:
            bucket: The name of the bucket
            key: The key to store the value under
            value: The value to store
            
        Raises:
            NAQError: If there's an error storing the value
        """
        try:
            logger.debug(f"Putting value in KV store: bucket={bucket}, key={key}")
            kv = await self.get_kv_store(bucket)
            await kv.put(key, value)
            logger.debug(f"Successfully put value in KV store: bucket={bucket}, key={key}")
        except Exception as e:
            logger.error(f"Failed to put value in KV store '{bucket}': {e}", exc_info=True)
            raise NaqException(f"Failed to put value in KV store '{bucket}': {e}") from e

    async def get(self, bucket: str, key: str, service_manager=None) -> Optional[Any]:
        """Get a value from the KeyValue store.
        
        Args:
            bucket: The name of the bucket
            key: The key to retrieve
            service_manager: The service manager to use for getting the connection service
            
        Returns:
            The value if found, None otherwise
            
        Raises:
            NAQError: If there's an error retrieving the value
        """
        try:
            kv = await self.get_kv_store(bucket, service_manager=service_manager)
            try:
                entry = await kv.get(key)
                return entry.value
            except KeyError:
                return None
        except Exception as e:
            raise NaqException(f"Failed to get value from KV store '{bucket}': {e}") from e

    async def delete(self, bucket: str, key: str) -> None:
        """Delete a key from the KeyValue store.
        
        Args:
            bucket: The name of the bucket
            key: The key to delete
            
        Raises:
            NAQError: If there's an error deleting the key
        """
        try:
            kv = await self.get_kv_store(bucket)
            await kv.delete(key)
        except Exception as e:
            raise NaqException(f"Failed to delete key from KV store '{bucket}': {e}") from e

    @asynccontextmanager
    async def kv_transaction(self, bucket: str) -> AsyncGenerator[KeyValue, None]:
        """Context manager for performing atomic KV operations.
        
        Args:
            bucket: The name of the bucket
            
        Yields:
            The KeyValue store instance for atomic operations
        """
        kv = await self.get_kv_store(bucket)
        try:
            yield kv
        except Exception as e:
            raise NaqException(f"KV transaction failed for bucket '{bucket}': {e}") from e


# Import at the end to avoid circular imports
from naq.services.connection import ConnectionService