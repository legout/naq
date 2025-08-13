# src/naq/services/kv_stores.py
"""
KeyValue Store Service for NAQ.

This service centralizes all KeyValue store operations, replacing the 34+
instances of direct KV store usage throughout the codebase.
"""

from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import cloudpickle
from loguru import logger
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from .base import BaseService
from .connection import ConnectionService
from ..exceptions import NaqConnectionError


class KVStoreService(BaseService):
    """
    KeyValue store management service.
    
    This service provides centralized KV store operations including
    store creation, data operations, and transaction support.
    """

    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        """
        Initialize the KV store service.
        
        Args:
            config: Service configuration
            connection_service: Connection service for NATS operations
        """
        super().__init__(config)
        self.connection_service = connection_service
        self._kv_stores: Dict[str, KeyValue] = {}

    async def _do_initialize(self) -> None:
        """Initialize the KV store service."""
        logger.debug("KV store service initialized")

    async def _do_cleanup(self) -> None:
        """Clean up KV store service resources."""
        self._kv_stores.clear()

    async def get_kv_store(
        self, 
        bucket: str, 
        *, 
        ttl: Optional[int] = None,
        description: Optional[str] = None,
        max_bucket_size: Optional[int] = None,
        history: int = 1,
        create_if_not_exists: bool = True,
        **kwargs
    ) -> KeyValue:
        """
        Get or create a KV store.
        
        Args:
            bucket: KV store bucket name
            ttl: Time to live in seconds (0 for no expiry)
            description: Description for the bucket
            max_bucket_size: Maximum bucket size
            history: Number of historical values to keep
            create_if_not_exists: Create bucket if it doesn't exist
            **kwargs: Additional KV store configuration
            
        Returns:
            KeyValue store instance
            
        Raises:
            NaqConnectionError: If KV store operations fail
        """
        if bucket in self._kv_stores:
            return self._kv_stores[bucket]

        async with self.connection_service.jetstream_scope() as js:
            try:
                # Try to get existing KV store
                kv = await js.key_value(bucket)
                logger.debug(f"Connected to existing KV store: {bucket}")
                
            except BucketNotFoundError:
                if not create_if_not_exists:
                    raise NaqConnectionError(f"KV store '{bucket}' not found")
                
                logger.info(f"Creating KV store: {bucket}")
                try:
                    # Create new KV store
                    kv = await js.create_key_value(
                        bucket=bucket,
                        ttl=ttl or 0,
                        description=description or f"NAQ KV store: {bucket}",
                        max_bucket_size=max_bucket_size,
                        history=history,
                        **kwargs
                    )
                    logger.info(f"KV store '{bucket}' created successfully")
                    
                except Exception as e:
                    raise NaqConnectionError(
                        f"Failed to create KV store '{bucket}': {e}"
                    ) from e
                    
            except Exception as e:
                raise NaqConnectionError(
                    f"Failed to access KV store '{bucket}': {e}"
                ) from e

            # Cache the KV store
            self._kv_stores[bucket] = kv
            return kv

    async def put(
        self, 
        bucket: str, 
        key: str, 
        value: Any, 
        *, 
        serialize: bool = True,
        ttl: Optional[int] = None
    ) -> None:
        """
        Put a value in a KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to store
            value: Value to store
            serialize: Whether to serialize the value with cloudpickle
            ttl: TTL override for this key
            
        Raises:
            NaqConnectionError: If put operation fails
        """
        kv = await self.get_kv_store(bucket, ttl=ttl)
        
        try:
            if serialize and not isinstance(value, (bytes, str)):
                data = cloudpickle.dumps(value)
            elif isinstance(value, str):
                data = value.encode('utf-8')
            else:
                data = value
                
            await kv.put(key, data)
            logger.debug(f"Stored key '{key}' in bucket '{bucket}'")
            
        except Exception as e:
            raise NaqConnectionError(
                f"Failed to put key '{key}' in bucket '{bucket}': {e}"
            ) from e

    async def get(
        self, 
        bucket: str, 
        key: str, 
        *, 
        deserialize: bool = True,
        default: Any = None
    ) -> Any:
        """
        Get a value from a KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to retrieve
            deserialize: Whether to deserialize the value with cloudpickle
            default: Default value if key not found
            
        Returns:
            The stored value or default if not found
        """
        kv = await self.get_kv_store(bucket)
        
        try:
            entry = await kv.get(key)
            if entry is None:
                return default
                
            if deserialize:
                try:
                    return cloudpickle.loads(entry.value)
                except Exception:
                    # Fallback to returning raw bytes if deserialization fails
                    return entry.value
            else:
                return entry.value
                
        except KeyNotFoundError:
            return default
            
        except Exception as e:
            logger.error(f"Error getting key '{key}' from bucket '{bucket}': {e}")
            return default

    async def delete(self, bucket: str, key: str, *, purge: bool = False) -> bool:
        """
        Delete a key from a KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to delete
            purge: Whether to purge the key completely
            
        Returns:
            True if key was deleted, False if not found
        """
        kv = await self.get_kv_store(bucket)
        
        try:
            if purge:
                await kv.delete(key, purge=True)
            else:
                await kv.delete(key)
            
            logger.debug(f"Deleted key '{key}' from bucket '{bucket}'")
            return True
            
        except KeyNotFoundError:
            return False
            
        except Exception as e:
            logger.error(f"Error deleting key '{key}' from bucket '{bucket}': {e}")
            return False

    async def exists(self, bucket: str, key: str) -> bool:
        """
        Check if a key exists in a KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to check
            
        Returns:
            True if key exists, False otherwise
        """
        kv = await self.get_kv_store(bucket)
        
        try:
            entry = await kv.get(key)
            return entry is not None
        except KeyNotFoundError:
            return False
        except Exception:
            return False

    async def keys(self, bucket: str) -> List[str]:
        """
        Get all keys in a KV store.
        
        Args:
            bucket: KV store bucket name
            
        Returns:
            List of keys in the bucket
        """
        kv = await self.get_kv_store(bucket)
        
        try:
            keys = await kv.keys()
            return [key.decode('utf-8') for key in keys]
        except Exception as e:
            logger.error(f"Error listing keys in bucket '{bucket}': {e}")
            return []

    async def update(
        self, 
        bucket: str, 
        key: str, 
        value: Any, 
        last_revision: Optional[int] = None,
        *, 
        serialize: bool = True
    ) -> bool:
        """
        Update a key with optimistic concurrency control.
        
        Args:
            bucket: KV store bucket name
            key: Key to update
            value: New value
            last_revision: Expected last revision for optimistic locking
            serialize: Whether to serialize the value
            
        Returns:
            True if update was successful, False on concurrency conflict
        """
        kv = await self.get_kv_store(bucket)
        
        try:
            if serialize and not isinstance(value, (bytes, str)):
                data = cloudpickle.dumps(value)
            elif isinstance(value, str):
                data = value.encode('utf-8')
            else:
                data = value
                
            if last_revision is not None:
                await kv.update(key, data, last=last_revision)
            else:
                # If no revision specified, use regular put
                await kv.put(key, data)
                
            logger.debug(f"Updated key '{key}' in bucket '{bucket}'")
            return True
            
        except Exception as e:
            if "wrong last sequence" in str(e).lower():
                logger.warning(f"Concurrent modification for key '{key}' in bucket '{bucket}'")
                return False
            else:
                logger.error(f"Error updating key '{key}' in bucket '{bucket}': {e}")
                return False

    async def bulk_put(self, bucket: str, items: Dict[str, Any], *, serialize: bool = True) -> int:
        """
        Put multiple items in a KV store.
        
        Args:
            bucket: KV store bucket name
            items: Dictionary of key-value pairs to store
            serialize: Whether to serialize values
            
        Returns:
            Number of items successfully stored
        """
        kv = await self.get_kv_store(bucket)
        success_count = 0
        
        for key, value in items.items():
            try:
                if serialize and not isinstance(value, (bytes, str)):
                    data = cloudpickle.dumps(value)
                elif isinstance(value, str):
                    data = value.encode('utf-8')
                else:
                    data = value
                    
                await kv.put(key, data)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Error storing key '{key}' in bucket '{bucket}': {e}")
        
        logger.debug(f"Bulk stored {success_count}/{len(items)} items in bucket '{bucket}'")
        return success_count

    async def bulk_get(self, bucket: str, keys: List[str], *, deserialize: bool = True) -> Dict[str, Any]:
        """
        Get multiple items from a KV store.
        
        Args:
            bucket: KV store bucket name
            keys: List of keys to retrieve
            deserialize: Whether to deserialize values
            
        Returns:
            Dictionary of key-value pairs (only includes found keys)
        """
        kv = await self.get_kv_store(bucket)
        result = {}
        
        for key in keys:
            try:
                entry = await kv.get(key)
                if entry is not None:
                    if deserialize:
                        try:
                            result[key] = cloudpickle.loads(entry.value)
                        except Exception:
                            result[key] = entry.value
                    else:
                        result[key] = entry.value
            except KeyNotFoundError:
                continue
            except Exception as e:
                logger.error(f"Error getting key '{key}' from bucket '{bucket}': {e}")
        
        return result

    @asynccontextmanager
    async def kv_transaction(self, bucket: str):
        """
        Transaction context for KV operations.
        
        Note: NATS KV doesn't support true transactions, but this provides
        a context for batching operations and handling errors consistently.
        
        Args:
            bucket: KV store bucket name
            
        Yields:
            The KV store instance for operations
        """
        kv = await self.get_kv_store(bucket)
        try:
            yield kv
        except Exception as e:
            logger.error(f"Error in KV transaction for bucket '{bucket}': {e}")
            raise

    def clear_cache(self) -> None:
        """Clear the KV store cache."""
        self._kv_stores.clear()
        logger.debug("KV store cache cleared")