# src/naq/services/kv_stores.py
from typing import Dict, Optional, Any, AsyncGenerator
from contextlib import asynccontextmanager
from loguru import logger
import asyncio
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from .base import BaseService
from .connection import ConnectionService
from ..exceptions import NaqConnectionError, NaqException


class KVStoreService(BaseService):
    """
    Centralized KeyValue store management service.
    
    This service provides KeyValue store creation, management, and operations for all
    KV stores in the NAQ system, centralizing KV-related functionality
    and providing a consistent interface for KV operations.
    """
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self._connection_service = connection_service
        self._kv_stores: Dict[str, KeyValue] = {}
        logger.info("KVStoreService initialized")
        
    async def _do_initialize(self) -> None:
        """Initialize the KV store service."""
        # No specific initialization needed beyond what BaseService provides
        logger.info("KVStoreService initialized successfully")
        
    async def cleanup(self) -> None:
        """Cleanup KV store service resources."""
        await super().cleanup()
        # Clean up KV store references
        self._kv_stores.clear()
        logger.info("KVStoreService cleaned up")
        
    async def _get_jetstream_context(self, url: Optional[str] = None) -> JetStreamContext:
        """
        Get a JetStream context for a specific URL.
        
        Args:
            url: NATS server URL. If None, uses the default URL.
            
        Returns:
            A JetStream context
            
        Raises:
            NaqConnectionError: If getting JetStream context fails
        """
        try:
            # Get JetStream context from connection service
            js = await self._connection_service.get_jetstream(url)
            return js
        except Exception as e:
            target_url = url or "default URL"
            error_msg = f"Failed to get JetStream context for {target_url}: {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def get_kv_store(self, bucket: str, **config) -> KeyValue:
        """
        Get or create a KV store.
        
        Args:
            bucket: Name of the KV bucket
            **config: Additional KV store configuration options
            
        Returns:
            A KeyValue store instance
            
        Raises:
            NaqConnectionError: If KV store creation or access fails
        """
        # Check if we already have this KV store
        if bucket in self._kv_stores:
            logger.debug(f"Reusing existing KV store '{bucket}'")
            return self._kv_stores[bucket]
            
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Try to get existing KV store
            try:
                kv = await js.key_value(bucket=bucket)
                logger.info(f"Connected to existing KV store '{bucket}'")
                self._kv_stores[bucket] = kv
                return kv
            except BucketNotFoundError:
                # Create new KV store if it doesn't exist
                logger.info(f"KV store '{bucket}' not found, creating...")
                
                # Set default configuration
                create_params = {
                    'bucket': bucket,
                    'description': f"NAQ KV store for {bucket}",
                }
                
                # Override with user-provided configuration
                create_params.update(config)
                
                # Create the KV store with explicit parameters
                kv = await js.create_key_value(
                    bucket=create_params['bucket'],
                    description=create_params.get('description', f"NAQ KV store for {bucket}"),
                    ttl=create_params.get('ttl', 0)
                )
                logger.info(f"KV store '{bucket}' created successfully")
                self._kv_stores[bucket] = kv
                return kv
                
        except Exception as e:
            error_msg = f"Failed to get or create KV store '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def put(self, bucket: str, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        """
        Put a value in the KV store.
        
        Args:
            bucket: Name of the KV bucket
            key: Key to store the value under
            value: Value to store (as bytes)
            ttl: Optional time-to-live in seconds (note: TTL is set at bucket level, not per key)
            
        Raises:
            NaqConnectionError: If the put operation fails
        """
        try:
            # If TTL is provided, we need to create/get a bucket with that TTL
            if ttl is not None:
                # Create a bucket-specific name with TTL suffix
                ttl_bucket = f"{bucket}_ttl_{ttl}"
                try:
                    kv = await self.get_kv_store(ttl_bucket, ttl=ttl)
                except Exception as e:
                    # If TTL bucket creation fails, fall back to regular bucket
                    logger.warning(f"Failed to create TTL bucket '{ttl_bucket}': {e}, using regular bucket")
                    kv = await self.get_kv_store(bucket)
            else:
                kv = await self.get_kv_store(bucket)
            
            # Store the value
            await kv.put(key, value)
                
            logger.debug(f"Stored value in KV store '{bucket}' under key '{key}'")
            
        except Exception as e:
            error_msg = f"Failed to put value in KV store '{bucket}' under key '{key}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def get(self, bucket: str, key: str) -> Optional[bytes]:
        """
        Get a value from the KV store.
        
        Args:
            bucket: Name of the KV bucket
            key: Key to retrieve
            
        Returns:
            The value as bytes, or None if the key doesn't exist
            
        Raises:
            NaqConnectionError: If the get operation fails
        """
        try:
            # First try the main bucket
            kv = await self.get_kv_store(bucket)
            
            try:
                entry = await kv.get(key)
                if entry and entry.value is not None:
                    logger.debug(f"Retrieved value from KV store '{bucket}' under key '{key}'")
                    return entry.value
            except KeyNotFoundError:
                logger.debug(f"Key '{key}' not found in main KV store '{bucket}'")
                
            # If not found in main bucket, try to find it in TTL buckets
            # This is a fallback mechanism for keys stored with TTL
            try:
                # List all buckets that might contain this key
                js = await self._get_jetstream_context()
                streams = await js.streams_info()
                
                for stream_info in streams:
                    if stream_info and stream_info.config and stream_info.config.name:
                        stream_name = stream_info.config.name
                        # Check if this is a TTL bucket for our main bucket
                        if stream_name.startswith(f"KV_{bucket}_ttl_"):
                            ttl_bucket_name = stream_name[3:]  # Remove "KV_" prefix
                            try:
                                ttl_kv = await self.get_kv_store(ttl_bucket_name)
                                entry = await ttl_kv.get(key)
                                if entry and entry.value is not None:
                                    logger.debug(f"Retrieved value from TTL KV store '{ttl_bucket_name}' under key '{key}'")
                                    return entry.value
                            except KeyNotFoundError:
                                continue
                            except Exception as e:
                                logger.warning(f"Error checking TTL bucket '{ttl_bucket_name}': {e}")
                                continue
            except Exception as e:
                logger.warning(f"Error searching TTL buckets: {e}")
                
            # Key not found in any bucket
            logger.debug(f"Key '{key}' not found in any KV store for bucket '{bucket}'")
            return None
                
        except Exception as e:
            error_msg = f"Failed to get value from KV store '{bucket}' under key '{key}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def delete(self, bucket: str, key: str) -> None:
        """
        Delete a key from the KV store.
        
        Args:
            bucket: Name of the KV bucket
            key: Key to delete
            
        Raises:
            NaqConnectionError: If the delete operation fails
        """
        try:
            # First try the main bucket
            kv = await self.get_kv_store(bucket)
            
            try:
                await kv.delete(key)
                logger.debug(f"Deleted key '{key}' from KV store '{bucket}'")
                return
            except KeyNotFoundError:
                logger.debug(f"Key '{key}' not found in main KV store '{bucket}'")
                
            # If not found in main bucket, try to find and delete it in TTL buckets
            try:
                # List all buckets that might contain this key
                js = await self._get_jetstream_context()
                streams = await js.streams_info()
                
                for stream_info in streams:
                    if stream_info and stream_info.config and stream_info.config.name:
                        stream_name = stream_info.config.name
                        # Check if this is a TTL bucket for our main bucket
                        if stream_name.startswith(f"KV_{bucket}_ttl_"):
                            ttl_bucket_name = stream_name[3:]  # Remove "KV_" prefix
                            try:
                                ttl_kv = await self.get_kv_store(ttl_bucket_name)
                                await ttl_kv.delete(key)
                                logger.debug(f"Deleted key '{key}' from TTL KV store '{ttl_bucket_name}'")
                                return
                            except KeyNotFoundError:
                                continue
                            except Exception as e:
                                logger.warning(f"Error deleting from TTL bucket '{ttl_bucket_name}': {e}")
                                continue
            except Exception as e:
                logger.warning(f"Error searching TTL buckets for deletion: {e}")
                
            # Key not found in any bucket
            logger.debug(f"Key '{key}' not found in any KV store for bucket '{bucket}', nothing to delete")
                
        except Exception as e:
            error_msg = f"Failed to delete key '{key}' from KV store '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    @asynccontextmanager
    async def kv_transaction(self, bucket: str) -> AsyncGenerator[KeyValue, None]:
        """
        Context manager for KV transaction operations.
        
        Provides a safe way to perform multiple KV operations with proper error handling.
        
        Args:
            bucket: Name of the KV bucket
            
        Yields:
            A KeyValue store instance for transaction operations
            
        Example:
            async with kv_service.kv_transaction("my_bucket") as kv:
                await kv.put("key1", b"value1")
                await kv.put("key2", b"value2")
        """
        kv = None
        try:
            kv = await self.get_kv_store(bucket)
            yield kv
        except Exception as e:
            error_msg = f"Error in KV transaction for bucket '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def list_keys(self, bucket: str) -> list:
        """
        List all keys in a KV store.
        
        Args:
            bucket: Name of the KV bucket
            
        Returns:
            List of keys in the KV store
            
        Raises:
            NaqConnectionError: If the list operation fails
        """
        try:
            all_keys = set()
            
            # Get keys from main bucket
            kv = await self.get_kv_store(bucket)
            keys = await kv.keys()
            all_keys.update(keys)
            
            # Get keys from TTL buckets
            try:
                # List all TTL buckets for this main bucket
                js = await self._get_jetstream_context()
                streams = await js.streams_info()
                
                for stream_info in streams:
                    if stream_info and stream_info.config and stream_info.config.name:
                        stream_name = stream_info.config.name
                        # Check if this is a TTL bucket for our main bucket
                        if stream_name.startswith(f"KV_{bucket}_ttl_"):
                            ttl_bucket_name = stream_name[3:]  # Remove "KV_" prefix
                            try:
                                ttl_kv = await self.get_kv_store(ttl_bucket_name)
                                ttl_keys = await ttl_kv.keys()
                                all_keys.update(ttl_keys)
                            except Exception as e:
                                logger.warning(f"Error listing keys from TTL bucket '{ttl_bucket_name}': {e}")
                                continue
            except Exception as e:
                logger.warning(f"Error searching TTL buckets for listing keys: {e}")
                
            logger.debug(f"Retrieved {len(all_keys)} keys from KV store '{bucket}' and its TTL buckets")
            return list(all_keys)
            
        except Exception as e:
            error_msg = f"Failed to list keys in KV store '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def purge_bucket(self, bucket: str) -> None:
        """
        Purge all keys from a KV store.
        
        Args:
            bucket: Name of the KV bucket
            
        Raises:
            NaqConnectionError: If the purge operation fails
        """
        try:
            # Purge main bucket
            kv = await self.get_kv_store(bucket)
            keys = await kv.keys()
            
            for key in keys:
                await kv.delete(key)
                
            logger.info(f"Purged all keys from main KV store '{bucket}'")
            
            # Purge TTL buckets
            try:
                # List all TTL buckets for this main bucket
                js = await self._get_jetstream_context()
                streams = await js.streams_info()
                
                for stream_info in streams:
                    if stream_info and stream_info.config and stream_info.config.name:
                        stream_name = stream_info.config.name
                        # Check if this is a TTL bucket for our main bucket
                        if stream_name.startswith(f"KV_{bucket}_ttl_"):
                            ttl_bucket_name = stream_name[3:]  # Remove "KV_" prefix
                            try:
                                ttl_kv = await self.get_kv_store(ttl_bucket_name)
                                ttl_keys = await ttl_kv.keys()
                                
                                for key in ttl_keys:
                                    await ttl_kv.delete(key)
                                    
                                logger.info(f"Purged all keys from TTL KV store '{ttl_bucket_name}'")
                            except Exception as e:
                                logger.warning(f"Error purging TTL bucket '{ttl_bucket_name}': {e}")
                                continue
            except Exception as e:
                logger.warning(f"Error searching TTL buckets for purging: {e}")
                
            logger.info(f"Purged all keys from KV store '{bucket}' and its TTL buckets")
            
        except Exception as e:
            error_msg = f"Failed to purge KV store '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e
            
    async def delete_bucket(self, bucket: str) -> None:
        """
        Delete a KV store.
        
        Args:
            bucket: Name of the KV bucket
            
        Raises:
            NaqConnectionError: If the delete operation fails
        """
        try:
            # Get JetStream context
            js = await self._get_jetstream_context()
            
            # Delete the main KV store
            try:
                await js.delete_key_value(bucket=bucket)
                # Remove from our cache
                self._kv_stores.pop(bucket, None)
                logger.info(f"Deleted main KV store '{bucket}'")
            except Exception as e:
                logger.warning(f"Failed to delete main KV store '{bucket}': {e}")
            
            # Delete TTL buckets
            try:
                # List all TTL buckets for this main bucket
                streams = await js.streams_info()
                
                for stream_info in streams:
                    if stream_info and stream_info.config and stream_info.config.name:
                        stream_name = stream_info.config.name
                        # Check if this is a TTL bucket for our main bucket
                        if stream_name.startswith(f"KV_{bucket}_ttl_"):
                            ttl_bucket_name = stream_name[3:]  # Remove "KV_" prefix
                            try:
                                await js.delete_key_value(bucket=ttl_bucket_name)
                                # Remove from our cache
                                self._kv_stores.pop(ttl_bucket_name, None)
                                logger.info(f"Deleted TTL KV store '{ttl_bucket_name}'")
                            except Exception as e:
                                logger.warning(f"Failed to delete TTL KV store '{ttl_bucket_name}': {e}")
                                continue
            except Exception as e:
                logger.warning(f"Error searching TTL buckets for deletion: {e}")
                
            logger.info(f"Deleted KV store '{bucket}' and its TTL buckets")
            
        except Exception as e:
            error_msg = f"Failed to delete KV store '{bucket}': {e}"
            logger.error(error_msg)
            raise NaqConnectionError(error_msg) from e