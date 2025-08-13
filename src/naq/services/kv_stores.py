"""
KeyValue Store Service

This module provides a centralized service for managing NATS KeyValue stores,
including pooling, transaction support, and TTL management for atomic operations.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional, Union

import cloudpickle
import msgspec
from loguru import logger
from nats.js.errors import BucketNotFoundError, KeyNotFoundError
from nats.js.kv import KeyValue

from ..exceptions import NaqException
from .base import BaseService, ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from .connection import ConnectionService


class KVStoreServiceConfig(msgspec.Struct):
    """
    Configuration for the KVStoreService.

    Attributes:
        default_bucket_ttl: Default TTL for KV buckets in seconds
        default_key_ttl: Default TTL for keys in seconds
        max_pool_size: Maximum number of KV stores to pool
        enable_transactions: Whether to enable transaction support
        auto_create_buckets: Whether to automatically create buckets if they don't exist
    """

    default_bucket_ttl: Optional[int] = None
    default_key_ttl: Optional[int] = None
    max_pool_size: int = 10
    enable_transactions: bool = True
    auto_create_buckets: bool = True

    def as_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "default_bucket_ttl": self.default_bucket_ttl,
            "default_key_ttl": self.default_key_ttl,
            "max_pool_size": self.max_pool_size,
            "enable_transactions": self.enable_transactions,
            "auto_create_buckets": self.auto_create_buckets,
        }


class KVStoreService(BaseService):
    """
    Centralized KeyValue store management service.

    This service provides pooled NATS KeyValue stores, transaction support,
    and TTL management for atomic operations.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        connection_service: Optional[ConnectionService] = None,
        nats_client: Optional[Any] = None
    ) -> None:
        """
        Initialize the KV store service.

        Args:
            config: Optional configuration for the service.
            connection_service: Optional ConnectionService dependency.
            nats_client: Optional direct NATS client for testing.
        """
        super().__init__(config)
        self._kv_config = self._extract_kv_config()
        self._connection_service = connection_service
        self._nats_client = nats_client
        self._kv_stores: Dict[str, KeyValue] = {}
        self._lock = asyncio.Lock()

    def _extract_kv_config(self) -> KVStoreServiceConfig:
        """
        Extract KV-specific configuration from the service config.

        Returns:
            KVStoreServiceConfig instance with KV parameters.
        """
        # Start with default config
        kv_config = KVStoreServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "default_bucket_ttl" in custom_settings:
                kv_config.default_bucket_ttl = custom_settings["default_bucket_ttl"]

            if "default_key_ttl" in custom_settings:
                kv_config.default_key_ttl = custom_settings["default_key_ttl"]

            if "max_pool_size" in custom_settings:
                kv_config.max_pool_size = custom_settings["max_pool_size"]

            if "enable_transactions" in custom_settings:
                kv_config.enable_transactions = custom_settings["enable_transactions"]

            if "auto_create_buckets" in custom_settings:
                kv_config.auto_create_buckets = custom_settings["auto_create_buckets"]

        return kv_config

    async def _do_initialize(self) -> None:
        """
        Initialize the KV store service.

        This method validates the configuration and ensures the connection
        service is available.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing KVStoreService")

            # Validate configuration
            if self._kv_config.max_pool_size <= 0:
                raise ServiceInitializationError("max_pool_size must be positive")

            # Ensure connection service or NATS client is available
            if self._connection_service is None and self._nats_client is None:
                raise ServiceInitializationError("ConnectionService or NATS client is required")

            # Ensure connection service is initialized if provided
            if self._connection_service is not None and not self._connection_service.is_initialized:
                await self._connection_service.initialize()

            self._logger.info("KVStoreService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize KVStoreService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up KV store service resources.

        This method clears the KV store pool.
        """
        try:
            self._logger.info("Cleaning up KVStoreService")

            # Clear the KV store pool
            async with self._lock:
                self._kv_stores.clear()

            self._logger.info("KVStoreService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup KVStoreService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def get_kv_store(self, bucket_name: str) -> KeyValue:
        """
        Get a KeyValue store instance from the pool.

        This method returns an existing KV store if available, or creates
        a new one with the configured parameters.

        Args:
            bucket_name: Name of the KV bucket.

        Returns:
            A KeyValue store instance.

        Raises:
            NaqException: If getting the KV store fails.
        """
        async with self._lock:
            # Check if we already have a cached KV store
            if bucket_name in self._kv_stores:
                return self._kv_stores[bucket_name]

            try:
                # Get JetStream context from connection service or NATS client
                if self._connection_service is not None:
                    js = await self._connection_service.get_jetstream()
                else:
                    # Use direct NATS client
                    js = self._nats_client.jetstream()

                # Try to get existing KV store
                try:
                    kv = await js.key_value(bucket=bucket_name)
                    self._logger.debug(f"Connected to KV store '{bucket_name}'")
                except Exception:
                    # Try to create if not found and auto-create is enabled
                    if self._kv_config.auto_create_buckets:
                        self._logger.info(
                            f"KV store '{bucket_name}' not found, creating..."
                        )
                        kv = await js.create_key_value(
                            bucket=bucket_name,
                            ttl=self._kv_config.default_bucket_ttl or 0,
                            description=f"NAQ KV store for {bucket_name}",
                        )
                        self._logger.info(f"KV store '{bucket_name}' created")
                    else:
                        raise NaqException(
                            f"KV store '{bucket_name}' not found and auto_create is disabled"
                        )

                # Cache the KV store
                self._kv_stores[bucket_name] = kv

                # Enforce pool size limit
                if len(self._kv_stores) > self._kv_config.max_pool_size:
                    # Remove the oldest entry (simple LRU)
                    oldest_bucket = next(iter(self._kv_stores))
                    del self._kv_stores[oldest_bucket]
                    self._logger.debug(f"Removed KV store '{oldest_bucket}' from pool")

                return kv

            except Exception as e:
                error_msg = f"Failed to get KV store '{bucket_name}': {e}"
                self._logger.error(error_msg)
                raise NaqException(error_msg) from e

    async def put(
        self,
        bucket_name: str,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        serialize: bool = True,
    ) -> None:
        """
        Store a key-value pair in the specified KV store.

        Args:
            bucket_name: Name of the KV bucket.
            key: Key to store the value under.
            value: Value to store.
            ttl: Optional TTL for the key in seconds.
            serialize: Whether to serialize the value using cloudpickle.

        Raises:
            NaqException: If storing the value fails.
        """
        try:
            kv = await self.get_kv_store(bucket_name)

            # Serialize value if requested
            if serialize:
                data = cloudpickle.dumps(value)
            else:
                data = value

            # Use configured TTL if not provided
            key_ttl = ttl if ttl is not None else self._kv_config.default_key_ttl

            # Store the value
            await kv.put(key, data)

            self._logger.debug(f"Stored value for key '{key}' in bucket '{bucket_name}'")

        except Exception as e:
            error_msg = f"Failed to put key '{key}' in bucket '{bucket_name}': {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def get(
        self, bucket_name: str, key: str, deserialize: bool = True
    ) -> Any:
        """
        Retrieve a value from the specified KV store.

        Args:
            bucket_name: Name of the KV bucket.
            key: Key to retrieve.
            deserialize: Whether to deserialize the value using cloudpickle.

        Returns:
            The retrieved value.

        Raises:
            NaqException: If retrieving the value fails.
        """
        try:
            kv = await self.get_kv_store(bucket_name)

            # Get the value
            entry = await kv.get(key)

            # Deserialize if requested
            if deserialize:
                return cloudpickle.loads(entry.value)
            else:
                return entry.value

        except KeyNotFoundError:
            raise NaqException(f"Key '{key}' not found in bucket '{bucket_name}'") from None
        except Exception as e:
            error_msg = f"Failed to get key '{key}' from bucket '{bucket_name}': {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def delete(self, bucket_name: str, key: str, purge: bool = False) -> bool:
        """
        Delete a key from the specified KV store.

        Args:
            bucket_name: Name of the KV bucket.
            key: Key to delete.
            purge: Whether to purge the key completely.

        Returns:
            True if the key was deleted, False if it didn't exist.

        Raises:
            NaqException: If deleting the key fails.
        """
        try:
            kv = await self.get_kv_store(bucket_name)

            # Check if key exists first
            try:
                await kv.get(key)
            except KeyNotFoundError:
                self._logger.debug(f"Key '{key}' not found in bucket '{bucket_name}'")
                return False

            # If we get here, the key exists, so delete it
            if purge:
                await kv.delete(key, purge=True)
            else:
                await kv.delete(key)
            
            self._logger.debug(f"Deleted key '{key}' from bucket '{bucket_name}'")
            return True

        except Exception as e:
            error_msg = f"Failed to delete key '{key}' from bucket '{bucket_name}': {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    @asynccontextmanager
    async def kv_transaction(self, bucket_name: str) -> AsyncIterator["KVTransaction"]:
        """
        Async context manager for atomic KV operations.

        This method provides a transaction context for performing atomic
        operations on a KV store.

        Args:
            bucket_name: Name of the KV bucket.

        Yields:
            A KVTransaction instance for atomic operations.

        Raises:
            NaqException: If transaction operations fail.
        """
        if not self._kv_config.enable_transactions:
            raise NaqException("Transactions are disabled in configuration")

        kv = await self.get_kv_store(bucket_name)
        transaction = KVTransaction(kv, bucket_name, self._logger)

        try:
            yield transaction
            await transaction.commit()
        except Exception as e:
            await transaction.rollback()
            raise NaqException(f"Transaction failed: {e}") from e

    @property
    def kv_config(self) -> KVStoreServiceConfig:
        """Get the KV store configuration."""
        return self._kv_config

    @property
    def active_buckets(self) -> list[str]:
        """Get the list of active bucket names."""
        return list(self._kv_stores.keys())


class KVTransaction:
    """
    Transaction context for atomic KV operations.

    This class provides a simple transaction mechanism for KV operations,
    supporting put, get, and delete operations that can be committed or rolled back.
    """

    def __init__(self, kv: KeyValue, bucket_name: str, logger: Any) -> None:
        """
        Initialize the transaction.

        Args:
            kv: The KeyValue store instance.
            bucket_name: Name of the KV bucket.
            logger: Logger instance.
        """
        self._kv = kv
        self._bucket_name = bucket_name
        self._logger = logger
        self._operations: list[dict] = []
        self._committed = False

    async def put(
        self, key: str, value: Any, ttl: Optional[int] = None, serialize: bool = True
    ) -> None:
        """
        Queue a put operation for the transaction.

        Args:
            key: Key to store the value under.
            value: Value to store.
            ttl: Optional TTL for the key in seconds.
            serialize: Whether to serialize the value using cloudpickle.
        """
        if self._committed:
            raise NaqException("Transaction already committed")

        # Serialize value if requested
        if serialize:
            data = cloudpickle.dumps(value)
        else:
            data = value

        self._operations.append({
            "type": "put",
            "key": key,
            "data": data,
            "ttl": ttl,
        })

    async def get(self, key: str, deserialize: bool = True) -> Any:
        """
        Get a value from the KV store.

        Args:
            key: Key to retrieve.
            deserialize: Whether to deserialize the value using cloudpickle.

        Returns:
            The retrieved value.
        """
        try:
            entry = await self._kv.get(key)
            if deserialize:
                return cloudpickle.loads(entry.value)
            else:
                return entry.value
        except KeyNotFoundError:
            raise NaqException(f"Key '{key}' not found") from None

    async def delete(self, key: str, purge: bool = False) -> None:
        """
        Queue a delete operation for the transaction.

        Args:
            key: Key to delete.
            purge: Whether to purge the key completely.
        """
        if self._committed:
            raise NaqException("Transaction already committed")

        self._operations.append({
            "type": "delete",
            "key": key,
            "purge": purge,
        })

    async def commit(self) -> None:
        """
        Commit the transaction.

        This method applies all queued operations in order.

        Raises:
            NaqException: If committing the transaction fails.
        """
        if self._committed:
            return

        try:
            for operation in self._operations:
                if operation["type"] == "put":
                    if operation["ttl"] is not None:
                        await self._kv.put(
                            operation["key"], 
                            operation["data"], 
                            ttl=operation["ttl"]
                        )
                    else:
                        await self._kv.put(operation["key"], operation["data"])
                elif operation["type"] == "delete":
                    if operation["purge"]:
                        await self._kv.delete(operation["key"], purge=True)
                    else:
                        await self._kv.delete(operation["key"])

            self._committed = True
            self._logger.debug(f"Committed transaction with {len(self._operations)} operations")

        except Exception as e:
            error_msg = f"Failed to commit transaction: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def rollback(self) -> None:
        """
        Rollback the transaction.

        This method clears all queued operations without applying them.
        """
        self._operations.clear()
        self._committed = False
        self._logger.debug("Rolled back transaction")