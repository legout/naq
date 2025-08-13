# Subtask 05.05: Implement KVStoreService

## Overview
Implement the KVStoreService to centralize KeyValue store operations and management across the codebase.

## Objectives
- Centralize all KV store operations through KVStoreService
- Implement KV store pooling and management
- Provide transaction support for atomic operations
- Add TTL management capabilities
- Support bulk operations for performance
- Include error handling and retry logic
- Integrate with ConnectionService for NATS operations

## Implementation Steps

### 1. Implement KVStoreService Class
Create the KVStoreService that inherits from BaseService:

```python
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union, AsyncIterator
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from .connection import ConnectionService
from naq.exceptions import KVStoreError

class KVStoreService(BaseService):
    """KeyValue store management service."""
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self.connection_service = connection_service
        self._kv_stores: Dict[str, Any] = {}  # Cache of KV store instances
        self._logger = logging.getLogger(__name__)
        self._cache_ttl = config.get("kv_cache_ttl", 300)  # 5 minutes
        
    async def _do_initialize(self) -> None:
        """Initialize KV store service."""
        self._logger.info("Initializing KVStoreService")
        
    async def _do_cleanup(self) -> None:
        """Cleanup KV store service."""
        self._logger.info("Cleaning up KVStoreService")
        self._kv_stores.clear()
        
    async def get_kv_store(self, bucket: str, **config) -> Any:
        """Get or create KV store instance.
        
        Args:
            bucket: KV store bucket name
            **config: Additional KV store configuration
            
        Returns:
            KV store instance
        """
        # Check if we already have a KV store for this bucket
        if bucket not in self._kv_stores:
            try:
                js = await self.connection_service.get_jetstream()
                kv = await js.key_value(bucket)
                self._kv_stores[bucket] = kv
                self._logger.debug(f"Created KV store for bucket {bucket}")
            except Exception as e:
                raise ServiceError(f"Failed to create KV store for bucket {bucket}: {e}")
                
        return self._kv_stores[bucket]
        
    async def put(self, bucket: str, key: str, value: Union[str, bytes, dict], 
                  ttl: Optional[int] = None, **headers) -> None:
        """Put value in KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to store value under
            value: Value to store (string, bytes, or dict)
            ttl: Time to live in seconds
            **headers: Additional headers for the operation
        """
        try:
            kv = await self.get_kv_store(bucket)
            
            # Convert dict to JSON string if needed
            if isinstance(value, dict):
                import json
                value = json.dumps(value).encode()
            elif isinstance(value, str):
                value = value.encode()
                
            # Prepare put options
            put_options = {}
            if ttl:
                put_options['ttl'] = ttl
                
            # Add headers if provided
            if headers:
                put_options['headers'] = headers
                
            await kv.put(key, value, **put_options)
            self._logger.debug(f"Put value for key {key} in bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to put value for key {key} in bucket {bucket}: {e}")
            
    async def get(self, bucket: str, key: str) -> Optional[bytes]:
        """Get value from KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to retrieve value for
            
        Returns:
            Value as bytes, or None if key doesn't exist
        """
        try:
            kv = await self.get_kv_store(bucket)
            entry = await kv.get(key)
            
            if entry is None:
                return None
                
            return entry.value
            
        except Exception as e:
            raise ServiceError(f"Failed to get value for key {key} in bucket {bucket}: {e}")
            
    async def get_json(self, bucket: str, key: str) -> Optional[dict]:
        """Get value from KV store as JSON.
        
        Args:
            bucket: KV store bucket name
            key: Key to retrieve value for
            
        Returns:
            Value as dict, or None if key doesn't exist
        """
        try:
            value = await self.get(bucket, key)
            if value is None:
                return None
                
            import json
            return json.loads(value.decode())
            
        except Exception as e:
            raise ServiceError(f"Failed to get JSON value for key {key} in bucket {bucket}: {e}")
            
    async def delete(self, bucket: str, key: str) -> None:
        """Delete key from KV store.
        
        Args:
            bucket: KV store bucket name
            key: Key to delete
        """
        try:
            kv = await self.get_kv_store(bucket)
            await kv.delete(key)
            self._logger.debug(f"Deleted key {key} from bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to delete key {key} from bucket {bucket}: {e}")
            
    async def update(self, bucket: str, key: str, value: Union[str, bytes, dict], 
                     revision: int, **headers) -> None:
        """Update value in KV store with specific revision.
        
        Args:
            bucket: KV store bucket name
            key: Key to update
            value: New value
            revision: Expected revision number
            **headers: Additional headers
        """
        try:
            kv = await self.get_kv_store(bucket)
            
            # Convert dict to JSON string if needed
            if isinstance(value, dict):
                import json
                value = json.dumps(value).encode()
            elif isinstance(value, str):
                value = value.encode()
                
            await kv.update(key, value, revision, **headers)
            self._logger.debug(f"Updated key {key} in bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to update key {key} in bucket {bucket}: {e}")
            
    async def history(self, bucket: str, key: str) -> List[dict]:
        """Get history for a key.
        
        Args:
            bucket: KV store bucket name
            key: Key to get history for
            
        Returns:
            List of historical entries
        """
        try:
            kv = await self.get_kv_store(bucket)
            history = await kv.history(key)
            return [entry.dict() for entry in history]
            
        except Exception as e:
            raise ServiceError(f"Failed to get history for key {key} in bucket {bucket}: {e}")
            
    async def purge(self, bucket: str) -> None:
        """Purge entire KV store bucket.
        
        Args:
            bucket: KV store bucket name
        """
        try:
            js = await self.connection_service.get_jetstream()
            await js.purge_kv(bucket)
            self._logger.info(f"Purged KV store bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to purge KV store bucket {bucket}: {e}")
            
    async def create_bucket(self, bucket: str, **config) -> None:
        """Create new KV store bucket.
        
        Args:
            bucket: Bucket name
            **config: Bucket configuration
        """
        try:
            js = await self.connection_service.get_jetstream()
            bucket_config = {
                'bucket': bucket,
                'description': config.get('description', ''),
                'max_value_size': config.get('max_value_size', 0),
                'history': config.get('history', 1),
                'ttl': config.get('ttl', 0),
                'replicas': config.get('replicas', 1),
                'placement': config.get('placement', {}),
                'mirror': config.get('mirror', {}),
                'sources': config.get('sources', []),
            }
            
            await js.create_key_value(bucket_config)
            self._logger.info(f"Created KV store bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to create KV store bucket {bucket}: {e}")
            
    async def delete_bucket(self, bucket: str) -> None:
        """Delete KV store bucket.
        
        Args:
            bucket: Bucket name
        """
        try:
            js = await self.connection_service.get_jetstream()
            await js.delete_key_value(bucket)
            
            # Remove from cache
            self._kv_stores.pop(bucket, None)
            self._logger.info(f"Deleted KV store bucket {bucket}")
            
        except Exception as e:
            raise ServiceError(f"Failed to delete KV store bucket {bucket}: {e}")
            
    @asynccontextmanager
    async def kv_transaction(self, bucket: str):
        """Transaction context for KV operations.
        
        This provides a simple transaction-like interface for multiple
        KV operations that should be atomic.
        """
        kv = await self.get_kv_store(bucket)
        operations = []
        
        def add_operation(op_type: str, *args, **kwargs):
            operations.append((op_type, args, kwargs))
            
        # Create transaction context
        class KVTransaction:
            async def commit(self):
                """Execute all operations in transaction."""
                for op_type, args, kwargs in operations:
                    if op_type == 'put':
                        await kv.put(*args, **kwargs)
                    elif op_type == 'delete':
                        await kv.delete(*args, **kwargs)
                    elif op_type == 'update':
                        await kv.update(*args, **kwargs)
                        
        transaction = KVTransaction()
        
        try:
            yield transaction
            await transaction.commit()
        except Exception as e:
            self._logger.error(f"Transaction failed for bucket {bucket}: {e}")
            raise
            
    async def bulk_put(self, bucket: str, items: Dict[str, Union[str, bytes, dict]], 
                       ttl: Optional[int] = None) -> None:
        """Put multiple values in KV store.
        
        Args:
            bucket: KV store bucket name
            items: Dictionary of key-value pairs
            ttl: Time to live in seconds (applied to all items)
        """
        tasks = []
        for key, value in items.items():
            task = self.put(bucket, key, value, ttl)
            tasks.append(task)
            
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def bulk_get(self, bucket: str, keys: List[str]) -> Dict[str, Optional[bytes]]:
        """Get multiple values from KV store.
        
        Args:
            bucket: KV store bucket name
            keys: List of keys to retrieve
            
        Returns:
            Dictionary mapping keys to values (None for missing keys)
        """
        tasks = []
        for key in keys:
            task = self.get(bucket, key)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {key: results[i] if not isinstance(results[i], Exception) else None 
                for i, key in enumerate(keys)}
```

### 2. Update Service Registration
Register the KVStoreService in the service system:

```python
# In __init__.py or base.py
from .kv_stores import KVStoreService

# Register the service
ServiceManager.register_service("kv_stores", KVStoreService)
```

### 3. Add Retry Logic
Implement retry logic for KV operations:

```python
async def put_with_retry(self, bucket: str, key: str, value: Union[str, bytes, dict], 
                        ttl: Optional[int] = None, max_retries: int = 3) -> None:
    """Put value with retry logic."""
    last_error = None
    
    for attempt in range(max_retries):
        try:
            await self.put(bucket, key, value, ttl)
            return
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                await asyncio.sleep(wait_time)
                
    raise ServiceError(f"Failed to put value after {max_retries} attempts: {last_error}")
```

### 4. Add Monitoring and Health Checks
Implement KV store monitoring:

```python
async def health_check(self, bucket: str) -> Dict[str, Any]:
    """Check KV store bucket health."""
    try:
        kv = await self.get_kv_store(bucket)
        info = await kv.bucket_info()
        
        return {
            'bucket': bucket,
            'values': info.values,
            'history': info.history,
            'ttl': info.ttl,
            'max_value_size': info.max_value_size,
            'replicas': info.replicas,
            'healthy': True,
        }
    except Exception as e:
        self._logger.error(f"Health check failed for KV bucket {bucket}: {e}")
        return {'bucket': bucket, 'healthy': False, 'error': str(e)}
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_kv_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("my_bucket")
        await kv.put("my_key", "my_value".encode())
        value = await kv.get("my_key")
        # ... KV operations
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_kv_pattern():
    async with ServiceManager(config) as services:
        kv_service = await services.get_service(KVStoreService)
        
        # Put value
        await kv_service.put("my_bucket", "my_key", "my_value")
        
        # Get value
        value = await kv_service.get("my_bucket", "my_key")
        
        # Transaction
        async with kv_service.kv_transaction("my_bucket") as tx:
            tx.add_operation('put', 'key1', 'value1')
            tx.add_operation('put', 'key2', 'value2')
            await tx.commit()
        
        # Bulk operations
        await kv_service.bulk_put("my_bucket", {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        })
```

## Success Criteria
- [ ] KVStoreService implemented with all core KV operations
- [ ] KV store pooling and management working
- [ ] Transaction support for atomic operations implemented
- [ ] TTL management capabilities functional
- [ ] Bulk operations for performance implemented
- [ ] Error handling and retry logic working
- [ ] Integration with ConnectionService successful
- [ ] All existing KV functionality preserved

## Dependencies
- **Depends on**: Subtask 05.04 (Implement StreamService)
- **Prepares for**: Subtask 05.06 (Implement JobService)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 1.5 hours
- **Total**: 4.5-5.5 hours