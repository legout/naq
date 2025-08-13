# Subtask 05.04: Implement StreamService

## Overview
Implement the StreamService to centralize JetStream stream creation, management, and operations across the codebase.

## Objectives
- Centralize all stream operations through StreamService
- Provide consistent stream creation and configuration
- Implement stream lifecycle management
- Add stream health monitoring capabilities
- Support batch stream operations
- Integrate with ConnectionService for NATS operations

## Implementation Steps

### 1. Implement StreamService Class
Create the StreamService that inherits from BaseService:

```python
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from .connection import ConnectionService
from naq.exceptions import StreamError

class StreamService(BaseService):
    """JetStream stream management service."""
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self.connection_service = connection_service
        self._stream_info_cache: Dict[str, Any] = {}
        self._logger = logging.getLogger(__name__)
        self._cache_ttl = config.get("stream_cache_ttl", 300)  # 5 minutes
        
    async def _do_initialize(self) -> None:
        """Initialize stream service."""
        self._logger.info("Initializing StreamService")
        
    async def _do_cleanup(self) -> None:
        """Cleanup stream service."""
        self._logger.info("Cleaning up StreamService")
        self._stream_info_cache.clear()
        
    async def ensure_stream(self, name: str, subjects: List[str], **config) -> None:
        """Ensure stream exists with proper configuration.
        
        Args:
            name: Stream name
            subjects: List of subjects for the stream
            **config: Additional stream configuration
        """
        try:
            js = await self.connection_service.get_jetstream()
            
            # Check if stream already exists
            try:
                existing_info = await js.stream_info(name)
                self._logger.debug(f"Stream {name} already exists")
                
                # Update stream if configuration differs
                if self._needs_stream_update(existing_info, subjects, config):
                    await self._update_stream(js, name, subjects, config)
                    
            except Exception as e:
                # Stream doesn't exist, create it
                self._logger.info(f"Creating stream {name}")
                stream_config = self._build_stream_config(name, subjects, config)
                await js.add_stream(stream_config)
                
            # Update cache
            self._stream_info_cache[name] = {
                'timestamp': asyncio.get_event_loop().time(),
                'subjects': subjects,
                'config': config
            }
            
        except Exception as e:
            raise ServiceError(f"Failed to ensure stream {name}: {e}")
            
    def _build_stream_config(self, name: str, subjects: List[str], config: Dict[str, Any]) -> Dict[str, Any]:
        """Build stream configuration."""
        default_config = {
            'name': name,
            'subjects': subjects,
            'retention': config.get('retention', 'limits'),
            'max_msgs': config.get('max_msgs', 1000000),
            'max_msgs_per_subject': config.get('max_msgs_per_subject', 100000),
            'max_age': config.get('max_age', 3600),  # 1 hour
            'storage': config.get('storage', 'file'),
            'num_replicas': config.get('num_replicas', 1),
        }
        
        # Merge with provided config
        default_config.update(config)
        return default_config
        
    def _needs_stream_update(self, existing_info: Any, subjects: List[str], config: Dict[str, Any]) -> bool:
        """Check if stream needs to be updated."""
        # Compare subjects
        if set(existing_info.config.subjects) != set(subjects):
            return True
            
        # Compare other configuration
        important_configs = ['retention', 'max_msgs', 'max_age', 'num_replicas']
        for key in important_configs:
            if key in config and str(existing_info.config.get(key)) != str(config.get(key)):
                return True
                
        return False
        
    async def _update_stream(self, js: Any, name: str, subjects: List[str], config: Dict[str, Any]) -> None:
        """Update existing stream configuration."""
        try:
            # For now, we'll recreate the stream
            # In production, you might want more sophisticated update logic
            self._logger.info(f"Updating stream {name} by recreating")
            await js.delete_stream(name)
            stream_config = self._build_stream_config(name, subjects, config)
            await js.add_stream(stream_config)
        except Exception as e:
            raise ServiceError(f"Failed to update stream {name}: {e}")
            
    async def get_stream_info(self, name: str) -> Any:
        """Get stream information."""
        # Check cache first
        cached = self._stream_info_cache.get(name)
        if cached and (asyncio.get_event_loop().time() - cached['timestamp']) < self._cache_ttl:
            return cached
            
        try:
            js = await self.connection_service.get_jetstream()
            info = await js.stream_info(name)
            
            # Update cache
            self._stream_info_cache[name] = {
                'timestamp': asyncio.get_event_loop().time(),
                'subjects': info.config.subjects,
                'config': info.config
            }
            
            return info
        except Exception as e:
            raise ServiceError(f"Failed to get stream info for {name}: {e}")
            
    async def delete_stream(self, name: str) -> None:
        """Delete stream."""
        try:
            js = await self.connection_service.get_jetstream()
            await js.delete_stream(name)
            
            # Remove from cache
            self._stream_info_cache.pop(name, None)
            self._logger.info(f"Deleted stream {name}")
            
        except Exception as e:
            raise ServiceError(f"Failed to delete stream {name}: {e}")
            
    async def purge_stream(self, name: str, subject: Optional[str] = None) -> None:
        """Purge stream messages."""
        try:
            js = await self.connection_service.get_jetstream()
            purge_req = {'stream': name}
            if subject:
                purge_req['filter'] = subject
                
            result = await js.purge(purge_req)
            self._logger.info(f"Purged stream {name}: {result.purged} messages")
            
        except Exception as e:
            raise ServiceError(f"Failed to purge stream {name}: {e}")
            
    async def stream_exists(self, name: str) -> bool:
        """Check if stream exists."""
        try:
            await self.get_stream_info(name)
            return True
        except ServiceError:
            return False
            
    async def list_streams(self) -> List[str]:
        """List all streams."""
        try:
            js = await self.connection_service.get_jetstream()
            streams = await js.streams_info()
            return [stream.name for stream in streams.streams]
        except Exception as e:
            raise ServiceError(f"Failed to list streams: {e}")
            
    @asynccontextmanager
    async def stream_scope(self, name: str, **config):
        """Context manager for stream operations."""
        await self.ensure_stream(name, **config)
        js = await self.connection_service.get_jetstream()
        try:
            yield js
        except Exception as e:
            self._logger.error(f"Error in stream scope for {name}: {e}")
            raise
            
    async def health_check(self, name: str) -> Dict[str, Any]:
        """Check stream health."""
        try:
            info = await self.get_stream_info(name)
            return {
                'name': info.name,
                'subjects': info.config.subjects,
                'messages': info.state.messages,
                'bytes': info.state.bytes,
                'first_seq': info.state.first_seq,
                'last_seq': info.state.last_seq,
                'consumer_count': info.state.consumer_count,
            }
        except Exception as e:
            self._logger.error(f"Health check failed for stream {name}: {e}")
            return {'name': name, 'healthy': False, 'error': str(e)}
```

### 2. Update Service Registration
Register the StreamService in the service system:

```python
# In __init__.py or base.py
from .streams import StreamService

# Register the service
ServiceManager.register_service("streams", StreamService)
```

### 3. Add Batch Operations
Implement batch stream operations for efficiency:

```python
async def ensure_streams(self, streams_config: Dict[str, Dict[str, Any]]) -> None:
    """Ensure multiple streams exist."""
    tasks = []
    for name, config in streams_config.items():
        subjects = config.pop('subjects', [])
        tasks.append(self.ensure_stream(name, subjects, **config))
        
    await asyncio.gather(*tasks, return_exceptions=True)
    
async def batch_delete_streams(self, stream_names: List[str]) -> None:
    """Delete multiple streams."""
    tasks = [self.delete_stream(name) for name in stream_names]
    await asyncio.gather(*tasks, return_exceptions=True)
```

### 4. Add Stream Monitoring
Implement stream monitoring capabilities:

```python
async def monitor_streams(self, interval: float = 60.0) -> AsyncIterator[Dict[str, Any]]:
    """Monitor stream health periodically."""
    while True:
        health_checks = {}
        streams = await self.list_streams()
        
        for stream_name in streams:
            health_checks[stream_name] = await self.health_check(stream_name)
            
        yield health_checks
        await asyncio.sleep(interval)
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_stream_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        await js.add_stream({
            'name': 'my_stream',
            'subjects': ['jobs.>'],
            'retention': 'limits',
            'max_msgs': 1000000
        })
        # ... stream operations
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_stream_pattern():
    async with ServiceManager(config) as services:
        stream_service = await services.get_service(StreamService)
        
        # Ensure stream exists
        await stream_service.ensure_stream(
            name='my_stream',
            subjects=['jobs.>'],
            retention='limits',
            max_msgs=1000000
        )
        
        # Get stream info
        info = await stream_service.get_stream_info('my_stream')
        
        # Stream operations...
```

## Success Criteria
- [ ] StreamService implemented with all core stream operations
- [ ] Stream creation and configuration centralized
- [ ] Stream lifecycle management working
- [ ] Stream health monitoring functional
- [ ] Batch stream operations implemented
- [ ] Integration with ConnectionService working
- [ ] Cache system improving performance
- [ ] All existing stream functionality preserved

## Dependencies
- **Depends on**: Subtask 05.03 (Implement ConnectionService)
- **Prepares for**: Subtask 05.05 (Implement KVStoreService)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 1.5 hours
- **Total**: 4.5-5.5 hours