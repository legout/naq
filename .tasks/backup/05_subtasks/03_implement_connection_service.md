# Subtask 05.03: Implement ConnectionService

## Overview
Implement the ConnectionService to centralize all NATS connection management and eliminate the 44+ duplications of connection patterns across the codebase.

## Objectives
- Replace all `get_nats_connection()` calls with ConnectionService
- Implement connection pooling and reuse
- Provide automatic connection lifecycle management
- Add context manager pattern for safe resource handling
- Include configuration-driven connection parameters
- Implement error recovery and reconnection logic

## Implementation Steps

### 1. Implement ConnectionService Class
Create the ConnectionService that inherits from BaseService:

```python
import asyncio
import logging
from typing import Dict, Optional, Any
from contextlib import asynccontextmanager

from .base import BaseService, ServiceError
from naq.connection import NATSClient, get_nats_connection, close_nats_connection
from naq.exceptions import NATSConnectionError

class ConnectionService(BaseService):
    """Centralized NATS connection management."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._connections: Dict[str, NATSClient] = {}
        self._js_contexts: Dict[str, Any] = {}  # JetStream contexts
        self._connection_locks: Dict[str, asyncio.Lock] = {}
        self._logger = logging.getLogger(__name__)
        
    async def _do_initialize(self) -> None:
        """Initialize connection service."""
        # Initialize connection locks for thread-safe access
        self._logger.info("Initializing ConnectionService")
        
    async def _do_cleanup(self) -> None:
        """Cleanup all connections."""
        self._logger.info("Cleaning up ConnectionService")
        
        # Close all connections
        for url, conn in self._connections.items():
            try:
                await close_nats_connection(conn)
                self._logger.debug(f"Closed connection to {url}")
            except Exception as e:
                self._logger.error(f"Error closing connection to {url}: {e}")
                
        self._connections.clear()
        self._js_contexts.clear()
        self._connection_locks.clear()
        
    def _get_connection_lock(self, url: str) -> asyncio.Lock:
        """Get or create connection lock for URL."""
        if url not in self._connection_locks:
            self._connection_locks[url] = asyncio.Lock()
        return self._connection_locks[url]
        
    async def get_connection(self, url: Optional[str] = None) -> NATSClient:
        """Get pooled NATS connection."""
        if url is None:
            url = self.config.get("nats_url", "nats://localhost:4222")
            
        # Check if we already have a connection for this URL
        if url in self._connections:
            conn = self._connections[url]
            # Verify connection is still alive
            try:
                await conn.ping()
                return conn
            except Exception as e:
                self._logger.warning(f"Connection to {url} appears dead: {e}")
                # Remove dead connection
                await close_nats_connection(conn)
                del self._connections[url]
                
        # Create new connection with lock to prevent race conditions
        async with self._get_connection_lock(url):
            # Double-check pattern in case connection was created while waiting for lock
            if url in self._connections:
                return await self.get_connection(url)
                
            self._logger.info(f"Creating new connection to {url}")
            try:
                conn = await get_nats_connection(url)
                self._connections[url] = conn
                return conn
            except Exception as e:
                raise ServiceError(f"Failed to create connection to {url}: {e}")
                
    async def get_jetstream(self, url: Optional[str] = None) -> Any:
        """Get JetStream context."""
        conn = await self.get_connection(url)
        
        # Use connection URL as key for JS context
        js_key = f"{url}_js" if url else "default_js"
        
        if js_key not in self._js_contexts:
            try:
                js = await conn.jetstream()
                self._js_contexts[js_key] = js
                self._logger.debug(f"Created JetStream context for {url}")
            except Exception as e:
                raise ServiceError(f"Failed to create JetStream context for {url}: {e}")
                
        return self._js_contexts[js_key]
        
    @asynccontextmanager
    async def connection_scope(self, url: Optional[str] = None):
        """Context manager for connection operations."""
        conn = await self.get_connection(url)
        try:
            yield conn
        except Exception as e:
            self._logger.error(f"Error in connection scope: {e}")
            raise
            
    @asynccontextmanager  
    async def jetstream_scope(self, url: Optional[str] = None):
        """Context manager for JetStream operations."""
        js = await self.get_jetstream(url)
        try:
            yield js
        except Exception as e:
            self._logger.error(f"Error in JetStream scope: {e}")
            raise
            
    async def health_check(self, url: Optional[str] = None) -> bool:
        """Check if connection is healthy."""
        try:
            conn = await self.get_connection(url)
            await conn.ping()
            return True
        except Exception as e:
            self._logger.warning(f"Health check failed for {url}: {e}")
            return False
            
    async def close_connection(self, url: Optional[str] = None) -> None:
        """Close specific connection."""
        if url is None:
            url = self.config.get("nats_url", "nats://localhost:4222")
            
        if url in self._connections:
            conn = self._connections[url]
            await close_nats_connection(conn)
            del self._connections[url]
            
            # Also remove associated JS context
            js_key = f"{url}_js" if url else "default_js"
            if js_key in self._js_contexts:
                del self._js_contexts[js_key]
                
            self._logger.info(f"Closed connection to {url}")
```

### 2. Update Service Registration
Register the ConnectionService in the service system:

```python
# In __init__.py or base.py
from .connection import ConnectionService

# Register the service
ServiceManager.register_service("connection", ConnectionService)
```

### 3. Add Configuration Support
Enhance configuration handling:

```python
def __init__(self, config: Dict[str, Any]):
    super().__init__(config)
    self._connections: Dict[str, NATSClient] = {}
    self._js_contexts: Dict[str, Any] = {}
    self._connection_locks: Dict[str, asyncio.Lock] = {}
    self._logger = logging.getLogger(__name__)
    
    # Connection-specific configuration
    self.pool_size = config.get("connection_pool_size", 10)
    self.connection_timeout = config.get("connection_timeout", 10)
    self.max_reconnect_attempts = config.get("max_reconnect_attempts", 5)
    self.reconnect_delay = config.get("reconnect_delay", 2)
```

### 4. Implement Connection Pooling
Add connection pooling for better performance:

```python
class ConnectionPool:
    """Simple connection pool for NATS connections."""
    
    def __init__(self, max_size: int = 10):
        self.max_size = max_size
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._created = 0
        
    async def get_connection(self, create_func, *args, **kwargs):
        """Get connection from pool or create new one."""
        try:
            return self._pool.get_nowait()
        except asyncio.QueueEmpty:
            if self._created < self.max_size:
                self._created += 1
                return await create_func(*args, **kwargs)
            else:
                # Pool is full, wait for available connection
                return await self._pool.get()
                
    async def return_connection(self, conn):
        """Return connection to pool."""
        try:
            self._pool.put_nowait(conn)
        except asyncio.QueueFull:
            # Pool is full, close the connection
            await close_nats_connection(conn)
```

### 5. Update Usage Examples
Create usage examples showing the new pattern:

```python
# Old pattern (to be replaced)
async def old_pattern():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        # ... operations
    finally:
        await close_nats_connection(nc)

# New pattern
async def new_pattern():
    async with ServiceManager(config) as services:
        conn_service = await services.get_service(ConnectionService)
        
        async with conn_service.connection_scope() as conn:
            # Use connection
            js = await conn_service.get_jetstream()
            # Operations...
```

## Success Criteria
- [ ] ConnectionService implemented with connection pooling
- [ ] All NATS connection patterns can use ConnectionService
- [ ] Context manager patterns working correctly
- [ ] Connection lifecycle management automated
- [ ] Error recovery and reconnection logic functional
- [ ] Configuration-driven connection parameters working
- [ ] Performance improvements from connection pooling
- [ ] No regression in connection functionality

## Dependencies
- **Depends on**: Subtask 05.02 (Implement Base Service Classes)
- **Prepares for**: Subtask 05.04 (Implement StreamService)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 1.5 hours
- **Total**: 4.5-5.5 hours