# Task 05: Create Centralized Service Layer

## Overview
Create a centralized service layer to eliminate code duplication and provide consistent interfaces for common operations across the NAQ codebase. This addresses the 44+ instances of repeated NATS connection patterns and other duplicated logic.

## Current State
- NATS connection patterns repeated 44+ times across codebase
- Inconsistent error handling and logging patterns
- Mixed responsibilities in queue, worker, and scheduler classes
- No centralized configuration or resource management

## Target Structure
```
src/naq/services/
├── __init__.py           # Service layer public API
├── connection.py         # Centralized NATS connection management
├── streams.py           # JetStream stream operations  
├── kv_stores.py         # KeyValue store operations
├── jobs.py              # Job execution and result services
├── events.py            # Event logging and processing services
├── scheduler.py         # Scheduler operations service
└── base.py              # Base service classes and utilities
```

## Service Layer Architecture

### Base Service Pattern
All services follow consistent patterns:
- Configuration management
- Connection lifecycle
- Error handling and logging
- Resource cleanup
- Context manager support

### Service Dependencies
```
ConnectionService (base)
├── StreamService
├── KVStoreService  
├── EventService
├── JobService
└── SchedulerService
```

## Detailed Implementation

### 1. base.py - Base Service Classes
**Purpose**: Foundation classes and utilities for all services

**Content:**
```python
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import asyncio
from contextlib import asynccontextmanager

class BaseService(ABC):
    """Base class for all NAQ services."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the service."""
        if not self._initialized:
            await self._do_initialize()
            self._initialized = True
            
    @abstractmethod
    async def _do_initialize(self) -> None:
        """Implement service-specific initialization."""
        
    async def cleanup(self) -> None:
        """Cleanup service resources."""
        self._initialized = False
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

class ServiceManager:
    """Manages service instances and dependencies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._services: Dict[str, BaseService] = {}
        
    async def get_service(self, service_type: type) -> BaseService:
        """Get or create service instance."""
```

**Requirements:**
- Abstract base class for consistent service interfaces
- Service lifecycle management (initialize, cleanup)
- Context manager support for resource management
- Service registry and dependency management
- Consistent error handling patterns

### 2. connection.py - NATS Connection Service
**Purpose**: Centralize all NATS connection management (eliminate 44+ duplications)

**Current Problems:**
- `get_nats_connection()` called in 44+ places
- Inconsistent connection lifecycle management
- No connection pooling optimization
- Mixed connection configuration

**Solution:**
```python
class ConnectionService(BaseService):
    """Centralized NATS connection management."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._connections: Dict[str, NATSClient] = {}
        self._js_contexts: Dict[str, JetStreamContext] = {}
        
    async def get_connection(self, url: Optional[str] = None) -> NATSClient:
        """Get pooled NATS connection."""
        
    async def get_jetstream(self, url: Optional[str] = None) -> JetStreamContext:
        """Get JetStream context."""
        
    @asynccontextmanager
    async def connection_scope(self, url: Optional[str] = None):
        """Context manager for connection operations."""
        conn = await self.get_connection(url)
        try:
            yield conn
        finally:
            # Connection returned to pool automatically
```

**Key Features:**
- Connection pooling and reuse
- Automatic connection lifecycle management
- Context manager pattern for safe resource handling
- Configuration-driven connection parameters
- Error recovery and reconnection logic

### 3. streams.py - JetStream Stream Service
**Purpose**: Centralize stream creation, management, and operations

**Content:**
```python
class StreamService(BaseService):
    """JetStream stream management service."""
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self.connection_service = connection_service
        
    async def ensure_stream(self, name: str, subjects: List[str], **config) -> None:
        """Ensure stream exists with proper configuration."""
        
    async def get_stream_info(self, name: str) -> StreamInfo:
        """Get stream information."""
        
    async def delete_stream(self, name: str) -> None:
        """Delete stream."""
        
    async def purge_stream(self, name: str, subject: Optional[str] = None) -> None:
        """Purge stream messages."""
```

**Key Features:**
- Consistent stream creation and configuration
- Stream lifecycle management
- Stream health monitoring
- Batch stream operations

### 4. kv_stores.py - KeyValue Store Service
**Purpose**: Centralize KV store operations and management

**Content:**
```python
class KVStoreService(BaseService):
    """KeyValue store management service."""
    
    def __init__(self, config: Dict[str, Any], connection_service: ConnectionService):
        super().__init__(config)
        self.connection_service = connection_service
        self._kv_stores: Dict[str, KeyValue] = {}
        
    async def get_kv_store(self, bucket: str, **config) -> KeyValue:
        """Get or create KV store."""
        
    async def put(self, bucket: str, key: str, value: bytes, ttl: Optional[int] = None) -> None:
        """Put value in KV store."""
        
    async def get(self, bucket: str, key: str) -> Optional[bytes]:
        """Get value from KV store."""
        
    async def delete(self, bucket: str, key: str) -> None:
        """Delete key from KV store."""
        
    @asynccontextmanager
    async def kv_transaction(self, bucket: str):
        """Transaction context for KV operations."""
```

**Key Features:**
- KV store pooling and management
- Transaction support for atomic operations
- TTL management
- Bulk operations support
- Error handling and retry logic

### 5. jobs.py - Job Execution Service  
**Purpose**: Centralize job execution, result storage, and lifecycle management

**Content:**
```python
class JobService(BaseService):
    """Job execution and lifecycle management service."""
    
    def __init__(self, config: Dict[str, Any], 
                 connection_service: ConnectionService,
                 kv_service: KVStoreService,
                 event_service: EventService):
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service
        
    async def execute_job(self, job: Job) -> JobResult:
        """Execute job with full lifecycle management."""
        
    async def store_result(self, job_id: str, result: JobResult) -> None:
        """Store job result with event logging."""
        
    async def get_result(self, job_id: str) -> Optional[JobResult]:
        """Get job result."""
        
    async def handle_job_failure(self, job: Job, error: Exception) -> None:
        """Handle job failure with retry logic."""
```

**Key Features:**
- Complete job lifecycle management
- Integrated event logging
- Result storage and retrieval
- Failure handling and retry logic
- Performance metrics collection

### 6. events.py - Event Service
**Purpose**: Centralize event logging, processing, and monitoring

**Content:**
```python
class EventService(BaseService):
    """Event logging and processing service."""
    
    def __init__(self, config: Dict[str, Any], 
                 connection_service: ConnectionService,
                 stream_service: StreamService):
        super().__init__(config)
        self.connection_service = connection_service
        self.stream_service = stream_service
        self._logger: Optional[AsyncJobEventLogger] = None
        
    async def log_event(self, event: JobEvent) -> None:
        """Log job event."""
        
    async def log_job_started(self, job_id: str, worker_id: str, queue: str) -> None:
        """Convenience method for job started event."""
        
    async def stream_events(self, filters: Optional[Dict] = None) -> AsyncIterator[JobEvent]:
        """Stream events with optional filtering."""
        
    async def get_event_history(self, job_id: str) -> List[JobEvent]:
        """Get event history for job."""
```

**Key Features:**
- High-performance event logging
- Event streaming and filtering
- Event history queries
- Batch event processing
- Event analytics support

### 7. scheduler.py - Scheduler Service
**Purpose**: Centralize scheduler operations and scheduled job management

**Content:**
```python
class SchedulerService(BaseService):
    """Scheduler operations service."""
    
    def __init__(self, config: Dict[str, Any],
                 connection_service: ConnectionService, 
                 kv_service: KVStoreService,
                 event_service: EventService):
        super().__init__(config)
        self.connection_service = connection_service
        self.kv_service = kv_service
        self.event_service = event_service
        
    async def schedule_job(self, job: Job, schedule: Schedule) -> str:
        """Schedule job for future execution."""
        
    async def trigger_due_jobs(self) -> List[str]:
        """Find and trigger jobs that are due."""
        
    async def cancel_scheduled_job(self, job_id: str) -> bool:
        """Cancel scheduled job."""
        
    async def pause_scheduled_job(self, job_id: str) -> bool:
        """Pause scheduled job."""
```

**Key Features:**
- Scheduled job lifecycle management
- Due job detection and triggering
- Schedule modification operations
- Leader election support
- Event integration

## Service Integration Pattern

### Before: Repeated Code Pattern
```python
# Repeated 44+ times across codebase
async def some_operation():
    nc = await get_nats_connection(nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("some_bucket")
        # ... operations
    finally:
        await close_nats_connection(nc)
```

### After: Service Pattern
```python
# Single pattern used everywhere
async def some_operation():
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        result = await job_service.execute_job(job)
        return result
```

## Implementation Steps

### Step 1: Create Service Infrastructure
1. Create `src/naq/services/` package
2. Implement `base.py` with base classes and service manager
3. Create service registration and dependency injection system

### Step 2: Implement Core Services  
1. **ConnectionService** - Replace all NATS connection patterns
2. **StreamService** - Centralize stream operations
3. **KVStoreService** - Centralize KV operations

### Step 3: Implement Business Services
1. **JobService** - Job execution and lifecycle
2. **EventService** - Event logging and processing  
3. **SchedulerService** - Scheduler operations

### Step 4: Service Integration
1. Update Queue class to use services
2. Update Worker class to use services
3. Update Scheduler class to use services
4. Update CLI commands to use services

### Step 5: Configuration Integration
1. Service configuration from config system
2. Environment variable mapping
3. Default service configurations

## Usage Examples

### Job Service Usage
```python
# In worker or queue code
async with ServiceManager(config) as services:
    job_service = await services.get_service(JobService)
    event_service = await services.get_service(EventService)
    
    # Execute job with integrated event logging
    result = await job_service.execute_job(job)
    
    # Events are automatically logged by JobService
    # Result is automatically stored
```

### Connection Service Usage
```python
# Replaces all get_nats_connection patterns
async with ServiceManager(config) as services:
    conn_service = await services.get_service(ConnectionService)
    
    async with conn_service.connection_scope() as conn:
        # Use connection
        js = await conn_service.get_jetstream()
        # Operations...
```

## Migration Strategy

### Phase 1: Core Services (ConnectionService, StreamService, KVStoreService)
- Replace most critical repeated patterns
- High impact, lower risk

### Phase 2: Business Services (JobService, EventService)
- Replace complex business logic patterns
- Medium impact, medium risk

### Phase 3: Integration Services (SchedulerService)  
- Complete service layer implementation
- Lower impact, higher integration complexity

## Files to Create

### New Files
- `src/naq/services/__init__.py`
- `src/naq/services/base.py`
- `src/naq/services/connection.py`
- `src/naq/services/streams.py`
- `src/naq/services/kv_stores.py`
- `src/naq/services/jobs.py`
- `src/naq/services/events.py`
- `src/naq/services/scheduler.py`

### Files to Update (integrate services)
- `src/naq/queue/` (all files)
- `src/naq/worker/` (all files)  
- `src/naq/scheduler.py`
- `src/naq/cli/` (command files)

## Success Criteria

- [ ] Service layer architecture implemented and working
- [ ] All 44+ NATS connection patterns replaced with ConnectionService
- [ ] Consistent error handling across all services
- [ ] Service dependency injection working correctly
- [ ] Resource management (connections, streams, KV stores) centralized
- [ ] Service context managers working correctly
- [ ] Configuration integration completed
- [ ] All existing functionality preserved
- [ ] Performance improvements from connection pooling
- [ ] Code duplication significantly reduced

## Testing Checklist

- [ ] Service initialization and cleanup work correctly
- [ ] Connection pooling and reuse working
- [ ] Service dependency injection working
- [ ] All service operations work correctly
- [ ] Resource cleanup happens properly
- [ ] Error handling consistent across services
- [ ] Performance tests show improvements
- [ ] Integration tests pass with service layer
- [ ] No regression in functionality

## Dependencies
- **Depends on**: 
  - Tasks 01-04 (Split packages) - services will use split components
  - Task 07 (YAML Configuration) - services need configuration system
- **Blocks**: Tasks 09-12 - other integration tasks depend on services

## Estimated Time
- **Implementation**: 12-15 hours
- **Integration**: 8-10 hours  
- **Testing**: 6-8 hours
- **Total**: 26-33 hours (largest task)