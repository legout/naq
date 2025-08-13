# Task 05: Create Service Layer - Subtasks

This directory contains the detailed subtasks for creating a centralized service layer to eliminate code duplication and provide consistent interfaces for common operations across the NAQ codebase.

## Overview

The service layer creation involves implementing 8 core services that will replace the 44+ instances of repeated NATS connection patterns and other duplicated logic:

- **base.py** - Base service classes and utilities
- **connection.py** - Centralized NATS connection management
- **streams.py** - JetStream stream operations
- **kv_stores.py** - KeyValue store operations
- **jobs.py** - Job execution services
- **events.py** - Event logging services
- **scheduler.py** - Scheduler operations

## Subtask List

### 05.01: Create Service Package Structure
- **File**: `01_create_service_package_structure.md`
- **Description**: Create the basic directory structure and files for the new service layer package
- **Duration**: 1 hour

### 05.02: Implement Base Service Classes
- **File**: `02_implement_base_service_classes.md`
- **Description**: Implement BaseService abstract base class and ServiceManager for service instance and dependency management
- **Duration**: 3-4 hours

### 05.03: Implement ConnectionService
- **File**: `03_implement_connection_service.md`
- **Description**: Implement ConnectionService to centralize all NATS connection management and eliminate 44+ duplications
- **Duration**: 4.5-5.5 hours

### 05.04: Implement StreamService
- **File**: `04_implement_stream_service.md`
- **Description**: Implement StreamService to centralize JetStream stream creation, management, and operations
- **Duration**: 4.5-5.5 hours

### 05.05: Implement KVStoreService
- **File**: `05_implement_kv_store_service.md`
- **Description**: Implement KVStoreService to centralize KeyValue store operations and management
- **Duration**: 4.5-5.5 hours

### 05.06: Implement JobService
- **File**: `06_implement_job_service.md`
- **Description**: Implement JobService to centralize job execution, result storage, and lifecycle management
- **Duration**: 6-7 hours

### 05.07: Implement EventService
- **File**: `07_implement_event_service.md`
- **Description**: Implement EventService to centralize event logging, processing, and monitoring
- **Duration**: 4.5-5.5 hours

### 05.08: Implement SchedulerService
- **File**: `08_implement_scheduler_service.md`
- **Description**: Implement SchedulerService to centralize scheduler operations and scheduled job management
- **Duration**: 6-7 hours

## Total Estimated Time
- **Implementation**: 34-42 hours
- **Testing**: 12-15 hours
- **Total**: 46-57 hours (largest task)

## Dependencies

### Upstream Dependencies
- **Tasks 01-04 (Split packages)**: Services will use split components
- **Task 07 (YAML Configuration)**: Services need configuration system

### Downstream Dependencies
- **Tasks 09-12**: Other integration tasks depend on services

### Subtask Dependencies
```
05.01 → 05.02 → 05.03 → 05.04 → 05.05 → 05.06 → 05.07 → 05.08
```

## Success Criteria

### Overall Task Success
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

### Testing Checklist
- [ ] Service initialization and cleanup work correctly
- [ ] Connection pooling and reuse working
- [ ] Service dependency injection working
- [ ] All service operations work correctly
- [ ] Resource cleanup happens properly
- [ ] Error handling consistent across services
- [ ] Performance tests show improvements
- [ ] Integration tests pass with service layer
- [ ] No regression in functionality

## Files to Create/Modify

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

## Implementation Strategy

### Phase 1: Core Infrastructure (05.01-05.03)
- Create package structure and base classes
- Implement ConnectionService to replace connection patterns
- High impact, lower risk

### Phase 2: Data Services (05.04-05.05)
- Implement StreamService for stream operations
- Implement KVStoreService for KV operations
- Medium impact, medium risk

### Phase 3: Business Services (05.06-05.07)
- Implement JobService for job execution
- Implement EventService for event logging
- Medium impact, medium risk

### Phase 4: Integration Services (05.08)
- Implement SchedulerService for scheduling
- Complete service layer implementation
- Lower impact, higher integration complexity

## Migration Strategy

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

## Key Features

### ConnectionService
- Connection pooling and reuse
- Automatic connection lifecycle management
- Context manager pattern for safe resource handling
- Configuration-driven connection parameters
- Error recovery and reconnection logic

### StreamService
- Consistent stream creation and configuration
- Stream lifecycle management
- Stream health monitoring
- Batch stream operations

### KVStoreService
- KV store pooling and management
- Transaction support for atomic operations
- TTL management
- Bulk operations support
- Error handling and retry logic

### JobService
- Complete job lifecycle management
- Integrated event logging
- Result storage and retrieval
- Failure handling and retry logic
- Performance metrics collection

### EventService
- High-performance event logging
- Event streaming and filtering
- Event history queries
- Batch event processing
- Event analytics support

### SchedulerService
- Scheduled job lifecycle management
- Due job detection and triggering
- Schedule modification operations
- Leader election support
- Event integration

## Configuration Integration

Services are configured through a central configuration system:

```python
config = {
    "nats_url": "nats://localhost:4222",
    "connection_pool_size": 10,
    "event_buffer_size": 100,
    "event_flush_interval": 5.0,
    "leader_check_interval": 30.0,
    # ... other service-specific configs
}
```

## Usage Examples

### Job Service Usage
```python
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
async with ServiceManager(config) as services:
    conn_service = await services.get_service(ConnectionService)
    
    async with conn_service.connection_scope() as conn:
        # Use connection
        js = await conn_service.get_jetstream()
        # Operations...
```

## Performance Benefits

- **Connection Pooling**: Reuse connections instead of creating new ones
- **Batch Operations**: Process multiple operations together
- **Caching**: Cache frequently accessed data
- **Async Operations**: Leverage async/await for better concurrency
- **Resource Management**: Automatic cleanup prevents resource leaks

## Error Handling

All services implement consistent error handling:
- Custom exception types for different error scenarios
- Automatic retry logic for transient failures
- Comprehensive logging for debugging
- Graceful degradation when services are unavailable