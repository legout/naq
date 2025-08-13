# Task 09: Integrate Service Layer Across Codebase

## Overview
Integrate the newly created service layer across all NAQ components (Queue, Worker, Scheduler, CLI) to eliminate code duplication and provide consistent interfaces throughout the codebase.

## Current State
After completing Tasks 01-08:
- Service layer implemented with consistent interfaces
- Common patterns extracted into utilities
- YAML configuration system available
- NATS connection management centralized
- Large files split into focused modules

## Goal
Replace direct NATS operations, connection management, and business logic with service layer calls throughout the codebase to achieve:
- Consistent error handling and logging
- Centralized resource management
- Reduced code duplication
- Improved testability
- Configuration-driven behavior

## Integration Strategy

### Service Dependency Structure
```
CLI Commands
    ↓
Queue/Worker/Scheduler Classes
    ↓  
Business Services (JobService, EventService, SchedulerService)
    ↓
Core Services (ConnectionService, StreamService, KVStoreService)
    ↓
NATS Infrastructure
```

## Detailed Integration Plan

### 1. Queue Package Integration

**Current Pattern (Before):**
```python
# In queue/core.py - Before service integration
class Queue:
    async def enqueue(self, job: Job) -> None:
        nc = await get_nats_connection(self.nats_url)
        try:
            js = await get_jetstream_context(nc)
            stream = await js.add_stream(name=self.queue_name, subjects=[f"{self.queue_name}.*"])
            
            # Serialize job
            serializer = get_serializer()
            payload = serializer.serialize_job(job)
            
            # Publish
            await js.publish(f"{self.queue_name}.job", payload)
            
            # Log event manually
            if events_enabled:
                # Manual event logging...
                
        finally:
            await close_nats_connection(nc)
```

**Service Pattern (After):**
```python
# In queue/core.py - After service integration
from ..services import ServiceManager, JobService, EventService

class Queue:
    def __init__(self, name: str, config: NAQConfig):
        self.name = name
        self.config = config
        self._service_manager: Optional[ServiceManager] = None
    
    async def _get_services(self) -> ServiceManager:
        """Get or create service manager."""
        if self._service_manager is None:
            self._service_manager = ServiceManager(self.config)
            await self._service_manager.initialize()
        return self._service_manager
    
    async def enqueue(self, job: Job) -> None:
        """Enqueue job using service layer."""
        services = await self._get_services()
        job_service = await services.get_service(JobService)
        
        # Service handles serialization, connection management, event logging, etc.
        await job_service.enqueue_job(job, self.name)
    
    async def close(self) -> None:
        """Cleanup resources."""
        if self._service_manager:
            await self._service_manager.cleanup()
```

**Integration Points:**
- Replace direct NATS operations with `JobService` calls
- Replace manual event logging with `EventService` integration  
- Replace connection management with `ConnectionService`
- Replace scheduled job operations with `SchedulerService`

### 2. Worker Package Integration

**Current Pattern (Before):**
```python
# In worker/core.py - Before service integration
class Worker:
    async def process_job(self, job: Job) -> JobResult:
        # Manual connection management
        nc = await get_nats_connection(self.nats_url)
        
        # Manual event logging  
        if events_enabled:
            # Log job started event manually...
            
        try:
            # Execute job
            result = await job.execute()
            
            # Store result manually
            js = await get_jetstream_context(nc)
            kv = await js.key_value("results")
            # Manual result storage...
            
            # Log completion event manually
            if events_enabled:
                # Log job completed event manually...
                
        except Exception as e:
            # Manual error handling and logging
            # Log failure event manually...
            
        finally:
            await close_nats_connection(nc)
```

**Service Pattern (After):**
```python  
# In worker/core.py - After service integration
from ..services import ServiceManager, JobService, EventService

class Worker:
    def __init__(self, queues: List[str], config: NAQConfig):
        self.queues = queues
        self.config = config
        self._service_manager: Optional[ServiceManager] = None
    
    async def _get_services(self) -> ServiceManager:
        """Get or create service manager.""" 
        if self._service_manager is None:
            self._service_manager = ServiceManager(self.config)
            await self._service_manager.initialize()
        return self._service_manager
    
    async def process_job(self, job: Job) -> JobResult:
        """Process job using service layer."""
        services = await self._get_services()
        job_service = await services.get_service(JobService)
        
        # Service handles execution, result storage, event logging, error handling
        return await job_service.execute_job(job, worker_id=self.worker_id)
```

### 3. Scheduler Integration

**Service Pattern:**
```python
# In scheduler.py - After service integration  
from .services import ServiceManager, SchedulerService, EventService

class Scheduler:
    def __init__(self, config: NAQConfig):
        self.config = config
        self._service_manager: Optional[ServiceManager] = None
    
    async def _get_services(self) -> ServiceManager:
        if self._service_manager is None:
            self._service_manager = ServiceManager(self.config)
            await self._service_manager.initialize()
        return self._service_manager
    
    async def run(self) -> None:
        """Run scheduler using service layer."""
        services = await self._get_services()
        scheduler_service = await services.get_service(SchedulerService)
        
        # Service handles leader election, job scanning, event logging
        await scheduler_service.run_scheduler()
```

### 4. CLI Commands Integration

**Current Pattern (Before):**
```python
# In cli/worker_commands.py - Before service integration
@worker_app.command()
def start(queues: List[str], ...):
    # Manual worker creation and configuration
    worker = Worker(queues=queues, nats_url=nats_url, ...)
    asyncio.run(worker.run())
```

**Service Pattern (After):**
```python
# In cli/worker_commands.py - After service integration
@worker_app.command()
def start(
    queues: List[str],
    config_file: Optional[str] = typer.Option(None, "--config", help="Configuration file"),
    ...
):
    # Load configuration
    config = load_config(config_file)
    
    # Create worker with service integration
    async def run_worker():
        async with ServiceManager(config) as services:
            worker = Worker(queues=queues, config=config)
            await worker.run()
    
    asyncio.run(run_worker())
```

## Step-by-Step Integration Process

### Step 1: Queue Package Integration

**Files to Update:**
- `src/naq/queue/core.py`
- `src/naq/queue/scheduled.py`  
- `src/naq/queue/async_api.py`
- `src/naq/queue/sync_api.py`

**Integration Pattern:**
1. Add service manager to Queue class constructor
2. Replace direct NATS operations with service calls
3. Update async API functions to use services
4. Update sync API functions to maintain compatibility
5. Add proper resource cleanup

**Example Changes:**
```python
# Before: Direct NATS operations
async def enqueue(func: Callable, *args, **kwargs) -> Job:
    nc = await get_nats_connection()
    # ... manual NATS operations

# After: Service-based operations  
async def enqueue(func: Callable, *args, **kwargs) -> Job:
    config = get_config()
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        return await job_service.enqueue(func, *args, **kwargs)
```

### Step 2: Worker Package Integration

**Files to Update:**
- `src/naq/worker/core.py`
- `src/naq/worker/status.py`
- `src/naq/worker/jobs.py`
- `src/naq/worker/failed.py`

**Integration Pattern:**
1. Add service manager to Worker class
2. Replace manual job processing with JobService
3. Replace status management with EventService integration  
4. Replace failed job handling with service layer
5. Ensure proper lifecycle management

### Step 3: Scheduler Integration

**Files to Update:**
- `src/naq/scheduler.py`

**Integration Pattern:**
1. Replace direct scheduled job management with SchedulerService
2. Replace manual event logging with EventService
3. Replace connection management with service layer
4. Maintain leader election functionality

### Step 4: CLI Integration

**Files to Update:**
- `src/naq/cli/worker_commands.py`
- `src/naq/cli/job_commands.py`
- `src/naq/cli/scheduler_commands.py`
- `src/naq/cli/event_commands.py`

**Integration Pattern:**
1. Load configuration in CLI commands
2. Pass configuration to components
3. Use service managers for CLI operations
4. Add configuration validation to commands

### Step 5: Event System Integration

**Files to Update:**
- `src/naq/events/logger.py`
- `src/naq/events/processor.py`

**Integration Pattern:**
1. Integrate EventService with existing event system
2. Replace manual NATS operations with service layer
3. Ensure backward compatibility

## Service Usage Patterns

### Pattern 1: Short-lived Operations (CLI Commands)
```python
async def cli_operation():
    config = get_config()
    async with ServiceManager(config) as services:
        job_service = await services.get_service(JobService)
        result = await job_service.some_operation()
        return result
```

### Pattern 2: Long-lived Components (Worker, Queue, Scheduler)
```python
class LongLivedComponent:
    def __init__(self, config: NAQConfig):
        self.config = config
        self._services: Optional[ServiceManager] = None
    
    async def start(self):
        self._services = ServiceManager(self.config)
        await self._services.initialize()
    
    async def operation(self):
        if not self._services:
            raise RuntimeError("Component not started")
        
        job_service = await self._services.get_service(JobService)
        return await job_service.some_operation()
    
    async def stop(self):
        if self._services:
            await self._services.cleanup()
```

### Pattern 3: Sync API Compatibility
```python
def sync_operation(*args, **kwargs):
    """Sync wrapper maintaining backward compatibility."""
    config = get_config()
    
    async def async_impl():
        async with ServiceManager(config) as services:
            job_service = await services.get_service(JobService)
            return await job_service.operation(*args, **kwargs)
    
    return asyncio.run(async_impl())
```

## Configuration Integration

### Service Configuration Passing
```python
# Services get configuration from ServiceManager
class ServiceManager:
    def __init__(self, config: NAQConfig):
        self.config = config
        self._services: Dict[Type, BaseService] = {}
    
    async def get_service(self, service_type: Type[T]) -> T:
        if service_type not in self._services:
            # Create service with configuration
            service = service_type(self.config)
            await service.initialize()
            self._services[service_type] = service
        
        return self._services[service_type]
```

### Component Configuration Updates
```python
# Components receive and use configuration
class Queue:
    def __init__(self, name: str, config: NAQConfig):
        self.name = name
        self.config = config
        
        # Service manager gets configuration
        self._service_manager = ServiceManager(config)
```

## Testing Strategy

### Unit Tests for Integration
- Test that services are properly initialized
- Test configuration is passed correctly  
- Test resource cleanup happens properly
- Test error handling through service layer

### Integration Tests  
- Test end-to-end operations use services
- Test backward compatibility maintained
- Test performance not degraded
- Test concurrent operations work correctly

### Service Mock Testing
```python
# Mock services for testing components
@pytest.fixture
async def mock_services():
    mock_job_service = Mock(spec=JobService)
    mock_event_service = Mock(spec=EventService)
    
    mock_manager = Mock(spec=ServiceManager)
    mock_manager.get_service.side_effect = lambda t: {
        JobService: mock_job_service,
        EventService: mock_event_service
    }[t]
    
    return mock_manager

async def test_queue_enqueue(mock_services):
    queue = Queue("test", config)
    queue._service_manager = mock_services
    
    await queue.enqueue(job)
    
    mock_job_service = await mock_services.get_service(JobService)
    mock_job_service.enqueue_job.assert_called_once()
```

## Error Handling Integration

### Service Error Propagation
- Services raise NAQ-specific exceptions
- Components handle service errors appropriately
- Error context preserved through service layers
- Logging integrated with service operations

### Backward Compatibility
- Existing exception types maintained
- Error messages remain consistent  
- Error handling behavior unchanged from user perspective

## Performance Considerations

### Service Initialization Optimization
- Lazy service initialization where possible
- Service instance reuse within components
- Connection pooling through services
- Avoid service overhead in hot paths

### Resource Management
- Proper service cleanup on component shutdown
- Connection sharing between services
- Memory usage monitoring for service instances

## Migration Validation

### Pre-Integration Checklist
- [ ] All services implemented and tested
- [ ] Configuration system working
- [ ] Common patterns extracted
- [ ] Performance benchmarks recorded

### Post-Integration Checklist
- [ ] All components use service layer
- [ ] No direct NATS operations outside services
- [ ] Configuration passed correctly throughout
- [ ] All tests pass
- [ ] Performance meets or exceeds benchmarks
- [ ] Resource cleanup working properly

## Files to Update

### Queue Package
- `src/naq/queue/core.py`
- `src/naq/queue/scheduled.py`
- `src/naq/queue/async_api.py`  
- `src/naq/queue/sync_api.py`

### Worker Package  
- `src/naq/worker/core.py`
- `src/naq/worker/status.py`
- `src/naq/worker/jobs.py`
- `src/naq/worker/failed.py`

### Scheduler
- `src/naq/scheduler.py`

### CLI Commands
- `src/naq/cli/worker_commands.py`
- `src/naq/cli/job_commands.py`
- `src/naq/cli/scheduler_commands.py`
- `src/naq/cli/event_commands.py`

### Event System
- `src/naq/events/logger.py`
- `src/naq/events/processor.py`

### Configuration
- Update all components to receive NAQConfig
- Ensure proper configuration propagation

## Success Criteria

### Functional Requirements
- [ ] All components integrated with service layer
- [ ] No direct NATS operations outside services  
- [ ] Configuration-driven behavior throughout
- [ ] Backward compatibility maintained
- [ ] All existing functionality preserved

### Quality Requirements
- [ ] Consistent error handling across all components
- [ ] Consistent logging patterns
- [ ] Resource management centralized
- [ ] Code duplication eliminated
- [ ] Testability improved significantly

### Performance Requirements
- [ ] No performance regressions
- [ ] Service overhead minimal
- [ ] Resource usage not increased
- [ ] Connection efficiency improved

## Dependencies
- **Depends on**: 
  - Task 05 (Service Layer) - services must be implemented
  - Task 07 (YAML Configuration) - configuration system needed
  - Task 08 (Common Patterns) - utilities for integration
- **Blocks**: None - this completes the major refactoring

## Estimated Time
- **Queue Integration**: 8-10 hours
- **Worker Integration**: 10-12 hours
- **Scheduler Integration**: 6-8 hours  
- **CLI Integration**: 8-10 hours
- **Testing**: 12-15 hours
- **Validation**: 6-8 hours
- **Total**: 50-63 hours