# Task 12: Testing Updates and Validation

## Overview
**COMPLEXITY: HIGH** - Update the entire test suite to work with the service layer integration, migrate existing tests to new architecture, add comprehensive service layer testing, and ensure all functionality works correctly after the major service integration.

## Current State (UPDATED ASSESSMENT)
After completing Tasks 01-09 and comprehensive service layer integration:
- ✅ **Service layer fully integrated** across all components (Queue, Worker, Scheduler, CLI, Events)
- ✅ **Configuration system** supports both YAML and legacy parameters with backward compatibility  
- ✅ **Backward compatibility maintained** for all user-facing APIs and import patterns
- ✅ **Event system integrated** with service layer architecture
- ❌ **Test infrastructure needs major overhaul** - current tests use old connection patterns
- ❌ **No service layer tests exist** - critical gap in testing coverage
- ❌ **Configuration testing completely missing** - YAML config → ServiceManager pipeline untested
- ❌ **Existing tests need migration** to ServiceManager architecture

## Testing Challenges (UPDATED)

### **Critical Issues Discovered in Current Test Infrastructure:**

1. **Test Architecture Mismatch:**
   - Current tests still use old `nats_jetstream()` connection patterns 
   - ServiceManager architecture not tested at all
   - Mock fixtures in `conftest.py` need complete overhaul for service layer

2. **Missing Service Layer Testing:**
   - No `test_*service*.py` files exist
   - Service dependency injection not tested
   - Service lifecycle management not validated
   - Cross-service communication not tested

3. **Configuration Testing Gap:**
   - YAML configuration loading completely untested
   - Environment variable overrides not validated
   - Configuration → ServiceManager → Components pipeline not tested
   - Configuration validation logic not covered

4. **Test Organization Conflicts:**
   ```
   CURRENT STRUCTURE:           NEEDS TO BECOME:
   tests/unit/test_unit_*.py    tests/unit/ + tests/test_services/
   tests/integration/           tests/integration/ (updated for services)
   (no config tests)            tests/test_config/ (new)
   (no service tests)           tests/test_services/ (new)
   ```

5. **Mock Infrastructure Outdated:**
   - Current mocks target individual NATS connections
   - Need ServiceManager mocking patterns
   - Service dependency mocking required
   - Configuration-driven mock setup needed

## Updated Testing Strategy

### **Phase-Based Implementation Approach**

#### **Phase 1: Test Infrastructure Overhaul (Priority: CRITICAL)**
1. **Update `conftest.py` for ServiceManager architecture:**
   - Add ServiceManager fixtures
   - Create service-layer mock patterns  
   - Update existing NATS mocks to work with services
   - Add configuration fixture support

2. **Create service layer test foundation:**
   - Establish service testing patterns
   - Create mock service dependencies
   - Set up service lifecycle testing utilities

#### **Phase 2: Service Layer Testing (Priority: HIGH)**
1. **Individual Service Unit Tests:**
   - `tests/test_services/test_connection_service.py` - Connection pooling, failover
   - `tests/test_services/test_job_service.py` - Job execution orchestration  
   - `tests/test_services/test_event_service.py` - Event logging
   - `tests/test_services/test_stream_service.py` - JetStream operations
   - `tests/test_services/test_kv_service.py` - KeyValue operations
   - `tests/test_services/test_scheduler_service.py` - Scheduling operations

2. **Service Integration Testing:**
   - Cross-service communication validation
   - Dependency injection testing
   - Service lifecycle management testing

#### **Phase 3: Configuration Testing (Priority: HIGH)**
1. **YAML Configuration Pipeline:**
   ```
   YAML File → ConfigLoader → TypedConfig → ServiceManager → Services
   ```
2. **Configuration validation testing**
3. **Environment variable override testing**
4. **Configuration error handling testing**

#### **Phase 4: Existing Test Migration (Priority: MEDIUM)**
1. **Migrate current tests to ServiceManager:**
   - Update `test_unit_queue.py` to use services
   - Update `test_unit_worker.py` to use services
   - Update `test_integration_*.py` files
   - Maintain backward compatibility in test coverage

#### **Phase 5: Compatibility & Performance (Priority: MEDIUM)**
1. **Backward Compatibility Tests:**
   - API compatibility validation
   - Import compatibility testing  
   - Legacy parameter support testing

2. **Performance Regression Testing:**
   - Service layer overhead measurement
   - Event logging performance impact
   - Memory usage analysis

## Detailed Implementation Plan (UPDATED)

### **Phase 1: Test Infrastructure Overhaul**

#### **Critical Update: `conftest.py` ServiceManager Integration**
Current `conftest.py` has sophisticated NATS mocking but no ServiceManager support. Analysis shows we need complete rewrite:

**CURRENT STATE ANALYSIS:**
- Line 97-154: Existing `mock_nats` fixture uses direct `nats_jetstream()` patterns
- Line 130-138: KV store mocking targets individual buckets, not service layer
- Line 21-46: Mock fixtures use old component patterns
- Missing: ServiceManager fixtures, configuration loading, service dependency mocking

**REQUIRED UPDATES:**

```python
# tests/conftest.py - COMPLETE REWRITE REQUIRED
import pytest
import asyncio
import tempfile
import shutil  
from pathlib import Path
from unittest.mock import Mock, AsyncMock, MagicMock
from typing import Dict, Any

# NEW: Service layer imports (currently missing)
from naq.services import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.streams import StreamService
from naq.services.kv_stores import KVStoreService
from naq.config import load_config
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS

# REPLACE: Current worker_dict fixture (line 50-58) with service-aware config
@pytest.fixture
def service_test_config() -> Dict[str, Any]:
    """ServiceManager-compatible test configuration."""
    return {
        'nats': {
            'url': 'nats://localhost:4222',
            'client_name': 'naq-test',
            'max_reconnect_attempts': 3
        },
        'workers': {
            'concurrency': 2,
            'heartbeat_interval': 5,
            'ttl': 30,
            'default_queue': 'test_queue'
        },
        'events': {
            'enabled': True,
            'batch_size': 10,
            'flush_interval': 0.1
        },
        'results': {
            'ttl': 300
        }
    }

# NEW: ServiceManager fixtures for different test scenarios
@pytest.fixture
async def service_manager(service_test_config):
    """Real ServiceManager for integration tests."""
    manager = ServiceManager(service_test_config)
    await manager.initialize_all()
    yield manager
    await manager.cleanup_all()

@pytest.fixture
def mock_service_manager():
    """Mock ServiceManager for unit tests."""
    mock_manager = AsyncMock(spec=ServiceManager)
    
    # Create mock services with proper specs
    mock_connection_service = AsyncMock(spec=ConnectionService)
    mock_job_service = AsyncMock(spec=JobService)
    mock_event_service = AsyncMock(spec=EventService)
    mock_stream_service = AsyncMock(spec=StreamService)
    mock_kv_service = AsyncMock(spec=KVStoreService)
    
    # Configure jetstream_scope context manager for ConnectionService
    @asyncio.contextmanager
    async def mock_jetstream_scope():
        mock_js = AsyncMock()
        mock_js.publish = AsyncMock(return_value=MagicMock(stream="test", seq=1))
        yield mock_js
    
    mock_connection_service.jetstream_scope = mock_jetstream_scope
    
    # Configure get_service to return appropriate mocks
    service_map = {
        ConnectionService: mock_connection_service,
        JobService: mock_job_service,
        EventService: mock_event_service,
        StreamService: mock_stream_service,
        KVStoreService: mock_kv_service,
    }
    
    mock_manager.get_service.side_effect = lambda service_type: service_map[service_type]
    mock_manager.initialize_all = AsyncMock()
    mock_manager.cleanup_all = AsyncMock()
    
    return mock_manager, service_map

# REPLACE: Current mock_nats fixture (line 97-154) with service-aware version
@pytest.fixture
async def service_aware_nats_mock(mock_service_manager):
    """NATS mock that integrates with ServiceManager architecture."""
    mock_manager, service_map = mock_service_manager
    
    # Return connection service with jetstream_scope already configured
    return service_map[ConnectionService], mock_manager

# NEW: Configuration testing fixtures
@pytest.fixture
def temp_config_file(service_test_config):
    """Create temporary YAML config file for testing."""
    import yaml
    
    temp_dir = tempfile.mkdtemp()
    config_path = Path(temp_dir) / "test_config.yaml"
    
    with open(config_path, 'w') as f:
        yaml.dump(service_test_config, f)
    
    yield str(config_path)
    shutil.rmtree(temp_dir)

# UPDATE: Existing test_job fixture to work with new Job model structure
@pytest.fixture
def service_test_job():
    """Create test job compatible with service layer."""
    def dummy_task(x: int) -> int:
        return x * 2
    
    return Job(
        function=dummy_task,
        args=(5,),
        queue_name="test_queue"
    )
```

**MIGRATION IMPACT:**
- Current tests using `mock_nats` (test_unit_queue.py:18-25) need to use `service_aware_nats_mock`
- Worker tests using `worker_dict` need to use `service_test_config`
- All component creation needs to pass `config` parameter instead of individual parameters

### 2. Model Tests

**Test Split Model Classes:**
```python
# tests/test_models/test_jobs.py
import pytest
from naq.models.jobs import Job, JobResult
from naq.models.enums import JOB_STATUS

class TestJob:
    def test_job_creation(self):
        """Test job creation with new structure."""
        def test_func(x):
            return x + 1
        
        job = Job(function=test_func, args=(1,))
        
        assert job.function == test_func
        assert job.args == (1,)
        assert job.status == JOB_STATUS.PENDING
    
    def test_job_serialization(self):
        """Test job serialization works after refactor."""
        def test_func():
            return "test"
        
        job = Job(function=test_func)
        
        # Test serialization/deserialization
        serialized = job.serialize()
        deserialized = Job.deserialize(serialized)
        
        assert deserialized.function.__name__ == test_func.__name__
    
    async def test_job_execution(self):
        """Test job execution after refactor."""
        def test_func(x):
            return x * 2
        
        job = Job(function=test_func, args=(5,))
        result = await job.execute()
        
        assert result == 10
        assert job.status == JOB_STATUS.COMPLETED

# tests/test_models/test_events.py  
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType

class TestJobEvent:
    def test_job_event_creation(self):
        """Test job event creation with factory methods."""
        event = JobEvent.enqueued("job-123", "test-queue")
        
        assert event.job_id == "job-123"
        assert event.event_type == JobEventType.ENQUEUED
        assert event.queue_name == "test-queue"
        assert event.message is not None
    
    def test_job_event_serialization(self):
        """Test job event to_dict method."""
        event = JobEvent.completed(
            "job-456", 
            "worker-1", 
            "high-priority", 
            duration_ms=1500
        )
        
        data = event.to_dict()
        
        assert data['job_id'] == "job-456"
        assert data['event_type'] == "completed"
        assert data['duration_ms'] == 1500

class TestWorkerEvent:
    def test_worker_event_creation(self):
        """Test worker event creation."""
        event = WorkerEvent.worker_started(
            "worker-1",
            "localhost",
            12345,
            ["queue1", "queue2"],
            10
        )
        
        assert event.worker_id == "worker-1"
        assert event.event_type == WorkerEventType.WORKER_STARTED
        assert event.concurrency_limit == 10
```

### **Phase 2: Service Layer Testing** 

#### **Individual Service Unit Tests** 
**Critical Gap:** No service tests exist. Need to create from scratch.

**NEW FILES REQUIRED:**

#### **tests/test_services/test_connection_service.py** - Missing entirely
```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from naq.services.connection import ConnectionService

class TestConnectionService:
    async def test_jetstream_scope_context_manager(self, service_test_config):
        """Test jetstream_scope provides proper context management."""
        service = ConnectionService(service_test_config)
        
        async with service.jetstream_scope() as js:
            assert js is not None
            # Test that js has expected methods
            assert hasattr(js, 'publish')
            assert hasattr(js, 'key_value')
    
    async def test_connection_pooling(self, service_test_config):
        """Test connection reuse and pooling."""
        service = ConnectionService(service_test_config)
        
        # Multiple jetstream_scope calls should reuse connection
        async with service.jetstream_scope() as js1:
            async with service.jetstream_scope() as js2:
                # Both should work (specific assertions depend on implementation)
                assert js1 is not None
                assert js2 is not None
```

#### **tests/test_services/test_job_service.py** - Missing entirely  
```python
import pytest
from unittest.mock import AsyncMock
from naq.services.jobs import JobService
from naq.models.jobs import Job

class TestJobService:
    async def test_enqueue_job_with_service_dependencies(self, mock_service_manager):
        """Test job enqueuing through service layer."""
        mock_manager, service_map = mock_service_manager
        
        job_service = service_map[JobService] 
        connection_service = service_map[ConnectionService]
        event_service = service_map[EventService]
        
        def test_func():
            return "test"
        
        job = Job(function=test_func)
        
        # Configure mock to simulate enqueue_job method
        job_service.enqueue_job.return_value = "job-123"
        
        # Test enqueuing
        job_id = await job_service.enqueue_job(job, "test-queue")
        
        assert job_id == "job-123"
        job_service.enqueue_job.assert_called_once()
```

#### **tests/test_services/test_event_service.py** - Missing entirely
```python  
import pytest
from unittest.mock import AsyncMock
from naq.services.events import EventService

class TestEventService:
    async def test_log_job_events(self, service_test_config):
        """Test event logging through service."""
        service = EventService(service_test_config)
        
        # Test event logging methods exist and can be called
        await service.log_job_started("job-123", "worker-1", "test-queue")
        await service.log_job_completed("job-123", "worker-1", duration_ms=1000)
        
        # Verify service handles events (specific assertions depend on implementation)
        assert True  # Placeholder - actual tests depend on EventService implementation
```

#### **tests/test_services/test_service_manager.py** - Missing entirely
```python
import pytest  
from naq.services import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.jobs import JobService

class TestServiceManager:
    async def test_service_initialization(self, service_test_config):
        """Test ServiceManager initializes all services."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        # Test service retrieval
        conn_service = await manager.get_service(ConnectionService)
        job_service = await manager.get_service(JobService)
        
        assert conn_service is not None
        assert job_service is not None
        
        await manager.cleanup_all()
    
    async def test_dependency_injection(self, service_test_config):
        """Test services are properly injected with dependencies."""
        manager = ServiceManager(service_test_config)
        await manager.initialize_all()
        
        job_service = await manager.get_service(JobService)
        
        # Verify job_service has injected dependencies
        # (specific assertions depend on JobService implementation)
        assert hasattr(job_service, '_connection_service') or hasattr(job_service, 'connection_service')
        
        await manager.cleanup_all()
```

### **Phase 3: Configuration Testing**

#### **YAML Configuration Pipeline Testing** - Missing entirely
**Critical Gap:** No configuration tests exist. YAML → ServiceManager pipeline completely untested.

```python
# tests/test_config/test_yaml_loading.py - NEW FILE REQUIRED
import pytest
import tempfile
from pathlib import Path
import yaml
from naq.config import load_config
from naq.services import ServiceManager

class TestConfigurationLoading:
    def test_yaml_config_loading(self, temp_config_file):
        """Test loading configuration from YAML file."""
        config = load_config(temp_config_file)
        
        # Test config structure matches expected format
        assert 'nats' in config
        assert 'workers' in config
        assert 'events' in config
        assert config['nats']['url'] == 'nats://localhost:4222'
    
    def test_environment_variable_overrides(self, temp_config_file):
        """Test environment variables override YAML config."""
        import os
        
        # Set environment variable
        original_value = os.environ.get('NAQ_WORKERS_CONCURRENCY')
        os.environ['NAQ_WORKERS_CONCURRENCY'] = '20'
        
        try:
            config = load_config(temp_config_file)
            assert config['workers']['concurrency'] == 20  # From env var
        finally:
            if original_value is not None:
                os.environ['NAQ_WORKERS_CONCURRENCY'] = original_value
            else:
                del os.environ['NAQ_WORKERS_CONCURRENCY']
    
    async def test_config_to_service_manager_pipeline(self, temp_config_file):
        """Test complete YAML → ServiceManager pipeline."""
        config = load_config(temp_config_file)
        
        # Test ServiceManager can be created from loaded config
        manager = ServiceManager(config)
        await manager.initialize_all()
        
        # Verify services are created correctly
        from naq.services.connection import ConnectionService
        conn_service = await manager.get_service(ConnectionService)
        assert conn_service is not None
        
        await manager.cleanup_all()

# tests/test_config/test_validation.py - NEW FILE REQUIRED  
class TestConfigurationValidation:
    def test_invalid_config_raises_error(self):
        """Test configuration validation catches errors."""
        invalid_config = {
            'nats': {
                'url': '',  # Empty URL should be invalid
                'max_reconnect_attempts': -1  # Negative value should be invalid
            }
        }
        
        # Test validation logic (depends on implementation)
        with pytest.raises(ValueError):
            ServiceManager(invalid_config)
```

### **Phase 4: Existing Test Migration**

#### **Current Test Migration Requirements**
**CRITICAL ISSUE:** All existing tests use old connection patterns and need migration.

**tests/unit/test_unit_queue.py Migration:**
- **Line 18-25**: Uses `mock_nats` fixture → needs `service_aware_nats_mock`
- **Line 24**: `Queue(name="test")` → needs `Queue(name="test", config=service_test_config)`
- **Line 21-22**: Uses direct `get_nats_connection`, `get_jetstream_context` patching → needs ServiceManager mocking

**Current Pattern (test_unit_queue.py:18-25):**
```python
@pytest_asyncio.fixture
async def queue(self, mock_nats, mocker):
    """Setup a test queue with mocked NATS."""
    mock_nc, mock_js = mock_nats
    mocker.patch('naq.queue.get_nats_connection', return_value=mock_nc)
    mocker.patch('naq.queue.get_jetstream_context', return_value=mock_js)
    mocker.patch('naq.queue.ensure_stream')
    q = Queue(name="test")
    return q
```

**Required New Pattern:**
```python
@pytest_asyncio.fixture
async def queue(self, service_aware_nats_mock, service_test_config):
    """Setup a test queue with ServiceManager architecture."""
    connection_service, mock_manager = service_aware_nats_mock
    
    # Queue now takes config parameter and uses ServiceManager internally
    q = Queue(name="test", config=service_test_config)
    return q
```

**tests/integration/test_integration_events.py Migration:**
- **Line 30-33**: Direct `AsyncJobEventLogger(nats_url=nats_url)` → needs ServiceManager configuration
- **Line 38-40**: Direct `NATSJobEventStorage(nats_url=nats_url)` → needs service integration
- **Line 214**: `Queue(name="test-event-queue", nats_url=nats_url)` → needs config parameter

### **Phase 5: Integration Tests Updates**

**Test Component Integration:**
```python
# tests/test_integration/test_queue_worker.py
import pytest
import asyncio
from naq.queue.core import Queue
from naq.worker.core import Worker
from naq.services import ServiceManager

class TestQueueWorkerIntegration:
    async def test_enqueue_and_process(self, test_config):
        """Test complete enqueue → process workflow."""
        async with ServiceManager(test_config) as services:
            # Create queue and worker
            queue = Queue("integration-test", test_config)
            worker = Worker(["integration-test"], test_config)
            
            # Test function
            def add_numbers(x, y):
                return x + y
            
            # Enqueue job
            job = await queue.enqueue(add_numbers, 3, 5)
            
            # Process single message (simulate worker)
            # This would be done by worker in real scenario
            result = await worker.process_single_job(job)
            
            assert result.status == "completed"
            assert result.result == 8

# tests/test_integration/test_event_flow.py
class TestEventFlow:
    async def test_complete_event_flow(self, service_manager):
        """Test events are logged throughout job lifecycle."""
        captured_events = []
        
        # Mock event processor to capture events
        async def capture_events(event):
            captured_events.append(event)
        
        event_service = await service_manager.get_service(EventService)
        event_service.register_event_processor(capture_events)
        
        # Execute complete workflow
        job_service = await service_manager.get_service(JobService)
        
        def test_task(x):
            return x * 2
        
        job = Job(function=test_task, args=(10,))
        
        # Enqueue and execute
        job_id = await job_service.enqueue_job(job, "test-queue")
        result = await job_service.execute_job(job, "test-worker")
        
        # Allow events to be processed
        await asyncio.sleep(0.1)
        
        # Verify events were captured
        assert len(captured_events) >= 3  # enqueued, started, completed
        event_types = [e.event_type.value for e in captured_events]
        assert "enqueued" in event_types
        assert "started" in event_types
        assert "completed" in event_types
```

### 5. Compatibility Tests

**Test Backward Compatibility:**
```python
# tests/test_compatibility/test_imports.py
class TestImportCompatibility:
    def test_main_imports(self):
        """Test main package imports still work."""
        # These should all work without errors
        from naq import Queue, Worker, Job, enqueue, enqueue_sync
        from naq import JOB_STATUS, JobEvent, DEFAULT_NATS_URL
        
        assert Queue is not None
        assert Worker is not None
        assert Job is not None
        assert callable(enqueue)
        assert callable(enqueue_sync)
    
    def test_model_imports(self):
        """Test model imports still work."""
        from naq.models import Job, JOB_STATUS, JobEvent, JobResult
        
        assert Job is not None
        assert JOB_STATUS is not None
        assert JobEvent is not None
        assert JobResult is not None
    
    def test_event_imports(self):
        """Test event system imports still work."""
        from naq.events import AsyncJobEventLogger, JobEventType
        
        assert AsyncJobEventLogger is not None
        assert JobEventType is not None

# tests/test_compatibility/test_user_workflows.py
class TestUserWorkflows:
    def test_basic_sync_workflow(self):
        """Test basic synchronous workflow still works."""
        from naq import enqueue_sync, JOB_STATUS
        
        def simple_task(x):
            return x + 1
        
        # This should work exactly as before refactoring
        job = enqueue_sync(simple_task, 5)
        
        assert job.job_id is not None
        assert job.function == simple_task
        assert job.args == (5,)
    
    async def test_basic_async_workflow(self):
        """Test basic async workflow still works."""
        from naq import enqueue, Queue
        
        async def async_task(x):
            return x * 2
        
        # This should work exactly as before refactoring
        job = await enqueue(async_task, 10, queue_name="test")
        
        assert job.job_id is not None
        assert job.function == async_task
        assert job.args == (10,)
    
    async def test_advanced_workflow(self):
        """Test advanced user workflow."""
        from naq import Queue, Worker
        from naq.models import Job, JOB_STATUS
        from naq.events import AsyncJobEventLogger
        
        # Advanced users might use these directly
        queue = Queue("advanced-test")
        worker = Worker(["advanced-test"])
        logger = AsyncJobEventLogger()
        
        def advanced_task(data):
            return {"processed": data}
        
        job = Job(function=advanced_task, args=({"input": "test"},))
        
        # Should work with new structure
        assert job.status == JOB_STATUS.PENDING
```

### 6. Performance Tests  

**Test Performance Regression:**
```python
# tests/test_performance/test_service_overhead.py
import time
import pytest
from naq.services import ServiceManager
from naq.queue.core import Queue

class TestPerformanceRegression:
    async def test_service_layer_overhead(self, test_config):
        """Test service layer doesn't add significant overhead."""
        
        # Test with service layer
        start_time = time.perf_counter()
        
        async with ServiceManager(test_config) as services:
            job_service = await services.get_service(JobService)
            
            for i in range(100):
                def test_func():
                    return i
                
                job = Job(function=test_func)
                await job_service.enqueue_job(job, "perf-test")
        
        service_time = time.perf_counter() - start_time
        
        # Service overhead should be minimal (< 10% of total time)
        assert service_time < 5.0  # Reasonable threshold
    
    async def test_event_logging_overhead(self, test_config):
        """Test event logging doesn't significantly impact performance."""
        
        # Test with events enabled
        test_config['events']['enabled'] = True
        events_start = time.perf_counter()
        
        async with ServiceManager(test_config) as services:
            job_service = await services.get_service(JobService)
            
            for i in range(100):
                job = Job(function=lambda: i)
                result = await job_service.execute_job(job, "perf-worker")
        
        events_time = time.perf_counter() - events_start
        
        # Test with events disabled  
        test_config['events']['enabled'] = False
        no_events_start = time.perf_counter()
        
        async with ServiceManager(test_config) as services:
            job_service = await services.get_service(JobService)
            
            for i in range(100):
                job = Job(function=lambda: i)
                result = await job_service.execute_job(job, "perf-worker")
        
        no_events_time = time.perf_counter() - no_events_start
        
        # Event overhead should be < 20%
        overhead = (events_time - no_events_time) / no_events_time
        assert overhead < 0.2  # 20% max overhead
```

### 7. Configuration Tests

**Test YAML Configuration:**
```python
# tests/test_config/test_yaml_loading.py
import yaml
import tempfile
from pathlib import Path
from naq.config import load_config

class TestYAMLConfiguration:
    def test_config_file_loading(self, temp_config_file):
        """Test loading configuration from YAML file."""
        config = load_config(temp_config_file)
        
        assert config.nats.servers == ['nats://localhost:4222']
        assert config.workers.concurrency == 2
        assert config.events.enabled is True
    
    def test_environment_override(self, temp_config_file):
        """Test environment variables override YAML config."""
        import os
        
        # Set environment variable
        os.environ['NAQ_WORKER_CONCURRENCY'] = '20'
        
        try:
            config = load_config(temp_config_file)
            assert config.workers.concurrency == 20  # From env var
        finally:
            del os.environ['NAQ_WORKER_CONCURRENCY']
    
    def test_config_validation(self):
        """Test configuration validation."""
        from naq.config.validator import ConfigValidator
        
        # Invalid config
        invalid_config = {
            'nats': {
                'servers': [],  # Empty servers list
                'max_reconnect_attempts': -1  # Negative value
            }
        }
        
        validator = ConfigValidator()
        
        with pytest.raises(Exception):  # Should raise validation error
            validator.validate(invalid_config)
```

### 8. CLI Tests

**Test CLI Commands:**
```python
# tests/test_cli/test_commands.py
import pytest
from typer.testing import CliRunner
from naq.cli.main import app

class TestCLICommands:
    def test_worker_command(self):
        """Test worker CLI command works."""
        runner = CliRunner()
        
        # Test help
        result = runner.invoke(app, ["worker", "--help"])
        assert result.exit_code == 0
        assert "Start worker" in result.stdout
    
    def test_events_command(self):
        """Test events CLI command works."""
        runner = CliRunner()
        
        # Test help  
        result = runner.invoke(app, ["events", "--help"])
        assert result.exit_code == 0
        assert "event" in result.stdout.lower()
    
    def test_config_command(self):
        """Test config CLI command works."""
        runner = CliRunner()
        
        # Test config validation
        result = runner.invoke(app, ["system", "config", "--validate"])
        # Should not crash (exit code 0 or 1 depending on config)
        assert result.exit_code in [0, 1]
```

## Test Execution Strategy

### Automated Testing Pipeline
1. **Unit Tests**: Run fast, isolated tests first
2. **Integration Tests**: Test component interactions  
3. **Compatibility Tests**: Ensure backward compatibility
4. **Performance Tests**: Check for regressions
5. **End-to-End Tests**: Full workflow validation

### Test Organization
```
tests/
├── conftest.py                    # Test configuration
├── test_models/                   # Model unit tests
│   ├── test_jobs.py
│   ├── test_events.py
│   ├── test_enums.py
│   └── test_schedules.py
├── test_services/                 # Service unit tests
│   ├── test_job_service.py
│   ├── test_event_service.py
│   ├── test_connection_service.py
│   └── test_scheduler_service.py
├── test_integration/              # Integration tests
│   ├── test_queue_worker.py
│   ├── test_event_flow.py
│   └── test_service_integration.py
├── test_compatibility/            # Backward compatibility
│   ├── test_imports.py
│   ├── test_user_workflows.py
│   └── test_api_compatibility.py
├── test_performance/              # Performance tests
│   ├── test_service_overhead.py
│   ├── test_event_overhead.py
│   └── test_connection_efficiency.py
├── test_config/                   # Configuration tests
│   ├── test_yaml_loading.py
│   ├── test_validation.py
│   └── test_environment_vars.py
└── test_cli/                      # CLI tests
    ├── test_commands.py
    ├── test_worker_commands.py
    └── test_event_commands.py
```

### CI/CD Integration
```yaml
# .github/workflows/test.yml (example)
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.12, 3.13]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Start NATS Server
      run: |
        docker run -d -p 4222:4222 nats:latest
    
    - name: Install dependencies
      run: |
        pip install -e .[dev,test]
    
    - name: Run unit tests
      run: pytest tests/test_models/ tests/test_services/ -v
    
    - name: Run integration tests
      run: pytest tests/test_integration/ -v
    
    - name: Run compatibility tests  
      run: pytest tests/test_compatibility/ -v
    
    - name: Run performance tests
      run: pytest tests/test_performance/ -v
```

## Success Criteria

### Test Coverage
- [ ] >95% code coverage for new service layer
- [ ] >90% code coverage for refactored modules
- [ ] All existing functionality covered by tests
- [ ] Edge cases and error conditions tested

### Compatibility
- [ ] All existing user workflows work unchanged
- [ ] All legacy imports function correctly
- [ ] No breaking changes in public API
- [ ] Configuration compatibility maintained

### Performance  
- [ ] No performance regressions detected
- [ ] Service layer overhead <10%
- [ ] Event logging overhead <20%
- [ ] Memory usage not increased significantly

### Quality
- [ ] All tests pass consistently
- [ ] Test suite runs in reasonable time (<5 minutes)
- [ ] Clear test organization and documentation
- [ ] Comprehensive error scenario coverage

## Dependencies
- **Depends on**: All previous tasks (01-11) - need completed refactored codebase
- **Blocks**: None - this completes and validates the refactoring

## Estimated Time (UPDATED)
- **Test Infrastructure Overhaul**: 15-20 hours (was 6-8 hours) - Major `conftest.py` rewrite, ServiceManager fixtures
- **Service Layer Testing**: 20-25 hours (new) - Individual service tests, service integration tests
- **Configuration Testing**: 8-12 hours (new) - YAML pipeline testing, validation, environment overrides
- **Existing Test Migration**: 15-20 hours (was included in Unit Tests) - Migrate all current tests to ServiceManager
- **Unit Test Updates**: 8-12 hours (reduced from 15-20) - Update remaining unit tests
- **Integration Tests**: 12-15 hours (unchanged) - Update existing integration tests
- **Compatibility Tests**: 8-10 hours (unchanged) - Ensure backward compatibility
- **Performance Tests**: 6-8 hours (unchanged) - Service layer overhead testing
- **CLI Tests**: 4-6 hours (unchanged) - Update CLI command tests
- **Test Organization and CI**: 6-8 hours (increased) - More complex organization needed
- **Total**: 85-108 hours (was 55-73 hours)

**COMPLEXITY INCREASE RATIONALE:**
- Existing test infrastructure needs complete overhaul, not just additions
- Service layer testing is entirely missing and needs to be built from scratch
- Configuration testing pipeline doesn't exist
- Current tests use deprecated patterns and need migration
- Mock infrastructure needs redesign for ServiceManager architecture