# Task 12: Testing Updates and Validation

## Overview
Update the entire test suite to work with the refactored codebase structure, add comprehensive tests for new components, and ensure all functionality works correctly after the major refactoring.

## Current State
After completing Tasks 01-11:
- Large files split into focused modules
- Service layer implemented
- YAML configuration system available
- Common patterns extracted
- Event system fully integrated
- Imports updated with backward compatibility

## Testing Challenges
- Test imports need updating to use new module structure
- Service layer needs comprehensive testing
- Configuration system needs validation
- Event system needs integration testing
- Performance regression testing required
- Backward compatibility must be validated

## Testing Strategy

### 1. Test Categories

#### Unit Tests
- **Model Tests**: Test individual model classes and methods
- **Service Tests**: Test each service in isolation with mocks
- **Utility Tests**: Test common patterns and helper functions
- **Configuration Tests**: Test configuration loading and validation

#### Integration Tests  
- **Component Integration**: Test interaction between major components
- **Service Integration**: Test services working together
- **End-to-End**: Test complete workflows (enqueue → process → complete)
- **Event Flow**: Test event logging throughout system

#### Compatibility Tests
- **Import Compatibility**: Test all legacy import patterns work
- **API Compatibility**: Test existing user code continues to work
- **Configuration Compatibility**: Test env vars still work

#### Performance Tests
- **Regression Tests**: Ensure no performance degradation  
- **Service Overhead**: Measure service layer impact
- **Event Logging**: Test event system performance impact
- **Connection Efficiency**: Test NATS connection improvements

## Detailed Implementation Plan

### 1. Update Test Infrastructure

**Test Configuration and Setup:**
```python
# tests/conftest.py - Updated test configuration
import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, AsyncMock

from naq.config import load_config, temp_config
from naq.services import ServiceManager
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS

@pytest.fixture
def test_config():
    """Test configuration with safe defaults."""
    return {
        'nats': {
            'servers': ['nats://localhost:4222'],
            'client_name': 'naq-test',
            'max_reconnect_attempts': 3
        },
        'workers': {
            'concurrency': 2,
            'heartbeat_interval': 5,
            'ttl': 30
        },
        'events': {
            'enabled': True,
            'batch_size': 10,
            'flush_interval': 0.1,
            'max_buffer_size': 100
        },
        'queues': {
            'default': 'test_queue'
        },
        'results': {
            'ttl': 300
        }
    }

@pytest.fixture
async def service_manager(test_config):
    """Service manager fixture for integration tests."""
    async with ServiceManager(test_config) as services:
        yield services

@pytest.fixture
def mock_services():
    """Mock services for unit testing."""
    services = {}
    
    # Mock each service type
    from naq.services.jobs import JobService
    from naq.services.events import EventService
    from naq.services.connection import ConnectionService
    
    services[JobService] = AsyncMock(spec=JobService)
    services[EventService] = AsyncMock(spec=EventService)
    services[ConnectionService] = AsyncMock(spec=ConnectionService)
    
    mock_manager = AsyncMock(spec=ServiceManager)
    mock_manager.get_service.side_effect = lambda t: services[t]
    
    return mock_manager

@pytest.fixture
def temp_config_file(test_config):
    """Create temporary YAML config file."""
    import yaml
    
    temp_dir = tempfile.mkdtemp()
    config_path = Path(temp_dir) / "test_config.yaml"
    
    with open(config_path, 'w') as f:
        yaml.dump(test_config, f)
    
    yield str(config_path)
    
    shutil.rmtree(temp_dir)

@pytest.fixture
async def test_job():
    """Create test job for testing."""
    def dummy_task(x: int) -> int:
        return x * 2
    
    return Job(
        function=dummy_task,
        args=(5,),
        queue_name="test_queue"
    )
```

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

### 3. Service Layer Tests

**Test Services in Isolation:**
```python
# tests/test_services/test_job_service.py
import pytest
from unittest.mock import AsyncMock, Mock
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.models.jobs import Job

class TestJobService:
    async def test_enqueue_job(self, test_config):
        """Test job enqueuing through service layer."""
        # Mock dependencies
        mock_event_service = AsyncMock(spec=EventService)
        mock_connection_service = AsyncMock()
        
        service = JobService(
            test_config,
            event_service=mock_event_service,
            connection_service=mock_connection_service
        )
        
        def test_func():
            return "test"
        
        job = Job(function=test_func)
        
        # Test enqueuing
        job_id = await service.enqueue_job(job, "test-queue")
        
        assert job_id is not None
        # Verify event was logged
        mock_event_service.log_job_enqueued.assert_called_once()
    
    async def test_execute_job_success(self, test_config):
        """Test successful job execution."""
        mock_event_service = AsyncMock(spec=EventService)
        
        service = JobService(
            test_config,
            event_service=mock_event_service
        )
        
        def test_func(x):
            return x * 2
        
        job = Job(function=test_func, args=(5,))
        
        # Test execution
        result = await service.execute_job(job, "worker-1")
        
        assert result.status == "completed"
        assert result.result == 10
        
        # Verify events were logged
        mock_event_service.log_job_started.assert_called_once()
        mock_event_service.log_job_completed.assert_called_once()
    
    async def test_execute_job_failure(self, test_config):
        """Test job execution failure handling."""
        mock_event_service = AsyncMock(spec=EventService)
        
        service = JobService(
            test_config,
            event_service=mock_event_service
        )
        
        def failing_func():
            raise ValueError("Test error")
        
        job = Job(function=failing_func, max_retries=0)
        
        # Test execution
        result = await service.execute_job(job, "worker-1")
        
        assert result.status == "failed"
        assert "Test error" in result.error
        
        # Verify failure event was logged
        mock_event_service.log_job_failed.assert_called_once()

# tests/test_services/test_event_service.py
from naq.services.events import EventService
from naq.events.logger import AsyncJobEventLogger

class TestEventService:
    async def test_event_logging(self, test_config):
        """Test event logging through service."""
        mock_logger = AsyncMock(spec=AsyncJobEventLogger)
        
        service = EventService(test_config)
        service._logger = mock_logger
        
        # Test event logging
        await service.log_job_started("job-123", "worker-1", "test-queue")
        
        # Verify logger was called
        mock_logger.log_job_started.assert_called_once_with(
            job_id="job-123",
            worker_id="worker-1",
            queue_name="test-queue"
        )
    
    async def test_event_disabled(self, test_config):
        """Test event logging when disabled."""
        test_config['events']['enabled'] = False
        
        service = EventService(test_config)
        
        # Should not create logger when disabled
        await service.log_job_started("job-123", "worker-1", "test-queue")
        
        assert service._logger is None
```

### 4. Integration Tests

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

## Estimated Time
- **Test Infrastructure Update**: 6-8 hours
- **Unit Test Creation/Updates**: 15-20 hours
- **Integration Tests**: 12-15 hours  
- **Compatibility Tests**: 8-10 hours
- **Performance Tests**: 6-8 hours
- **CLI Tests**: 4-6 hours
- **Test Organization and CI**: 4-6 hours
- **Total**: 55-73 hours