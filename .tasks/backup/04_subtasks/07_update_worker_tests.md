# Subtask 04.07: Update Worker Tests

## Overview
Update all existing tests to work with the new worker package structure and ensure comprehensive test coverage for the split modules.

## Objectives
- Update existing test files to use new worker package imports
- Create new tests for extracted manager classes
- Ensure comprehensive test coverage for all worker functionality
- Verify that all tests pass with the new structure

## Implementation Steps

### 1. Identify Test Files to Update
Find all test files that import or test worker functionality:
- `tests/unit/test_unit_worker.py`
- `tests/integration/test_integration_worker.py`
- `tests/scenario/test_scenario_worker.py`
- `tests/smoke/test_smoke_worker.py`
- Any other files that import from `naq.worker`

### 2. Update Import Statements
Change imports from the old single-file structure to the new package structure:
```python
# Old import
from naq.worker import Worker, WorkerStatusManager, JobStatusManager, FailedJobHandler

# New import
from naq.worker import Worker
from naq.worker import WorkerStatusManager
from naq.worker import JobStatusManager
from naq.worker import FailedJobHandler
```

### 3. Create Manager Class Tests
Create dedicated tests for each extracted manager class:
- **WorkerStatusManager tests** (`tests/unit/test_worker_status_manager.py`)
  - Test status updates and heartbeat functionality
  - Test KV store operations
  - Test status transition handling

- **JobStatusManager tests** (`tests/unit/test_job_status_manager.py`)
  - Test job status tracking
  - Test dependency resolution
  - Test job lifecycle management

- **FailedJobHandler tests** (`tests/unit/test_failed_job_handler.py`)
  - Test failed job classification
  - Test retry logic and delay calculations
  - Test failed job stream operations

### 4. Update Worker Integration Tests
Update existing worker tests to work with the new structure:
- Test Worker instantiation with manager classes
- Test coordination between managers
- Test end-to-end job processing pipeline
- Test error conditions and recovery

### 5. Add New Test Cases
Add test cases for the new modular structure:
- Test that manager classes work independently
- Test that Worker properly coordinates with managers
- Test that all public methods are accessible
- Test backward compatibility of imports

### 6. Update Test Configuration
- Update `conftest.py` if it contains worker-related fixtures
- Update any test utilities that import worker classes
- Ensure test dependencies are properly configured

### 7. Run and Validate Tests
- Run all worker-related tests
- Fix any failing tests due to import or structural changes
- Ensure test coverage is maintained or improved
- Verify that all functionality is tested

## Test Files to Create/Update

### New Test Files
- `tests/unit/test_worker_status_manager.py`
- `tests/unit/test_job_status_manager.py`
- `tests/unit/test_failed_job_handler.py`

### Updated Test Files
- `tests/unit/test_unit_worker.py`
- `tests/integration/test_integration_worker.py`
- `tests/scenario/test_scenario_worker.py`
- `tests/smoke/test_smoke_worker.py`
- `tests/conftest.py` (if needed)

## Test Coverage Requirements

### WorkerStatusManager Tests
- Status update operations
- Heartbeat scheduling and management
- KV store integration
- Status transition validation
- Error handling

### JobStatusManager Tests
- Job status lifecycle (pending → started → completed/failed)
- Dependency resolution logic
- Job status persistence
- Status transition validation
- Error handling

### FailedJobHandler Tests
- Failed job classification
- Retry logic and delay calculations
- Failed job stream operations
- Error recovery mechanisms
- Integration with job processing

### Worker Integration Tests
- Worker instantiation and startup
- Job processing end-to-end
- Manager coordination
- Error conditions and recovery
- Worker status reporting
- Failed job handling

## Success Criteria
- [ ] All existing tests updated to work with new structure
- [ ] New tests created for extracted manager classes
- [ ] All tests pass without modification
- [ ] Test coverage maintained or improved
- [ ] No functionality lost in testing
- [ ] Backward compatibility verified through tests

## Dependencies
- **Depends on**: Subtask 04.06 (Create Public API)
- **Prepares for**: Subtask 04.08 (Update Documentation)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 2-3 hours
- **Total**: 5-7 hours