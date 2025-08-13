# Task 04: Split Worker Package - Subtasks

This directory contains the detailed subtasks for splitting the large `worker.py` file into focused modules.

## Overview

The worker package split involves dividing the 1276-line `worker.py` file into four focused modules:
- `core.py` - Main Worker class
- `status.py` - WorkerStatusManager
- `jobs.py` - JobStatusManager  
- `failed.py` - FailedJobHandler

## Subtask List

### 04.01: Create Worker Package Structure
- **File**: `01_create_worker_package_structure.md`
- **Description**: Create the basic directory structure and files for the new worker package
- **Duration**: 45 minutes

### 04.02: Extract WorkerStatusManager
- **File**: `02_extract_worker_status_manager.md`
- **Description**: Extract WorkerStatusManager class to dedicated status.py module
- **Duration**: 3-4 hours

### 04.03: Extract JobStatusManager
- **File**: `03_extract_job_status_manager.md`
- **Description**: Extract JobStatusManager class to dedicated jobs.py module
- **Duration**: 4.5-5.5 hours

### 04.04: Extract FailedJobHandler
- **File**: `04_extract_failed_job_handler.md`
- **Description**: Extract FailedJobHandler class to dedicated failed.py module
- **Duration**: 2-2.75 hours

### 04.05: Extract Core Worker Class
- **File**: `05_extract_core_worker_class.md`
- **Description**: Extract main Worker class to core.py and update to use manager classes
- **Duration**: 5-6 hours

### 04.06: Create Worker Public API
- **File**: `06_create_worker_public_api.md`
- **Description**: Set up __init__.py exports for backward compatibility
- **Duration**: 1.75 hours

### 04.07: Update Worker Tests
- **File**: `07_update_worker_tests.md`
- **Description**: Update existing tests and create new tests for extracted manager classes
- **Duration**: 5-7 hours

### 04.08: Update Worker Documentation
- **File**: `08_update_worker_documentation.md`
- **Description**: Update all documentation to reflect new worker package structure
- **Duration**: 3-4 hours

## Total Estimated Time
- **Implementation**: 20-25 hours
- **Testing**: 4-6 hours
- **Documentation**: 3-4 hours
- **Total**: 27-35 hours

## Dependencies

### Upstream Dependencies
- **Task 01 (Split Models)**: Provides updated model imports
- **Task 03 (Split Queue Package)**: Worker depends on queue functionality

### Downstream Dependencies
- **Task 05 (Service Layer)**: Services will use worker components

### Subtask Dependencies
```
04.01 → 04.02 → 04.03 → 04.04 → 04.05 → 04.06 → 04.07 → 04.08
```

## Success Criteria

### Overall Task Success
- [ ] All worker functionality successfully split into focused modules
- [ ] Worker class maintains all existing public methods
- [ ] Manager classes work independently where appropriate
- [ ] Worker coordination with managers works correctly
- [ ] All existing imports continue to work (backward compatibility)
- [ ] Job processing pipeline works end-to-end
- [ ] Worker status reporting works correctly
- [ ] Failed job handling works correctly
- [ ] All tests pass without modification
- [ ] No file exceeds 400 lines
- [ ] Clear separation of concerns achieved

### Testing Checklist
- [ ] `from naq.worker import Worker` works
- [ ] Worker instantiation and startup works
- [ ] Job processing works correctly
- [ ] Worker status updates work correctly
- [ ] Job status tracking works correctly
- [ ] Failed job handling works correctly
- [ ] Worker shutdown works correctly
- [ ] Heartbeat functionality works
- [ ] No circular import issues
- [ ] Integration with queue and scheduler unchanged

## Files to Create/Modify

### New Files
- `src/naq/worker/__init__.py`
- `src/naq/worker/core.py`
- `src/naq/worker/status.py`
- `src/naq/worker/jobs.py`
- `src/naq/worker/failed.py`

### Files to Remove
- `src/naq/worker.py`

### Files to Update
- `src/naq/__init__.py`
- `src/naq/cli/` (worker command files)
- Test files in `tests/`
- Documentation files in `docs/`

## Implementation Notes

### Shared Resource Management
Some resources are shared between managers:
- NATS connections and JetStream contexts
- Configuration settings
- Logging instances
- Worker metadata (ID, hostname, etc.)

**Solution**: Pass shared resources through Worker class or create shared context object.

### Interface Design
Each manager class needs a clean interface for Worker integration:
```python
class WorkerStatusManager:
    def __init__(self, worker: "Worker"):
        self.worker = worker

class JobStatusManager:
    def __init__(self, worker: "Worker"):
        self.worker = worker

class FailedJobHandler:
    def __init__(self, worker: "Worker"):
        self.worker = worker
```

### Backward Compatibility
The `__init__.py` file must export all classes to maintain backward compatibility:
```python
from .core import Worker
from .status import WorkerStatusManager
from .jobs import JobStatusManager
from .failed import FailedJobHandler

__all__ = ["Worker", "WorkerStatusManager", "JobStatusManager", "FailedJobHandler"]
```