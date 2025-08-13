# Subtask 04.05: Extract Core Worker Class

## Overview
Extract the main `Worker` class from the large `worker.py` file into `core.py` and update it to use the extracted manager classes, completing the worker package split.

## Objectives
- Move Worker class (lines ~612+, ~664+ lines) to `core.py`
- Update Worker to import and use extracted manager classes
- Maintain all existing Worker functionality and public methods
- Ensure proper coordination between manager classes
- Test all worker operations work correctly

## Implementation Steps

### 1. Prepare Core Worker Module
- Create `core.py` with proper imports
- Import manager classes from their respective modules:
  - `from .status import WorkerStatusManager`
  - `from .jobs import JobStatusManager`
  - `from .failed import FailedJobHandler`

### 2. Extract Worker Class Definition
- Copy the Worker class definition from worker.py
- Include all methods, properties, and inner classes
- Preserve all existing functionality and logic

### 3. Update Worker Constructor
- Replace direct manager class instantiation with imports
- Update constructor to use imported manager classes:
  ```python
  def __init__(self, ...):
      self.status_manager = WorkerStatusManager(self, ...)
      self.job_manager = JobStatusManager(self, ...)
      self.failed_handler = FailedJobHandler(self, ...)
  ```

### 4. Update Method References
- Replace direct calls to manager methods with calls through manager instances
- Update all worker methods to use the new manager interface
- Ensure proper coordination between managers

### 5. Remove Original worker.py
- Once Worker class is successfully extracted and tested
- Remove the original `src/naq/worker.py` file
- Update any remaining imports that referenced the old file

### 6. Update Public API
- Ensure `__init__.py` exports the Worker class correctly
- Verify all existing imports continue to work
- Test backward compatibility

### 7. Integration Testing
- Test full worker lifecycle with all managers
- Test job processing end-to-end
- Test error conditions and recovery
- Test worker status reporting
- Test failed job handling

## Key Components to Update

### Worker Class Integration
- Constructor initialization with manager classes
- Job processing coordination between managers
- Status updates through WorkerStatusManager
- Job status tracking through JobStatusManager
- Failed job handling through FailedJobHandler

### Manager Coordination
- Worker → WorkerStatusManager (status updates)
- Worker → JobStatusManager (job status tracking)
- Worker → FailedJobHandler (failed job processing)
- Cross-manager communication where needed

### Core Functionality to Preserve
- Worker initialization and lifecycle management
- Message processing and job execution
- Worker startup and shutdown procedures
- Job processing pipeline
- Error handling and recovery
- Configuration management

## Interface Updates Required
```python
# In core.py
from .status import WorkerStatusManager
from .jobs import JobStatusManager
from .failed import FailedJobHandler

class Worker:
    def __init__(self, ...):
        # Initialize with manager classes
        self.status_manager = WorkerStatusManager(self, ...)
        self.job_manager = JobStatusManager(self, ...)
        self.failed_handler = FailedJobHandler(self, ...)
        
    async def process_job(self, job):
        # Coordinate with managers
        await self.status_manager.update_status(WORKER_STATUS.BUSY)
        await self.job_manager.mark_job_started(job.job_id)
        try:
            # ... job processing logic
            await self.job_manager.mark_job_completed(job.job_id, result)
        except Exception as e:
            await self.failed_handler.handle_failed_job(job, e)
            await self.job_manager.mark_job_failed(job.job_id, e)
        finally:
            await self.status_manager.update_status(WORKER_STATUS.IDLE)
```

## Success Criteria
- [ ] Worker class successfully extracted to core.py
- [ ] Worker properly imports and uses manager classes
- [ ] All existing Worker functionality preserved
- [ ] Manager coordination works correctly
- [ ] No circular import issues
- [ ] All existing imports continue to work
- [ ] Integration tests pass
- [ ] Original worker.py file removed

## Dependencies
- **Depends on**: Subtasks 04.02-04.04 (Manager class extractions)
- **Prepares for**: Subtask 04.06 (Create Public API)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 2 hours
- **Total**: 5-6 hours