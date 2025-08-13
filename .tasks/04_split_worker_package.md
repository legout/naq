# Task 04: Split Worker Package into Focused Modules

## Overview
Split the large `worker.py` file (1276 lines) into focused modules to separate worker concerns and improve code organization.

## Current State
- Single file `src/naq/worker.py` contains all worker-related functionality
- Mixed concerns: worker status, job status, failed job handling, main worker logic
- 1276 lines with 4 major classes and complex interdependencies

## Target Structure
```
src/naq/worker/
├── __init__.py         # Public API exports (backward compatibility)
├── core.py            # Main Worker class
├── status.py          # WorkerStatusManager
├── jobs.py            # JobStatusManager
└── failed.py          # FailedJobHandler
```

## Current Code Analysis

Based on structure analysis:
- **WorkerStatusManager class** (lines ~52-280) - ~228 lines
- **JobStatusManager class** (lines ~281-524) - ~243 lines
- **FailedJobHandler class** (lines ~525-611) - ~86 lines
- **Worker class** (lines ~612+) - ~664+ lines

## Detailed Implementation

### 1. core.py - Main Worker Class
**Content to move:**
- Main `Worker` class (~664+ lines)
- Core worker operations and job processing logic
- Worker initialization and lifecycle management
- Message processing and job execution

**Requirements:**
- Import manager classes from other modules
- Maintain all existing Worker methods and properties
- Coordinate with status, job, and failed job managers
- Clean separation of worker concerns
- Comprehensive error handling

### 2. status.py - Worker Status Management
**Content to move:**
- `WorkerStatusManager` class (~228 lines)
- Worker status reporting and heartbeat functionality
- Status update logic and KV store management

**Requirements:**
- Self-contained status management functionality
- NATS KV store operations for worker status
- Heartbeat scheduling and management
- Status transition handling
- Clear interface for Worker class to use

### 3. jobs.py - Job Status Management  
**Content to move:**
- `JobStatusManager` class (~243 lines)
- Job status tracking and dependency management
- Job completion status handling

**Requirements:**
- Job status lifecycle management
- Dependency resolution logic
- KV store operations for job status
- Clean interface for worker job processing
- Error handling for status operations

### 4. failed.py - Failed Job Handling
**Content to move:**
- `FailedJobHandler` class (~86 lines)
- Failed job processing and retry logic
- Failed job stream management

**Requirements:**
- Failed job classification and handling
- Retry logic and delay calculations
- Failed job stream operations
- Integration with job execution pipeline
- Error recovery mechanisms

### 5. __init__.py - Public API Exports
**Requirements:**
- Export Worker class and related utilities
- Maintain complete backward compatibility
- Import from focused modules
- Provide clean public API

**Example structure:**
```python
# Import main Worker class
from .core import Worker

# Import manager classes (if they need to be public)
from .status import WorkerStatusManager
from .jobs import JobStatusManager  
from .failed import FailedJobHandler

__all__ = [
    "Worker",
    "WorkerStatusManager", 
    "JobStatusManager",
    "FailedJobHandler",
]
```

## Implementation Strategy

### Step 1: Analyze Dependencies
Before splitting, identify the interdependencies between classes:
- Worker → WorkerStatusManager (status updates)
- Worker → JobStatusManager (job status tracking)
- Worker → FailedJobHandler (failed job processing)
- Managers may need references back to Worker or shared resources

### Step 2: Create Package Structure
```bash
mkdir -p src/naq/worker
touch src/naq/worker/__init__.py
touch src/naq/worker/core.py
touch src/naq/worker/status.py
touch src/naq/worker/jobs.py
touch src/naq/worker/failed.py
```

### Step 3: Extract Manager Classes First

**Extract WorkerStatusManager (status.py)**
- Move class with all dependencies and imports
- Ensure it's self-contained for status management
- Keep clear interface for Worker to use
- Test status operations independently

**Extract JobStatusManager (jobs.py)**  
- Move class with job status functionality
- Maintain dependency resolution logic
- Keep clean interface for job processing
- Test job status tracking independently

**Extract FailedJobHandler (failed.py)**
- Move failed job handling logic
- Keep retry and recovery mechanisms
- Maintain failed job stream operations
- Test failed job processing independently

### Step 4: Extract Core Worker Class (core.py)
- Move main Worker class
- Import manager classes from other modules
- Update references to use imported managers
- Maintain all existing public methods and behavior
- Ensure proper coordination between managers

### Step 5: Create Public API (__init__.py)
- Set up exports for backward compatibility
- Test existing imports continue to work
- Ensure no functionality is lost

## Interface Design

### Worker Class Interface with Managers
```python
# In core.py
from .status import WorkerStatusManager
from .jobs import JobStatusManager
from .failed import FailedJobHandler

class Worker:
    def __init__(self, ...):
        self.status_manager = WorkerStatusManager(self)
        self.job_manager = JobStatusManager(self) 
        self.failed_handler = FailedJobHandler(self)
        
    async def process_job(self, job):
        # Coordinate with managers
        await self.status_manager.update_status(WORKER_STATUS.BUSY)
        await self.job_manager.mark_job_started(job.job_id)
        # ... job processing
```

### Manager Class Interfaces
```python
# Each manager class needs clean interface
class WorkerStatusManager:
    def __init__(self, worker: "Worker"):
        self.worker = worker
    
    async def update_status(self, status: WORKER_STATUS):
        # Status management logic
        
class JobStatusManager:
    def __init__(self, worker: "Worker"):  
        self.worker = worker
        
    async def mark_job_started(self, job_id: str):
        # Job status logic

class FailedJobHandler:
    def __init__(self, worker: "Worker"):
        self.worker = worker
        
    async def handle_failed_job(self, job: Job, error: Exception):
        # Failed job logic
```

## Implementation Steps

1. **Create package structure**
   - Create worker package directory and files
   - Set up basic imports and structure

2. **Extract WorkerStatusManager**
   - Move class to `status.py` with all dependencies
   - Test status operations work independently
   - Ensure clean interface design

3. **Extract JobStatusManager**  
   - Move class to `jobs.py` with all dependencies
   - Test job status operations work independently
   - Maintain dependency resolution functionality

4. **Extract FailedJobHandler**
   - Move class to `failed.py` with all dependencies  
   - Test failed job handling works independently
   - Maintain retry and recovery logic

5. **Extract Worker class**
   - Move main class to `core.py`
   - Update to import and use manager classes
   - Test all worker operations work correctly
   - Ensure coordination between managers works

6. **Create public API**
   - Set up `__init__.py` with appropriate exports
   - Test backward compatibility
   - Verify all existing imports work

7. **Integration testing**
   - Test full worker lifecycle with all managers
   - Test job processing end-to-end
   - Test error conditions and recovery
   - Test worker status reporting

## Shared Resource Management

Some resources may be shared between managers:
- NATS connections and JetStream contexts
- Configuration settings
- Logging instances
- Worker metadata (ID, hostname, etc.)

**Solution:** Pass shared resources through Worker class or create shared context object.

## Files to Create/Modify

### New Files  
- `src/naq/worker/__init__.py`
- `src/naq/worker/core.py`
- `src/naq/worker/status.py`
- `src/naq/worker/jobs.py`
- `src/naq/worker/failed.py`

### Files to Remove
- `src/naq/worker.py`

### Files to Update (verify imports still work)
- `src/naq/__init__.py`
- `src/naq/cli/` (worker command files)
- Test files importing worker functionality

## Success Criteria

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

## Testing Checklist

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

## Dependencies
- **Depends on**: 
  - Task 01 (Split Models) - for updated model imports
  - Task 03 (Split Queue Package) - worker depends on queue functionality
- **Blocks**: Task 05 (Service Layer) - services will use worker components

## Estimated Time
- **Implementation**: 8-10 hours
- **Testing**: 4-5 hours  
- **Total**: 12-15 hours