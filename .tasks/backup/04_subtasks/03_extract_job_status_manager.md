# Subtask 04.03: Extract JobStatusManager

## Overview
Extract the `JobStatusManager` class from the large `worker.py` file into a dedicated `jobs.py` module to separate job status tracking and dependency management concerns.

## Objectives
- Move JobStatusManager class (lines ~281-524, ~243 lines) to `jobs.py`
- Maintain all job status tracking functionality
- Preserve dependency resolution logic
- Ensure clean interface for Worker class integration
- Test job status operations independently

## Implementation Steps

### 1. Analyze JobStatusManager Dependencies
- Identify all imports and dependencies used by JobStatusManager
- Map relationships with other classes and modules
- Note any shared resources that need to be passed
- Understand dependency resolution logic

### 2. Extract Class Definition
- Copy the JobStatusManager class definition from worker.py
- Include all methods, properties, and inner classes
- Preserve all existing functionality and logic
- Include any job status-related constants or enums

### 3. Update Imports
- Add necessary imports to jobs.py
- Remove any imports that are no longer needed
- Ensure imports follow the project's import order conventions
- Import from other worker modules as needed

### 4. Update Worker Class Reference
- Remove JobStatusManager from worker.py
- Add import statement for JobStatusManager from .jobs
- Update any direct references to use the imported class

### 5. Interface Design
- Ensure JobStatusManager has a clean interface for Worker integration
- Verify constructor parameters are appropriate
- Test that job status operations work independently
- Maintain dependency resolution functionality

### 6. Testing
- Create basic tests for JobStatusManager functionality
- Test job status tracking operations
- Test dependency resolution logic
- Verify integration with Worker class still works

## Key Components to Extract

### JobStatusManager Class
- Constructor and initialization logic
- Job status tracking methods (started, completed, failed, etc.)
- Dependency resolution logic
- KV store operations for job status
- Job completion status handling

### Dependencies to Include
- Job-related enums and constants
- NATS KV store operations
- Configuration and logging utilities
- Any helper functions used for job status management
- Dependency resolution algorithms

### Core Functionality to Preserve
- Job lifecycle management (pending → started → completed/failed)
- Dependency tracking and resolution
- Job status persistence
- Status transition validation
- Error handling for status operations

## Interface Requirements
```python
class JobStatusManager:
    def __init__(self, worker: "Worker", ...):
        # Initialize with worker reference and shared resources
        
    async def mark_job_started(self, job_id: str):
        # Mark job as started in KV store
        
    async def mark_job_completed(self, job_id: str, result=None):
        # Mark job as completed with optional result
        
    async def mark_job_failed(self, job_id: str, error: Exception):
        # Mark job as failed with error details
        
    async def resolve_dependencies(self, job: Job):
        # Resolve job dependencies and return ready status
        
    # ... other job status management methods
```

## Success Criteria
- [ ] JobStatusManager successfully extracted to jobs.py
- [ ] All job status tracking functionality preserved
- [ ] Dependency resolution logic maintained
- [ ] Clean interface for Worker integration
- [ ] Job status operations work independently
- [ ] No circular import issues
- [ ] Basic tests pass

## Dependencies
- **Depends on**: Subtask 04.02 (Extract WorkerStatusManager)
- **Prepares for**: Subtask 04.04 (Extract FailedJobHandler)

## Estimated Time
- **Implementation**: 3-4 hours
- **Testing**: 1.5 hours
- **Total**: 4.5-5.5 hours