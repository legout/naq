# Subtask 04.02: Extract WorkerStatusManager

## Overview
Extract the `WorkerStatusManager` class from the large `worker.py` file into a dedicated `status.py` module to separate worker status management concerns.

## Objectives
- Move WorkerStatusManager class (lines ~52-280, ~228 lines) to `status.py`
- Maintain all status management functionality
- Ensure clean interface for Worker class integration
- Test status operations independently

## Implementation Steps

### 1. Analyze WorkerStatusManager Dependencies
- Identify all imports and dependencies used by WorkerStatusManager
- Map relationships with other classes and modules
- Note any shared resources that need to be passed

### 2. Extract Class Definition
- Copy the WorkerStatusManager class definition from worker.py
- Include all methods, properties, and inner classes
- Preserve all existing functionality and logic

### 3. Update Imports
- Add necessary imports to status.py
- Remove any imports that are no longer needed
- Ensure imports follow the project's import order conventions

### 4. Update Worker Class Reference
- Remove WorkerStatusManager from worker.py
- Add import statement for WorkerStatusManager from .status
- Update any direct references to use the imported class

### 5. Interface Design
- Ensure WorkerStatusManager has a clean interface for Worker integration
- Verify constructor parameters are appropriate
- Test that status operations work independently

### 6. Testing
- Create basic tests for WorkerStatusManager functionality
- Test status updates and heartbeat operations
- Verify integration with Worker class still works

## Key Components to Extract

### WorkerStatusManager Class
- Constructor and initialization logic
- Status update methods
- Heartbeat scheduling and management
- KV store operations for worker status
- Status transition handling

### Dependencies to Include
- Any enums or constants related to worker status
- NATS KV store operations
- Configuration and logging utilities
- Any helper functions used by status management

## Interface Requirements
```python
class WorkerStatusManager:
    def __init__(self, worker: "Worker", ...):
        # Initialize with worker reference and shared resources
        
    async def update_status(self, status: WORKER_STATUS):
        # Update worker status in KV store
        
    async def start_heartbeat(self):
        # Start periodic heartbeat updates
        
    async def stop_heartbeat(self):
        # Stop heartbeat updates
        
    # ... other status management methods
```

## Success Criteria
- [ ] WorkerStatusManager successfully extracted to status.py
- [ ] All status management functionality preserved
- [ ] Clean interface for Worker integration
- [ ] Status operations work independently
- [ ] No circular import issues
- [ ] Basic tests pass

## Dependencies
- **Depends on**: Subtask 04.01 (Create Package Structure)
- **Prepares for**: Subtask 04.03 (Extract JobStatusManager)

## Estimated Time
- **Implementation**: 2-3 hours
- **Testing**: 1 hour
- **Total**: 3-4 hours