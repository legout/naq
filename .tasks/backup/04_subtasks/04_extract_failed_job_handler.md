# Subtask 04.04: Extract FailedJobHandler

## Overview
Extract the `FailedJobHandler` class from the large `worker.py` file into a dedicated `failed.py` module to separate failed job processing and retry logic concerns.

## Objectives
- Move FailedJobHandler class (lines ~525-611, ~86 lines) to `failed.py`
- Maintain all failed job processing functionality
- Preserve retry logic and delay calculations
- Ensure clean interface for Worker class integration
- Test failed job handling operations independently

## Implementation Steps

### 1. Analyze FailedJobHandler Dependencies
- Identify all imports and dependencies used by FailedJobHandler
- Map relationships with other classes and modules
- Note any shared resources that need to be passed
- Understand retry logic and delay calculations

### 2. Extract Class Definition
- Copy the FailedJobHandler class definition from worker.py
- Include all methods, properties, and inner classes
- Preserve all existing functionality and logic
- Include any failed job-related constants or enums

### 3. Update Imports
- Add necessary imports to failed.py
- Remove any imports that are no longer needed
- Ensure imports follow the project's import order conventions
- Import from other worker modules as needed

### 4. Update Worker Class Reference
- Remove FailedJobHandler from worker.py
- Add import statement for FailedJobHandler from .failed
- Update any direct references to use the imported class

### 5. Interface Design
- Ensure FailedJobHandler has a clean interface for Worker integration
- Verify constructor parameters are appropriate
- Test that failed job operations work independently
- Maintain retry and recovery mechanisms

### 6. Testing
- Create basic tests for FailedJobHandler functionality
- Test failed job classification and handling
- Test retry logic and delay calculations
- Test failed job stream operations
- Verify integration with Worker class still works

## Key Components to Extract

### FailedJobHandler Class
- Constructor and initialization logic
- Failed job classification methods
- Retry logic and delay calculations
- Failed job stream management
- Error recovery mechanisms

### Dependencies to Include
- Failed job-related enums and constants
- NATS stream operations
- Configuration and logging utilities
- Any helper functions used for failed job handling
- Retry algorithms and backoff strategies

### Core Functionality to Preserve
- Failed job classification (permanent vs temporary failures)
- Retry delay calculations (exponential backoff, etc.)
- Failed job stream publishing
- Error recovery mechanisms
- Failed job tracking and reporting

## Interface Requirements
```python
class FailedJobHandler:
    def __init__(self, worker: "Worker", ...):
        # Initialize with worker reference and shared resources
        
    async def handle_failed_job(self, job: Job, error: Exception):
        # Handle failed job with classification and retry logic
        
    async def should_retry(self, job: Job, error: Exception) -> bool:
        # Determine if job should be retried
        
    async def calculate_retry_delay(self, job: Job, attempt: int) -> float:
        # Calculate delay before retry based on attempt number
        
    async def publish_failed_job(self, job: Job, error: Exception):
        # Publish failed job to failed job stream
        
    # ... other failed job handling methods
```

## Success Criteria
- [ ] FailedJobHandler successfully extracted to failed.py
- [ ] All failed job processing functionality preserved
- [ ] Retry logic and delay calculations maintained
- [ ] Clean interface for Worker integration
- [ ] Failed job operations work independently
- [ ] No circular import issues
- [ ] Basic tests pass

## Dependencies
- **Depends on**: Subtask 04.03 (Extract JobStatusManager)
- **Prepares for**: Subtask 04.05 (Extract Core Worker Class)

## Estimated Time
- **Implementation**: 1.5-2 hours
- **Testing**: 45 minutes
- **Total**: 2-2.75 hours