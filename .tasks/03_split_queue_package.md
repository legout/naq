# Task 03: Split Queue Package into Focused Modules

## Overview
Split the large `queue.py` file (1323 lines) into focused modules to separate concerns and improve maintainability of queue operations.

## Current State
- Single file `src/naq/queue.py` contains all queue-related functionality
- Mixed concerns: core Queue class, scheduled job management, async API, sync wrappers
- 1323 lines with multiple classes and dozens of functions

## Target Structure
```
src/naq/queue/
├── __init__.py         # Public API exports (backward compatibility)
├── core.py            # Main Queue class
├── scheduled.py       # ScheduledJobManager and schedule operations
├── async_api.py       # Async functions (enqueue, enqueue_at, etc.)
└── sync_api.py        # Sync wrapper functions
```

## Current Code Analysis

Based on structure analysis:
- **ScheduledJobManager class** (lines ~35-335) - ~300 lines
- **Queue class** (lines ~336-885) - ~550 lines  
- **Async functions** (lines ~886-1050) - ~164 lines
- **Sync functions** (lines ~1051-1323) - ~272 lines

## Detailed Implementation

### 1. core.py - Main Queue Class
**Content to move:**
- Main `Queue` class (~550 lines)
- Core queue operations and job enqueuing logic
- Queue-specific imports and utilities

**Requirements:**
- Import scheduled job manager from `scheduled.py`
- Maintain all existing Queue methods and properties
- Clean separation from scheduled job functionality
- Comprehensive docstrings for all public methods

### 2. scheduled.py - Scheduled Job Management  
**Content to move:**
- `ScheduledJobManager` class (~300 lines)
- All scheduled job management logic
- Schedule-related utilities and helpers

**Requirements:**
- Self-contained module for scheduled job operations
- Import necessary models and settings
- Maintain all existing scheduled job functionality
- Clear interface for Queue class to use

### 3. async_api.py - Async API Functions
**Content to move:**
- `enqueue()` function
- `enqueue_at()` function  
- `enqueue_in()` function
- `schedule()` function
- `purge_queue()` function
- `cancel_scheduled_job()` function
- `pause_scheduled_job()` function
- `resume_scheduled_job()` function
- `modify_scheduled_job()` function

**Requirements:**
- High-level async API functions
- Import Queue class from `core.py`
- Maintain exact same function signatures
- Proper error handling and documentation

### 4. sync_api.py - Synchronous Wrapper Functions
**Content to move:**
- All `*_sync()` functions (enqueue_sync, enqueue_at_sync, etc.)
- Sync wrapper implementations
- Thread-local connection management for sync operations

**Requirements:**
- Import corresponding async functions from `async_api.py`
- Maintain exact same function signatures
- Preserve thread-local connection optimization
- Proper error handling for sync operations

### 5. __init__.py - Public API Exports
**Requirements:**
- Export all classes and functions that were previously available
- Maintain complete backward compatibility
- Import from focused modules
- Clean public API presentation

**Example structure:**
```python
# Import core classes
from .core import Queue
from .scheduled import ScheduledJobManager

# Import async API
from .async_api import (
    enqueue, enqueue_at, enqueue_in, schedule,
    purge_queue, cancel_scheduled_job, pause_scheduled_job,
    resume_scheduled_job, modify_scheduled_job
)

# Import sync API  
from .sync_api import (
    enqueue_sync, enqueue_at_sync, enqueue_in_sync, schedule_sync,
    purge_queue_sync, cancel_scheduled_job_sync, pause_scheduled_job_sync,
    resume_scheduled_job_sync, modify_scheduled_job_sync
)

__all__ = [
    # Classes
    "Queue", "ScheduledJobManager",
    # Async API
    "enqueue", "enqueue_at", "enqueue_in", "schedule", "purge_queue",
    "cancel_scheduled_job", "pause_scheduled_job", "resume_scheduled_job", 
    "modify_scheduled_job",
    # Sync API
    "enqueue_sync", "enqueue_at_sync", "enqueue_in_sync", "schedule_sync",
    "purge_queue_sync", "cancel_scheduled_job_sync", "pause_scheduled_job_sync",
    "resume_scheduled_job_sync", "modify_scheduled_job_sync",
]
```

## Implementation Steps

### Step 1: Create Package Structure
```bash
mkdir -p src/naq/queue
touch src/naq/queue/__init__.py
touch src/naq/queue/core.py
touch src/naq/queue/scheduled.py
touch src/naq/queue/async_api.py
touch src/naq/queue/sync_api.py
```

### Step 2: Extract ScheduledJobManager (scheduled.py)
- Move `ScheduledJobManager` class to `scheduled.py`
- Include all related imports and dependencies
- Ensure class is self-contained and testable
- Add comprehensive docstrings

### Step 3: Extract Core Queue Class (core.py)
- Move main `Queue` class to `core.py`
- Update imports to use `ScheduledJobManager` from `scheduled.py`
- Remove scheduled job functionality (delegate to ScheduledJobManager)
- Ensure all Queue methods work correctly

### Step 4: Extract Async API (async_api.py)
- Move all async functions to `async_api.py`
- Import `Queue` class from `core.py`
- Ensure all functions work with new Queue location
- Maintain exact same function signatures and behavior

### Step 5: Extract Sync API (sync_api.py)  
- Move all sync wrapper functions to `sync_api.py`
- Import async functions from `async_api.py`
- Preserve thread-local connection optimizations
- Ensure sync wrappers work correctly

### Step 6: Create Public API (__init__.py)
- Set up all exports for backward compatibility
- Test that existing imports continue to work
- Ensure no functionality is lost

### Step 7: Update Dependencies
- Update all files that import from `naq.queue`
- Verify all imports continue to work
- Run tests to ensure no breakage

## Separation of Concerns

### Core Queue Class Responsibilities
- NATS connection management
- Stream creation and configuration
- Basic job enqueuing to streams
- Queue purging operations
- Status monitoring

### ScheduledJobManager Responsibilities
- Scheduled job storage and retrieval
- Schedule validation and parsing
- Cron expression handling
- Scheduled job lifecycle management

### Async API Responsibilities  
- High-level user-facing async functions
- Parameter validation and processing
- Coordination between Queue and ScheduledJobManager
- Error handling and logging

### Sync API Responsibilities
- Synchronous wrappers for async functions
- Thread-local connection management
- Sync-specific error handling
- Backward compatibility for sync users

## Files to Create/Modify

### New Files
- `src/naq/queue/__init__.py`
- `src/naq/queue/core.py`
- `src/naq/queue/scheduled.py`  
- `src/naq/queue/async_api.py`
- `src/naq/queue/sync_api.py`

### Files to Remove
- `src/naq/queue.py`

### Files to Update (verify imports still work)
- `src/naq/__init__.py`
- `src/naq/scheduler.py`
- `src/naq/worker.py`
- `src/naq/cli/` (various command files)
- Test files importing queue functionality

## Success Criteria

- [ ] All queue functionality successfully split into focused modules
- [ ] Public API unchanged (backward compatibility)
- [ ] All existing imports continue to work
- [ ] Queue class operations work correctly
- [ ] Scheduled job operations work correctly  
- [ ] Async API functions work correctly
- [ ] Sync API wrappers work correctly
- [ ] All tests pass without modification
- [ ] No file exceeds 500 lines
- [ ] Clear separation of concerns achieved

## Testing Checklist

- [ ] `from naq.queue import Queue, enqueue, enqueue_sync` works
- [ ] `Queue` class instantiation and methods work
- [ ] `enqueue()`, `enqueue_at()`, `enqueue_in()` functions work
- [ ] `schedule()` function works correctly
- [ ] All sync wrapper functions work correctly
- [ ] Scheduled job management operations work
- [ ] Thread-local connection optimization preserved
- [ ] No circular import issues
- [ ] Integration with worker and scheduler unchanged

## Dependencies
- **Depends on**: Task 01 (Split Models) - for updated model imports
- **Blocks**: Task 04 (Split Worker Package) - worker depends on queue

## Estimated Time
- **Implementation**: 8-10 hours
- **Testing**: 3-4 hours
- **Total**: 11-14 hours