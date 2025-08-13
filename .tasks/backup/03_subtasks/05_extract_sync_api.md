# Subtask 03.5: Extract Sync API

## Overview
Extract the synchronous wrapper functions from the current `queue.py` file into `sync_api.py`.

## Current State
- Sync functions are in `src/naq/queue.py` (lines ~1051-1323)
- Sync wrapper functions are mixed with other queue functionality
- ~272 lines of sync API code

## Target State
- `src/naq/queue/sync_api.py` contains:
  - All `*_sync()` functions (enqueue_sync, enqueue_at_sync, etc.)
  - Sync wrapper implementations
  - Thread-local connection management for sync operations
  - Imports corresponding async functions from `async_api.py`

## Implementation Steps
1. Read current `src/naq/queue.py` file
2. Identify sync functions and related code (lines ~1051-1323)
3. Extract `enqueue_sync()` function
4. Extract `enqueue_at_sync()` function
5. Extract `enqueue_in_sync()` function
6. Extract `schedule_sync()` function
7. Extract `purge_queue_sync()` function
8. Extract `cancel_scheduled_job_sync()` function
9. Extract `pause_scheduled_job_sync()` function
10. Extract `resume_scheduled_job_sync()` function
11. Extract `modify_scheduled_job_sync()` function
12. Move extracted functions to `sync_api.py`
13. Import corresponding async functions from `async_api.py`
14. Preserve thread-local connection optimization
15. Ensure sync wrappers work correctly
16. Maintain exact same function signatures and behavior
17. Add proper error handling for sync operations

## sync_api.py Structure
```python
"""
Synchronous API functions for queue operations.

This module provides synchronous wrappers for async queue operations,
with thread-local connection management for optimal performance.
"""

from typing import List, Optional, Dict, Any
import threading
from ..models import Job, ScheduledJob
from ..settings import Settings
from .async_api import (
    enqueue, enqueue_at, enqueue_in, schedule,
    purge_queue, cancel_scheduled_job, pause_scheduled_job,
    resume_scheduled_job, modify_scheduled_job
)

# Thread-local storage for connections
_local = threading.local()

def enqueue_sync(
    func: callable,
    *args,
    queue_name: str = "default",
    **kwargs
) -> str:
    """
    Synchronous version of enqueue().
    
    Args:
        func: Function to enqueue
        *args: Function arguments
        queue_name: Name of the queue
        **kwargs: Function keyword arguments
    
    Returns:
        Job ID string
    """
    # Move existing enqueue_sync logic

def enqueue_at_sync(
    func: callable,
    scheduled_time: datetime,
    *args,
    queue_name: str = "default",
    **kwargs
) -> str:
    """
    Synchronous version of enqueue_at().
    
    Args:
        func: Function to enqueue
        scheduled_time: When to execute the job
        *args: Function arguments
        queue_name: Name of the queue
        **kwargs: Function keyword arguments
    
    Returns:
        Job ID string
    """
    # Move existing enqueue_at_sync logic

# ... other sync API functions
```

## Success Criteria
- [ ] All sync functions moved to `sync_api.py`
- [ ] Async functions imported from `async_api.py`
- [ ] Thread-local connection optimization preserved
- [ ] Sync wrappers work correctly
- [ ] Exact same function signatures and behavior maintained
- [ ] Proper error handling for sync operations added
- [ ] No sync API code left in original `queue.py`

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.4 (Extract Async API)

## Estimated Time
- 1.5 hours