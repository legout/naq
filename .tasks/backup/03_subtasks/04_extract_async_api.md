# Subtask 03.4: Extract Async API

## Overview
Extract the async API functions from the current `queue.py` file into `async_api.py`.

## Current State
- Async functions are in `src/naq/queue.py` (lines ~886-1050)
- High-level async API functions are mixed with other queue functionality
- ~164 lines of async API code

## Target State
- `src/naq/queue/async_api.py` contains:
  - `enqueue()` function
  - `enqueue_at()` function
  - `enqueue_in()` function
  - `schedule()` function
  - `purge_queue()` function
  - `cancel_scheduled_job()` function
  - `pause_scheduled_job()` function
  - `resume_scheduled_job()` function
  - `modify_scheduled_job()` function
  - High-level async API functions
  - Imports Queue class from `core.py`

## Implementation Steps
1. Read current `src/naq/queue.py` file
2. Identify async functions and related code (lines ~886-1050)
3. Extract `enqueue()` function
4. Extract `enqueue_at()` function
5. Extract `enqueue_in()` function
6. Extract `schedule()` function
7. Extract `purge_queue()` function
8. Extract `cancel_scheduled_job()` function
9. Extract `pause_scheduled_job()` function
10. Extract `resume_scheduled_job()` function
11. Extract `modify_scheduled_job()` function
12. Move extracted functions to `async_api.py`
13. Import Queue class from `core.py`
14. Ensure all functions work with new Queue location
15. Maintain exact same function signatures and behavior
16. Add proper error handling and documentation

## async_api.py Structure
```python
"""
Async API functions for queue operations.

This module provides high-level async functions for job enqueuing,
scheduling, and queue management operations.
"""

from typing import List, Optional, Dict, Any
from ..models import Job, ScheduledJob
from ..settings import Settings
from .core import Queue

async def enqueue(
    func: callable,
    *args,
    queue_name: str = "default",
    **kwargs
) -> str:
    """
    Enqueue a job for execution.
    
    Args:
        func: Function to enqueue
        *args: Function arguments
        queue_name: Name of the queue
        **kwargs: Function keyword arguments
    
    Returns:
        Job ID string
    """
    # Move existing enqueue logic

async def enqueue_at(
    func: callable,
    scheduled_time: datetime,
    *args,
    queue_name: str = "default",
    **kwargs
) -> str:
    """
    Enqueue a job for execution at a specific time.
    
    Args:
        func: Function to enqueue
        scheduled_time: When to execute the job
        *args: Function arguments
        queue_name: Name of the queue
        **kwargs: Function keyword arguments
    
    Returns:
        Job ID string
    """
    # Move existing enqueue_at logic

# ... other async API functions
```

## Success Criteria
- [ ] All async functions moved to `async_api.py`
- [ ] Queue class imported from `core.py`
- [ ] All functions work with new Queue location
- [ ] Exact same function signatures and behavior maintained
- [ ] Proper error handling and documentation added
- [ ] No async API code left in original `queue.py`

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.3 (Extract Core Queue Class)

## Estimated Time
- 1.5 hours