# Subtask 03.3: Extract Core Queue Class

## Overview
Extract the main Queue class and core queue operations from the current `queue.py` file into `core.py`.

## Current State
- Queue class is in `src/naq/queue.py` (lines ~336-885)
- Core queue operations are mixed with scheduled job functionality
- ~550 lines of queue related code

## Target State
- `src/naq/queue/core.py` contains:
  - Main `Queue` class (~550 lines)
  - Core queue operations and job enqueuing logic
  - Queue-specific imports and utilities
  - Clean separation from scheduled job functionality
  - Imports ScheduledJobManager from `scheduled.py`

## Implementation Steps
1. Read current `src/naq/queue.py` file
2. Identify Queue class and related code (lines ~336-885)
3. Extract Queue class definition
4. Extract core queue operations and job enqueuing logic
5. Extract queue-specific imports and utilities
6. Move extracted code to `core.py`
7. Update imports to use ScheduledJobManager from `scheduled.py`
8. Remove scheduled job functionality (delegate to ScheduledJobManager)
9. Add comprehensive docstrings for all public methods
10. Ensure all Queue methods work correctly

## core.py Structure
```python
"""
Core queue functionality.

This module contains the main Queue class which handles NATS connection
management, stream creation, basic job enqueuing, and queue operations.
"""

from typing import List, Optional, Dict, Any
from ..models import Job, JobStatus
from ..models.enums import RETRY_STRATEGY
from ..settings import Settings
from .scheduled import ScheduledJobManager

class Queue:
    """
    Main queue class for job management and NATS operations.
    
    Handles NATS connection management, stream creation, basic job
    enqueuing to streams, queue purging operations, and status monitoring.
    """
    
    def __init__(self, settings: Settings):
        # Move existing initialization logic
        self.scheduled_job_manager = ScheduledJobManager(settings)
    
    async def enqueue(self, job: Job) -> str:
        # Move existing enqueue logic
    
    async def purge(self) -> int:
        # Move existing purge logic
    
    # ... other core queue methods
```

## Success Criteria
- [ ] Queue class moved to `core.py`
- [ ] Core queue operations and job enqueuing logic extracted
- [ ] Queue-specific imports and utilities moved
- [ ] Imports updated to use ScheduledJobManager from `scheduled.py`
- [ ] Scheduled job functionality removed (delegated to ScheduledJobManager)
- [ ] Comprehensive docstrings added
- [ ] All Queue methods work correctly
- [ ] No queue class code left in original `queue.py`

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.2 (Extract ScheduledJobManager)

## Estimated Time
- 3 hours