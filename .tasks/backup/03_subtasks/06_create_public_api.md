# Subtask 03.6: Create Public API

## Overview
Set up the public API exports in `__init__.py` to maintain complete backward compatibility.

## Current State
- `src/naq/queue/__init__.py` is empty
- No exports defined
- Backward compatibility not considered

## Target State
- `src/naq/queue/__init__.py` contains:
  - Export all classes and functions that were previously available
  - Maintain complete backward compatibility
  - Import from focused modules
  - Clean public API presentation

## Implementation Steps
1. Read current `src/naq/queue/__init__.py` file
2. Determine what needs to be exported from the package
3. Check what was previously available from `naq.queue`
4. Set up imports from focused modules
5. Create comprehensive `__all__` list
6. Test that existing imports continue to work
7. Ensure no functionality is lost

## __init__.py Structure
```python
"""
NAQ Queue Package

A simple NATS-based queueing system, similar to RQ.
"""

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

## Success Criteria
- [ ] All previously available classes and functions exported
- [ ] Complete backward compatibility maintained
- [ ] Imports from focused modules working
- [ ] Clean public API presentation
- [ ] Existing imports continue to work
- [ ] No functionality lost
- [ ] No circular imports

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.2 (Extract ScheduledJobManager)
- Subtask 03.3 (Extract Core Queue Class)
- Subtask 03.4 (Extract Async API)
- Subtask 03.5 (Extract Sync API)

## Estimated Time
- 30 minutes