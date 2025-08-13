# Subtask 03.2: Extract ScheduledJobManager

## Overview
Extract the ScheduledJobManager class and related functionality from the current `queue.py` file into `scheduled.py`.

## Current State
- ScheduledJobManager class is in `src/naq/queue.py` (lines ~35-335)
- Scheduled job management logic is mixed with other queue functionality
- ~300 lines of scheduled job related code

## Target State
- `src/naq/queue/scheduled.py` contains:
  - `ScheduledJobManager` class (~300 lines)
  - All scheduled job management logic
  - Schedule-related utilities and helpers
  - Self-contained module for scheduled job operations
  - Necessary imports from models and settings

## Implementation Steps
1. Read current `src/naq/queue.py` file
2. Identify ScheduledJobManager class and related code (lines ~35-335)
3. Extract ScheduledJobManager class definition
4. Extract all scheduled job management methods
5. Extract schedule-related utilities and helpers
6. Move extracted code to `scheduled.py`
7. Add necessary imports (models, settings, etc.)
8. Add comprehensive docstrings for all public methods
9. Ensure class is self-contained and testable

## scheduled.py Structure
```python
"""
Scheduled job management functionality.

This module contains the ScheduledJobManager class which handles
all scheduled job operations including storage, retrieval, validation,
and lifecycle management.
"""

from typing import List, Optional, Dict, Any
from ..models import ScheduledJob, JobStatus
from ..models.enums import RETRY_STRATEGY
from ..settings import Settings

class ScheduledJobManager:
    """
    Manages scheduled jobs including storage, retrieval, validation,
    and lifecycle management.
    """
    
    def __init__(self, settings: Settings):
        # Move existing initialization logic
    
    async def schedule_job(self, job: ScheduledJob) -> str:
        # Move existing schedule_job logic
    
    async def get_scheduled_job(self, job_id: str) -> Optional[ScheduledJob]:
        # Move existing get_scheduled_job logic
    
    # ... other scheduled job management methods
```

## Success Criteria
- [ ] ScheduledJobManager class moved to `scheduled.py`
- [ ] All scheduled job management logic extracted
- [ ] Schedule-related utilities and helpers moved
- [ ] Necessary imports added
- [ ] Comprehensive docstrings added
- [ ] Class is self-contained and testable
- [ ] No scheduled job code left in original `queue.py`

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)

## Estimated Time
- 2 hours