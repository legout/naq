# Subtask 01.6: Create public API in __init__.py

## Overview
Create the public API facade in models/__init__.py to maintain backward compatibility.

## Current State
- Empty `src/naq/models/__init__.py` file
- No public API defined

## Target State
- Complete `__init__.py` with all exports
- Backward compatibility maintained
- All symbols available from `naq.models`

## Implementation Steps
1. Import all classes and enums from sub-modules:
   ```python
   from .enums import JOB_STATUS, JobEventType, WorkerEventType
   from .jobs import Job, JobResult, RetryDelayType
   from .events import JobEvent, WorkerEvent
   from .schedules import Schedule
   ```
2. Define `__all__` list with all exported symbols:
   ```python
   __all__ = [
       "JOB_STATUS", "JobEventType", "WorkerEventType",
       "Job", "JobResult", "RetryDelayType", 
       "JobEvent", "WorkerEvent",
       "Schedule",
   ]
   ```
3. Add module docstring explaining the structure
4. Add deprecation warnings if needed

## Success Criteria
- [ ] All classes and enums imported from sub-modules
- [ ] `__all__` list properly defined with all symbols
- [ ] Module docstring added
- [ ] `from naq.models import *` works correctly
- [ ] `from naq.models import Job, JOB_STATUS` works correctly
- [ ] Backward compatibility maintained

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enums
- Subtask 01.3: Move events
- Subtask 01.4: Move jobs
- Subtask 01.5: Move schedules

## Estimated Time
- 30 minutes