# Task 01: Split models.py into Focused Modules

## Overview
Split the large `models.py` file (1039 lines) into focused, single-responsibility modules to improve maintainability and code organization.

## Current State
- Single file `src/naq/models.py` contains all model definitions
- Mixed concerns: enums, events, jobs, schedules, results
- 1039 lines with 8 different classes and multiple concerns

## Target Structure
```
src/naq/models/
├── __init__.py         # Public API exports (backward compatibility)
├── enums.py           # All enum definitions
├── jobs.py            # Job and JobResult models
├── events.py          # JobEvent and WorkerEvent models
└── schedules.py       # Schedule model
```

## Detailed Implementation

### 1. Create models package directory
- Create `src/naq/models/` directory
- Move model definitions to focused files

### 2. enums.py - All Enum Definitions
**Content to move:**
- `JOB_STATUS` (lines 23-34)
- `JobEventType` (lines 36-54) 
- `WorkerEventType` (lines 56-65)
- `RETRY_STRATEGY` import and `VALID_RETRY_STRATEGIES`

**Additional requirements:**
- Add comprehensive docstrings for each enum
- Include usage examples in docstrings
- Add type annotations where beneficial

### 3. events.py - Event Models
**Content to move:**
- `JobEvent` class (lines 67-341)
- `WorkerEvent` class (lines 343-529)

**Additional requirements:**
- Import required enums from `enums.py`
- Ensure all class methods work correctly
- Add comprehensive docstrings
- Include usage examples

### 4. jobs.py - Job Models  
**Content to move:**
- `Job` class (lines 608-1040)
- `JobResult` class (lines 531-565)
- `RetryDelayType` type hint (line 20)

**Additional requirements:**
- Import required enums from `enums.py`
- Import event models from `events.py` if needed for type hints
- Ensure all methods and properties work correctly
- Add comprehensive docstrings

### 5. schedules.py - Schedule Model
**Content to move:**
- `Schedule` class (lines 566-599)

**Additional requirements:**
- Import required enums from `enums.py`
- Add comprehensive docstrings
- Include usage examples

### 6. __init__.py - Public API Exports
**Requirements:**
- Import all classes and enums from sub-modules
- Export everything that was previously available from `models.py`
- Maintain exact same public API for backward compatibility
- Add deprecation warnings if needed

**Example structure:**
```python
# Maintain backward compatibility
from .enums import JOB_STATUS, JobEventType, WorkerEventType
from .jobs import Job, JobResult, RetryDelayType
from .events import JobEvent, WorkerEvent
from .schedules import Schedule

__all__ = [
    "JOB_STATUS", "JobEventType", "WorkerEventType",
    "Job", "JobResult", "RetryDelayType", 
    "JobEvent", "WorkerEvent",
    "Schedule",
]
```

## Implementation Steps

1. **Create package structure**
   - Create `src/naq/models/` directory
   - Create empty files: `__init__.py`, `enums.py`, `jobs.py`, `events.py`, `schedules.py`

2. **Move enums first** (least dependencies)
   - Copy enum definitions to `enums.py`
   - Add proper imports and docstrings
   - Test imports work correctly

3. **Move event models** 
   - Copy `JobEvent` and `WorkerEvent` classes to `events.py`
   - Update imports to use `from .enums import ...`
   - Test all methods work correctly

4. **Move job models**
   - Copy `Job` and `JobResult` classes to `jobs.py`
   - Update imports for enums and events
   - Test all methods work correctly

5. **Move schedule model**
   - Copy `Schedule` class to `schedules.py`  
   - Update imports
   - Test methods work correctly

6. **Create public API**
   - Set up `__init__.py` with all exports
   - Test that imports from `naq.models` still work
   - Test that existing code using models continues to work

7. **Update dependent imports**
   - Find all files importing from `naq.models`
   - Update imports to still work with new structure
   - Run tests to ensure no breakage

8. **Clean up**
   - Remove original `models.py` file
   - Update any internal imports if needed

## Files to Update

### New Files
- `src/naq/models/__init__.py`
- `src/naq/models/enums.py`  
- `src/naq/models/jobs.py`
- `src/naq/models/events.py`
- `src/naq/models/schedules.py`

### Files to Remove
- `src/naq/models.py`

### Files Importing from models.py (to verify still work)
- `src/naq/__init__.py`
- `src/naq/events/__init__.py`
- `src/naq/worker.py`
- `src/naq/queue.py`
- `src/naq/scheduler.py`
- `src/naq/results.py`
- `src/naq/cli.py`

## Success Criteria

- [ ] All model classes successfully moved to focused modules
- [ ] Public API from `naq.models` unchanged (backward compatibility)
- [ ] All existing imports continue to work without changes
- [ ] All tests pass without modification
- [ ] Each new file has comprehensive docstrings
- [ ] No file exceeds 400 lines
- [ ] Import performance not degraded
- [ ] Type checking passes

## Testing Checklist

- [ ] `from naq.models import Job, JOB_STATUS, JobEvent` still works
- [ ] All Job methods and properties work correctly
- [ ] All event model factory methods work correctly  
- [ ] Enum values accessible as before
- [ ] Schedule model methods work correctly
- [ ] No circular import issues
- [ ] All downstream modules still import successfully

## Dependencies
- None (first task, foundational)

## Estimated Time
- **Implementation**: 4-6 hours
- **Testing**: 2-3 hours  
- **Total**: 6-9 hours