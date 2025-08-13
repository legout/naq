# Subtask 01.4: Move job models to jobs.py

## Overview
Extract Job and JobResult classes from models.py into a dedicated jobs.py module.

## Current State
- Job classes are defined in `src/naq/models.py` (lines 531-1040)
- Mixed with other model definitions

## Target State
- Job classes moved to `src/naq/models/jobs.py`
- Imports updated to use enums and events modules
- Comprehensive docstrings added

## Content to Move
- `Job` class (lines 608-1040)
- `JobResult` class (lines 531-565)
- `RetryDelayType` type hint (line 20)

## Implementation Steps
1. Copy job classes from `models.py` to `jobs.py`
2. Update imports to use `from .enums import ...` for required enums
3. Update imports to use `from .events import ...` for event type hints
4. Ensure all methods and properties work correctly
5. Add comprehensive docstrings for each class
6. Include usage examples in docstrings
7. Add `__all__` list for public API

## Success Criteria
- [ ] Job class successfully moved to `jobs.py`
- [ ] JobResult class successfully moved to `jobs.py`
- [ ] RetryDelayType type hint moved to `jobs.py`
- [ ] Imports correctly reference enums and events modules
- [ ] All methods and properties work correctly
- [ ] Comprehensive docstrings added
- [ ] Usage examples included
- [ ] `__all__` list properly defined
- [ ] Job classes can be imported from `naq.models.jobs`

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enums
- Subtask 01.3: Move events

## Estimated Time
- 2 hours