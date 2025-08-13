# Subtask 01.3: Move event models to events.py

## Overview
Extract JobEvent and WorkerEvent classes from models.py into a dedicated events.py module.

## Current State
- Event classes are defined in `src/naq/models.py` (lines 67-529)
- Mixed with other model definitions

## Target State
- Event classes moved to `src/naq/models/events.py`
- Imports updated to use enums from enums.py
- Comprehensive docstrings added

## Content to Move
- `JobEvent` class (lines 67-341)
- `WorkerEvent` class (lines 343-529)

## Implementation Steps
1. Copy event classes from `models.py` to `events.py`
2. Update imports to use `from .enums import JobEventType, WorkerEventType`
3. Ensure all class methods work correctly
4. Add comprehensive docstrings for each class
5. Include usage examples in docstrings
6. Add `__all__` list for public API

## Success Criteria
- [ ] JobEvent class successfully moved to `events.py`
- [ ] WorkerEvent class successfully moved to `events.py`
- [ ] Imports correctly reference enums module
- [ ] All class methods work correctly
- [ ] Comprehensive docstrings added
- [ ] Usage examples included
- [ ] `__all__` list properly defined
- [ ] Event classes can be imported from `naq.models.events`

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enums

## Estimated Time
- 1.5 hours