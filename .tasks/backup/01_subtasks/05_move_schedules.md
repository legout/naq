# Subtask 01.5: Move schedule model to schedules.py

## Overview
Extract Schedule class from models.py into a dedicated schedules.py module.

## Current State
- Schedule class is defined in `src/naq/models.py` (lines 566-599)
- Mixed with other model definitions

## Target State
- Schedule class moved to `src/naq/models/schedules.py`
- Imports updated to use enums from enums.py
- Comprehensive docstrings added

## Content to Move
- `Schedule` class (lines 566-599)

## Implementation Steps
1. Copy Schedule class from `models.py` to `schedules.py`
2. Update imports to use `from .enums import ...` for required enums
3. Add comprehensive docstrings for the class
4. Include usage examples in docstrings
5. Add `__all__` list for public API

## Success Criteria
- [ ] Schedule class successfully moved to `schedules.py`
- [ ] Imports correctly reference enums module
- [ ] Comprehensive docstrings added
- [ ] Usage examples included
- [ ] `__all__` list properly defined
- [ ] Schedule class can be imported from `naq.models.schedules`

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enums

## Estimated Time
- 45 minutes