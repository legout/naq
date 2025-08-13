# Subtask 01.2: Move enum definitions to enums.py

## Overview
Extract all enum definitions from models.py into a dedicated enums.py module.

## Current State
- Enums are defined in `src/naq/models.py` (lines 23-65)
- Mixed with other model definitions

## Target State
- All enums moved to `src/naq/models/enums.py`
- Comprehensive docstrings added
- Type annotations included

## Content to Move
- `JOB_STATUS` (lines 23-34)
- `JobEventType` (lines 36-54) 
- `WorkerEventType` (lines 56-65)
- `RETRY_STRATEGY` import and `VALID_RETRY_STRATEGIES`

## Implementation Steps
1. Copy enum definitions from `models.py` to `enums.py`
2. Add comprehensive docstrings for each enum
3. Include usage examples in docstrings
4. Add type annotations where beneficial
5. Add necessary imports (e.g., `from enum import Enum`)
6. Add `__all__` list for public API

## Success Criteria
- [ ] All enums successfully moved to `enums.py`
- [ ] Each enum has comprehensive docstrings
- [ ] Usage examples included in docstrings
- [ ] Type annotations added where beneficial
- [ ] `__all__` list properly defined
- [ ] Enums can be imported from `naq.models.enums`

## Dependencies
- Subtask 01.1: Create package structure

## Estimated Time
- 1 hour