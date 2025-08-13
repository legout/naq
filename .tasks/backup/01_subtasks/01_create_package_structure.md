# Subtask 01.1: Create models package structure

## Overview
Create the basic package directory structure for the new models module.

## Current State
- Single file `src/naq/models.py` contains all model definitions
- No package structure exists

## Target State
```
src/naq/models/
├── __init__.py         # Empty file for package initialization
├── enums.py           # Empty file for enum definitions
├── jobs.py            # Empty file for job models
├── events.py          # Empty file for event models
└── schedules.py       # Empty file for schedule model
```

## Implementation Steps
1. Create `src/naq/models/` directory
2. Create empty `__init__.py` file
3. Create empty `enums.py` file
4. Create empty `jobs.py` file
5. Create empty `events.py` file
6. Create empty `schedules.py` file

## Success Criteria
- [ ] `src/naq/models/` directory exists
- [ ] All 5 empty files are created
- [ ] Package can be imported (`import naq.models`)

## Dependencies
- None

## Estimated Time
- 15 minutes