# Subtask 04.01: Create Worker Package Structure

## Overview
Create the basic directory structure and files for the new worker package to support the split of the large `worker.py` file into focused modules.

## Objectives
- Create the worker package directory structure
- Set up basic module files with proper imports
- Prepare for extraction of manager classes and core worker functionality

## Implementation Steps

### 1. Create Package Directory
```bash
mkdir -p src/naq/worker
```

### 2. Create Module Files
Create the following empty module files:
- `src/naq/worker/__init__.py` - Public API exports
- `src/naq/worker/core.py` - Main Worker class
- `src/naq/worker/status.py` - WorkerStatusManager class
- `src/naq/worker/jobs.py` - JobStatusManager class
- `src/naq/worker/failed.py` - FailedJobHandler class

### 3. Set Up Basic Imports
Add initial import structure to prepare for class extraction:
- `__init__.py` should import from the respective modules
- Each module should have proper imports from other naq modules
- Ensure no circular imports are introduced

### 4. Verify Structure
Confirm the following structure exists:
```
src/naq/worker/
├── __init__.py
├── core.py
├── status.py
├── jobs.py
└── failed.py
```

## Success Criteria
- [ ] Worker package directory structure created
- [ ] All module files exist with proper basic imports
- [ ] No syntax errors in the basic structure
- [ ] Ready for class extraction in subsequent subtasks

## Dependencies
- **Depends on**: Task 01 (Split Models) - for updated model imports
- **Prepares for**: Subtasks 04.02-04.06 (class extraction and integration)

## Estimated Time
- **Implementation**: 30 minutes
- **Testing**: 15 minutes
- **Total**: 45 minutes