# Subtask 03.1: Create Queue Package Structure

## Overview
Create the basic package directory structure for the new queue module.

## Current State
- Single file `src/naq/queue.py` contains all queue-related functionality
- No package structure exists

## Target State
```
src/naq/queue/
├── __init__.py         # Public API exports (backward compatibility)
├── core.py            # Main Queue class
├── scheduled.py       # ScheduledJobManager and schedule operations
├── async_api.py       # Async functions (enqueue, enqueue_at, etc.)
└── sync_api.py        # Sync wrapper functions
```

## Implementation Steps
1. Create `src/naq/queue/` directory
2. Create empty `__init__.py` file
3. Create empty `core.py` file
4. Create empty `scheduled.py` file
5. Create empty `async_api.py` file
6. Create empty `sync_api.py` file

## Success Criteria
- [ ] `src/naq/queue/` directory exists
- [ ] All 5 empty files are created
- [ ] Package can be imported (`import naq.queue`)

## Dependencies
- None

## Estimated Time
- 15 minutes