# Subtask 03.7: Update Dependencies

## Overview
Update all files that import from `naq.queue` to work with the new package structure.

## Current State
- Files import from `src/naq/queue.py`
- Old queue file still exists
- Dependencies need to be updated to work with new package structure

## Target State
- All files updated to import from new queue package structure
- Old `queue.py` file can be safely removed
- All imports continue to work correctly

## Implementation Steps
1. Identify all files that import from `naq.queue`
2. Update imports in `src/naq/__init__.py`
3. Update imports in `src/naq/scheduler.py`
4. Update imports in `src/naq/worker.py`
5. Update imports in CLI command files
6. Update any other files that import queue functionality
7. Test that all imports work correctly
8. Remove old `src/naq/queue.py` file
9. Verify all functionality still works

## Files to Update

### Core Files
- `src/naq/__init__.py` - Main package imports
- `src/naq/scheduler.py` - Scheduler imports
- `src/naq/worker.py` - Worker imports

### CLI Files
- `src/naq/cli/main.py` - CLI app imports
- `src/naq/cli/worker_commands.py` - Worker command imports
- `src/naq/cli/job_commands.py` - Job command imports
- `src/naq/cli/scheduler_commands.py` - Scheduler command imports
- `src/naq/cli/event_commands.py` - Event command imports
- `src/naq/cli/system_commands.py` - System command imports

### Other Files
- Any other files that import from `naq.queue`

## Import Updates Needed
```python
# Old imports (should still work due to backward compatibility)
from naq.queue import Queue, enqueue, enqueue_sync

# New imports (more specific)
from naq.queue.core import Queue
from naq.queue.async_api import enqueue
from naq.queue.sync_api import enqueue_sync
from naq.queue.scheduled import ScheduledJobManager
```

## Success Criteria
- [ ] All files that import from `naq.queue` updated
- [ ] Old imports still work (backward compatibility)
- [ ] New specific imports work correctly
- [ ] Old `queue.py` file removed
- [ ] No broken references to old queue file
- [ ] All existing functionality preserved
- [ ] Integration with worker and scheduler unchanged

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.2 (Extract ScheduledJobManager)
- Subtask 03.3 (Extract Core Queue Class)
- Subtask 03.4 (Extract Async API)
- Subtask 03.5 (Extract Sync API)
- Subtask 03.6 (Create Public API)

## Estimated Time
- 1 hour