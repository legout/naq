# Subtask 03.9: Update Tests

## Overview
Update all relevant tests in the `tests` directory to work with the new queue package structure.

## Current State
- Tests likely import from `src/naq/queue.py`
- Test references to queue classes and functions may be broken
- Test structure may not match new module organization
- Queue-related tests may fail

## Target State
- All tests updated to work with new queue structure
- Test imports updated to point to new locations
- Queue-related tests pass
- Test coverage maintained or improved
- Documentation examples in tests updated

## Implementation Steps
1. Analyze current test structure in `tests/` directory
2. Identify tests that import from or test queue functionality
3. Update import statements to use new package structure
4. Update test references to queue classes and functions
5. Update queue integration tests to work with new modules
6. Update any documentation examples in tests
7. Run tests to identify failures
8. Fix any issues found

## Files to Update

### Test Files to Check
- `tests/unit/test_unit_queue.py` - Queue unit tests
- `tests/integration/test_integration_job.py` - Job integration tests
- `tests/integration/test_integration_worker.py` - Worker integration tests
- `tests/scenario/test_scenario_job.py` - Job scenario tests
- `tests/scenario/test_scenario_worker.py` - Worker scenario tests
- `tests/conftest.py` - Test configuration with queue imports
- Any other test files that import queue functionality

### Import Updates Needed
```python
# Old imports (to be updated)
from naq.queue import Queue, enqueue, enqueue_sync
from naq.queue import ScheduledJobManager
# ... etc

# New imports (backward compatible)
from naq.queue import Queue, enqueue, enqueue_sync
from naq.queue import ScheduledJobManager

# New imports (more specific)
from naq.queue.core import Queue
from naq.queue.async_api import enqueue
from naq.queue.sync_api import enqueue_sync
from naq.queue.scheduled import ScheduledJobManager
```

### Test Reference Updates
```python
# Old test references (to be updated)
def test_queue_creation():
    queue = Queue(settings)
    job_id = await enqueue(test_func)
    
def test_scheduled_jobs():
    manager = ScheduledJobManager(settings)
    # test scheduled job logic

# New test references
def test_queue_creation():
    from naq.queue.core import Queue
    from naq.queue.async_api import enqueue
    queue = Queue(settings)
    job_id = await enqueue(test_func)
    
def test_scheduled_jobs():
    from naq.queue.scheduled import ScheduledJobManager
    manager = ScheduledJobManager(settings)
    # test scheduled job logic
```

## Success Criteria
- [ ] All test imports updated to new package structure
- [ ] Queue class references updated in tests
- [ ] Async API function references updated in tests
- [ ] Sync API function references updated in tests
- [ ] ScheduledJobManager references updated in tests
- [ ] Queue integration tests pass
- [ ] Test coverage maintained
- [ ] Documentation examples in tests updated
- [ ] No broken test references
- [ ] All existing test functionality preserved

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.2 (Extract ScheduledJobManager)
- Subtask 03.3 (Extract Core Queue Class)
- Subtask 03.4 (Extract Async API)
- Subtask 03.5 (Extract Sync API)
- Subtask 03.6 (Create Public API)
- Subtask 03.7 (Update Dependencies)
- Subtask 03.8 (Test and Validate)

## Estimated Time
- 2-3 hours