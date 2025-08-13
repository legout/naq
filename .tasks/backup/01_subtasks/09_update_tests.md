# Subtask 01.9: Update Tests

## Overview
Update all relevant tests in the `tests` directory to work with the new models package structure.

## Current State
- Tests likely import from `src/naq/models.py`
- Test references to model classes may be broken
- Test structure may not match new module organization
- Model-related tests may fail

## Target State
- All tests updated to work with new models structure
- Test imports updated to point to new locations
- Model-related tests pass
- Test coverage maintained or improved
- Documentation examples in tests updated

## Implementation Steps
1. Analyze current test structure in `tests/` directory
2. Identify tests that import from or test model functionality
3. Update import statements to use new package structure
4. Update test references to model classes and enums
5. Update model integration tests to work with new modules
6. Update any documentation examples in tests
7. Run tests to identify failures
8. Fix any issues found

## Files to Update

### Test Files to Check
- `tests/unit/test_unit_job.py` - Job model tests
- `tests/unit/test_unit_queue.py` - Queue model tests
- `tests/unit/test_unit_worker.py` - Worker model tests
- `tests/integration/test_integration_job.py` - Job integration tests
- `tests/integration/test_integration_worker.py` - Worker integration tests
- `tests/scenario/test_scenario_job.py` - Job scenario tests
- `tests/scenario/test_scenario_worker.py` - Worker scenario tests
- `tests/conftest.py` - Test configuration with model imports
- Any other test files that import model functionality

### Import Updates Needed
```python
# Old imports (to be updated)
from naq.models import Job, JobStatus, JobEventType
from naq.models import ScheduledJob, RETRY_STRATEGY
# ... etc

# New imports
from naq.models.jobs import Job
from naq.models.enums import JobStatus, JobEventType, RETRY_STRATEGY
from naq.models.schedules import ScheduledJob
# ... etc
```

### Test Reference Updates
```python
# Old test references (to be updated)
def test_job_creation():
    job = Job(...)
    assert job.status == JobStatus.QUEUED
    
def test_enum_values():
    assert JobEventType.JOB_QUEUED.value == "job_queued"

# New test references
def test_job_creation():
    from naq.models.jobs import Job
    from naq.models.enums import JobStatus
    job = Job(...)
    assert job.status == JobStatus.QUEUED
    
def test_enum_values():
    from naq.models.enums import JobEventType
    assert JobEventType.JOB_QUEUED.value == "job_queued"
```

## Success Criteria
- [ ] All test imports updated to new package structure
- [ ] Model class references updated in tests
- [ ] Enum references updated in tests
- [ ] Model integration tests pass
- [ ] Test coverage maintained
- [ ] Documentation examples in tests updated
- [ ] No broken test references
- [ ] All existing test functionality preserved

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enum definitions to enums.py
- Subtask 01.3: Move event models to events.py
- Subtask 01.4: Move job models to jobs.py
- Subtask 01.5: Move schedule model to schedules.py
- Subtask 01.6: Create public API in __init__.py
- Subtask 01.7: Update dependent imports
- Subtask 01.8: Cleanup and final verification

## Estimated Time
- 1.5-2 hours