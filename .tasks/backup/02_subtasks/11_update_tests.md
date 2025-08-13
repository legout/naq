# Subtask 02.11: Update Tests

## Overview
Update all relevant tests in the `tests` directory to work with the new CLI package structure.

## Current State
- Tests likely import from `src/naq/cli.py`
- Test references to CLI functions may be broken
- Test structure may not match new command organization
- CLI integration tests may fail

## Target State
- All tests updated to work with new CLI structure
- Test imports updated to point to new locations
- CLI integration tests pass
- Test coverage maintained or improved
- Documentation examples updated

## Implementation Steps
1. Analyze current test structure in `tests/` directory
2. Identify tests that import from or test CLI functionality
3. Update import statements to use new package structure
4. Update test references to CLI functions and commands
5. Update CLI integration tests to work with new command groups
6. Update any documentation examples in tests
7. Run tests to identify failures
8. Fix any issues found

## Files to Update

### Test Files to Check
- `tests/unit/test_unit_cli.py` (if exists)
- `tests/integration/test_integration_cli.py` (if exists)
- `tests/smoke/test_smoke_cli.py` (if exists)
- Any other test files that import CLI functionality

### Import Updates Needed
```python
# Old imports (to be updated)
from naq.cli import app, main
from naq.cli import worker, list_workers_command
from naq.cli import purge, job_control
# ... etc

# New imports
from naq.cli.main import app, main
from naq.cli.worker_commands import worker_app
from naq.cli.job_commands import job_app
# ... etc
```

### Test Reference Updates
```python
# Old test references (to be updated)
def test_worker_command():
    # Test worker() function directly
    
def test_cli_help():
    # Test help output with old structure

# New test references
def test_worker_command():
    # Test worker_app.commands['start']
    
def test_cli_help():
    # Test help output with new command groups
```

## Success Criteria
- [ ] All test imports updated to new package structure
- [ ] CLI function references updated in tests
- [ ] CLI integration tests pass
- [ ] Test coverage maintained
- [ ] Documentation examples in tests updated
- [ ] No broken test references
- [ ] All existing test functionality preserved

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)
- Subtask 02.3 (Move Worker Commands)
- Subtask 02.4 (Move Job Commands)
- Subtask 02.5 (Move Scheduler Commands)
- Subtask 02.6 (Move Event Commands)
- Subtask 02.7 (Move System Commands)
- Subtask 02.8 (Update Package Initialization)
- Subtask 02.9 (Update Entry Points)

## Estimated Time
- 2-3 hours