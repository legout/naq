# Subtask 02.8: Update Package Initialization

## Overview
Set up the CLI package initialization and ensure backward compatibility.

## Current State
- `src/naq/cli/__init__.py` is empty
- No exports defined
- Backward compatibility not considered

## Target State
- `src/naq/cli/__init__.py` contains:
  - Proper exports for main CLI app
  - Backward compatibility imports if needed
  - Package version and metadata
  - Clean import structure

## Implementation Steps
1. Read current `src/naq/cli/__init__.py` file
2. Determine what needs to be exported from the package
3. Check if `src/naq/__init__.py` imports CLI functions
4. Set up proper exports in `__init__.py`
5. Add backward compatibility imports if needed
6. Add package metadata
7. Test imports work correctly

## __init__.py Structure
```python
"""
NAQ CLI Package

A simple NATS-based queueing system, similar to RQ.
"""

from .main import app, console, main

__all__ = ["app", "console", "main"]

# For backward compatibility, if needed
try:
    from .worker_commands import worker_app
    from .job_commands import job_app
    from .scheduler_commands import scheduler_app
    from .event_commands import event_app
    from .system_commands import system_app
    
    __all__.extend([
        "worker_app", "job_app", "scheduler_app", 
        "event_app", "system_app"
    ])
except ImportError:
    pass
```

## Success Criteria
- [ ] `__init__.py` properly exports main CLI app
- [ ] Backward compatibility maintained if needed
- [ ] Package imports work correctly
- [ ] All command modules can be imported
- [ ] No circular imports
- [ ] Package metadata included

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)
- Subtask 02.3 (Move Worker Commands)
- Subtask 02.4 (Move Job Commands)
- Subtask 02.5 (Move Scheduler Commands)
- Subtask 02.6 (Move Event Commands)
- Subtask 02.7 (Move System Commands)

## Estimated Time
- 30 minutes