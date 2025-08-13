# Subtask 02.5: Move Scheduler Commands

## Overview
Extract scheduler and scheduled job commands from the current `cli.py` file into `scheduler_commands.py`.

## Current State
- Scheduler commands are mixed in `src/naq/cli.py`
- Functions to move: `scheduler()`, `list_scheduled_jobs()`
- Scheduler-related utilities are scattered

## Target State
- `src/naq/cli/scheduler_commands.py` contains:
  - `scheduler_app = typer.Typer(help="Scheduler and scheduled job commands")`
  - `start()` command function (moved from `scheduler()`)
  - `jobs()` command function (moved from `list_scheduled_jobs()`)
  - Scheduler-related imports and utilities
  - Registration with main CLI app

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Identify scheduler-related functions and code
3. Extract `scheduler()` function and rename to `start()`
4. Extract `list_scheduled_jobs()` function and rename to `jobs()`
5. Extract scheduler-related imports and utilities
6. Create `scheduler_app` typer instance
7. Set up command functions with proper decorators
8. Import shared utilities from `main.py`
9. Register with main app

## scheduler_commands.py Structure
```python
import typer
from ..main import console  # Import shared utilities

scheduler_app = typer.Typer(help="Scheduler and scheduled job commands")

@scheduler_app.command()
def start(
    # ... parameters
):
    # Move existing scheduler() function logic here

@scheduler_app.command("jobs")
def jobs(
    # ... parameters
):
    # Move existing list_scheduled_jobs() function logic here
```

## Success Criteria
- [ ] Scheduler commands moved to `scheduler_commands.py`
- [ ] `scheduler_app` typer instance created
- [ ] `start()` command function properly implemented
- [ ] `jobs()` command function properly implemented
- [ ] Scheduler-related imports and utilities moved
- [ ] Commands registered with main app
- [ ] No scheduler-related code left in original `cli.py`

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)

## Estimated Time
- 1 hour