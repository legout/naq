# Subtask 02.4: Move Job Commands

## Overview
Extract job and queue management commands from the current `cli.py` file into `job_commands.py`.

## Current State
- Job commands are mixed in `src/naq/cli.py`
- Functions to move: `purge()`, `job_control()` (if exists)
- Queue management related functions are scattered

## Target State
- `src/naq/cli/job_commands.py` contains:
  - `job_app = typer.Typer(help="Job and queue management commands")`
  - `purge()` command function
  - `control()` command function (if exists)
  - Job and queue-related imports and utilities
  - Registration with main CLI app

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Identify job and queue-related functions and code
3. Extract `purge()` function
4. Extract `job_control()` function (if exists)
5. Extract job and queue-related imports and utilities
6. Create `job_app` typer instance
7. Set up command functions with proper decorators
8. Import shared utilities from `main.py`
9. Register with main app

## job_commands.py Structure
```python
import typer
from ..main import console  # Import shared utilities

job_app = typer.Typer(help="Job and queue management commands")

@job_app.command()
def purge(
    # ... parameters
):
    # Move existing purge() function logic here

@job_app.command("control")
def control(
    # ... parameters
):
    # Move existing job_control() function logic here
```

## Success Criteria
- [ ] Job commands moved to `job_commands.py`
- [ ] `job_app` typer instance created
- [ ] `purge()` command function properly implemented
- [ ] `control()` command function properly implemented (if exists)
- [ ] Job and queue-related imports and utilities moved
- [ ] Commands registered with main app
- [ ] No job-related code left in original `cli.py`

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)

## Estimated Time
- 1 hour