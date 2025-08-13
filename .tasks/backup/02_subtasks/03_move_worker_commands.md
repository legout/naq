# Subtask 02.3: Move Worker Commands

## Overview
Extract worker-related commands from the current `cli.py` file into `worker_commands.py`.

## Current State
- Worker commands are mixed in `src/naq/cli.py`
- Functions to move: `worker()`, `list_workers_command()`
- Worker-related imports and utilities are scattered

## Target State
- `src/naq/cli/worker_commands.py` contains:
  - `worker_app = typer.Typer(help="Worker management commands")`
  - `start()` command function (moved from `worker()`)
  - `list_workers()` command function (moved from `list_workers_command()`)
  - Worker-related imports and utilities
  - Registration with main CLI app

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Identify worker-related functions and code
3. Extract `worker()` function and rename to `start()`
4. Extract `list_workers_command()` function and rename to `list_workers()`
5. Extract worker-related imports and utilities
6. Create `worker_app` typer instance
7. Set up command functions with proper decorators
8. Import shared utilities from `main.py`
9. Register with main app

## worker_commands.py Structure
```python
import typer
from ..main import console  # Import shared utilities

worker_app = typer.Typer(help="Worker management commands")

@worker_app.command()
def start(
    queues: List[str] = typer.Argument(...),
    # ... other parameters
):
    # Move existing worker() function logic here

@worker_app.command("list")
def list_workers(
    # ... parameters
):
    # Move existing list_workers_command() logic here
```

## Success Criteria
- [ ] Worker commands moved to `worker_commands.py`
- [ ] `worker_app` typer instance created
- [ ] `start()` command function properly implemented
- [ ] `list_workers()` command function properly implemented
- [ ] Worker-related imports and utilities moved
- [ ] Commands registered with main app
- [ ] No worker-related code left in original `cli.py`

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)

## Estimated Time
- 1.5 hours