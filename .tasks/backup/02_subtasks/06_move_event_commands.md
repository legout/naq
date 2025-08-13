# Subtask 02.6: Move Event Commands

## Overview
Extract event monitoring commands from the current `cli.py` file into `event_commands.py`.

## Current State
- Event commands are mixed in `src/naq/cli.py`
- Functions to move: `events()`, `event_history()`, `event_stats()`, `worker_events()`
- Event-related imports and utilities are scattered

## Target State
- `src/naq/cli/event_commands.py` contains:
  - `event_app = typer.Typer(help="Event monitoring commands")`
  - `stream()` command function (moved from `events()`)
  - `history()` command function (moved from `event_history()`)
  - `stats()` command function (moved from `event_stats()`)
  - `workers()` command function (moved from `worker_events()`)
  - Event-related imports and utilities
  - Rich formatting for event display
  - Registration with main CLI app

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Identify event-related functions and code
3. Extract `events()` function and rename to `stream()`
4. Extract `event_history()` function and rename to `history()`
5. Extract `event_stats()` function and rename to `stats()`
6. Extract `worker_events()` function and rename to `workers()`
7. Extract event-related imports and utilities
8. Create `event_app` typer instance
9. Set up command functions with proper decorators
10. Import shared utilities from `main.py`
11. Register with main app

## event_commands.py Structure
```python
import typer
from ..main import console  # Import shared utilities

event_app = typer.Typer(help="Event monitoring commands")

@event_app.command()
def stream(
    # ... parameters
):
    # Move existing events() function logic here

@event_app.command("history")
def history(
    # ... parameters
):
    # Move existing event_history() function logic here

@event_app.command("stats")
def stats(
    # ... parameters
):
    # Move existing event_stats() function logic here

@event_app.command("workers")
def workers(
    # ... parameters
):
    # Move existing worker_events() function logic here
```

## Success Criteria
- [ ] Event commands moved to `event_commands.py`
- [ ] `event_app` typer instance created
- [ ] `stream()` command function properly implemented
- [ ] `history()` command function properly implemented
- [ ] `stats()` command function properly implemented
- [ ] `workers()` command function properly implemented
- [ ] Event-related imports and utilities moved
- [ ] Rich formatting preserved
- [ ] Commands registered with main app
- [ ] No event-related code left in original `cli.py`

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)

## Estimated Time
- 2 hours