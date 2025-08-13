# Subtask 02.2: Extract Main CLI App

## Overview
Extract the main CLI application definition and shared utilities from the current `cli.py` file into `main.py`.

## Current State
- Main CLI app definition is in `src/naq/cli.py`
- Shared utilities are mixed with command implementations
- Version callback and main function are in the same file

## Target State
- `src/naq/cli/main.py` contains:
  - Main `app = typer.Typer(...)` definition
  - `version_callback()` function
  - `main()` function
  - Shared imports (typer, rich, console, etc.)
  - Common helper functions used across commands
  - Subcommand registration framework

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Extract main CLI app definition
3. Extract version callback function
4. Extract main function
5. Extract shared imports and utilities
6. Set up subcommand registration framework
7. Create basic structure in `main.py`

## main.py Structure
```python
import typer
from rich.console import Console

# Create main CLI app
app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Shared console instance
console = Console()

def version_callback(value: bool):
    # Move existing version callback

# Register subcommand groups
from .worker_commands import worker_app
from .job_commands import job_app
from .scheduler_commands import scheduler_app
from .event_commands import event_app
from .system_commands import system_app

app.add_typer(worker_app, name="worker")
app.add_typer(job_app, name="job")
# ... etc

def main():
    # Move existing main function
```

## Success Criteria
- [ ] Main CLI app definition moved to `main.py`
- [ ] Version callback function moved to `main.py`
- [ ] Main function moved to `main.py`
- [ ] Shared imports and utilities moved to `main.py`
- [ ] Subcommand registration framework set up
- [ ] `main.py` can be imported without errors

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)

## Estimated Time
- 1 hour