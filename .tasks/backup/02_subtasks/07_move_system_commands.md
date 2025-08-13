# Subtask 02.7: Move System Commands

## Overview
Extract system and utility commands from the current `cli.py` file into `system_commands.py`.

## Current State
- System commands are mixed in `src/naq/cli.py`
- Functions to move: `dashboard()`, any other utility/system commands
- System-related utilities are scattered

## Target State
- `src/naq/cli/system_commands.py` contains:
  - `system_app = typer.Typer(help="System and utility commands")`
  - `dashboard()` command function
  - `version()` command function (if exists)
  - `config()` command function (if exists)
  - System-related imports and utilities
  - Registration with main CLI app

## Implementation Steps
1. Read current `src/naq/cli.py` file
2. Identify system-related functions and code
3. Extract `dashboard()` function
4. Extract any other system/utility functions
5. Extract system-related imports and utilities
6. Create `system_app` typer instance
7. Set up command functions with proper decorators
8. Import shared utilities from `main.py`
9. Register with main app

## system_commands.py Structure
```python
import typer
from ..main import console  # Import shared utilities

system_app = typer.Typer(help="System and utility commands")

@system_app.command()
def dashboard(
    # ... parameters
):
    # Move existing dashboard() function logic here

@system_app.command("version")
def version():
    # Move version display logic here (if separate from version_callback)

@system_app.command("config")
def config(
    # ... parameters
):
    # Move configuration utility logic here (if exists)
```

## Success Criteria
- [ ] System commands moved to `system_commands.py`
- [ ] `system_app` typer instance created
- [ ] `dashboard()` command function properly implemented
- [ ] Other system commands properly implemented (if exist)
- [ ] System-related imports and utilities moved
- [ ] Commands registered with main app
- [ ] No system-related code left in original `cli.py`

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)

## Estimated Time
- 1 hour