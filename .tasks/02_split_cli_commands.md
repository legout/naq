# Task 02: Split CLI Commands into Focused Modules

## Overview
Split the large `cli.py` file (1220 lines) into focused command modules to improve maintainability and logical organization of CLI functionality.

## Current State
- Single file `src/naq/cli.py` contains all CLI commands
- Mixed concerns: worker, job, scheduler, event, system commands
- 1220 lines with 12+ different command functions

## Target Structure
```
src/naq/cli/
├── __init__.py              # CLI package initialization
├── main.py                  # Main CLI app, shared utilities, version
├── worker_commands.py       # Worker management commands
├── job_commands.py          # Job control and queue management
├── scheduler_commands.py    # Scheduler and scheduled job commands
├── event_commands.py        # Event monitoring commands
└── system_commands.py       # Dashboard, utility commands
```

## Detailed Implementation

### 1. main.py - Main CLI App & Shared Utilities
**Content to move:**
- Main `app = typer.Typer(...)` definition
- `version_callback()` function
- `main()` function 
- Shared imports (typer, rich, console, etc.)
- Common helper functions used across commands

**Requirements:**
- Create main CLI app with subcommand registration
- Provide shared utilities for other command modules
- Handle version display and global options

### 2. worker_commands.py - Worker Management
**Content to move:**
- `worker()` command function
- `list_workers_command()` function
- Worker-related imports and utilities

**Requirements:**
- Register with main CLI app
- Handle worker start, monitoring, configuration
- Worker status and health commands

### 3. job_commands.py - Job & Queue Management  
**Content to move:**
- `purge()` command function
- `job_control()` command function (if exists)
- Queue management related functions

**Requirements:**
- Register with main CLI app
- Handle job operations, queue purging
- Job status and control commands

### 4. scheduler_commands.py - Scheduler & Scheduled Jobs
**Content to move:**
- `scheduler()` command function
- `list_scheduled_jobs()` function
- Scheduler-related utilities

**Requirements:**
- Register with main CLI app  
- Handle scheduler start/stop
- Scheduled job management and listing

### 5. event_commands.py - Event Monitoring
**Content to move:**
- `events()` command function
- `event_history()` function
- `event_stats()` function  
- `worker_events()` function
- Event-related imports and utilities

**Requirements:**
- Register with main CLI app
- Handle all event monitoring functionality
- Rich formatting for event display

### 6. system_commands.py - System & Utility Commands
**Content to move:**
- `dashboard()` command function
- Any other utility/system commands

**Requirements:**
- Register with main CLI app
- Handle dashboard, configuration, system utilities

## Implementation Strategy

### Step 1: Create Package Structure
```bash
mkdir -p src/naq/cli
touch src/naq/cli/__init__.py
touch src/naq/cli/main.py
touch src/naq/cli/worker_commands.py
touch src/naq/cli/job_commands.py  
touch src/naq/cli/scheduler_commands.py
touch src/naq/cli/event_commands.py
touch src/naq/cli/system_commands.py
```

### Step 2: Extract Shared Code to main.py
**main.py structure:**
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

### Step 3: Create Command Modules

**worker_commands.py structure:**
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

**Similar structure for other command modules**

### Step 4: Update Imports
- Update `src/naq/__init__.py` if it imports CLI functions
- Update entry points in `pyproject.toml` to point to new CLI location
- Update any external references to CLI functions

## Files to Create/Modify

### New Files
- `src/naq/cli/__init__.py`
- `src/naq/cli/main.py`  
- `src/naq/cli/worker_commands.py`
- `src/naq/cli/job_commands.py`
- `src/naq/cli/scheduler_commands.py`
- `src/naq/cli/event_commands.py`
- `src/naq/cli/system_commands.py`

### Files to Remove  
- `src/naq/cli.py`

### Files to Update
- `src/naq/__init__.py` (if importing CLI functions)
- `pyproject.toml` (entry points)

## Implementation Steps

1. **Create package structure**
   - Create `src/naq/cli/` directory and files
   - Set up basic structure in each file

2. **Extract main CLI app**
   - Move main app definition to `main.py`
   - Move shared utilities and imports
   - Set up subcommand registration framework

3. **Move worker commands**
   - Extract worker and list_workers functions
   - Create `worker_app` typer instance
   - Register with main app

4. **Move job commands**
   - Extract purge and job control functions  
   - Create `job_app` typer instance
   - Register with main app

5. **Move scheduler commands**
   - Extract scheduler and list_scheduled_jobs functions
   - Create `scheduler_app` typer instance
   - Register with main app

6. **Move event commands**
   - Extract all event monitoring functions
   - Create `event_app` typer instance  
   - Register with main app

7. **Move system commands**
   - Extract dashboard and utility functions
   - Create `system_app` typer instance
   - Register with main app

8. **Update package initialization**  
   - Set up `__init__.py` to export main app
   - Ensure backward compatibility if needed

9. **Update entry points**
   - Update `pyproject.toml` entry points
   - Test CLI works from command line

10. **Test and validate**
    - Test all commands work correctly
    - Test help system works
    - Test command grouping is intuitive

## Command Organization

### Worker Commands (`naq worker`)
- `naq worker start` - Start worker processes
- `naq worker list` - List active workers  
- `naq worker status` - Worker status overview

### Job Commands (`naq job`)
- `naq job purge` - Purge queues
- `naq job control` - Job control operations

### Scheduler Commands (`naq scheduler`)
- `naq scheduler start` - Start scheduler
- `naq scheduler jobs` - List scheduled jobs

### Event Commands (`naq events`)
- `naq events stream` - Real-time event streaming
- `naq events history` - Historical event queries
- `naq events stats` - Event statistics
- `naq events workers` - Worker event monitoring

### System Commands (`naq system`)
- `naq system dashboard` - Launch dashboard
- `naq system version` - Show version
- `naq system config` - Configuration utilities

## Success Criteria

- [ ] All CLI commands successfully moved to focused modules
- [ ] Command line interface works identically to before
- [ ] Help system shows organized command groups
- [ ] All existing command functionality preserved
- [ ] No individual file exceeds 400 lines
- [ ] Shared code appropriately centralized
- [ ] Entry points updated and working
- [ ] All CLI tests pass

## Testing Checklist

- [ ] `naq --help` shows all command groups
- [ ] `naq worker start` works correctly
- [ ] `naq events stream` works correctly  
- [ ] `naq scheduler start` works correctly
- [ ] All command parameters work as before
- [ ] Error handling works correctly
- [ ] Rich formatting displays properly

## Dependencies
- **Depends on**: Task 01 (Split Models) - for updated model imports
- **Blocks**: None - can be done in parallel with other tasks

## Estimated Time
- **Implementation**: 6-8 hours
- **Testing**: 3-4 hours
- **Total**: 9-12 hours