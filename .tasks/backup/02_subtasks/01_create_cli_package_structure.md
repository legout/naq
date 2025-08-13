# Subtask 02.1: Create CLI Package Structure

## Overview
Create the basic package directory structure for the new CLI module.

## Current State
- Single file `src/naq/cli.py` contains all CLI commands
- No package structure exists

## Target State
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

## Implementation Steps
1. Create `src/naq/cli/` directory
2. Create empty `__init__.py` file
3. Create empty `main.py` file
4. Create empty `worker_commands.py` file
5. Create empty `job_commands.py` file
6. Create empty `scheduler_commands.py` file
7. Create empty `event_commands.py` file
8. Create empty `system_commands.py` file

## Success Criteria
- [ ] `src/naq/cli/` directory exists
- [ ] All 7 empty files are created
- [ ] Package can be imported (`import naq.cli`)

## Dependencies
- None

## Estimated Time
- 15 minutes