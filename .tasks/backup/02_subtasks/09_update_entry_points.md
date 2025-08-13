# Subtask 02.9: Update Entry Points

## Overview
Update the entry points in `pyproject.toml` to point to the new CLI location.

## Current State
- Entry points in `pyproject.toml` point to `src/naq/cli.py`
- Old CLI file still exists
- Entry points need to be updated to work with new package structure

## Target State
- `pyproject.toml` entry points updated to point to `src/naq/cli/main.py`
- CLI works from command line with new structure
- Old CLI file can be safely removed

## Implementation Steps
1. Read current `pyproject.toml` file
2. Identify CLI entry points
3. Update entry points to point to new location
4. Test CLI works from command line
5. Remove old `src/naq/cli.py` file
6. Verify all commands work correctly

## Entry Points Update
```toml
[project.scripts]
naq = "naq.cli.main:main"
```

## Success Criteria
- [ ] Entry points updated in `pyproject.toml`
- [ ] CLI works from command line (`naq --help`)
- [ ] All command groups show correctly
- [ ] Old `cli.py` file removed
- [ ] No broken references to old CLI file
- [ ] All existing functionality preserved

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)
- Subtask 02.3 (Move Worker Commands)
- Subtask 02.4 (Move Job Commands)
- Subtask 02.5 (Move Scheduler Commands)
- Subtask 02.6 (Move Event Commands)
- Subtask 02.7 (Move System Commands)
- Subtask 02.8 (Update Package Initialization)

## Estimated Time
- 1 hour