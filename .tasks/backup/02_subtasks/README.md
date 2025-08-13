# CLI Commands Split Subtasks

This directory contains the subtasks for splitting the CLI commands into focused modules.

## Subtasks Overview

### 02.1: Create CLI Package Structure
- Create the basic package directory structure
- Set up empty files for all CLI modules

### 02.2: Extract Main CLI App
- Move main CLI app definition to `main.py`
- Extract shared utilities and imports
- Set up subcommand registration framework

### 02.3: Move Worker Commands
- Extract worker management commands
- Create `worker_app` typer instance
- Register with main app

### 02.4: Move Job Commands
- Extract job and queue management commands
- Create `job_app` typer instance
- Register with main app

### 02.5: Move Scheduler Commands
- Extract scheduler and scheduled job commands
- Create `scheduler_app` typer instance
- Register with main app

### 02.6: Move Event Commands
- Extract event monitoring commands
- Create `event_app` typer instance
- Register with main app

### 02.7: Move System Commands
- Extract system and utility commands
- Create `system_app` typer instance
- Register with main app

### 02.8: Update Package Initialization
- Set up `__init__.py` exports
- Ensure backward compatibility
- Test imports work correctly

### 02.9: Update Entry Points
- Update `pyproject.toml` entry points
- Test CLI works from command line
- Remove old CLI file

### 02.10: Test and Validate
- Test all CLI commands work correctly
- Verify help system and command groups
- Run existing CLI tests

### 02.11: Update Tests
- Update test imports to new package structure
- Fix CLI integration tests
- Update test references and examples

### 02.12: Update Documentation
- Update quarto documentation files
- Update command examples and syntax
- Update API documentation and imports

## Execution Order

Subtasks should be executed in numerical order as each depends on the previous ones:

1. 02.1 - Create CLI Package Structure
2. 02.2 - Extract Main CLI App
3. 02.3 - Move Worker Commands
4. 02.4 - Move Job Commands
5. 02.5 - Move Scheduler Commands
6. 02.6 - Move Event Commands
7. 02.7 - Move System Commands
8. 02.8 - Update Package Initialization
9. 02.9 - Update Entry Points
10. 02.10 - Test and Validate
11. 02.11 - Update Tests
12. 02.12 - Update Documentation

## Total Estimated Time

- **Implementation**: 9-10 hours
- **Testing**: 3-4 hours
- **Test Updates**: 2-3 hours
- **Documentation Updates**: 2-3 hours
- **Total**: 16-20 hours