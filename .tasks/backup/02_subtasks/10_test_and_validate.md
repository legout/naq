# Subtask 02.10: Test and Validate

## Overview
Test all CLI commands to ensure they work correctly with the new package structure.

## Current State
- CLI commands have been moved to separate modules
- Entry points have been updated
- Need to verify all functionality works as expected

## Target State
- All CLI commands work correctly
- Help system shows organized command groups
- All existing command functionality preserved
- All CLI tests pass

## Implementation Steps
1. Test basic CLI functionality
2. Test help system
3. Test each command group
4. Test individual commands
5. Test error handling
6. Test rich formatting
7. Run existing CLI tests
8. Fix any issues found

## Testing Checklist

### Basic CLI Tests
- [ ] `naq --help` shows all command groups
- [ ] `naq --version` works correctly
- [ ] CLI can be imported without errors

### Worker Command Tests
- [ ] `naq worker --help` shows worker commands
- [ ] `naq worker start --help` shows correct parameters
- [ ] `naq worker list --help` shows correct parameters
- [ ] Worker commands execute without errors

### Job Command Tests
- [ ] `naq job --help` shows job commands
- [ ] `naq job purge --help` shows correct parameters
- [ ] `naq job control --help` shows correct parameters (if exists)
- [ ] Job commands execute without errors

### Scheduler Command Tests
- [ ] `naq scheduler --help` shows scheduler commands
- [ ] `naq scheduler start --help` shows correct parameters
- [ ] `naq scheduler jobs --help` shows correct parameters
- [ ] Scheduler commands execute without errors

### Event Command Tests
- [ ] `naq events --help` shows event commands
- [ ] `naq events stream --help` shows correct parameters
- [ ] `naq events history --help` shows correct parameters
- [ ] `naq events stats --help` shows correct parameters
- [ ] `naq events workers --help` shows correct parameters
- [ ] Event commands execute without errors

### System Command Tests
- [ ] `naq system --help` shows system commands
- [ ] `naq system dashboard --help` shows correct parameters
- [ ] `naq system version --help` shows correct parameters (if exists)
- [ ] `naq system config --help` shows correct parameters (if exists)
- [ ] System commands execute without errors

### Integration Tests
- [ ] All command parameters work as before
- [ ] Error handling works correctly
- [ ] Rich formatting displays properly
- [ ] No circular imports
- [ ] All existing CLI tests pass

## Success Criteria
- [ ] All CLI commands work correctly
- [ ] Help system shows organized command groups
- [ ] All existing command functionality preserved
- [ ] No individual file exceeds 400 lines
- [ ] Shared code appropriately centralized
- [ ] Entry points working correctly
- [ ] All CLI tests pass
- [ ] Error handling works correctly
- [ ] Rich formatting displays properly

## Dependencies
- All previous subtasks (02.1 through 02.9)

## Estimated Time
- 3-4 hours