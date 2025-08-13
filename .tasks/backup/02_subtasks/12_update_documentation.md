# Subtask 02.12: Update Documentation

## Overview
Update all documentation in the `docs` directory (managed by quarto) to reflect the new CLI package structure and command organization.

## Current State
- Documentation references old CLI structure
- Command examples use old command syntax
- API documentation may reference old CLI imports
- Quarto files need updates for new command groups

## Target State
- All documentation updated to reflect new CLI structure
- Command examples use new command syntax
- API documentation updated with new imports
- Quarto files regenerated with correct information
- Documentation builds successfully

## Implementation Steps
1. Analyze current documentation structure in `docs/` directory
2. Identify files that reference CLI functionality
3. Update command examples to use new syntax
4. Update API documentation with new import paths
5. Update command group organization in documentation
6. Update any CLI-related code examples
7. Test documentation build process
8. Regenerate documentation site

## Files to Update

### Quarto Files to Check
- `docs/index.qmd` - Main documentation
- `docs/quickstart.qmd` - Quick start guide with CLI examples
- `docs/examples.qmd` - CLI usage examples
- `docs/api/*.qmd` - API documentation files
- `docs/installation.qmd` - Installation and CLI usage
- Any other qmd files with CLI references

### Command Syntax Updates
```markdown
# Old command examples (to be updated)
```bash
naq worker start queue1 queue2
naq events stream
naq scheduler start
```

# New command examples
```bash
naq worker start queue1 queue2
naq events stream
naq scheduler start
```
```

### Import Documentation Updates
```markdown
# Old import examples (to be updated)
```python
from naq.cli import app
from naq.cli import worker
```

# New import examples
```python
from naq.cli.main import app
from naq.cli.worker_commands import worker_app
```
```

## Documentation Updates Needed

### Command Group Documentation
- [ ] Update worker command group documentation
- [ ] Update job command group documentation
- [ ] Update scheduler command group documentation
- [ ] Update event command group documentation
- [ ] Update system command group documentation

### API Documentation
- [ ] Update CLI module API documentation
- [ ] Update import path examples
- [ ] Update function signatures and parameters
- [ ] Update class and method documentation

### Examples and Tutorials
- [ ] Update quick start guide CLI examples
- [ ] Update feature examples with new commands
- [ ] Update installation instructions
- [ ] Update troubleshooting guides

## Success Criteria
- [ ] All CLI command examples updated to new syntax
- [ ] API documentation updated with new import paths
- [ ] Command group organization documented
- [ ] Documentation builds successfully with quarto
- [ ] Generated documentation site is correct
- [ ] No broken links or references
- [ ] All existing documentation functionality preserved

## Dependencies
- Subtask 02.1 (Create CLI Package Structure)
- Subtask 02.2 (Extract Main CLI App)
- Subtask 02.3 (Move Worker Commands)
- Subtask 02.4 (Move Job Commands)
- Subtask 02.5 (Move Scheduler Commands)
- Subtask 02.6 (Move Event Commands)
- Subtask 02.7 (Move System Commands)
- Subtask 02.8 (Update Package Initialization)
- Subtask 02.9 (Update Entry Points)
- Subtask 02.10 (Test and Validate)
- Subtask 02.11 (Update Tests)

## Estimated Time
- 2-3 hours