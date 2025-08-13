# Subtask 01.10: Update Documentation

## Overview
Update all documentation in the `docs` directory (managed by quarto) to reflect the new models package structure and module organization.

## Current State
- Documentation references old models.py structure
- Model examples use old import paths
- API documentation may reference old model imports
- Quarto files need updates for new module organization

## Target State
- All documentation updated to reflect new models structure
- Model examples use new import paths
- API documentation updated with new imports
- Quarto files regenerated with correct information
- Documentation builds successfully

## Implementation Steps
1. Analyze current documentation structure in `docs/` directory
2. Identify files that reference model functionality
3. Update model examples to use new import syntax
4. Update API documentation with new import paths
5. Update module organization in documentation
6. Update any model-related code examples
7. Test documentation build process
8. Regenerate documentation site

## Files to Update

### Quarto Files to Check
- `docs/index.qmd` - Main documentation
- `docs/quickstart.qmd` - Quick start guide with model examples
- `docs/examples.qmd` - Model usage examples
- `docs/api/job.qmd` - Job model API documentation
- `docs/api/queue.qmd` - Queue model API documentation
- `docs/api/worker.qmd` - Worker model API documentation
- `docs/api/scheduler.qmd` - Scheduler model API documentation
- `docs/api/events.qmd` - Event model API documentation
- `docs/installation.qmd` - Installation and model usage
- Any other qmd files with model references

### Import Documentation Updates
```markdown
# Old import examples (to be updated)
```python
from naq.models import Job, JobStatus
from naq.models import ScheduledJob, JobEventType
```

# New import examples
```python
from naq.models.jobs import Job
from naq.models.enums import JobStatus, JobEventType
from naq.models.schedules import ScheduledJob
```
```

### Module Organization Updates
```markdown
# Old module documentation (to be updated)
## Models Module
The `naq.models` module contains all data models and enums.

# New module documentation
## Models Package
The `naq.models` package is organized into focused modules:
- `naq.models.jobs` - Job and queue-related models
- `naq.models.enums` - Enum definitions
- `naq.models.events` - Event-related models
- `naq.models.schedules` - Schedule-related models
```

## Documentation Updates Needed

### API Documentation
- [ ] Update job model API documentation
- [ ] Update queue model API documentation
- [ ] Update worker model API documentation
- [ ] Update scheduler model API documentation
- [ ] Update event model API documentation
- [ ] Update enum API documentation

### Examples and Tutorials
- [ ] Update quick start guide model examples
- [ ] Update feature examples with new imports
- [ ] Update installation instructions
- [ ] Update troubleshooting guides

### Module Organization
- [ ] Update models package overview
- [ ] Document new module structure
- [ ] Update import examples throughout
- [ ] Update migration guide if needed

## Success Criteria
- [ ] All model import examples updated to new syntax
- [ ] API documentation updated with new import paths
- [ ] Module organization documented
- [ ] Documentation builds successfully with quarto
- [ ] Generated documentation site is correct
- [ ] No broken links or references
- [ ] All existing documentation functionality preserved

## Dependencies
- Subtask 01.1: Create package structure
- Subtask 01.2: Move enum definitions to enums.py
- Subtask 01.3: Move event models to events.py
- Subtask 01.4: Move job models to jobs.py
- Subtask 01.5: Move schedule model to schedules.py
- Subtask 01.6: Create public API in __init__.py
- Subtask 01.7: Update dependent imports
- Subtask 01.8: Cleanup and final verification
- Subtask 01.9: Update Tests

## Estimated Time
- 1.5-2 hours