# Subtask 03.10: Update Documentation

## Overview
Update all documentation in the `docs` directory (managed by quarto) to reflect the new queue package structure and module organization.

## Current State
- Documentation references old queue.py structure
- Queue examples use old import paths
- API documentation may reference old queue imports
- Quarto files need updates for new module organization

## Target State
- All documentation updated to reflect new queue structure
- Queue examples use new import paths
- API documentation updated with new imports
- Quarto files regenerated with correct information
- Documentation builds successfully

## Implementation Steps
1. Analyze current documentation structure in `docs/` directory
2. Identify files that reference queue functionality
3. Update queue examples to use new import syntax
4. Update API documentation with new import paths
5. Update module organization in documentation
6. Update any queue-related code examples
7. Test documentation build process
8. Regenerate documentation site

## Files to Update

### Quarto Files to Check
- `docs/index.qmd` - Main documentation
- `docs/quickstart.qmd` - Quick start guide with queue examples
- `docs/examples.qmd` - Queue usage examples
- `docs/api/queue.qmd` - Queue API documentation
- `docs/api/job.qmd` - Job API documentation (may reference queue)
- `docs/api/scheduler.qmd` - Scheduler API documentation (may reference queue)
- `docs/api/worker.qmd` - Worker API documentation (may reference queue)
- `docs/installation.qmd` - Installation and queue usage
- Any other qmd files with queue references

### Import Documentation Updates
```markdown
# Old import examples (to be updated)
```python
from naq.queue import Queue, enqueue, enqueue_sync
from naq.queue import ScheduledJobManager
```

# New import examples
```python
from naq.queue import Queue, enqueue, enqueue_sync
from naq.queue import ScheduledJobManager

# More specific imports
from naq.queue.core import Queue
from naq.queue.async_api import enqueue
from naq.queue.sync_api import enqueue_sync
from naq.queue.scheduled import ScheduledJobManager
```
```

### Module Organization Updates
```markdown
# Old module documentation (to be updated)
## Queue Module
The `naq.queue` module contains all queue-related functionality including the Queue class, scheduled job management, and async/sync API functions.

# New module documentation
## Queue Package
The `naq.queue` package is organized into focused modules:
- `naq.queue.core` - Main Queue class and core operations
- `naq.queue.scheduled` - ScheduledJobManager and schedule operations
- `naq.queue.async_api` - Async functions (enqueue, enqueue_at, etc.)
- `naq.queue.sync_api` - Sync wrapper functions
```

## Documentation Updates Needed

### API Documentation
- [ ] Update Queue class API documentation
- [ ] Update ScheduledJobManager API documentation
- [ ] Update async API functions documentation
- [ ] Update sync API functions documentation
- [ ] Update import examples throughout

### Examples and Tutorials
- [ ] Update quick start guide queue examples
- [ ] Update feature examples with new imports
- [ ] Update installation instructions
- [ ] Update troubleshooting guides

### Module Organization
- [ ] Update queue package overview
- [ ] Document new module structure
- [ ] Update import examples throughout
- [ ] Update migration guide if needed

## Success Criteria
- [ ] All queue import examples updated to new syntax
- [ ] API documentation updated with new import paths
- [ ] Module organization documented
- [ ] Documentation builds successfully with quarto
- [ ] Generated documentation site is correct
- [ ] No broken links or references
- [ ] All existing documentation functionality preserved

## Dependencies
- Subtask 03.1 (Create Queue Package Structure)
- Subtask 03.2 (Extract ScheduledJobManager)
- Subtask 03.3 (Extract Core Queue Class)
- Subtask 03.4 (Extract Async API)
- Subtask 03.5 (Extract Sync API)
- Subtask 03.6 (Create Public API)
- Subtask 03.7 (Update Dependencies)
- Subtask 03.8 (Test and Validate)
- Subtask 03.9 (Update Tests)

## Estimated Time
- 2-3 hours