# Subtask 04.08: Update Worker Documentation

## Overview
Update all documentation to reflect the new worker package structure and ensure users understand how to use the modular worker components.

## Objectives
- Update API documentation for the new worker package structure
- Update examples and tutorials to use new import patterns
- Create documentation for extracted manager classes
- Ensure all documentation is accurate and up-to-date

## Implementation Steps

### 1. Update API Documentation
Update `docs/api/worker.qmd` to reflect the new structure:
- Document the new worker package organization
- Update class documentation to reflect module locations
- Add documentation for extracted manager classes
- Update import examples

### 2. Update Import Examples
Update all documentation examples to use new import patterns:
```python
# Old import
from naq.worker import Worker

# New imports (still works for backward compatibility)
from naq.worker import Worker
from naq.worker import WorkerStatusManager
from naq.worker import JobStatusManager
from naq.worker import FailedJobHandler
```

### 3. Document Manager Classes
Add documentation for the extracted manager classes:
- **WorkerStatusManager**: Status management and heartbeat functionality
- **JobStatusManager**: Job status tracking and dependency management
- **FailedJobHandler**: Failed job processing and retry logic

### 4. Update Architecture Documentation
Update `docs/architecture.qmd` to reflect the new worker architecture:
- Document the separation of concerns in the worker package
- Explain the relationship between Worker and manager classes
- Update architecture diagrams if applicable

### 5. Update Installation and Quick Start
Update installation and quick start guides:
- Ensure examples work with new structure
- Update any worker-related setup instructions
- Verify that all examples are runnable

### 6. Update Examples Directory
Update examples in the `examples/` directory:
- Update worker-related examples to use new imports
- Ensure all examples continue to work
- Add examples demonstrating manager class usage if appropriate

### 7. Update README and Contributing Guidelines
Update project-level documentation:
- Update `README.md` with any worker-related changes
- Update `contributing.md` with guidelines for worker development
- Ensure consistency across all documentation

## Documentation Files to Update

### Core Documentation
- `docs/api/worker.qmd` - Worker API documentation
- `docs/architecture.qmd` - Architecture documentation
- `docs/quickstart.qmd` - Quick start guide
- `docs/installation.qmd` - Installation guide

### Examples
- `examples/01-basics/03-running-workers/` - Basic worker examples
- `examples/02-features/` - Feature-specific worker examples
- `examples/03-production/` - Production worker examples
- `examples/04-applications/` - Application integration examples

### Project Documentation
- `README.md` - Project overview
- `contributing.md` - Contribution guidelines
- `SECURITY.md` - Security considerations (if worker-related)

## Documentation Content Requirements

### Worker Package Overview
- Explain the new modular structure
- Describe the purpose of each module
- Show the package organization
- Explain benefits of the split

### Class Documentation
- **Worker**: Main worker class with all public methods
- **WorkerStatusManager**: Status management functionality
- **JobStatusManager**: Job status tracking functionality
- **FailedJobHandler**: Failed job handling functionality

### Usage Examples
- Basic worker setup and usage
- Worker status monitoring
- Job status tracking
- Failed job handling
- Integration with other components

### Migration Guide
- Information for users upgrading from old structure
- Backward compatibility notes
- Migration steps if needed

## Success Criteria
- [ ] All documentation updated to reflect new worker structure
- [ ] API documentation accurate and complete
- [ ] Examples work with new import patterns
- [ ] Manager classes properly documented
- [ ] Architecture documentation updated
- [ ] All documentation is consistent and accurate

## Dependencies
- **Depends on**: Subtask 04.07 (Update Tests)
- **Completes**: Task 04 (Split Worker Package)

## Estimated Time
- **Implementation**: 2-3 hours
- **Review**: 1 hour
- **Total**: 3-4 hours