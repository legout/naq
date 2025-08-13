# Subtask 04.06: Create Worker Public API

## Overview
Set up the public API for the worker package through `__init__.py` to ensure backward compatibility and provide a clean interface for external users.

## Objectives
- Configure `__init__.py` to export the Worker class and related utilities
- Maintain complete backward compatibility with existing imports
- Provide clean public API for the worker package
- Test that all existing imports continue to work

## Implementation Steps

### 1. Configure __init__.py Exports
Set up the `__init__.py` file to export the main classes:
```python
# Import main Worker class
from .core import Worker

# Import manager classes (if they need to be public)
from .status import WorkerStatusManager
from .jobs import JobStatusManager  
from .failed import FailedJobHandler

__all__ = [
    "Worker",
    "WorkerStatusManager", 
    "JobStatusManager",
    "FailedJobHandler",
]
```

### 2. Verify Backward Compatibility
- Test that existing imports continue to work:
  ```python
  from naq.worker import Worker
  from naq.worker import WorkerStatusManager
  from naq.worker import JobStatusManager
  from naq.worker import FailedJobHandler
  ```
- Test that the package can be imported as a whole:
  ```python
  import naq.worker
  ```

### 3. Update Package-Level References
- Update `src/naq/__init__.py` if it exports worker classes
- Update any CLI commands that import worker functionality
- Update documentation examples to use new import structure

### 4. Type Hints and Documentation
- Ensure all exported classes have proper type hints
- Add docstrings for public API if missing
- Verify the API is intuitive and well-documented

### 5. Integration Testing
- Test all existing import patterns work correctly
- Test that Worker instantiation works as before
- Test that all public methods are accessible
- Test that no functionality is lost

## Public API Design

### Core Exports
- `Worker` - Main worker class for job processing
- `WorkerStatusManager` - Worker status management (if public)
- `JobStatusManager` - Job status tracking (if public)  
- `FailedJobHandler` - Failed job handling (if public)

### Import Patterns to Support
```python
# Main worker import
from naq.worker import Worker

# Individual class imports
from naq.worker import WorkerStatusManager
from naq.worker import JobStatusManager
from naq.worker import FailedJobHandler

# Package import
import naq.worker

# Access through package
worker_class = naq.worker.Worker
status_manager_class = naq.worker.WorkerStatusManager
```

## Success Criteria
- [ ] __init__.py properly configured with exports
- [ ] All existing imports continue to work (backward compatibility)
- [ ] Worker class and related utilities accessible through public API
- [ ] No functionality is lost in the public interface
- [ ] Type hints and documentation are complete
- [ ] Integration tests pass

## Dependencies
- **Depends on**: Subtask 04.05 (Extract Core Worker Class)
- **Prepares for**: Subtask 04.07 (Update Tests)

## Estimated Time
- **Implementation**: 1 hour
- **Testing**: 45 minutes
- **Total**: 1.75 hours