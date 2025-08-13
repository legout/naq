# Task 11: Update Imports and Maintain Backward Compatibility

## Overview
Update all import statements throughout the NAQ codebase to use the new modular structure while maintaining complete backward compatibility for existing users. Ensure that all existing code continues to work without modification.

## Current State
After completing the refactoring tasks:
- Large files split into focused modules (models/, queue/, worker/, cli/, services/)
- Service layer implemented with centralized functionality
- Common patterns extracted to utilities
- New configuration system available
- Event system fully integrated

## Challenge
Update hundreds of import statements across the codebase while ensuring:
- Zero breaking changes for existing users
- All existing imports continue to work
- New imports are available for internal use
- Clean migration path for users who want to adopt new structure

## Import Migration Strategy

### 1. Backward Compatibility Approach

**Maintain Existing Public API:**
```python
# Users should continue to be able to use:
from naq import Queue, Worker, Job, enqueue, enqueue_sync
from naq.models import JOB_STATUS, JobEvent
from naq.events import AsyncJobEventLogger
```

**Internal Imports Use New Structure:**
```python
# Internal code uses new modular imports:
from naq.queue.core import Queue
from naq.worker.core import Worker  
from naq.models.jobs import Job
from naq.models.enums import JOB_STATUS
from naq.services.jobs import JobService
```

### 2. Import Mapping Strategy

#### Legacy Import Patterns → New Module Locations

**Models Import Mapping:**
```python
# OLD: from naq.models import Job, JOB_STATUS, JobEvent
# NEW LOCATIONS:
# - Job, JobResult → naq.models.jobs
# - JOB_STATUS, JobEventType, WorkerEventType → naq.models.enums  
# - JobEvent, WorkerEvent → naq.models.events
# - Schedule → naq.models.schedules

# BACKWARD COMPATIBILITY (in naq/models/__init__.py):
from .jobs import Job, JobResult
from .enums import JOB_STATUS, JobEventType, WorkerEventType
from .events import JobEvent, WorkerEvent
from .schedules import Schedule

# All legacy imports continue to work
```

**Queue Import Mapping:**
```python
# OLD: from naq.queue import Queue, enqueue, enqueue_sync
# NEW LOCATIONS:
# - Queue → naq.queue.core
# - enqueue, enqueue_at, schedule → naq.queue.async_api
# - enqueue_sync, enqueue_at_sync → naq.queue.sync_api

# BACKWARD COMPATIBILITY (in naq/queue/__init__.py):
from .core import Queue
from .async_api import enqueue, enqueue_at, enqueue_in, schedule
from .sync_api import enqueue_sync, enqueue_at_sync, enqueue_in_sync
```

**Worker Import Mapping:**
```python
# OLD: from naq.worker import Worker
# NEW LOCATION: naq.worker.core

# BACKWARD COMPATIBILITY (in naq/worker/__init__.py):
from .core import Worker
```

## Detailed Implementation Plan

### Step 1: Update Main Package Imports

**Update `src/naq/__init__.py`:**
```python
# naq/__init__.py - Main public API
import asyncio
import logging
from typing import Optional

from loguru import logger

# Import from new locations but expose as before
from .models.jobs import Job, JobResult
from .models.enums import JOB_STATUS, JobEventType, WorkerEventType, RetryDelayType
from .models.events import JobEvent, WorkerEvent
from .models.schedules import Schedule

from .queue import (
    Queue,
    cancel_scheduled_job,
    cancel_scheduled_job_sync,
    enqueue,
    enqueue_at,
    enqueue_at_sync,
    enqueue_in,
    enqueue_in_sync,
    enqueue_sync,
    modify_scheduled_job,
    modify_scheduled_job_sync,
    pause_scheduled_job,
    pause_scheduled_job_sync,
    purge_queue,
    purge_queue_sync,
    resume_scheduled_job,
    resume_scheduled_job_sync,
    schedule,
    schedule_sync,
)

from .worker import Worker
from .scheduler import Scheduler
from .results import Results

# Import events module for event logging capabilities
from . import events

# Connection management (updated to use service layer)
from .connection import (
    close_nats_connection,
    get_jetstream_context,
    get_nats_connection,
)

# Configuration (new)
from .config import get_config, load_config

# Settings for backward compatibility
from .settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME, SCHEDULED_JOB_STATUS, WORKER_STATUS

# Exception classes
from .exceptions import (
    ConfigurationError,
    NaqConnectionError,
    JobExecutionError,
    JobNotFoundError,
    NaqException,
    SerializationError,
)

__version__ = "0.2.0"  # Bump version for major refactoring

# Existing convenience functions maintained
fetch_job_result = Job.fetch_result
fetch_job_result_sync = Job.fetch_result_sync
list_workers = Worker.list_workers
list_workers_sync = Worker.list_workers_sync

# Connection management functions (maintain for backward compatibility)
async def connect(url: str = DEFAULT_NATS_URL):
    """Convenience function to establish default NATS connection."""
    return await get_nats_connection(url=url)

async def disconnect():
    """Convenience function to close default NATS connection."""
    await close_nats_connection()
```

### Step 2: Update Package __init__.py Files

**Models Package (`src/naq/models/__init__.py`):**
```python
# models/__init__.py - Backward compatibility for model imports
from .enums import JOB_STATUS, JobEventType, WorkerEventType, VALID_RETRY_STRATEGIES
from .jobs import Job, JobResult, RetryDelayType
from .events import JobEvent, WorkerEvent
from .schedules import Schedule

__all__ = [
    # Status and event enums
    "JOB_STATUS", "JobEventType", "WorkerEventType", "VALID_RETRY_STRATEGIES",
    # Job models
    "Job", "JobResult", "RetryDelayType", 
    # Event models
    "JobEvent", "WorkerEvent",
    # Schedule models
    "Schedule",
]
```

**Queue Package (`src/naq/queue/__init__.py`):**
```python
# queue/__init__.py - Backward compatibility for queue imports
from .core import Queue, ScheduledJobManager
from .async_api import (
    enqueue, enqueue_at, enqueue_in, schedule,
    purge_queue, cancel_scheduled_job, pause_scheduled_job,
    resume_scheduled_job, modify_scheduled_job
)
from .sync_api import (
    enqueue_sync, enqueue_at_sync, enqueue_in_sync, schedule_sync,
    purge_queue_sync, cancel_scheduled_job_sync, pause_scheduled_job_sync,
    resume_scheduled_job_sync, modify_scheduled_job_sync,
    close_sync_connections
)

__all__ = [
    # Core classes
    "Queue", "ScheduledJobManager",
    # Async API
    "enqueue", "enqueue_at", "enqueue_in", "schedule", "purge_queue",
    "cancel_scheduled_job", "pause_scheduled_job", "resume_scheduled_job", 
    "modify_scheduled_job",
    # Sync API
    "enqueue_sync", "enqueue_at_sync", "enqueue_in_sync", "schedule_sync",
    "purge_queue_sync", "cancel_scheduled_job_sync", "pause_scheduled_job_sync",
    "resume_scheduled_job_sync", "modify_scheduled_job_sync", "close_sync_connections",
]
```

**Worker Package (`src/naq/worker/__init__.py`):**
```python
# worker/__init__.py - Backward compatibility for worker imports
from .core import Worker

# Export manager classes if they were previously available
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

**CLI Package (`src/naq/cli/__init__.py`):**
```python
# cli/__init__.py - Export main CLI app
from .main import app, main

__all__ = ["app", "main"]
```

**Services Package (`src/naq/services/__init__.py`):**
```python
# services/__init__.py - Service layer public API  
from .base import BaseService, ServiceManager
from .connection import ConnectionService
from .jobs import JobService
from .events import EventService
from .streams import StreamService
from .kv_stores import KVStoreService
from .scheduler import SchedulerService

__all__ = [
    "BaseService", "ServiceManager",
    "ConnectionService", "JobService", "EventService",
    "StreamService", "KVStoreService", "SchedulerService",
]
```

**Utils Package (`src/naq/utils/__init__.py`):**
```python
# utils/__init__.py - Utility functions public API
from .decorators import retry, timing, log_errors
from .context_managers import managed_resource, timeout_context, error_context
from .async_helpers import run_in_thread, gather_with_concurrency, retry_async
from .error_handling import ErrorHandler, wrap_naq_exception
from .logging import StructuredLogger, setup_structured_logging
from .serialization import SerializationHelper, serialize_with_metadata

# Keep existing utilities for backward compatibility
from .run_async_from_sync import run_async_from_sync
from .setup_logging import setup_logging

__all__ = [
    # New utilities
    "retry", "timing", "log_errors",
    "managed_resource", "timeout_context", "error_context", 
    "run_in_thread", "gather_with_concurrency", "retry_async",
    "ErrorHandler", "wrap_naq_exception",
    "StructuredLogger", "setup_structured_logging",
    "SerializationHelper", "serialize_with_metadata",
    # Legacy utilities
    "run_async_from_sync", "setup_logging",
]
```

### Step 3: Update Internal Imports

**Systematic Import Update Process:**
1. **Create Import Mapping Document** - Map old → new imports
2. **Update Imports File by File** - Systematic replacement
3. **Test Each Update** - Ensure functionality unchanged
4. **Update Tests** - Update test imports to match

**Example Internal Import Updates:**

```python
# OLD internal imports
from .models import Job, JOB_STATUS, JobEvent
from .queue import Queue, enqueue
from .worker import Worker
from .serializers import get_serializer

# NEW internal imports  
from .models.jobs import Job
from .models.enums import JOB_STATUS
from .models.events import JobEvent
from .queue.core import Queue
from .queue.async_api import enqueue
from .worker.core import Worker
from .services.serialization import get_serializer
```

### Step 4: Handle Complex Import Dependencies

**Circular Import Prevention:**
```python
# Before: Potential circular imports
from .queue import Queue  # in worker.py
from .worker import Worker  # in queue.py

# After: Use typing imports for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .queue.core import Queue
    from .worker.core import Worker
```

**Dynamic Imports for Optional Dependencies:**
```python
# For optional imports that may not exist
def get_optional_service(service_name: str):
    try:
        if service_name == "JobService":
            from .services.jobs import JobService
            return JobService
    except ImportError:
        return None
```

### Step 5: Update Entry Points

**Update `pyproject.toml` Entry Points:**
```toml
[project.scripts]
# Main CLI entry point updated to use new location
naq = "naq.cli:main"

[project.entry-points."naq.plugins"]
# Plugin system for extensibility (if needed)
```

### Step 6: Create Import Deprecation Warnings (Optional)

**For Future Migration Guidance:**
```python
# In legacy import locations
import warnings
from .new_location import SomeClass

def deprecated_import_warning(old_path: str, new_path: str):
    warnings.warn(
        f"Importing from '{old_path}' is deprecated. "
        f"Use '{new_path}' instead. "
        f"Support for old imports will be removed in version 1.0.0",
        DeprecationWarning,
        stacklevel=3
    )

# Example usage in naq/__init__.py
from .models.jobs import Job as _Job

class Job(_Job):
    def __init__(self, *args, **kwargs):
        # Only warn on direct instantiation, not internal use
        if not _is_internal_call():
            deprecated_import_warning("naq.Job", "naq.models.jobs.Job") 
        super().__init__(*args, **kwargs)
```

## Testing Strategy

### Import Compatibility Tests

**Test All Legacy Imports Work:**
```python
def test_legacy_imports():
    """Test that all legacy import patterns continue to work."""
    
    # Test main package imports
    from naq import Queue, Worker, Job, enqueue, enqueue_sync
    assert Queue is not None
    assert Worker is not None
    assert Job is not None
    assert callable(enqueue)
    assert callable(enqueue_sync)
    
    # Test model imports
    from naq.models import JOB_STATUS, JobEvent
    assert JOB_STATUS is not None
    assert JobEvent is not None
    
    # Test event imports  
    from naq.events import AsyncJobEventLogger
    assert AsyncJobEventLogger is not None

def test_new_imports_available():
    """Test that new import structure is available."""
    
    # Test new modular imports
    from naq.models.jobs import Job
    from naq.models.enums import JOB_STATUS
    from naq.queue.core import Queue
    from naq.services.jobs import JobService
    
    assert Job is not None
    assert JOB_STATUS is not None
    assert Queue is not None
    assert JobService is not None

def test_import_equivalence():
    """Test that old and new imports refer to same objects."""
    
    # Legacy import
    from naq import Job as LegacyJob
    
    # New import
    from naq.models.jobs import Job as NewJob
    
    # Should be the same class
    assert LegacyJob is NewJob
```

### Integration Tests with Imports

**Test Real Usage Patterns:**
```python
def test_typical_user_workflow():
    """Test that typical user code continues to work."""
    
    # This represents typical user code
    from naq import enqueue_sync, Worker, JOB_STATUS
    from naq.models import Job
    
    def dummy_task():
        return "completed"
    
    # Should work exactly as before
    job = enqueue_sync(dummy_task)
    assert job.job_id is not None
    
    worker = Worker(['default'])
    assert worker is not None

def test_advanced_user_workflow():
    """Test advanced usage patterns."""
    
    # Advanced users might use these imports
    from naq import Queue, AsyncJobEventLogger
    from naq.models import JobEvent, JobEventType
    
    queue = Queue("test_queue")
    logger = AsyncJobEventLogger()
    event = JobEvent(job_id="test", event_type=JobEventType.ENQUEUED)
    
    assert queue is not None
    assert logger is not None 
    assert event.job_id == "test"
```

## Documentation Updates

### Import Guide Documentation

**Create Import Migration Guide:**
```markdown
# Import Migration Guide

## Current Imports (Backward Compatible)
All existing imports continue to work:

```python
# Main functionality
from naq import Queue, Worker, Job, enqueue, enqueue_sync

# Models and types  
from naq.models import JOB_STATUS, JobEvent, JobResult

# Event logging
from naq.events import AsyncJobEventLogger

# Configuration
from naq import get_config, DEFAULT_NATS_URL
```

## New Import Structure (Optional)
For new code, you can use the new modular structure:

```python
# Models by category
from naq.models.jobs import Job, JobResult
from naq.models.enums import JOB_STATUS, JobEventType
from naq.models.events import JobEvent, WorkerEvent

# Services (new)
from naq.services import JobService, EventService, ServiceManager

# Utilities (new)
from naq.utils import retry, timing, StructuredLogger
```

## Migration Recommendations
- **Existing Code**: No changes needed
- **New Projects**: Consider using new modular imports
- **Large Codebases**: Gradual migration over time
```

### API Reference Updates

**Update All Documentation:**
- Update code examples to use consistent imports
- Show both legacy and new import patterns where relevant
- Update docstring examples in modules
- Update README examples

## Files to Update

### Package Structure Files
- `src/naq/__init__.py` - Main public API
- `src/naq/models/__init__.py` - Model exports
- `src/naq/queue/__init__.py` - Queue exports  
- `src/naq/worker/__init__.py` - Worker exports
- `src/naq/cli/__init__.py` - CLI exports
- `src/naq/services/__init__.py` - Service exports
- `src/naq/utils/__init__.py` - Utility exports

### Internal Import Updates
- **All** `.py` files in `src/naq/` and subdirectories
- Update every `from .` and `from naq.` import
- Systematic file-by-file updates

### Configuration and Build
- `pyproject.toml` - Update entry points
- `setup.py` (if exists) - Update entry points

### Tests
- Update all test files to use correct imports
- Add import compatibility tests
- Update test fixtures and helpers

### Documentation
- Update all `.md` files with new import examples
- Update docstring examples
- Create import migration guide

## Implementation Process

### Phase 1: Update Package Structure (2-4 hours)
1. Update all `__init__.py` files with backward compatibility
2. Test basic imports work
3. Update main `naq/__init__.py`

### Phase 2: Update Internal Imports (8-12 hours)
1. Create import mapping document
2. Update imports file by file
3. Test each file after update
4. Fix any circular import issues

### Phase 3: Update Tests (4-6 hours)  
1. Update test imports
2. Add compatibility tests
3. Test import equivalence
4. Test real usage patterns

### Phase 4: Update Documentation (2-4 hours)
1. Update README and docs
2. Create migration guide
3. Update docstring examples
4. Update API reference

### Phase 5: Integration Testing (2-3 hours)
1. Full test suite run
2. Integration tests
3. Performance testing
4. User workflow testing

## Success Criteria

### Backward Compatibility
- [ ] All existing imports continue to work without modification
- [ ] No breaking changes for existing users
- [ ] All legacy import patterns preserved
- [ ] Existing code runs without changes

### New Structure Available  
- [ ] New modular imports available
- [ ] Services accessible via imports
- [ ] Utilities available for new code
- [ ] Clear migration path provided

### Code Quality
- [ ] No circular import issues
- [ ] Clean import structure
- [ ] Consistent import patterns
- [ ] Proper type hinting maintained

### Testing
- [ ] All import patterns tested
- [ ] Integration tests pass
- [ ] Performance not degraded
- [ ] User workflows validated

## Dependencies
- **Depends on**: All previous tasks (01-10) - need completed modular structure
- **Blocks**: None - this completes the refactoring

## Estimated Time
- **Package Structure Updates**: 2-4 hours
- **Internal Import Updates**: 8-12 hours  
- **Test Updates**: 4-6 hours
- **Documentation Updates**: 2-4 hours
- **Integration Testing**: 2-3 hours
- **Total**: 18-29 hours