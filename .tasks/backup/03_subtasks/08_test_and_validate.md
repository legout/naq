# Subtask 03.8: Test and Validate

## Overview
Test all queue functionality to ensure it works correctly with the new package structure.

## Current State
- Queue functionality has been moved to separate modules
- Dependencies have been updated
- Need to verify all functionality works as expected

## Target State
- All queue functionality works correctly
- Public API unchanged (backward compatibility)
- All existing imports continue to work
- All tests pass without modification

## Implementation Steps
1. Test basic queue functionality
2. Test Queue class operations
3. Test scheduled job operations
4. Test async API functions
5. Test sync API functions
6. Test backward compatibility
7. Run existing tests
8. Fix any issues found

## Testing Checklist

### Basic Queue Tests
- [ ] `from naq.queue import Queue, enqueue, enqueue_sync` works
- [ ] Queue class instantiation works
- [ ] Basic queue operations work
- [ ] No circular import issues

### Queue Class Tests
- [ ] `Queue` class methods work correctly
- [ ] NATS connection management works
- [ ] Stream creation and configuration works
- [ ] Basic job enqueuing works
- [ ] Queue purging operations work
- [ ] Status monitoring works

### Scheduled Job Tests
- [ ] `ScheduledJobManager` class works correctly
- [ ] Scheduled job storage and retrieval works
- [ ] Schedule validation and parsing works
- [ ] Cron expression handling works
- [ ] Scheduled job lifecycle management works

### Async API Tests
- [ ] `enqueue()` function works correctly
- [ ] `enqueue_at()` function works correctly
- [ ] `enqueue_in()` function works correctly
- [ ] `schedule()` function works correctly
- [ ] `purge_queue()` function works correctly
- [ ] `cancel_scheduled_job()` function works correctly
- [ ] `pause_scheduled_job()` function works correctly
- [ ] `resume_scheduled_job()` function works correctly
- [ ] `modify_scheduled_job()` function works correctly

### Sync API Tests
- [ ] All `*_sync()` functions work correctly
- [ ] Thread-local connection optimization preserved
- [ ] Sync-specific error handling works
- [ ] Backward compatibility for sync users maintained

### Integration Tests
- [ ] Integration with worker works correctly
- [ ] Integration with scheduler works correctly
- [ ] All existing imports continue to work
- [ ] No functionality lost
- [ ] All existing tests pass without modification

## Success Criteria
- [ ] All queue functionality successfully split into focused modules
- [ ] Public API unchanged (backward compatibility)
- [ ] All existing imports continue to work
- [ ] Queue class operations work correctly
- [ ] Scheduled job operations work correctly
- [ ] Async API functions work correctly
- [ ] Sync API wrappers work correctly
- [ ] All tests pass without modification
- [ ] No file exceeds 500 lines
- [ ] Clear separation of concerns achieved
- [ ] Thread-local connection optimization preserved
- [ ] No circular import issues
- [ ] Integration with worker and scheduler unchanged

## Dependencies
- All previous subtasks (03.1 through 03.7)

## Estimated Time
- 3-4 hours