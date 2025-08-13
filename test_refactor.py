#!/usr/bin/env python3
"""
Simple test script to verify that the queue refactoring works correctly.
"""

def test_imports():
    """Test that all imports work correctly."""
    print("Testing imports...")
    
    # Test importing from the new queue package
    try:
        from naq.queue import Queue, ScheduledJobManager
        print("✓ Successfully imported Queue and ScheduledJobManager from naq.queue")
    except ImportError as e:
        print(f"✗ Failed to import from naq.queue: {e}")
        return False
    
    # Test importing from the main naq package (backward compatibility)
    try:
        from naq import Queue as MainQueue
        print("✓ Successfully imported Queue from naq")
    except ImportError as e:
        print(f"✗ Failed to import Queue from naq: {e}")
        return False
    
    # Test that they are the same objects
    if Queue is MainQueue:
        print("✓ Queue objects are the same (backward compatibility)")
    else:
        print("✗ Queue objects are different (backward compatibility issue)")
        return False
    
    # Test importing async and sync API functions
    try:
        from naq.queue import enqueue, schedule, enqueue_sync, schedule_sync
        print("✓ Successfully imported async and sync API functions")
    except ImportError as e:
        print(f"✗ Failed to import API functions: {e}")
        return False
    
    # Test importing from the queue package submodules
    try:
        from naq.queue.core import Queue as CoreQueue
        from naq.queue.scheduled import ScheduledJobManager as CoreScheduledJobManager
        from naq.queue.async_api import enqueue as async_enqueue, schedule as async_schedule
        from naq.queue.sync_api import enqueue_sync as sync_enqueue, schedule_sync as sync_schedule
        print("✓ Successfully imported from queue package submodules")
    except ImportError as e:
        print(f"✗ Failed to import from submodules: {e}")
        return False
    
    # Test that they are the same objects
    if Queue is CoreQueue:
        print("✓ Queue objects from main and core are the same")
    else:
        print("✗ Queue objects from main and core are different")
        return False
    
    if ScheduledJobManager is CoreScheduledJobManager:
        print("✓ ScheduledJobManager objects from main and scheduled are the same")
    else:
        print("✗ ScheduledJobManager objects from main and scheduled are different")
        return False
    
    if enqueue is async_enqueue and schedule is async_schedule:
        print("✓ Async API functions from main and async_api are the same")
    else:
        print("✗ Async API functions from main and async_api are different")
        return False
    
    if enqueue_sync is sync_enqueue and schedule_sync is sync_schedule:
        print("✓ Sync API functions from main and sync_api are the same")
    else:
        print("✗ Sync API functions from main and sync_api are different")
        return False
    
    print("All import tests passed!")
    return True

def test_basic_functionality():
    """Test basic functionality of the refactored classes."""
    print("\nTesting basic functionality...")
    
    try:
        # Test Queue instantiation
        from naq.queue import Queue
        queue = Queue("test-queue")
        print("✓ Successfully created Queue instance")
        
        # Test ScheduledJobManager instantiation
        from naq.queue import ScheduledJobManager
        scheduler = ScheduledJobManager("test-queue")
        print("✓ Successfully created ScheduledJobManager instance")
        
        print("Basic functionality tests passed!")
        return True
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        return False

if __name__ == "__main__":
    print("Running refactoring tests...\n")
    
    success = True
    success &= test_imports()
    success &= test_basic_functionality()
    
    if success:
        print("\n🎉 All tests passed! The refactoring was successful.")
    else:
        print("\n❌ Some tests failed. Please check the issues above.")
    
    exit(0 if success else 1)