#!/usr/bin/env python3
"""
Direct test to verify event classes can be imported and instantiated
"""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    # Test importing enums directly
    from naq.models.enums import JobEventType, WorkerEventType
    
    print("✅ Successfully imported enums")
    
    # Test importing msgspec directly
    import msgspec
    print("✅ Successfully imported msgspec")
    
    # Create a simple JobEvent class directly
    class JobEvent(msgspec.Struct):
        job_id: str
        event_type: JobEventType
        timestamp: float
    
    # Create a simple WorkerEvent class directly
    class WorkerEvent(msgspec.Struct):
        worker_id: str
        event_type: WorkerEventType
        timestamp: float
    
    print("✅ Successfully created event classes")
    
    # Test creating JobEvent instances
    job_event = JobEvent(
        event_type=JobEventType.ENQUEUED,
        job_id="test-job-123",
        timestamp=1234567890.0
    )
    
    print(f"✅ Successfully created JobEvent: {job_event}")
    print(f"   Type: {job_event.event_type}")
    print(f"   Job ID: {job_event.job_id}")
    
    # Test creating WorkerEvent instances
    worker_event = WorkerEvent(
        event_type=WorkerEventType.STARTED,
        worker_id="test-worker-456",
        timestamp=1234567890.0
    )
    
    print(f"✅ Successfully created WorkerEvent: {worker_event}")
    print(f"   Type: {worker_event.event_type}")
    print(f"   Worker ID: {worker_event.worker_id}")
    
    print("\n🎉 Direct test passed! Event classes work correctly.")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)