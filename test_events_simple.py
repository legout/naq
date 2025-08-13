#!/usr/bin/env python3
"""
Simple test to verify event classes can be imported and instantiated
"""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    # Test importing event classes directly
    from naq.models.events import JobEvent, WorkerEvent
    from naq.models.enums import JobEventType, WorkerEventType
    
    print("✅ Successfully imported event classes")
    
    # Test creating JobEvent instances
    job_event = JobEvent(
        type=JobEventType.CREATED,
        job_id="test-job-123",
        timestamp="2023-01-01T00:00:00Z",
        payload={"key": "value"}
    )
    
    print(f"✅ Successfully created JobEvent: {job_event}")
    print(f"   Type: {job_event.type}")
    print(f"   Job ID: {job_event.job_id}")
    
    # Test creating WorkerEvent instances
    worker_event = WorkerEvent(
        type=WorkerEventType.STARTED,
        worker_id="test-worker-456",
        timestamp="2023-01-01T00:00:00Z",
        payload={"status": "running"}
    )
    
    print(f"✅ Successfully created WorkerEvent: {worker_event}")
    print(f"   Type: {worker_event.type}")
    print(f"   Worker ID: {worker_event.worker_id}")
    
    # Test serialization
    job_json = job_event.to_json()
    worker_json = worker_event.to_json()
    
    print(f"✅ JobEvent JSON: {job_json}")
    print(f"✅ WorkerEvent JSON: {worker_json}")
    
    print("\n🎉 All tests passed! Event classes are working correctly.")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)