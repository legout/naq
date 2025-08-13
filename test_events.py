#!/usr/bin/env python3
"""
Simple test script to verify the events module works correctly.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Test importing the modules directly
try:
    from naq.models.events import JobEvent, WorkerEvent
    from naq.models.enums import JobEventType, WorkerEventType
    print("✅ Successfully imported JobEvent, WorkerEvent, JobEventType, and WorkerEventType")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

# Test creating instances
try:
    # Test JobEvent
    job_event = JobEvent.enqueued(
        job_id="test-job-123",
        queue_name="test-queue",
        worker_id="test-worker"
    )
    print(f"✅ Created JobEvent: {job_event.event_type.value} for job {job_event.job_id}")
    
    # Test WorkerEvent
    worker_event = WorkerEvent.started(
        worker_id="test-worker",
        queue_names=["test-queue"]
    )
    print(f"✅ Created WorkerEvent: {worker_event.event_type.value} for worker {worker_event.worker_id}")
    
    print("✅ All tests passed!")
except Exception as e:
    print(f"❌ Test failed: {e}")
    sys.exit(1)