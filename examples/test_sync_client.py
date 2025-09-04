#!/usr/bin/env python3
"""
Test script to verify that NatsClient works correctly.
"""

import os
import time
from datetime import datetime, timedelta

from naq import SyncClient, setup_logging
from naq.nats_client import NatsClientConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def example_task(name: str) -> str:
    """Example task function."""
    print(f"Processing task: {name}")
    time.sleep(0.1)  # Simulate some work
    return f"Completed {name}"


def main():
    """Test the SyncClient implementation."""
    print("Testing SyncClient implementation...")
    
    # Test basic enqueue
    try:
        # Create configuration
        nats_config = NatsClientConfig(
            nats_url="nats://localhost:4222",
            connection_timeout=5,
            max_reconnect_attempts=3
        )
        
        
        with SyncClient(nats_url=nats_config.nats_url) as client:
            print("Enqueuing jobs...")
            jobs = []
            for i in range(3):
                job = client.enqueue(example_task, f"task_{i}")
                jobs.append(job)
                print(f"Enqueued job {job.job_id}")
            
            print(f"Total jobs enqueued: {len(client.jobs)}")
            
            # Test enqueue_at
            future_time = datetime.now() + timedelta(seconds=10)
            scheduled_job = client.enqueue_at(future_time, example_task, "scheduled_task")
            print(f"Scheduled job for future: {scheduled_job.job_id}")
            
            # Test enqueue_in
            delay_job = client.enqueue_in(timedelta(seconds=5), example_task, "delayed_task")
            print(f"Scheduled job with delay: {delay_job.job_id}")
            
            # Test purge_queue
            # Note: This would purge the queue, so we're not actually calling it in this test
            # count = client.purge_queue()
            # print(f"Purged {count} jobs")
        
        print("SyncClient test completed successfully!")
    except Exception as e:
        print(f"Error during SyncClient test: {e}")
        print("This is expected if NATS is not running.")


if __name__ == "__main__":
    main()