#!/usr/bin/env python3
"""
NAQ SyncClient Demo

This example demonstrates the efficient SyncClient for batch operations.
SyncClient reuses NATS connections, providing better performance than
individual sync calls.

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default batch_jobs`
4. Run this script: `python sync_client_demo.py`
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List

# Import NAQ components
from naq import SyncClient, setup_logging
from naq.nats_client import NatsClientConfig
from naq.settings import QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def process_data(data_id: int, category: str = "general") -> str:
    """
    Simulate data processing task.
    
    Args:
        data_id: Unique identifier for the data
        category: Category of data being processed
        
    Returns:
        Processing result message
    """
    print(f"🔄 Processing {category} data {data_id}")
    time.sleep(1)  # Simulate work
    result = f"Processed {category} data {data_id}"
    print(f"✅ {result}")
    return result


def send_notification(user_id: str, message: str) -> str:
    """
    Simulate sending a notification.
    
    Args:
        user_id: Target user ID
        message: Notification message
        
    Returns:
        Delivery confirmation
    """
    print(f"📧 Sending notification to {user_id}: {message}")
    time.sleep(0.5)  # Simulate quick notification
    result = f"Notification sent to {user_id}"
    print(f"✅ {result}")
    return result


def generate_report(report_type: str, date: str) -> str:
    """
    Simulate report generation.
    
    Args:
        report_type: Type of report to generate
        date: Date for the report
        
    Returns:
        Report generation result
    """
    print(f"📊 Generating {report_type} report for {date}")
    time.sleep(2)  # Simulate longer processing
    result = f"{report_type} report for {date} completed"
    print(f"✅ {result}")
    return result


def performance_test():
    """
    Compare performance between individual sync calls and SyncClient.
    """
    print("📊 Performance Test: Enqueueing 10 jobs")
    print("-" * 40)
    
    # Test without SyncClient (inefficient)
    start_time = time.time()
    
    jobs_individual = []
    nats_config = NatsClientConfig(
        nats_url="nats://localhost:4222",
        connection_timeout=5,
        max_reconnect_attempts=3
    )
    
    queue_config = QueueConfig(
        default_name="batch_jobs"
    )
    
    for i in range(10):
        with SyncClient(nats_url=nats_config.nats_url) as client:
            job = client.enqueue(
                process_data,
                data_id=i,
                category="performance_test"
            )
            jobs_individual.append(job)
    
    individual_time = time.time() - start_time
    
    # Test with SyncClient (efficient)
    start_time = time.time()
    
    jobs_batch = []
    with SyncClient(nats_url=nats_config.nats_url) as client:
        for i in range(10, 20):  # Different IDs to avoid confusion
            job = client.enqueue(
                process_data,
                data_id=i,
                category="performance_test"
            )
            jobs_batch.append(job)
    
    batch_time = time.time() - start_time
    
    # Show results
    improvement = ((individual_time - batch_time) / individual_time) * 100
    
    print(f"⏱️  Without SyncClient: {individual_time:.2f} seconds")
    print(f"⚡ With SyncClient: {batch_time:.2f} seconds")
    print(f"🚀 Improvement: {improvement:.0f}% faster!")
    
    return len(jobs_individual) + len(jobs_batch)


def batch_operations_demo():
    """
    Demonstrate various batch operations using SyncClient.
    """
    print("\n📦 Batch Operations Demo")
    print("-" * 30)
    
    # Create configuration
    nats_config = NatsClientConfig(
        nats_url="nats://localhost:4222",
        connection_timeout=5,
        max_reconnect_attempts=3
    )
    
    default_queue_config = QueueConfig(
        default_name="default"
    )
    
    batch_queue_config = QueueConfig(
        default_name="batch_jobs"
    )
    
    test_queue_config = QueueConfig(
        default_name="test_queue"
    )
    
    with SyncClient(nats_url=nats_config.nats_url) as client:
        # Batch 1: Data processing jobs
        print("📤 Enqueueing data processing jobs...")
        data_jobs = []
        for i in range(5):
            job = client.enqueue(
                process_data,
                data_id=i + 1000,
                category="batch_demo"
            )
            data_jobs.append(job)
        
        print(f"✅ Enqueued {len(data_jobs)} data processing jobs")
        
        # Batch 2: Notification jobs
        print("📤 Enqueueing notification jobs...")
        notification_jobs = []
        users = ["alice", "bob", "charlie"]
        for user in users:
            # Switch to default queue
            job = client.enqueue(
                send_notification,
                user_id=user,
                message="Batch processing completed",
                queue_name=default_queue_config.default_name
            )
            notification_jobs.append(job)
        
        print(f"✅ Enqueued {len(notification_jobs)} notification jobs")
        
        # Batch 3: Scheduled jobs
        print("📤 Scheduling future report jobs...")
        scheduled_jobs = []
        
        # Schedule a job for 30 seconds from now
        future_time = datetime.now(timezone.utc) + timedelta(seconds=30)
        job1 = client.enqueue_at(
            future_time,
            generate_report,
            report_type="daily",
            date="2024-01-15",
            queue_name=default_queue_config.default_name
        )
        scheduled_jobs.append(job1)
        
        # Schedule a job for 1 minute from now
        job2 = client.enqueue_in(
            timedelta(minutes=1),
            generate_report,
            report_type="weekly",
            date="2024-01-15",
            queue_name=default_queue_config.default_name
        )
        scheduled_jobs.append(job2)
        
        print(f"✅ Scheduled {len(scheduled_jobs)} future jobs")
        
        # Batch 4: Queue management
        print("📤 Managing queues...")
        
        # Create some test jobs then purge them
        test_jobs = []
        for i in range(3):
            job = client.enqueue(
                process_data,
                data_id=i + 2000,
                category="test",
                queue_name=test_queue_config.default_name
            )
            test_jobs.append(job)
        
        # Purge the test queue
        purged_count = client.purge_queue(queue_name=test_queue_config.default_name)
        print(f"✅ Purged {purged_count} jobs from test queue")
        
        total_jobs = len(data_jobs) + len(notification_jobs) + len(scheduled_jobs)
        print(f"\n🎯 Batch operations completed! Total jobs: {total_jobs}")
        
        return total_jobs


def main():
    """
    Main demonstration function.
    """
    print("🚀 NAQ SyncClient Demo")
    print("=" * 40)
    
    try:
        # Performance comparison
        perf_jobs = performance_test()
        
        # Batch operations demo
        batch_jobs = batch_operations_demo()
        
        total_jobs = perf_jobs + batch_jobs
        
        print(f"\n🎉 Demo completed successfully!")
        print(f"📊 Total jobs enqueued: {total_jobs}")
        print("\n💡 Key Benefits of SyncClient:")
        print("   - Connection reuse reduces overhead")
        print("   - Better performance for batch operations")
        print("   - Automatic resource cleanup")
        print("   - Simpler code with context managers")
        
        print("\n📋 Next steps:")
        print("   - Check your worker logs to see job processing")
        print("   - Try the worker management example")
        print("   - Explore advanced features like job dependencies")
        
    except Exception as e:
        print(f"❌ Error running demo: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default batch_jobs)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())