#!/usr/bin/env python3
"""
Basic Worker Example

This example demonstrates a simple worker setup that processes jobs
from a single queue. Perfect for getting started with NAQ workers.

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Run this script to enqueue jobs: `python basic_worker.py`
4. In another terminal, start worker: `naq worker default`
"""

import os
import time
from typing import Any

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def simple_task(task_name: str, duration: int = 2) -> str:
    """
    A simple task for workers to process.
    
    Args:
        task_name: Name of the task
        duration: How long the task takes (seconds)
        
    Returns:
        Task completion message
    """
    print(f"🔄 Starting task: {task_name}")
    print(f"⏱️  Task will take {duration} seconds")
    
    # Simulate work
    time.sleep(duration)
    
    result = f"Task '{task_name}' completed successfully"
    print(f"✅ {result}")
    
    return result


def cpu_intensive_task(size: int) -> str:
    """
    Simulate a CPU-intensive task.
    
    Args:
        size: Size of computation (affects duration)
        
    Returns:
        Computation result
    """
    print(f"🧮 Starting CPU-intensive computation (size: {size})")
    
    # Simulate CPU work
    total = 0
    for i in range(size * 100000):
        total += i * i
    
    result = f"CPU task completed. Final value: {total}"
    print(f"✅ {result}")
    
    return result


def io_simulation_task(file_count: int) -> str:
    """
    Simulate an I/O-intensive task.
    
    Args:
        file_count: Number of files to "process"
        
    Returns:
        I/O task result
    """
    print(f"📁 Processing {file_count} files...")
    
    for i in range(file_count):
        print(f"  📄 Processing file {i + 1}/{file_count}")
        time.sleep(0.5)  # Simulate I/O wait
    
    result = f"Processed {file_count} files successfully"
    print(f"✅ {result}")
    
    return result


def enqueue_sample_jobs():
    """
    Enqueue various types of jobs for the worker to process.
    """
    print("🚀 Basic Worker Demo - Enqueueing Sample Jobs")
    print("=" * 50)
    
    with SyncClient() as client:
        jobs = []
        
        # Simple tasks
        print("📤 Enqueueing simple tasks...")
        simple_jobs = [
            ("Welcome Setup", 1),
            ("Data Validation", 3),
            ("Report Generation", 2),
            ("Cache Refresh", 1),
            ("Cleanup Process", 2)
        ]
        
        for task_name, duration in simple_jobs:
            job = client.enqueue(
                simple_task,
                task_name=task_name,
                duration=duration,
                queue_name="default"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued: {task_name} (ID: {job.job_id})")
        
        # CPU-intensive tasks
        print("\n📤 Enqueueing CPU-intensive tasks...")
        cpu_sizes = [10, 50, 100]
        
        for size in cpu_sizes:
            job = client.enqueue(
                cpu_intensive_task,
                size=size,
                queue_name="default"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued CPU task (size {size}, ID: {job.job_id})")
        
        # I/O simulation tasks
        print("\n📤 Enqueueing I/O simulation tasks...")
        file_counts = [3, 5, 8]
        
        for count in file_counts:
            job = client.enqueue(
                io_simulation_task,
                file_count=count,
                queue_name="default"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued I/O task ({count} files, ID: {job.job_id})")
        
        print(f"\n🎉 Enqueued {len(jobs)} jobs successfully!")
        
        return jobs


def main():
    """
    Main function that demonstrates basic worker job enqueueing.
    """
    try:
        jobs = enqueue_sample_jobs()
        
        print("\n" + "=" * 50)
        print("📋 Worker Instructions:")
        print("=" * 50)
        print()
        print("1. Open a new terminal")
        print("2. Set the secure serializer:")
        print("   export NAQ_JOB_SERIALIZER=json")
        print()
        print("3. Start a basic worker:")
        print("   naq worker default")
        print()
        print("4. Watch the worker process these jobs:")
        print(f"   - {len([j for j in jobs if 'simple_task' in str(j.function)])} simple tasks")
        print(f"   - {len([j for j in jobs if 'cpu_intensive' in str(j.function)])} CPU-intensive tasks")
        print(f"   - {len([j for j in jobs if 'io_simulation' in str(j.function)])} I/O simulation tasks")
        print()
        print("💡 Worker Tips:")
        print("   - Default concurrency is 10 (adjustable with --concurrency)")
        print("   - Worker will process jobs in queue order")
        print("   - Use Ctrl+C to gracefully stop the worker")
        print("   - Check 'naq list-workers' to see active workers")
        print()
        print("📊 Try different worker configurations:")
        print("   naq worker default --concurrency 5    # Lower concurrency")
        print("   naq worker default --concurrency 20   # Higher concurrency")
        print("   naq worker default --log-level DEBUG  # More verbose logging")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Check NATS connection settings")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())