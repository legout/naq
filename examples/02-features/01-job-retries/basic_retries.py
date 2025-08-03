#!/usr/bin/env python3
"""
Basic Job Retry Examples

This example demonstrates fundamental retry patterns in NAQ:
- Linear retry with fixed delays
- Exponential backoff retry
- Custom retry delay sequences
- Basic retry monitoring

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker with debug logging: `naq worker default retry_queue --log-level DEBUG`
4. Run this script: `python basic_retries.py`
"""

import os
import random
import time
from typing import str

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging to see retry behavior
setup_logging(level="INFO")

# Global counters to simulate realistic failure patterns
failure_counters = {}


def reset_failure_counter(task_name: str):
    """Reset the failure counter for a task."""
    failure_counters[task_name] = 0


def flaky_network_task(task_id: str, success_threshold: int = 2) -> str:
    """
    Simulate a flaky network operation that fails a few times before succeeding.
    
    Args:
        task_id: Unique identifier for this task
        success_threshold: Number of attempts before success
        
    Returns:
        Success message
        
    Raises:
        ConnectionError: When simulating network failures
    """
    if task_id not in failure_counters:
        failure_counters[task_id] = 0
    
    failure_counters[task_id] += 1
    attempt = failure_counters[task_id]
    
    print(f"🔄 Network task {task_id} - Attempt {attempt}")
    
    if attempt <= success_threshold:
        print(f"❌ Network task {task_id} failed on attempt {attempt}")
        raise ConnectionError(f"Network timeout on attempt {attempt}")
    else:
        print(f"✅ Network task {task_id} succeeded on attempt {attempt}!")
        result = f"Network task {task_id} completed after {attempt} attempts"
        return result


def random_failure_task(task_id: str, failure_rate: float = 0.7) -> str:
    """
    Simulate a task with random failures.
    
    Args:
        task_id: Unique identifier for this task
        failure_rate: Probability of failure (0.0 to 1.0)
        
    Returns:
        Success message
        
    Raises:
        RuntimeError: When simulating random failures
    """
    print(f"🎲 Random task {task_id} starting...")
    
    if random.random() < failure_rate:
        print(f"❌ Random task {task_id} failed (random failure)")
        raise RuntimeError(f"Random failure in task {task_id}")
    else:
        print(f"✅ Random task {task_id} succeeded!")
        return f"Random task {task_id} completed successfully"


def api_simulation_task(endpoint: str, timeout_chance: float = 0.5) -> str:
    """
    Simulate an API call that might timeout or fail.
    
    Args:
        endpoint: API endpoint being called
        timeout_chance: Probability of timeout
        
    Returns:
        API response simulation
        
    Raises:
        TimeoutError: When simulating API timeouts
        ConnectionError: When simulating connection issues
    """
    print(f"🌐 Calling API endpoint: {endpoint}")
    
    # Simulate API call delay
    time.sleep(0.5)
    
    failure_type = random.random()
    
    if failure_type < timeout_chance:
        print(f"⏱️  API call to {endpoint} timed out")
        raise TimeoutError(f"API timeout for {endpoint}")
    elif failure_type < timeout_chance + 0.2:  # 20% chance of connection error
        print(f"🔌 Connection lost to {endpoint}")
        raise ConnectionError(f"Connection failed for {endpoint}")
    else:
        print(f"✅ API call to {endpoint} successful")
        return f"API response from {endpoint}: OK"


def demonstrate_linear_retries():
    """
    Demonstrate linear retry strategy with fixed delays.
    """
    print("📍 Linear Retry Strategy Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Reset counters for consistent behavior
        reset_failure_counter("linear-1")
        reset_failure_counter("linear-2")
        
        # Example 1: Fixed 3-second delays
        print("📤 Job 1: Fixed 3-second delays (3 retries)")
        job1 = client.enqueue(
            flaky_network_task,
            task_id="linear-1",
            success_threshold=2,  # Will succeed on 3rd attempt
            queue_name="retry_queue",
            max_retries=3,
            retry_delay=3,  # 3 seconds between each retry
            retry_strategy="linear"
        )
        jobs.append(job1)
        print(f"  ✅ Enqueued: {job1.job_id}")
        
        # Example 2: Custom delay sequence
        print("\n📤 Job 2: Custom delay sequence [2, 5, 10] seconds")
        job2 = client.enqueue(
            flaky_network_task,
            task_id="linear-2", 
            success_threshold=3,  # Will succeed on 4th attempt
            queue_name="retry_queue",
            max_retries=3,
            retry_delay=[2, 5, 10],  # Custom delays: 2s, then 5s, then 10s
            retry_strategy="linear"
        )
        jobs.append(job2)
        print(f"  ✅ Enqueued: {job2.job_id}")
        
        return jobs


def demonstrate_exponential_retries():
    """
    Demonstrate exponential backoff retry strategy.
    """
    print("\n📍 Exponential Backoff Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Example 1: Standard exponential backoff
        print("📤 Job 3: Exponential backoff starting at 2 seconds")
        job3 = client.enqueue(
            api_simulation_task,
            endpoint="/users/123",
            timeout_chance=0.8,  # High failure rate
            queue_name="retry_queue",
            max_retries=4,
            retry_delay=2,  # Will become: 2s, 4s, 8s, 16s
            retry_strategy="exponential"
        )
        jobs.append(job3)
        print(f"  ✅ Enqueued: {job3.job_id}")
        print("  ⏱️  Retry delays: 2s → 4s → 8s → 16s")
        
        # Example 2: Shorter exponential backoff
        print("\n📤 Job 4: Short exponential backoff starting at 1 second")
        job4 = client.enqueue(
            api_simulation_task,
            endpoint="/orders/456", 
            timeout_chance=0.6,
            queue_name="retry_queue",
            max_retries=3,
            retry_delay=1,  # Will become: 1s, 2s, 4s
            retry_strategy="exponential"
        )
        jobs.append(job4)
        print(f"  ✅ Enqueued: {job4.job_id}")
        print("  ⏱️  Retry delays: 1s → 2s → 4s")
        
        return jobs


def demonstrate_high_retry_jobs():
    """
    Demonstrate jobs that require many retries.
    """
    print("\n📍 High-Retry Jobs Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Multiple random failure jobs
        print("📤 Jobs 5-8: Random failure jobs (70% failure rate)")
        for i in range(4):
            job = client.enqueue(
                random_failure_task,
                task_id=f"random-{i+1}",
                failure_rate=0.7,
                queue_name="retry_queue",
                max_retries=5,  # Give them plenty of chances
                retry_delay=2,
                retry_strategy="linear"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued random job {i+1}: {job.job_id}")
        
        return jobs


def main():
    """
    Main function demonstrating basic retry patterns.
    """
    print("🚀 NAQ Basic Retry Strategies Demo")
    print("=" * 50)
    
    try:
        # Demonstrate different retry strategies
        linear_jobs = demonstrate_linear_retries()
        exponential_jobs = demonstrate_exponential_retries()
        random_jobs = demonstrate_high_retry_jobs()
        
        all_jobs = linear_jobs + exponential_jobs + random_jobs
        
        print(f"\n🎉 Enqueued {len(all_jobs)} jobs with different retry strategies!")
        
        print("\n" + "=" * 50)
        print("📊 Retry Strategy Summary:")
        print("=" * 50)
        print(f"Linear retries: {len(linear_jobs)} jobs")
        print(f"Exponential retries: {len(exponential_jobs)} jobs") 
        print(f"High-retry jobs: {len(random_jobs)} jobs")
        
        print("\n💡 Watch your worker logs to see:")
        print("   - Initial job failures")
        print("   - Retry delays in action")
        print("   - Eventual job success")
        print("   - Different timing patterns")
        
        print("\n🔧 Worker Tips:")
        print("   - Use DEBUG logging to see detailed retry info")
        print("   - Monitor job completion times")
        print("   - Check for jobs that consistently fail")
        
        print("\n📋 Next steps:")
        print("   - Try advanced_retries.py for selective retry patterns")
        print("   - Check realistic_scenarios.py for real-world examples")
        print("   - Monitor jobs with 'naq list-workers'")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default retry_queue)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())