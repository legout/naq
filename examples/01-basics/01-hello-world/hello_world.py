#!/usr/bin/env python3
"""
NAQ Hello World Example

This example demonstrates the basics of NAQ job processing:
- Creating a simple job function
- Enqueueing jobs securely using JSON serialization
- Proper error handling and logging

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default`
4. Run this script: `python hello_world.py`
"""

import os
import time
from typing import Any

# Import NAQ components
from naq import enqueue_sync, setup_logging

# Configure secure JSON serialization (recommended for production)
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging to see what's happening
setup_logging(level="INFO")


def say_hello(name: str = "World") -> str:
    """
    A simple job function that greets someone.
    
    Note: This function must be importable (not a lambda or nested function)
    when using JSON serialization for security.
    
    Args:
        name: The name to greet
        
    Returns:
        A greeting message
    """
    print(f"👋 Hello, {name}!")
    
    # Simulate some work being done
    print("🔄 Processing your greeting...")
    time.sleep(2)
    
    result = f"Greeting completed for {name}"
    print(f"✅ {result}")
    
    return result


def calculate_sum(a: int, b: int) -> int:
    """
    A simple calculation job to demonstrate different job types.
    
    Args:
        a: First number
        b: Second number
        
    Returns:
        The sum of a and b
    """
    print(f"🧮 Calculating {a} + {b}")
    time.sleep(1)  # Simulate work
    result = a + b
    print(f"✅ Result: {result}")
    return result


def main():
    """
    Main function that demonstrates enqueueing different types of jobs.
    """
    print("🚀 NAQ Hello World Example")
    print("=" * 40)
    
    try:
        # Example 1: Basic greeting job
        print("📤 Enqueueing greeting job...")
        greeting_job = enqueue_sync(
            say_hello,
            name="Alice",
            queue_name="default"  # Using the default queue
        )
        print(f"✅ Enqueued greeting job: {greeting_job.job_id}")
        
        # Example 2: Calculation job with multiple arguments
        print("\n📤 Enqueueing calculation job...")
        calc_job = enqueue_sync(
            calculate_sum,
            a=15,
            b=27,
            queue_name="default"
        )
        print(f"✅ Enqueued calculation job: {calc_job.job_id}")
        
        # Example 3: Multiple jobs for demonstration
        print("\n📤 Enqueueing multiple greeting jobs...")
        names = ["Bob", "Charlie", "Diana"]
        jobs = []
        
        for name in names:
            job = enqueue_sync(say_hello, name=name, queue_name="default")
            jobs.append(job)
            print(f"✅ Enqueued job for {name}: {job.job_id}")
        
        print(f"\n🎉 Successfully enqueued {len(jobs) + 2} jobs!")
        print("\n💡 Check your worker terminal to see the jobs being processed")
        print("\n📋 Next steps:")
        print("   - Watch the worker process these jobs")
        print("   - Try the SyncClient example for batch operations") 
        print("   - Explore job retries and error handling")
        
    except Exception as e:
        print(f"❌ Error enqueueing jobs: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Check the NATS connection URL")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())