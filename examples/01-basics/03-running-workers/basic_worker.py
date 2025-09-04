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

from loguru import logger

from naq import NatsClient, setup_logging
from naq.config import NatsConfig, QueueConfig

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
    logger.info(f"🔄 Starting task: {task_name}")
    logger.info(f"⏱️  Task will take {duration} seconds")
    
    # Simulate work
    time.sleep(duration)
    
    result = f"Task '{task_name}' completed successfully"
    logger.success(f"✅ {result}")
    
    return result


def cpu_intensive_task(size: int) -> str:
    """
    Simulate a CPU-intensive task.
    
    Args:
        size: Size of computation (affects duration)
        
    Returns:
        Computation result
    """
    logger.info(f"🧮 Starting CPU-intensive computation (size: {size})")
    
    # Simulate CPU work
    total = 0
    for i in range(size * 100000):
        total += i * i
    
    result = f"CPU task completed. Final value: {total}"
    logger.success(f"✅ {result}")
    
    return result


def io_simulation_task(file_count: int) -> str:
    """
    Simulate an I/O-intensive task.
    
    Args:
        file_count: Number of files to "process"
        
    Returns:
        I/O task result
    """
    logger.info(f"📁 Processing {file_count} files...")
    
    for i in range(file_count):
        logger.info(f"  📄 Processing file {i + 1}/{file_count}")
        time.sleep(0.5)  # Simulate I/O wait
    
    result = f"Processed {file_count} files successfully"
    logger.success(f"✅ {result}")
    
    return result


def enqueue_sample_jobs():
    """
    Enqueue various types of jobs for the worker to process.
    
    Returns:
        List of enqueued jobs
    """
    logger.info("🚀 Basic Worker Demo - Enqueueing Sample Jobs")
    logger.info("=" * 50)
    
    # Create configuration
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5,
        max_reconnect_attempts=3
    )
    
    queue_config = QueueConfig(
        name="default",
        stream_name="NAQ_JOBS",
        consumer_name="basic_worker_consumer"
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Simple tasks
        logger.info("📤 Enqueueing simple tasks...")
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
                duration=duration
            )
            jobs.append(job)
            logger.info(f"  ✅ Enqueued: {task_name} (ID: {job.job_id})")
        
        # CPU-intensive tasks
        logger.info("\n📤 Enqueueing CPU-intensive tasks...")
        cpu_sizes = [10, 50, 100]
        
        for size in cpu_sizes:
            job = client.enqueue(
                cpu_intensive_task,
                size=size
            )
            jobs.append(job)
            logger.info(f"  ✅ Enqueued CPU task (size {size}, ID: {job.job_id})")
        
        # I/O simulation tasks
        logger.info("\n📤 Enqueueing I/O simulation tasks...")
        file_counts = [3, 5, 8]
        
        for count in file_counts:
            job = client.enqueue(
                io_simulation_task,
                file_count=count
            )
            jobs.append(job)
            logger.info(f"  ✅ Enqueued I/O task ({count} files, ID: {job.job_id})")
        
        logger.info(f"\n🎉 Enqueued {len(jobs)} jobs successfully!")
        
        return jobs


def main() -> int:
    """
    Main function that demonstrates basic worker job enqueueing.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        jobs = enqueue_sample_jobs()
        
        logger.info("\n" + "=" * 50)
        logger.info("📋 Worker Instructions:")
        logger.info("=" * 50)
        logger.info("")
        logger.info("1. Open a new terminal")
        logger.info("2. Set the secure serializer:")
        logger.info("   export NAQ_JOB_SERIALIZER=json")
        logger.info("")
        logger.info("3. Start a basic worker:")
        logger.info("   naq worker default")
        logger.info("")
        logger.info("4. Watch the worker process these jobs:")
        logger.info(f"   - {len([j for j in jobs if 'simple_task' in str(j.function)])} simple tasks")
        logger.info(f"   - {len([j for j in jobs if 'cpu_intensive' in str(j.function)])} CPU-intensive tasks")
        logger.info(f"   - {len([j for j in jobs if 'io_simulation' in str(j.function)])} I/O simulation tasks")
        logger.info("")
        logger.info("💡 Worker Tips:")
        logger.info("   - Default concurrency is 10 (adjustable with --concurrency)")
        logger.info("   - Worker will process jobs in queue order")
        logger.info("   - Use Ctrl+C to gracefully stop the worker")
        logger.info("   - Check 'naq list-workers' to see active workers")
        logger.info("")
        logger.info("📊 Try different worker configurations:")
        logger.info("   naq worker default --concurrency 5    # Lower concurrency")
        logger.info("   naq worker default --concurrency 20   # Higher concurrency")
        logger.info("   naq worker default --log-level DEBUG  # More verbose logging")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.error("\n🔧 Troubleshooting:")
        logger.error("   - Is NATS running? (cd docker && docker-compose up -d)")
        logger.error("   - Is NAQ_JOB_SERIALIZER=json set?")
        logger.error("   - Check NATS connection settings")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())