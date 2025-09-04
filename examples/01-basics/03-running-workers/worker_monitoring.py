#!/usr/bin/env python3
"""
Worker Monitoring Example

This example demonstrates how to monitor NAQ workers, check their status,
and understand worker health metrics.

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start some workers: `naq worker default` (in separate terminals)
4. Run this script: `python worker_monitoring.py`
"""

import os
import time
from datetime import datetime
from typing import List, Dict, Any

from loguru import logger

from naq import list_workers_sync, NatsClient, setup_logging
from naq.config import NatsConfig, QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def long_running_task(task_id: int, duration: int = 10) -> str:
    """
    A long-running task to demonstrate worker monitoring.
    
    Args:
        task_id: Unique task identifier
        duration: How long the task runs
        
    Returns:
        Task completion message
    """
    logger.info(f"🔄 Long task {task_id} starting (will run {duration}s)")
    
    for i in range(duration):
        time.sleep(1)
        if i % 3 == 0:  # Progress update every 3 seconds
            logger.info(f"  📊 Task {task_id} progress: {i+1}/{duration} seconds")
    
    result = f"Long task {task_id} completed after {duration} seconds"
    logger.success(f"✅ {result}")
    
    return result


def quick_task(task_id: int) -> str:
    """
    A quick task for comparison.
    
    Args:
        task_id: Unique task identifier
        
    Returns:
        Task completion message
    """
    logger.info(f"⚡ Quick task {task_id} executing...")
    time.sleep(1)
    result = f"Quick task {task_id} completed"
    logger.success(f"✅ {result}")
    return result


def display_worker_info(workers: List[Dict[str, Any]]) -> None:
    """
    Display formatted worker information.
    
    Args:
        workers: List of worker status dictionaries
    """
    if not workers:
        logger.error("❌ No workers found!")
        logger.info("💡 Start a worker with: naq worker default")
        return
    
    logger.info(f"👥 Found {len(workers)} active workers:")
    logger.info("-" * 80)
    
    for i, worker in enumerate(workers, 1):
        worker_id = worker.get('worker_id', 'unknown')
        status = worker.get('status', 'unknown')
        queues = worker.get('queues', [])
        concurrency = worker.get('concurrency', 'unknown')
        current_job = worker.get('current_job_id', None)
        last_heartbeat = worker.get('last_heartbeat', None)
        
        # Format status with emoji
        status_emoji = {
            'idle': '😴',
            'busy': '🔄',
            'starting': '🚀',
            'stopping': '🛑'
        }.get(status.lower(), '❓')
        
        logger.info(f"Worker {i}: {worker_id}")
        logger.info(f"  Status: {status_emoji} {status.upper()}")
        logger.info(f"  Queues: {', '.join(queues) if queues else 'none'}")
        logger.info(f"  Concurrency: {concurrency}")
        
        if current_job:
            logger.info(f"  Current Job: {current_job}")
        
        if last_heartbeat:
            try:
                hb_time = datetime.fromtimestamp(last_heartbeat)
                time_diff = datetime.now() - hb_time
                logger.info(f"  Last Heartbeat: {hb_time.strftime('%H:%M:%S')} ({time_diff.seconds}s ago)")
            except Exception:
                logger.info(f"  Last Heartbeat: {last_heartbeat}")
        
        if i < len(workers):
            logger.info("")


def monitor_workers_during_jobs() -> bool:
    """
    Monitor workers while they process jobs.
    
    Returns:
        True if monitoring was successful, False otherwise
    """
    logger.info("🚀 Worker Monitoring Demo")
    logger.info("=" * 50)
    
    # First, check initial worker status
    logger.info("📊 Initial worker status:")
    try:
        workers = list_workers_sync()
        display_worker_info(workers)
    except Exception as e:
        logger.error(f"❌ Could not fetch worker info: {e}")
        logger.info("💡 Make sure NATS is running and workers are started")
        return False
    
    if not workers:
        return False
    
    logger.info("\n" + "=" * 50)
    logger.info("📤 Enqueueing jobs to monitor worker activity...")
    
    # Enqueue some jobs to see workers in action
    # Create configuration
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5,
        max_reconnect_attempts=3
    )
    
    queue_config = QueueConfig(
        name="default",
        stream_name="NAQ_JOBS",
        consumer_name="worker_monitoring_consumer"
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Enqueue long-running tasks
        logger.info("📤 Enqueueing long-running tasks...")
        for i in range(3):
            job = client.enqueue(
                long_running_task,
                task_id=i + 1,
                duration=8
            )
            jobs.append(job)
            logger.info(f"  ✅ Enqueued long task {i + 1} (ID: {job.job_id})")
        
        # Enqueue quick tasks
        logger.info("\n📤 Enqueueing quick tasks...")
        for i in range(5):
            job = client.enqueue(
                quick_task,
                task_id=i + 1
            )
            jobs.append(job)
            logger.info(f"  ✅ Enqueued quick task {i + 1} (ID: {job.job_id})")
    
    logger.info(f"\n🎯 Enqueued {len(jobs)} jobs total")
    logger.info("\n" + "=" * 50)
    logger.info("📈 Monitoring workers as they process jobs...")
    logger.info("(Updates every 5 seconds for 30 seconds)")
    logger.info("-" * 50)
    
    # Monitor workers for 30 seconds
    for round_num in range(6):  # 6 rounds * 5 seconds = 30 seconds
        time.sleep(5)
        
        try:
            workers = list_workers_sync()
            logger.info(f"\n📊 Worker Status Update #{round_num + 1}:")
            display_worker_info(workers)
            
            # Show summary stats
            total_workers = len(workers)
            busy_workers = len([w for w in workers if w.get('status', '').lower() == 'busy'])
            idle_workers = total_workers - busy_workers
            
            logger.info(f"📈 Summary: {total_workers} total, {busy_workers} busy, {idle_workers} idle")
            
        except Exception as e:
            logger.error(f"❌ Error monitoring workers: {e}")
    
    logger.info("\n" + "=" * 50)
    logger.info("🏁 Monitoring completed!")
    
    return True


def main() -> int:
    """
    Main monitoring demonstration.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        success = monitor_workers_during_jobs()
        
        if success:
            logger.info("\n💡 Worker Monitoring Tips:")
            logger.info("=" * 30)
            logger.info("• Use 'naq list-workers' to check worker status anytime")
            logger.info("• Workers send heartbeats every 30 seconds by default")
            logger.info("• Monitor worker logs for detailed job processing info")
            logger.info("• Use the dashboard for web-based monitoring: 'naq dashboard'")
            logger.info("• Scale workers based on queue length and processing time")
            
            logger.info("\n🔧 Worker Management Commands:")
            logger.info("• Start worker: naq worker default")
            logger.info("• Multiple queues: naq worker default emails notifications")
            logger.info("• Custom concurrency: naq worker default --concurrency 5")
            logger.info("• Custom name: naq worker default --worker-name 'web-1'")
            logger.info("• Graceful shutdown: Ctrl+C (SIGTERM)")
            
            logger.info("\n📊 Production Monitoring:")
            logger.info("• Set up monitoring dashboards")
            logger.info("• Alert on worker failures or high queue length")
            logger.info("• Monitor worker CPU/memory usage")
            logger.info("• Track job processing times and error rates")
        else:
            logger.info("\n🔧 Setup Instructions:")
            logger.info("=" * 25)
            logger.info("1. Start NATS server:")
            logger.info("   cd docker && docker-compose up -d")
            logger.info("")
            logger.info("2. Set secure serialization:")
            logger.info("   export NAQ_JOB_SERIALIZER=json")
            logger.info("")
            logger.info("3. Start one or more workers (in separate terminals):")
            logger.info("   naq worker default")
            logger.info("   naq worker default --worker-name 'worker-2'")
            logger.info("")
            logger.info("4. Run this monitoring script again:")
            logger.info("   python worker_monitoring.py")
        
    except KeyboardInterrupt:
        logger.info("\n\n🛑 Monitoring stopped by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())