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

from naq import list_workers_sync, SyncClient, setup_logging

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
    print(f"🔄 Long task {task_id} starting (will run {duration}s)")
    
    for i in range(duration):
        time.sleep(1)
        if i % 3 == 0:  # Progress update every 3 seconds
            print(f"  📊 Task {task_id} progress: {i+1}/{duration} seconds")
    
    result = f"Long task {task_id} completed after {duration} seconds"
    print(f"✅ {result}")
    
    return result


def quick_task(task_id: int) -> str:
    """
    A quick task for comparison.
    
    Args:
        task_id: Unique task identifier
        
    Returns:
        Task completion message
    """
    print(f"⚡ Quick task {task_id} executing...")
    time.sleep(1)
    result = f"Quick task {task_id} completed"
    print(f"✅ {result}")
    return result


def display_worker_info(workers: List[Dict[str, Any]]):
    """
    Display formatted worker information.
    
    Args:
        workers: List of worker status dictionaries
    """
    if not workers:
        print("❌ No workers found!")
        print("💡 Start a worker with: naq worker default")
        return
    
    print(f"👥 Found {len(workers)} active workers:")
    print("-" * 80)
    
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
        
        print(f"Worker {i}: {worker_id}")
        print(f"  Status: {status_emoji} {status.upper()}")
        print(f"  Queues: {', '.join(queues) if queues else 'none'}")
        print(f"  Concurrency: {concurrency}")
        
        if current_job:
            print(f"  Current Job: {current_job}")
        
        if last_heartbeat:
            try:
                hb_time = datetime.fromtimestamp(last_heartbeat)
                time_diff = datetime.now() - hb_time
                print(f"  Last Heartbeat: {hb_time.strftime('%H:%M:%S')} ({time_diff.seconds}s ago)")
            except:
                print(f"  Last Heartbeat: {last_heartbeat}")
        
        if i < len(workers):
            print()


def monitor_workers_during_jobs():
    """
    Monitor workers while they process jobs.
    """
    print("🚀 Worker Monitoring Demo")
    print("=" * 50)
    
    # First, check initial worker status
    print("📊 Initial worker status:")
    try:
        workers = list_workers_sync()
        display_worker_info(workers)
    except Exception as e:
        print(f"❌ Could not fetch worker info: {e}")
        print("💡 Make sure NATS is running and workers are started")
        return False
    
    if not workers:
        return False
    
    print("\n" + "=" * 50)
    print("📤 Enqueueing jobs to monitor worker activity...")
    
    # Enqueue some jobs to see workers in action
    with SyncClient() as client:
        jobs = []
        
        # Enqueue long-running tasks
        print("📤 Enqueueing long-running tasks...")
        for i in range(3):
            job = client.enqueue(
                long_running_task,
                task_id=i + 1,
                duration=8,
                queue_name="default"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued long task {i + 1} (ID: {job.job_id})")
        
        # Enqueue quick tasks
        print("\n📤 Enqueueing quick tasks...")
        for i in range(5):
            job = client.enqueue(
                quick_task,
                task_id=i + 1,
                queue_name="default"
            )
            jobs.append(job)
            print(f"  ✅ Enqueued quick task {i + 1} (ID: {job.job_id})")
    
    print(f"\n🎯 Enqueued {len(jobs)} jobs total")
    print("\n" + "=" * 50)
    print("📈 Monitoring workers as they process jobs...")
    print("(Updates every 5 seconds for 30 seconds)")
    print("-" * 50)
    
    # Monitor workers for 30 seconds
    for round_num in range(6):  # 6 rounds * 5 seconds = 30 seconds
        time.sleep(5)
        
        try:
            workers = list_workers_sync()
            print(f"\n📊 Worker Status Update #{round_num + 1}:")
            display_worker_info(workers)
            
            # Show summary stats
            total_workers = len(workers)
            busy_workers = len([w for w in workers if w.get('status', '').lower() == 'busy'])
            idle_workers = total_workers - busy_workers
            
            print(f"📈 Summary: {total_workers} total, {busy_workers} busy, {idle_workers} idle")
            
        except Exception as e:
            print(f"❌ Error monitoring workers: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 Monitoring completed!")
    
    return True


def main():
    """
    Main monitoring demonstration.
    """
    try:
        success = monitor_workers_during_jobs()
        
        if success:
            print("\n💡 Worker Monitoring Tips:")
            print("=" * 30)
            print("• Use 'naq list-workers' to check worker status anytime")
            print("• Workers send heartbeats every 30 seconds by default")
            print("• Monitor worker logs for detailed job processing info")
            print("• Use the dashboard for web-based monitoring: 'naq dashboard'")
            print("• Scale workers based on queue length and processing time")
            
            print("\n🔧 Worker Management Commands:")
            print("• Start worker: naq worker default")
            print("• Multiple queues: naq worker default emails notifications")
            print("• Custom concurrency: naq worker default --concurrency 5")
            print("• Custom name: naq worker default --worker-name 'web-1'")
            print("• Graceful shutdown: Ctrl+C (SIGTERM)")
            
            print("\n📊 Production Monitoring:")
            print("• Set up monitoring dashboards")
            print("• Alert on worker failures or high queue length")
            print("• Monitor worker CPU/memory usage")
            print("• Track job processing times and error rates")
        else:
            print("\n🔧 Setup Instructions:")
            print("=" * 25)
            print("1. Start NATS server:")
            print("   cd docker && docker-compose up -d")
            print()
            print("2. Set secure serialization:")
            print("   export NAQ_JOB_SERIALIZER=json")
            print()
            print("3. Start one or more workers (in separate terminals):")
            print("   naq worker default")
            print("   naq worker default --worker-name 'worker-2'")
            print()
            print("4. Run this monitoring script again:")
            print("   python worker_monitoring.py")
        
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())