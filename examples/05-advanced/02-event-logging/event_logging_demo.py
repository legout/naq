#!/usr/bin/env python3
"""
NAQ Event Logging System Demo

This example demonstrates the comprehensive event logging system in NAQ:
- Job lifecycle event tracking
- Worker event monitoring
- Event filtering and analysis
- Real-time event streaming
- Performance metrics collection

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default event_queue --log-level INFO`
4. Start event monitoring: `naq events list --follow` (in another terminal)
5. Run this script: `python event_logging_demo.py`
"""

import os
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from naq import SyncClient, setup_logging
from naq.models.events import JobEvent, WorkerEvent
from naq.models.enums import JobEventType, WorkerEventType

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


# ===== JOB FUNCTIONS FOR DEMONSTRATION =====

def data_processing_task(data_size: int, processing_time: float = 1.0) -> str:
    """
    Simulate a data processing task that generates various events.
    
    Args:
        data_size: Size of data to process (affects duration)
        processing_time: Base processing time in seconds
        
    Returns:
        Processing result
    """
    task_id = str(uuid.uuid4())[:8]
    
    print(f"🔄 Processing data: {data_size} records")
    print(f"   Task ID: {task_id}")
    
    # Simulate processing stages
    stages = ["validation", "transformation", "aggregation", "output"]
    total_time = processing_time * (1 + data_size / 1000)  # Scale with data size
    
    for i, stage in enumerate(stages):
        stage_time = total_time / len(stages)
        print(f"   📊 Stage {i+1}: {stage}")
        time.sleep(stage_time)
        
        # Random chance of stage failure (very low)
        if random.random() < 0.01:
            raise RuntimeError(f"Processing failed at {stage} stage")
    
    result = f"Processed {data_size} records in {total_time:.1f}s ({task_id})"
    print(f"✅ {result}")
    
    return result


def network_request_task(endpoint: str, timeout: float = 5.0) -> str:
    """
    Simulate a network request that may fail and retry.
    
    Args:
        endpoint: API endpoint to call
        timeout: Request timeout
        
    Returns:
        Request result
    """
    request_id = str(uuid.uuid4())[:8]
    
    print(f"🌐 Making network request: {endpoint}")
    print(f"   Request ID: {request_id}")
    print(f"   Timeout: {timeout}s")
    
    # Simulate network delay
    time.sleep(random.uniform(0.5, 2.0))
    
    # Simulate various failure types
    failure_chance = random.random()
    
    if failure_chance < 0.2:  # 20% chance of timeout
        raise TimeoutError(f"Request to {endpoint} timed out after {timeout}s")
    elif failure_chance < 0.3:  # 10% chance of connection error
        raise ConnectionError(f"Failed to connect to {endpoint}")
    elif failure_chance < 0.35:  # 5% chance of server error
        raise RuntimeError(f"Server error from {endpoint}: 500 Internal Server Error")
    
    # Success
    result = f"Network request successful: {endpoint} ({request_id})"
    print(f"✅ {result}")
    
    return result


def batch_processing_job(batch_id: str, items_count: int) -> str:
    """
    Process a batch of items with detailed progress tracking.
    
    Args:
        batch_id: Unique batch identifier
        items_count: Number of items to process
        
    Returns:
        Batch processing result
    """
    print(f"📦 Processing batch: {batch_id}")
    print(f"   Items: {items_count}")
    
    processed = 0
    failed = 0
    batch_size = 10
    
    # Process items in smaller batches
    for i in range(0, items_count, batch_size):
        batch_end = min(i + batch_size, items_count)
        current_batch = batch_end - i
        
        print(f"   🔄 Processing items {i+1}-{batch_end}")
        
        # Simulate batch processing
        time.sleep(0.5)
        
        # Simulate some failures
        batch_failures = 0
        for _ in range(current_batch):
            if random.random() < 0.02:  # 2% failure rate per item
                batch_failures += 1
        
        processed += current_batch - batch_failures
        failed += batch_failures
        
        if batch_failures > 0:
            print(f"     ⚠️  {batch_failures} items failed in this batch")
    
    success_rate = (processed / items_count) * 100 if items_count > 0 else 0
    
    if failed > items_count * 0.1:  # More than 10% failed
        raise RuntimeError(f"Batch {batch_id} failed: {failed}/{items_count} items failed")
    
    result = f"Batch {batch_id} processed: {processed}/{items_count} successful ({success_rate:.1f}%)"
    print(f"✅ {result}")
    
    return result


def scheduled_maintenance_task(maintenance_type: str) -> str:
    """
    Simulate a scheduled maintenance task.
    
    Args:
        maintenance_type: Type of maintenance to perform
        
    Returns:
        Maintenance result
    """
    print(f"🔧 Running maintenance: {maintenance_type}")
    
    maintenance_steps = {
        "cleanup": ["clean_temp_files", "purge_old_logs", "optimize_database"],
        "backup": ["backup_database", "backup_config", "verify_backups"],
        "monitoring": ["check_disk_space", "check_memory_usage", "check_connections"],
        "security": ["update_certificates", "scan_vulnerabilities", "audit_permissions"]
    }
    
    steps = maintenance_steps.get(maintenance_type, ["generic_maintenance"])
    
    for i, step in enumerate(steps):
        print(f"   🔄 Step {i+1}: {step}")
        time.sleep(0.8)
        
        # Random chance of step failure (low)
        if random.random() < 0.05:
            raise RuntimeError(f"Maintenance step '{step}' failed")
    
    result = f"Maintenance '{maintenance_type}' completed successfully ({len(steps)} steps)"
    print(f"✅ {result}")
    
    return result


# ===== EVENT MONITORING AND ANALYSIS =====

def simulate_event_generation(events_to_create: int = 50) -> str:
    """
    Generate a variety of events for demonstration purposes.
    
    Args:
        events_to_create: Number of events to generate
        
    Returns:
        Event generation summary
    """
    print(f"📊 Generating {events_to_create} demonstration events")
    
    # Create job events programmatically
    job_events = []
    worker_events = []
    
    # Simulate different job types and their events
    job_types = [
        ("data_proc_001", "Data Processing Job"),
        ("network_req_002", "Network Request Job"), 
        ("batch_proc_003", "Batch Processing Job"),
        ("maintenance_004", "Scheduled Maintenance")
    ]
    
    worker_ids = ["worker_001", "worker_002", "worker_003"]
    queue_names = ["default", "event_queue", "maintenance", "high_priority"]
    
    print("   🔄 Creating job lifecycle events...")
    
    for i in range(events_to_create // 4):  # 25% for each event type
        job_id = f"job_{i:03d}"
        worker_id = random.choice(worker_ids)
        queue_name = random.choice(queue_names)
        
        # Create ENQUEUED event
        enqueued_event = JobEvent.enqueued(
            job_id=job_id,
            queue_name=queue_name,
            details={"priority": random.choice(["low", "normal", "high"])}
        )
        job_events.append(enqueued_event)
        
        # Create STARTED event
        started_event = JobEvent.started(
            job_id=job_id,
            worker_id=worker_id,
            queue_name=queue_name
        )
        job_events.append(started_event)
        
        # Randomly create SUCCESS or FAILURE
        if random.random() < 0.85:  # 85% success rate
            duration_ms = random.randint(500, 5000)
            completed_event = JobEvent.completed(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                duration_ms=duration_ms,
                details={"performance_metrics": {"cpu_usage": random.uniform(20, 80)}}
            )
            job_events.append(completed_event)
        else:
            # Failed job - might have retries
            duration_ms = random.randint(1000, 3000)
            error_types = ["TimeoutError", "ConnectionError", "RuntimeError", "ValueError"]
            error_type = random.choice(error_types)
            
            failed_event = JobEvent.failed(
                job_id=job_id,
                worker_id=worker_id,
                queue_name=queue_name,
                error_type=error_type,
                error_message=f"Simulated {error_type.lower()}",
                duration_ms=duration_ms
            )
            job_events.append(failed_event)
            
            # Maybe schedule a retry
            if random.random() < 0.7:  # 70% of failed jobs get retried
                retry_event = JobEvent.retry_scheduled(
                    job_id=job_id,
                    worker_id=worker_id,
                    queue_name=queue_name,
                    retry_count=1,
                    retry_delay=random.choice([2, 5, 10])
                )
                job_events.append(retry_event)
    
    print("   🔄 Creating worker events...")
    
    # Create worker events
    for worker_id in worker_ids:
        # Worker started
        started_event = WorkerEvent.worker_started(
            worker_id=worker_id,
            hostname=f"worker-host-{worker_id[-1]}",
            pid=random.randint(1000, 9999),
            queue_names=random.sample(queue_names, k=2),
            concurrency_limit=random.choice([1, 2, 4])
        )
        worker_events.append(started_event)
        
        # Worker heartbeats
        for _ in range(3):
            heartbeat_event = WorkerEvent.worker_heartbeat(
                worker_id=worker_id,
                hostname=f"worker-host-{worker_id[-1]}",
                pid=random.randint(1000, 9999),
                active_jobs=random.randint(0, 2),
                concurrency_limit=2,
                current_job_id=f"job_{random.randint(1, 100):03d}" if random.random() < 0.6 else None
            )
            worker_events.append(heartbeat_event)
    
    # Simulate processing these events
    time.sleep(1)
    
    total_events = len(job_events) + len(worker_events)
    
    result = f"Generated {total_events} events ({len(job_events)} job, {len(worker_events)} worker)"
    print(f"✅ {result}")
    
    return result


def analyze_event_patterns(analysis_type: str = "performance") -> str:
    """
    Analyze event patterns for insights.
    
    Args:
        analysis_type: Type of analysis to perform
        
    Returns:
        Analysis result
    """
    print(f"📈 Analyzing event patterns: {analysis_type}")
    
    # Simulate event data analysis
    time.sleep(1.5)
    
    if analysis_type == "performance":
        # Performance analysis
        metrics = {
            "avg_job_duration": random.uniform(2.5, 8.5),
            "success_rate": random.uniform(0.85, 0.95),
            "avg_queue_time": random.uniform(0.1, 2.0),
            "peak_concurrency": random.randint(8, 24),
            "worker_utilization": random.uniform(0.65, 0.85)
        }
        
        print("   📊 Performance Metrics:")
        print(f"     Average job duration: {metrics['avg_job_duration']:.1f}s")
        print(f"     Success rate: {metrics['success_rate']:.1%}")
        print(f"     Average queue time: {metrics['avg_queue_time']:.1f}s")
        print(f"     Peak concurrency: {metrics['peak_concurrency']} jobs")
        print(f"     Worker utilization: {metrics['worker_utilization']:.1%}")
        
        result = f"Performance analysis: {metrics['success_rate']:.1%} success rate, {metrics['avg_job_duration']:.1f}s avg duration"
        
    elif analysis_type == "errors":
        # Error analysis
        error_stats = {
            "TimeoutError": random.randint(5, 15),
            "ConnectionError": random.randint(2, 8), 
            "RuntimeError": random.randint(3, 12),
            "ValueError": random.randint(1, 5)
        }
        
        total_errors = sum(error_stats.values())
        
        print("   ❌ Error Analysis:")
        for error_type, count in error_stats.items():
            percentage = (count / total_errors) * 100
            print(f"     {error_type}: {count} ({percentage:.1f}%)")
        
        most_common = max(error_stats.items(), key=lambda x: x[1])
        result = f"Error analysis: {total_errors} total errors, most common: {most_common[0]} ({most_common[1]})"
        
    elif analysis_type == "trends":
        # Trend analysis
        trends = {
            "job_volume_trend": random.choice(["increasing", "decreasing", "stable"]),
            "error_rate_trend": random.choice(["improving", "degrading", "stable"]),
            "performance_trend": random.choice(["improving", "degrading", "stable"]),
            "peak_hours": [9, 10, 14, 15, 16]  # Business hours
        }
        
        print("   📈 Trend Analysis:")
        print(f"     Job volume: {trends['job_volume_trend']}")
        print(f"     Error rate: {trends['error_rate_trend']}")
        print(f"     Performance: {trends['performance_trend']}")
        print(f"     Peak hours: {trends['peak_hours']}")
        
        result = f"Trend analysis: job volume {trends['job_volume_trend']}, performance {trends['performance_trend']}"
        
    else:
        result = f"Unknown analysis type: {analysis_type}"
    
    print(f"✅ {result}")
    
    return result


# ===== DEMONSTRATION FUNCTIONS =====

def demonstrate_job_lifecycle_events():
    """Demonstrate job lifecycle event generation."""
    print("📍 Job Lifecycle Events Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Create various job types that will generate different events
        print("📤 Creating jobs with different characteristics...")
        
        # Quick processing jobs
        for i in range(3):
            job = client.enqueue(
                data_processing_task,
                data_size=random.randint(100, 500),
                processing_time=0.5,
                queue_name="event_queue"
            )
            jobs.append(job)
            print(f"  ✅ Quick data job {i+1}: {job.job_id}")
        
        # Network jobs (higher failure rate)
        endpoints = ["/api/users", "/api/orders", "/api/reports"]
        for i, endpoint in enumerate(endpoints):
            job = client.enqueue(
                network_request_task,
                endpoint=endpoint,
                timeout=3.0,
                queue_name="event_queue",
                max_retries=2,
                retry_delay=1,
                retry_strategy="linear"
            )
            jobs.append(job)
            print(f"  ✅ Network job {i+1}: {job.job_id}")
        
        # Batch processing jobs
        for i in range(2):
            job = client.enqueue(
                batch_processing_job,
                batch_id=f"BATCH_{i+1:03d}",
                items_count=random.randint(50, 200),
                queue_name="event_queue"
            )
            jobs.append(job)
            print(f"  ✅ Batch job {i+1}: {job.job_id}")
        
        return jobs


def demonstrate_scheduled_job_events():
    """Demonstrate scheduled job event generation."""
    print("\n📍 Scheduled Job Events Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Schedule maintenance tasks
        maintenance_types = ["cleanup", "backup", "monitoring"]
        
        print("📤 Scheduling maintenance tasks...")
        for i, maintenance_type in enumerate(maintenance_types):
            # Schedule for different times
            schedule_delay = (i + 1) * 2  # 2, 4, 6 seconds from now
            
            job = client.enqueue_in(
                scheduled_maintenance_task,
                maintenance_type=maintenance_type,
                delay=timedelta(seconds=schedule_delay),
                queue_name="maintenance"
            )
            jobs.append(job)
            print(f"  ✅ Scheduled {maintenance_type} in {schedule_delay}s: {job.job_id}")
        
        return jobs


def demonstrate_event_monitoring():
    """Demonstrate event monitoring and analysis."""
    print("\n📍 Event Monitoring Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Generate events for analysis
        print("📤 Generating events for monitoring...")
        job1 = client.enqueue(
            simulate_event_generation,
            events_to_create=30,
            queue_name="event_queue"
        )
        jobs.append(job1)
        print(f"  ✅ Event generation job: {job1.job_id}")
        
        # Schedule analysis jobs
        analysis_types = ["performance", "errors", "trends"]
        
        print("\n📤 Scheduling event analysis...")
        for analysis_type in analysis_types:
            job = client.enqueue(
                analyze_event_patterns,
                analysis_type=analysis_type,
                queue_name="event_queue"
            )
            jobs.append(job)
            print(f"  ✅ {analysis_type.title()} analysis: {job.job_id}")
        
        return jobs


def demonstrate_high_volume_events():
    """Demonstrate handling of high-volume event scenarios."""
    print("\n📍 High Volume Events Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Create many small jobs to generate lots of events
        print("📤 Creating high-volume job batch...")
        
        job_count = 15
        for i in range(job_count):
            # Mix of different job types
            if i % 3 == 0:
                job = client.enqueue(
                    data_processing_task,
                    data_size=random.randint(10, 100),
                    processing_time=0.2,
                    queue_name="event_queue"
                )
            elif i % 3 == 1:
                job = client.enqueue(
                    network_request_task,
                    endpoint=f"/api/test/{i}",
                    timeout=2.0,
                    queue_name="event_queue"
                )
            else:
                job = client.enqueue(
                    batch_processing_job,
                    batch_id=f"MINI_{i:03d}",
                    items_count=random.randint(10, 50),
                    queue_name="event_queue"
                )
            
            jobs.append(job)
        
        print(f"  ✅ Created {len(jobs)} jobs for high-volume event generation")
        
        return jobs


def main():
    """
    Main function demonstrating the NAQ event logging system.
    """
    print("📊 NAQ Event Logging System Demo")
    print("=" * 50)
    
    # Check configuration
    serializer = os.environ.get('NAQ_JOB_SERIALIZER', 'pickle')
    if serializer != 'json':
        print("❌ SECURITY WARNING: Not using JSON serialization!")
        print("   Set: export NAQ_JOB_SERIALIZER=json")
        return 1
    else:
        print("✅ Using secure JSON serialization")
    
    print("\n🔍 Event Monitoring Instructions:")
    print("   Run in another terminal: naq events list --follow")
    print("   This will show real-time events as jobs are processed")
    
    try:
        # Run demonstrations
        lifecycle_jobs = demonstrate_job_lifecycle_events()
        scheduled_jobs = demonstrate_scheduled_job_events()
        monitoring_jobs = demonstrate_event_monitoring()
        volume_jobs = demonstrate_high_volume_events()
        
        # Summary
        total_jobs = len(lifecycle_jobs) + len(scheduled_jobs) + len(monitoring_jobs) + len(volume_jobs)
        
        print(f"\n🎉 Event logging demo completed!")
        print(f"📊 Enqueued {total_jobs} jobs total:")
        print(f"   🔄 {len(lifecycle_jobs)} lifecycle demonstration jobs")
        print(f"   📅 {len(scheduled_jobs)} scheduled maintenance jobs")
        print(f"   📈 {len(monitoring_jobs)} event monitoring jobs")
        print(f"   📊 {len(volume_jobs)} high-volume demonstration jobs")
        
        print("\n📊 Event Types You'll See:")
        print("   • Job Events: ENQUEUED, STARTED, COMPLETED, FAILED")
        print("   • Retry Events: RETRY_SCHEDULED")
        print("   • Schedule Events: SCHEDULED, SCHEDULE_TRIGGERED")
        print("   • Worker Events: WORKER_BUSY, WORKER_IDLE, WORKER_HEARTBEAT")
        print("   • Error Events: Job failures with error details")
        
        print("\n🔍 Monitoring Features Demonstrated:")
        print("   • Real-time event streaming")
        print("   • Job lifecycle tracking")
        print("   • Performance metrics collection")
        print("   • Error pattern analysis")
        print("   • Scheduled job monitoring")
        print("   • High-volume event handling")
        
        print("\n💡 Watch for these in your event stream:")
        print("   - Job enqueue → start → completion flow")
        print("   - Failed jobs with retry scheduling")
        print("   - Worker status changes")
        print("   - Performance metrics in event details")
        print("   - Scheduled job triggers")
        
        print("\n🔧 Event CLI Commands:")
        print("   naq events list                 # List recent events")
        print("   naq events list --follow        # Stream events in real-time")
        print("   naq events list --type=failed   # Filter by event type")
        print("   naq events list --job-id=<id>   # Events for specific job")
        
        print("\n📋 Next Steps:")
        print("   - Set up event storage and persistence")
        print("   - Create event-based monitoring dashboards")
        print("   - Implement alerting on error patterns")
        print("   - Build performance analytics from events")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default event_queue)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Try: naq events list (to check event system)")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())