#!/usr/bin/env python3
"""
Basic Job Scheduling

This example demonstrates fundamental scheduling patterns in NAQ:
- One-time scheduled jobs (enqueue_at, enqueue_in)
- Basic recurring jobs with cron expressions
- Schedule monitoring and management
- Timezone handling

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start scheduler: `naq scheduler --log-level INFO &`
4. Start worker: `naq worker default scheduled_queue --log-level INFO`
5. Run this script: `python basic_scheduling.py`
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any

import msgspec
from loguru import logger
from naq import NatsClient
from naq.config import NatsConfig, QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
logger.add(level="INFO")


class TaskResult(msgspec.Struct):
    """Result of task execution."""
    task_name: str
    message: str
    executed_at: str
    execution_time: float
    status: str


def simple_task(task_name: str, message: str) -> TaskResult:
    """
    A simple task for scheduling demonstrations.
    
    Args:
        task_name: Name of the task
        message: Message to process
        
    Returns:
        Task execution results
    """
    logger.info(f"Executing scheduled task: {task_name}")
    logger.info(f"Message: {message}")
    
    # Simulate some work
    time.sleep(1)
    
    result = TaskResult(
        task_name=task_name,
        message=message,
        executed_at=datetime.now().isoformat(),
        execution_time=1.0,
        status="completed"
    )
    
    logger.info(f"Task {task_name} completed successfully")
    return result


class ReportResult(msgspec.Struct):
    """Result of report generation task."""
    report_type: str
    date: str
    generated_at: str
    pages: int
    status: str


def daily_report_task(report_type: str, date: str) -> ReportResult:
    """
    Simulate a daily report generation task.
    
    Args:
        report_type: Type of report to generate
        date: Date for the report
        
    Returns:
        Report generation results
    """
    logger.info(f"Generating {report_type} report for {date}")
    
    # Simulate report generation
    time.sleep(2)
    
    result = ReportResult(
        report_type=report_type,
        date=date,
        generated_at=datetime.now().isoformat(),
        pages=25,
        status="completed"
    )
    
    logger.info(f"{report_type} report generated ({result.pages} pages)")
    return result


class MaintenanceResult(msgspec.Struct):
    """Result of system maintenance task."""
    maintenance_type: str
    duration_minutes: int
    started_at: str
    status: str


def system_maintenance_task(maintenance_type: str, duration_minutes: int) -> MaintenanceResult:
    """
    Simulate a system maintenance task.
    
    Args:
        maintenance_type: Type of maintenance
        duration_minutes: Expected duration in minutes
        
    Returns:
        Maintenance results
    """
    logger.info(f"Starting {maintenance_type} maintenance")
    logger.info(f"Expected duration: {duration_minutes} minutes")
    
    # Simulate maintenance work
    time.sleep(min(duration_minutes * 0.1, 3))  # Scale down for demo
    
    result = MaintenanceResult(
        maintenance_type=maintenance_type,
        duration_minutes=duration_minutes,
        started_at=datetime.now().isoformat(),
        status="completed"
    )
    
    logger.info(f"{maintenance_type} maintenance completed")
    return result


def notification_task(recipient: str, notification_type: str, content: str) -> str:
    """
    Simulate sending notifications.
    
    Args:
        recipient: Notification recipient
        notification_type: Type of notification
        content: Notification content
        
    Returns:
        Notification status
    """
    logger.info(f"Sending {notification_type} notification to {recipient}")
    logger.info(f"Content: {content}")
    
    # Simulate notification sending
    time.sleep(0.5)
    
    logger.info(f"Notification sent to {recipient}")
    return f"Notification sent to {recipient}"


def demonstrate_one_time_scheduling() -> List[Any]:
    """
    Demonstrate one-time scheduled jobs.
    
    Returns:
        List of created jobs
    """
    logger.info("One-Time Scheduled Jobs Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure scheduled queue
    queue_config = QueueConfig(
        name="scheduled_queue",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Schedule job to run in 30 seconds
        logger.info("Scheduling job to run in 30 seconds:")
        future_time = datetime.now() + timedelta(seconds=30)
        job1 = client.enqueue_at(
            simple_task,
            run_at=future_time,
            task_name="delayed_task",
            message="This job was scheduled 30 seconds ago"
        )
        jobs.append(job1)
        logger.info(f"Job scheduled for {future_time.strftime('%H:%M:%S')}: {job1.job_id}")
        
        # Schedule job to run in 1 minute using enqueue_in
        logger.info("Scheduling job to run in 1 minute:")
        job2 = client.enqueue_in(
            simple_task,
            delay=timedelta(minutes=1),
            task_name="reminder_task",
            message="This is your 1-minute reminder"
        )
        jobs.append(job2)
        scheduled_time = datetime.now() + timedelta(minutes=1)
        logger.info(f"Job scheduled for {scheduled_time.strftime('%H:%M:%S')}: {job2.job_id}")
        
        # Schedule job to run in 2 minutes
        logger.info("Scheduling job to run in 2 minutes:")
        job3 = client.enqueue_in(
            notification_task,
            delay=timedelta(minutes=2),
            recipient="admin@example.com",
            notification_type="system_alert",
            content="Scheduled notification test completed"
        )
        jobs.append(job3)
        scheduled_time = datetime.now() + timedelta(minutes=2)
        logger.info(f"Notification scheduled for {scheduled_time.strftime('%H:%M:%S')}: {job3.job_id}")
        
        return jobs


def demonstrate_recurring_jobs() -> List[Any]:
    """
    Demonstrate recurring jobs with cron expressions.
    
    Returns:
        List of created schedules
    """
    logger.info("Recurring Jobs Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure scheduled queue
    queue_config = QueueConfig(
        name="scheduled_queue",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        schedules = []
        
        # Schedule daily report at 9 AM
        logger.info("Scheduling daily report (9 AM every day):")
        schedule1 = client.schedule(
            daily_report_task,
            cron="0 9 * * *",  # Daily at 9 AM
            report_type="daily_summary",
            date=datetime.now().strftime("%Y-%m-%d"),
            schedule_id="daily-report"
        )
        schedules.append(schedule1)
        logger.info(f"Daily report scheduled: {schedule1.schedule_id}")
        logger.info(f"Cron: 0 9 * * * (daily at 9 AM)")
        
        # Schedule system maintenance every Sunday at 3 AM
        logger.info("Scheduling weekly maintenance (Sunday 3 AM):")
        schedule2 = client.schedule(
            system_maintenance_task,
            cron="0 3 * * 0",  # Sunday at 3 AM
            maintenance_type="database_cleanup",
            duration_minutes=30,
            schedule_id="weekly-maintenance"
        )
        schedules.append(schedule2)
        logger.info(f"Weekly maintenance scheduled: {schedule2.schedule_id}")
        logger.info(f"Cron: 0 3 * * 0 (Sunday at 3 AM)")
        
        # Schedule health check every 5 minutes
        logger.info("Scheduling frequent health check (every 5 minutes):")
        schedule3 = client.schedule(
            simple_task,
            cron="*/5 * * * *",  # Every 5 minutes
            task_name="health_check",
            message="System health check",
            schedule_id="health-check"
        )
        schedules.append(schedule3)
        logger.info(f"Health check scheduled: {schedule3.schedule_id}")
        logger.info(f"Cron: */5 * * * * (every 5 minutes)")
        
        # Schedule business hours notification (weekdays 9 AM - 5 PM)
        logger.info("Scheduling business hours notification (weekdays 9-5):")
        schedule4 = client.schedule(
            notification_task,
            cron="0 9-17 * * 1-5",  # Weekdays, 9 AM to 5 PM
            recipient="team@company.com",
            notification_type="business_hours",
            content="Business hours reminder",
            schedule_id="business-hours"
        )
        schedules.append(schedule4)
        logger.info(f"Business hours notification scheduled: {schedule4.schedule_id}")
        logger.info(f"Cron: 0 9-17 * * 1-5 (weekdays 9 AM - 5 PM)")
        
        return schedules


def demonstrate_schedule_management() -> Any:
    """
    Demonstrate schedule management operations.
    
    Returns:
        Created test schedule
    """
    logger.info("Schedule Management Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure scheduled queue
    queue_config = QueueConfig(
        name="scheduled_queue",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Create a test schedule
        logger.info("Creating test schedule:")
        test_schedule = client.schedule(
            simple_task,
            cron="*/2 * * * *",  # Every 2 minutes
            task_name="test_schedule",
            message="This is a test schedule",
            schedule_id="test-schedule"
        )
        logger.info(f"Test schedule created: {test_schedule.schedule_id}")
        
        # List all schedules
        logger.info("Listing all schedules:")
        try:
            schedules = client.list_schedules()
            for schedule in schedules:
                logger.info(f"{schedule['id']}: {schedule['cron']} (enabled: {schedule['enabled']})")
        except Exception as e:
            logger.info(f"Schedule listing not available in this demo: {e}")
        
        # Get specific schedule details
        logger.info(f"Getting details for test schedule:")
        try:
            schedule_details = client.get_schedule("test-schedule")
            logger.info(f"Schedule ID: {schedule_details['id']}")
            logger.info(f"Cron: {schedule_details['cron']}")
            logger.info(f"Enabled: {schedule_details['enabled']}")
            if 'next_run' in schedule_details:
                logger.info(f"Next run: {schedule_details['next_run']}")
        except Exception as e:
            logger.info(f"Schedule details not available in this demo: {e}")
        
        # Update schedule (change frequency)
        logger.info(f"Updating test schedule (changing to every 3 minutes):")
        try:
            client.update_schedule("test-schedule", cron="*/3 * * * *")
            logger.info(f"Schedule updated to run every 3 minutes")
        except Exception as e:
            logger.info(f"Schedule update not available in this demo: {e}")
        
        # Disable schedule
        logger.info(f"Disabling test schedule:")
        try:
            client.update_schedule("test-schedule", enabled=False)
            logger.info(f"Schedule disabled")
        except Exception as e:
            logger.info(f"Schedule disable not available in this demo: {e}")
        
        # Re-enable schedule
        logger.info(f"Re-enabling test schedule:")
        try:
            client.update_schedule("test-schedule", enabled=True)
            logger.info(f"Schedule re-enabled")
        except Exception as e:
            logger.info(f"Schedule enable not available in this demo: {e}")
        
        return test_schedule


def demonstrate_timezone_scheduling() -> List[Any]:
    """
    Demonstrate timezone-aware scheduling.
    
    Returns:
        List of created schedules
    """
    logger.info("Timezone-Aware Scheduling Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure scheduled queue
    queue_config = QueueConfig(
        name="scheduled_queue",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        schedules = []
        
        # Schedule in different timezones
        timezones = [
            ("UTC", "Global maintenance"),
            ("America/New_York", "US East Coast report"),
            ("Europe/London", "UK business hours"),
            ("Asia/Tokyo", "Japan morning summary")
        ]
        
        logger.info("Scheduling jobs in different timezones:")
        for i, (timezone, description) in enumerate(timezones):
            try:
                schedule = client.schedule(
                    simple_task,
                    cron="0 9 * * 1-5",  # 9 AM weekdays in respective timezone
                    task_name=f"timezone_task_{i+1}",
                    message=description,
                    schedule_id=f"timezone-{timezone.lower().replace('/', '-')}",
                    timezone=timezone
                )
                schedules.append(schedule)
                logger.info(f"{description}: {schedule.schedule_id} ({timezone})")
            except Exception as e:
                logger.info(f"Timezone scheduling not available in this demo: {e}")
                # Create without timezone for demo
                schedule = client.schedule(
                    simple_task,
                    cron="0 9 * * 1-5",
                    task_name=f"timezone_task_{i+1}",
                    message=description,
                    schedule_id=f"timezone-{timezone.lower().replace('/', '-')}"
                )
                schedules.append(schedule)
                logger.info(f"{description}: {schedule.schedule_id} (default timezone)")
        
        return schedules


def main() -> int:
    """
    Main function demonstrating basic scheduling patterns.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("NAQ Basic Job Scheduling Demo")
    
    current_time = datetime.now()
    logger.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Demonstrate different scheduling patterns
        one_time_jobs = demonstrate_one_time_scheduling()
        recurring_schedules = demonstrate_recurring_jobs()
        management_demo = demonstrate_schedule_management()
        timezone_schedules = demonstrate_timezone_scheduling()
        
        logger.info(f"Scheduling demo completed!")
        
        logger.info("Scheduling Summary:")
        logger.info(f"One-time jobs: {len(one_time_jobs)} scheduled")
        logger.info(f"Recurring schedules: {len(recurring_schedules)} created")
        logger.info(f"Timezone schedules: {len(timezone_schedules)} created")
        logger.info(f"Management demo: Schedule operations demonstrated")
        
        logger.info("Scheduling Highlights:")
        logger.info("   • One-time: Jobs scheduled for specific future times")
        logger.info("   • Recurring: Cron-based repeating schedules")
        logger.info("   • Management: Create, update, enable/disable schedules")
        logger.info("   • Timezones: Schedule jobs in different time zones")
        
        logger.info("Watch for these events:")
        logger.info("   • One-time jobs executing at scheduled times")
        logger.info("   • Recurring jobs running according to cron schedule")
        logger.info("   • Schedule management operations taking effect")
        
        logger.info("Next Steps:")
        logger.info("   • Try recurring_jobs.py for advanced cron patterns")
        logger.info("   • Check schedule_management.py for full lifecycle")
        logger.info("   • Monitor schedules with 'naq list-schedules'")
        logger.info("   • Use 'naq dashboard' for visual schedule tracking")
        
        logger.info("Scheduled Events (next few minutes):")
        logger.info("   • 30 seconds: delayed_task execution")
        logger.info("   • 1 minute: reminder_task execution")
        logger.info("   • 2 minutes: notification_task execution")
        logger.info("   • Every 5 minutes: health_check (if enabled)")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.info("Troubleshooting:")
        logger.info("   - Is NATS running? (cd docker && docker-compose up -d)")
        logger.info("   - Is scheduler running? (naq scheduler --log-level INFO)")
        logger.info("   - Are workers running? (naq worker default scheduled_queue)")
        logger.info("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())