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

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def simple_task(task_name: str, message: str) -> Dict[str, Any]:
    """
    A simple task for scheduling demonstrations.
    
    Args:
        task_name: Name of the task
        message: Message to process
        
    Returns:
        Task execution results
    """
    print(f"🎯 Executing scheduled task: {task_name}")
    print(f"📝 Message: {message}")
    
    # Simulate some work
    time.sleep(1)
    
    result = {
        "task_name": task_name,
        "message": message,
        "executed_at": datetime.now().isoformat(),
        "execution_time": 1.0,
        "status": "completed"
    }
    
    print(f"✅ Task {task_name} completed successfully")
    return result


def daily_report_task(report_type: str, date: str) -> Dict[str, Any]:
    """
    Simulate a daily report generation task.
    
    Args:
        report_type: Type of report to generate
        date: Date for the report
        
    Returns:
        Report generation results
    """
    print(f"📊 Generating {report_type} report for {date}")
    
    # Simulate report generation
    time.sleep(2)
    
    result = {
        "report_type": report_type,
        "date": date,
        "generated_at": datetime.now().isoformat(),
        "pages": 25,
        "status": "completed"
    }
    
    print(f"✅ {report_type} report generated ({result['pages']} pages)")
    return result


def system_maintenance_task(maintenance_type: str, duration_minutes: int) -> Dict[str, Any]:
    """
    Simulate a system maintenance task.
    
    Args:
        maintenance_type: Type of maintenance
        duration_minutes: Expected duration in minutes
        
    Returns:
        Maintenance results
    """
    print(f"🔧 Starting {maintenance_type} maintenance")
    print(f"⏱️  Expected duration: {duration_minutes} minutes")
    
    # Simulate maintenance work
    time.sleep(min(duration_minutes * 0.1, 3))  # Scale down for demo
    
    result = {
        "maintenance_type": maintenance_type,
        "duration_minutes": duration_minutes,
        "started_at": datetime.now().isoformat(),
        "status": "completed"
    }
    
    print(f"✅ {maintenance_type} maintenance completed")
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
    print(f"📧 Sending {notification_type} notification to {recipient}")
    print(f"💬 Content: {content}")
    
    # Simulate notification sending
    time.sleep(0.5)
    
    print(f"✅ Notification sent to {recipient}")
    return f"Notification sent to {recipient}"


def demonstrate_one_time_scheduling():
    """
    Demonstrate one-time scheduled jobs.
    """
    print("📍 One-Time Scheduled Jobs Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Schedule job to run in 30 seconds
        print("📤 Scheduling job to run in 30 seconds:")
        future_time = datetime.now() + timedelta(seconds=30)
        job1 = client.enqueue_at(
            simple_task,
            run_at=future_time,
            task_name="delayed_task",
            message="This job was scheduled 30 seconds ago",
            queue_name="scheduled_queue"
        )
        jobs.append(job1)
        print(f"  ✅ Job scheduled for {future_time.strftime('%H:%M:%S')}: {job1.job_id}")
        
        # Schedule job to run in 1 minute using enqueue_in
        print("\n📤 Scheduling job to run in 1 minute:")
        job2 = client.enqueue_in(
            simple_task,
            delay=timedelta(minutes=1),
            task_name="reminder_task",
            message="This is your 1-minute reminder",
            queue_name="scheduled_queue"
        )
        jobs.append(job2)
        scheduled_time = datetime.now() + timedelta(minutes=1)
        print(f"  ✅ Job scheduled for {scheduled_time.strftime('%H:%M:%S')}: {job2.job_id}")
        
        # Schedule job to run in 2 minutes
        print("\n📤 Scheduling job to run in 2 minutes:")
        job3 = client.enqueue_in(
            notification_task,
            delay=timedelta(minutes=2),
            recipient="admin@example.com",
            notification_type="system_alert",
            content="Scheduled notification test completed",
            queue_name="scheduled_queue"
        )
        jobs.append(job3)
        scheduled_time = datetime.now() + timedelta(minutes=2)
        print(f"  ✅ Notification scheduled for {scheduled_time.strftime('%H:%M:%S')}: {job3.job_id}")
        
        return jobs


def demonstrate_recurring_jobs():
    """
    Demonstrate recurring jobs with cron expressions.
    """
    print("\n📍 Recurring Jobs Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        schedules = []
        
        # Schedule daily report at 9 AM
        print("📤 Scheduling daily report (9 AM every day):")
        schedule1 = client.schedule(
            daily_report_task,
            cron="0 9 * * *",  # Daily at 9 AM
            report_type="daily_summary",
            date=datetime.now().strftime("%Y-%m-%d"),
            schedule_id="daily-report",
            queue_name="scheduled_queue"
        )
        schedules.append(schedule1)
        print(f"  ✅ Daily report scheduled: {schedule1.schedule_id}")
        print(f"  📅 Cron: 0 9 * * * (daily at 9 AM)")
        
        # Schedule system maintenance every Sunday at 3 AM
        print("\n📤 Scheduling weekly maintenance (Sunday 3 AM):")
        schedule2 = client.schedule(
            system_maintenance_task,
            cron="0 3 * * 0",  # Sunday at 3 AM
            maintenance_type="database_cleanup",
            duration_minutes=30,
            schedule_id="weekly-maintenance",
            queue_name="scheduled_queue"
        )
        schedules.append(schedule2)
        print(f"  ✅ Weekly maintenance scheduled: {schedule2.schedule_id}")
        print(f"  📅 Cron: 0 3 * * 0 (Sunday at 3 AM)")
        
        # Schedule health check every 5 minutes
        print("\n📤 Scheduling frequent health check (every 5 minutes):")
        schedule3 = client.schedule(
            simple_task,
            cron="*/5 * * * *",  # Every 5 minutes
            task_name="health_check",
            message="System health check",
            schedule_id="health-check",
            queue_name="scheduled_queue"
        )
        schedules.append(schedule3)
        print(f"  ✅ Health check scheduled: {schedule3.schedule_id}")
        print(f"  📅 Cron: */5 * * * * (every 5 minutes)")
        
        # Schedule business hours notification (weekdays 9 AM - 5 PM)
        print("\n📤 Scheduling business hours notification (weekdays 9-5):")
        schedule4 = client.schedule(
            notification_task,
            cron="0 9-17 * * 1-5",  # Weekdays, 9 AM to 5 PM
            recipient="team@company.com",
            notification_type="business_hours",
            content="Business hours reminder",
            schedule_id="business-hours",
            queue_name="scheduled_queue"
        )
        schedules.append(schedule4)
        print(f"  ✅ Business hours notification scheduled: {schedule4.schedule_id}")
        print(f"  📅 Cron: 0 9-17 * * 1-5 (weekdays 9 AM - 5 PM)")
        
        return schedules


def demonstrate_schedule_management():
    """
    Demonstrate schedule management operations.
    """
    print("\n📍 Schedule Management Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        # Create a test schedule
        print("📤 Creating test schedule:")
        test_schedule = client.schedule(
            simple_task,
            cron="*/2 * * * *",  # Every 2 minutes
            task_name="test_schedule",
            message="This is a test schedule",
            schedule_id="test-schedule",
            queue_name="scheduled_queue"
        )
        print(f"  ✅ Test schedule created: {test_schedule.schedule_id}")
        
        # List all schedules
        print("\n📋 Listing all schedules:")
        try:
            schedules = client.list_schedules()
            for schedule in schedules:
                print(f"  📅 {schedule['id']}: {schedule['cron']} (enabled: {schedule['enabled']})")
        except Exception as e:
            print(f"  ℹ️  Schedule listing not available in this demo: {e}")
        
        # Get specific schedule details
        print(f"\n🔍 Getting details for test schedule:")
        try:
            schedule_details = client.get_schedule("test-schedule")
            print(f"  📅 Schedule ID: {schedule_details['id']}")
            print(f"  ⏰ Cron: {schedule_details['cron']}")
            print(f"  ✅ Enabled: {schedule_details['enabled']}")
            if 'next_run' in schedule_details:
                print(f"  ⏭️  Next run: {schedule_details['next_run']}")
        except Exception as e:
            print(f"  ℹ️  Schedule details not available in this demo: {e}")
        
        # Update schedule (change frequency)
        print(f"\n📝 Updating test schedule (changing to every 3 minutes):")
        try:
            client.update_schedule("test-schedule", cron="*/3 * * * *")
            print(f"  ✅ Schedule updated to run every 3 minutes")
        except Exception as e:
            print(f"  ℹ️  Schedule update not available in this demo: {e}")
        
        # Disable schedule
        print(f"\n⏸️  Disabling test schedule:")
        try:
            client.update_schedule("test-schedule", enabled=False)
            print(f"  ✅ Schedule disabled")
        except Exception as e:
            print(f"  ℹ️  Schedule disable not available in this demo: {e}")
        
        # Re-enable schedule
        print(f"\n▶️  Re-enabling test schedule:")
        try:
            client.update_schedule("test-schedule", enabled=True)
            print(f"  ✅ Schedule re-enabled")
        except Exception as e:
            print(f"  ℹ️  Schedule enable not available in this demo: {e}")
        
        return test_schedule


def demonstrate_timezone_scheduling():
    """
    Demonstrate timezone-aware scheduling.
    """
    print("\n📍 Timezone-Aware Scheduling Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        schedules = []
        
        # Schedule in different timezones
        timezones = [
            ("UTC", "Global maintenance"),
            ("America/New_York", "US East Coast report"),
            ("Europe/London", "UK business hours"),
            ("Asia/Tokyo", "Japan morning summary")
        ]
        
        print("📤 Scheduling jobs in different timezones:")
        for i, (timezone, description) in enumerate(timezones):
            try:
                schedule = client.schedule(
                    simple_task,
                    cron="0 9 * * 1-5",  # 9 AM weekdays in respective timezone
                    task_name=f"timezone_task_{i+1}",
                    message=description,
                    schedule_id=f"timezone-{timezone.lower().replace('/', '-')}",
                    timezone=timezone,
                    queue_name="scheduled_queue"
                )
                schedules.append(schedule)
                print(f"  ✅ {description}: {schedule.schedule_id} ({timezone})")
            except Exception as e:
                print(f"  ℹ️  Timezone scheduling not available in this demo: {e}")
                # Create without timezone for demo
                schedule = client.schedule(
                    simple_task,
                    cron="0 9 * * 1-5",
                    task_name=f"timezone_task_{i+1}",
                    message=description,
                    schedule_id=f"timezone-{timezone.lower().replace('/', '-')}",
                    queue_name="scheduled_queue"
                )
                schedules.append(schedule)
                print(f"  ✅ {description}: {schedule.schedule_id} (default timezone)")
        
        return schedules


def main():
    """
    Main function demonstrating basic scheduling patterns.
    """
    print("🚀 NAQ Basic Job Scheduling Demo")
    print("=" * 50)
    
    current_time = datetime.now()
    print(f"🕐 Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Demonstrate different scheduling patterns
        one_time_jobs = demonstrate_one_time_scheduling()
        recurring_schedules = demonstrate_recurring_jobs()
        management_demo = demonstrate_schedule_management()
        timezone_schedules = demonstrate_timezone_scheduling()
        
        print(f"\n🎉 Scheduling demo completed!")
        
        print("\n" + "=" * 50)
        print("📊 Scheduling Summary:")
        print("=" * 50)
        print(f"One-time jobs: {len(one_time_jobs)} scheduled")
        print(f"Recurring schedules: {len(recurring_schedules)} created")
        print(f"Timezone schedules: {len(timezone_schedules)} created")
        print(f"Management demo: Schedule operations demonstrated")
        
        print("\n🎯 Scheduling Highlights:")
        print("   • One-time: Jobs scheduled for specific future times")
        print("   • Recurring: Cron-based repeating schedules")
        print("   • Management: Create, update, enable/disable schedules")
        print("   • Timezones: Schedule jobs in different time zones")
        
        print("\n💡 Watch for these events:")
        print("   • One-time jobs executing at scheduled times")
        print("   • Recurring jobs running according to cron schedule")
        print("   • Schedule management operations taking effect")
        
        print("\n📋 Next Steps:")
        print("   • Try recurring_jobs.py for advanced cron patterns")
        print("   • Check schedule_management.py for full lifecycle")
        print("   • Monitor schedules with 'naq list-schedules'")
        print("   • Use 'naq dashboard' for visual schedule tracking")
        
        print("\n⏰ Scheduled Events (next few minutes):")
        print("   • 30 seconds: delayed_task execution")
        print("   • 1 minute: reminder_task execution")
        print("   • 2 minutes: notification_task execution")
        print("   • Every 5 minutes: health_check (if enabled)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Is scheduler running? (naq scheduler --log-level INFO)")
        print("   - Are workers running? (naq worker default scheduled_queue)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())