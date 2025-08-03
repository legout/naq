# Scheduled Jobs - Time-Based Processing

This example demonstrates NAQ's comprehensive scheduling system for time-based job execution, including cron-style scheduling, recurring jobs, and scheduled job management.

## What You'll Learn

- Cron-style job scheduling
- One-time scheduled jobs
- Recurring job patterns
- Schedule management and monitoring
- Timezone handling and best practices

## Scheduling Types

### One-Time Scheduled Jobs
Jobs that run at a specific future time:
```python
from datetime import datetime, timedelta

# Schedule job to run in 1 hour
future_time = datetime.now() + timedelta(hours=1)
job = enqueue_at_sync(process_data, run_at=future_time)

# Schedule job to run in 30 minutes  
job = enqueue_in_sync(send_reminder, delay=timedelta(minutes=30))
```

### Recurring Jobs (Cron-style)
Jobs that run on a schedule:
```python
# Every day at 2 AM
job = schedule_sync(daily_backup, cron="0 2 * * *")

# Every weekday at 9 AM
job = schedule_sync(send_reports, cron="0 9 * * 1-5")

# Every 15 minutes
job = schedule_sync(health_check, cron="*/15 * * * *")
```

### Interval-Based Scheduling
Simple recurring intervals:
```python
# Every 5 minutes
job = schedule_sync(monitor_system, interval=timedelta(minutes=5))

# Every hour
job = schedule_sync(cleanup_temp_files, interval=timedelta(hours=1))
```

## Cron Expression Format

NAQ uses standard cron expressions:

```
┌───────────── minute (0 - 59)
│ ┌─────────── hour (0 - 23)
│ │ ┌───────── day of month (1 - 31)
│ │ │ ┌─────── month (1 - 12)
│ │ │ │ ┌───── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

### Common Patterns
```bash
# Every minute
"* * * * *"

# Every hour at minute 0
"0 * * * *"

# Every day at midnight
"0 0 * * *"

# Every Sunday at 3 AM
"0 3 * * 0"

# Every weekday at 9 AM
"0 9 * * 1-5"

# Every 15 minutes
"*/15 * * * *"

# First day of every month at midnight
"0 0 1 * *"
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Scheduler** running: `naq scheduler --log-level INFO`
3. **NAQ Worker** running: `naq worker default scheduled_queue`
4. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start scheduler (required for scheduled jobs)
naq scheduler --log-level INFO &

# Start worker for scheduled jobs
naq worker default scheduled_queue --log-level INFO
```

### 2. Run Examples

```bash
cd examples/02-features/03-scheduled-jobs

# Basic scheduling patterns
python basic_scheduling.py

# Recurring job examples
python recurring_jobs.py

# Schedule management
python schedule_management.py
```

## Expected Output

You'll see scheduled jobs executing at their designated times:

```
📅 Scheduled job for 2024-01-15 14:30:00
⏰ Job will run in 45 minutes
🔄 Recurring job scheduled: daily at 2:00 AM
⏱️  Next run: 2024-01-16 02:00:00
✅ Scheduled job executed successfully
```

## Scheduling Patterns

### Business Hours
```python
# Monday-Friday, 9 AM to 5 PM, every hour
business_hours = [
    "0 9-17 * * 1-5"  # Hourly during business hours
]

# Send daily reports at end of business day
end_of_day = "0 17 * * 1-5"  # 5 PM on weekdays
```

### Maintenance Windows
```python
# Weekly maintenance: Sunday 3 AM
weekly_maintenance = "0 3 * * 0"

# Monthly reports: First Monday of each month at 8 AM
monthly_reports = "0 8 1-7 * 1"  # First week, Monday
```

### Monitoring and Alerts
```python
# System health checks every 5 minutes
health_check = "*/5 * * * *"

# Daily backup at 2 AM
daily_backup = "0 2 * * *"

# Weekly log rotation: Sunday midnight
log_rotation = "0 0 * * 0"
```

## Schedule Management

### Creating Schedules
```python
from naq import schedule_sync

# Create a recurring schedule
schedule = schedule_sync(
    my_function,
    cron="0 9 * * 1-5",  # Weekdays at 9 AM
    schedule_id="daily-reports",  # Optional unique ID
    timezone="America/New_York",  # Optional timezone
    enabled=True  # Can be disabled without deletion
)
```

### Managing Schedules
```python
from naq import (
    list_schedules_sync,
    get_schedule_sync,
    update_schedule_sync,
    delete_schedule_sync
)

# List all schedules
schedules = list_schedules_sync()

# Get specific schedule
schedule = get_schedule_sync("daily-reports")

# Update schedule (change time, enable/disable)
update_schedule_sync("daily-reports", cron="0 10 * * 1-5")

# Delete schedule
delete_schedule_sync("daily-reports")
```

### Schedule Status
```python
# Check schedule status
schedule = get_schedule_sync("daily-reports")
print(f"Enabled: {schedule['enabled']}")
print(f"Next run: {schedule['next_run']}")
print(f"Last run: {schedule['last_run']}")
```

## Timezone Handling

### Explicit Timezone
```python
# Schedule in specific timezone
schedule_sync(
    generate_report,
    cron="0 9 * * 1-5",
    timezone="America/New_York"  # Always 9 AM Eastern
)

# Multiple timezone schedules
timezones = ["America/New_York", "Europe/London", "Asia/Tokyo"]
for tz in timezones:
    schedule_sync(
        regional_report,
        cron="0 9 * * 1-5",
        timezone=tz,
        schedule_id=f"report-{tz.split('/')[-1].lower()}"
    )
```

### UTC Default
```python
# Default to UTC (recommended for production)
schedule_sync(
    system_maintenance,
    cron="0 2 * * *",  # 2 AM UTC daily
    timezone="UTC"
)
```

## Error Handling

### Failed Scheduled Jobs
```python
# Scheduled jobs support retries
schedule_sync(
    unreliable_task,
    cron="0 */6 * * *",  # Every 6 hours
    max_retries=3,
    retry_delay=300,  # 5 minutes
    retry_strategy="exponential"
)
```

### Schedule Conflicts
```python
# Prevent overlapping executions
schedule_sync(
    long_running_task,
    cron="0 * * * *",  # Every hour
    prevent_overlap=True,  # Skip if previous run still active
    max_runtime=timedelta(minutes=50)  # Timeout after 50 minutes
)
```

## Monitoring Schedules

### Schedule Dashboard
```bash
naq dashboard
# Visit http://localhost:8000/schedules
```

### Command Line Monitoring
```bash
# List all schedules
naq list-schedules

# Show schedule details
naq schedule-info <schedule_id>

# Check scheduler status
naq scheduler-status
```

### Programmatic Monitoring
```python
from naq import list_schedules_sync

schedules = list_schedules_sync()
for schedule in schedules:
    print(f"Schedule: {schedule['id']}")
    print(f"  Cron: {schedule['cron']}")
    print(f"  Enabled: {schedule['enabled']}")
    print(f"  Next run: {schedule['next_run']}")
    if schedule['last_run']:
        print(f"  Last run: {schedule['last_run']['started_at']}")
        print(f"  Status: {schedule['last_run']['status']}")
```

## Best Practices

### 1. Use Meaningful Schedule IDs
```python
# Good: descriptive IDs
schedule_sync(daily_backup, cron="0 2 * * *", schedule_id="daily-db-backup")

# Bad: generic IDs
schedule_sync(daily_backup, cron="0 2 * * *", schedule_id="job1")
```

### 2. Handle Timezone Carefully
```python
# Explicit timezone for business schedules
schedule_sync(
    business_report,
    cron="0 9 * * 1-5",
    timezone="America/New_York"
)

# UTC for system maintenance
schedule_sync(
    system_cleanup,
    cron="0 3 * * *",
    timezone="UTC"
)
```

### 3. Plan for Failures
```python
# Retries for transient failures
schedule_sync(
    api_sync,
    cron="0 */2 * * *",
    max_retries=3,
    retry_strategy="exponential"
)

# Monitoring for critical schedules
schedule_sync(
    health_monitor,
    cron="*/5 * * * *",
    alert_on_failure=True
)
```

### 4. Prevent Resource Conflicts
```python
# Avoid overlapping long-running jobs
schedule_sync(
    data_export,
    cron="0 1 * * *",
    prevent_overlap=True,
    max_runtime=timedelta(hours=2)
)
```

## Production Considerations

### High Availability
- Run multiple scheduler instances for redundancy
- Use leader election to prevent duplicate schedules
- Monitor scheduler health and failover

### Scale Management
- Separate queues for different schedule types
- Use appropriate worker pools for scheduled jobs
- Monitor queue depths and processing times

### Security
- Restrict schedule creation permissions
- Audit schedule changes
- Use secure serialization for scheduled job data

## Troubleshooting

### Common Issues

1. **Schedules Not Running**
   - Check if scheduler is running
   - Verify cron expression syntax
   - Check timezone settings

2. **Missed Executions**
   - Review scheduler logs
   - Check system clock synchronization
   - Monitor resource availability

3. **Overlapping Jobs**
   - Use `prevent_overlap=True`
   - Set appropriate `max_runtime`
   - Monitor job execution times

### Debugging Commands
```bash
# Check scheduler status
naq scheduler-status

# View scheduler logs
naq logs scheduler

# List pending scheduled jobs
naq list-schedules --status pending

# Check specific schedule
naq schedule-info <schedule_id>
```

## Next Steps

- Explore [multiple queues](../04-multiple-queues/) for organizing scheduled jobs
- Learn about [high availability](../../05-advanced/02-high-availability/) for robust scheduling
- Check out [monitoring](../../03-production/02-monitoring-dashboard/) for schedule oversight