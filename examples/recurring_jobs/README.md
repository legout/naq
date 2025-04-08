# Recurring Jobs Example

This example demonstrates how to schedule recurring jobs using `naq.schedule_sync`, both with interval-based and cron-based scheduling.

**Note:** Cron-based scheduling requires the `croniter` library. Install it using: `pip install croniter`

## Running the Example

1.  Ensure your NATS server is running.
2.  Start the naq scheduler in a separate terminal:
    ```bash
    naq scheduler
    ```
3.  Start a worker to process jobs from the default queue in another terminal:
    ```bash
    naq worker naq_default_queue
    ```
4.  Run the script to schedule the recurring jobs:
    ```bash
    python examples/recurring_jobs/recurring_jobs.py
    ```
    You will see output indicating that two recurring jobs have been scheduled: an interval-based job (running every 15 seconds, 5 times) and a cron-based job (running every minute indefinitely).

    Example output from the script:
    ```
    Scheduling recurring jobs using naq.schedule_sync...
    Ensure 'naq scheduler' and 'naq worker naq_default_queue' are running in separate terminals.
    ----------------------------------------
    Scheduling a job to run every 15 seconds, 5 times...
    Scheduled interval job with ID: <interval_job_id>
    ----------------------------------------
    Scheduling a job to run every minute using a cron string ('* * * * *')...
    This job will repeat indefinitely until the scheduler is stopped.
    Make sure you have 'croniter' installed: pip install croniter
    Scheduled cron job with ID: <cron_job_id>
    ----------------------------------------
    Recurring jobs scheduled. Check the worker output.
    The interval job will run 5 times. The cron job will run every minute.
    ```

5.  Observe the worker output. You should see messages from both "Interval job" and "Cron job" being logged periodically according to their schedules.