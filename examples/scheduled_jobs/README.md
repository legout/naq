# Scheduled Jobs Example

This example demonstrates how to schedule jobs to run at a specific time (`enqueue_at_sync`) or after a certain delay (`enqueue_in_sync`).

## Running the Example

1.  Ensure your NATS server is running.
2.  Run the script to schedule the jobs:
    ```bash
    python examples/scheduled_jobs/scheduled_jobs.py
    ```
    The script will output the IDs of the scheduled jobs and their target execution times.
3.  To process these scheduled jobs, you need both the NAQ scheduler and a worker running:
    *   Start the scheduler in one terminal:
        ```bash
        naq scheduler
        ```
    *   Start a worker for the default queue in another terminal:
        ```bash
        naq worker naq_default_queue
        ```
4.  The scheduler will monitor for jobs whose time has come and enqueue them. The worker will then pick up the jobs from the queue and execute the `print_message` function. You'll see the output "Job executed: ..." in the worker's terminal when the jobs run.