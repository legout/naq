# Multiple Queues and Results Example

This example demonstrates how to enqueue jobs to different queues (`data_processing` and `notifications`) and fetch their results using `Job.fetch_result_sync`.

## Running the Example

1.  Ensure your NATS server is running.
2.  To process jobs from both queues, you need to start two separate workers in different terminals:
    ```bash
    naq worker data_processing
    ```
    ```bash
    naq worker notifications
    ```
3.  Run the script to enqueue jobs to these queues and attempt to fetch results:
    ```bash
    python examples/multiple_queues_results/multiple_queues_results.py
    ```
    You will see output indicating jobs being enqueued to `data_processing` and `notifications` queues, followed by attempts to fetch and display their results.

    Example output from the script:
    ```
    Enqueuing jobs to different queues...
    Enqueued job <job_id_1> to queue 'data_processing' for function 'process_data'
    Enqueued job <job_id_2> to queue 'notifications' for function 'send_notification'

    To process these jobs, run separate workers:
      naq worker data_processing
      naq worker notifications

    Waiting a few seconds to allow workers to potentially pick up and finish jobs...

    Attempting to fetch job results...

    Fetching result for job <job_id_1> (data_processing)...
      Result: Processed 123

    Fetching result for job <job_id_2> (notifications)...
      Result: Notification sent to user_abc

    Example finished.