# Job Retries Example

This example demonstrates how to configure job retries using `max_retries` and `retry_delay` parameters in `naq.enqueue`. The `flaky_task` is designed to fail a few times before succeeding, showcasing naq's retry mechanism.

## Running the Example

1.  Ensure your NATS server is running.
2.  Run the script to enqueue the job:
    ```bash
    python examples/job_retries/job_retries.py
    ```
    You will see output similar to:
    ```
    Reset counter file: examples/job_retries/retry_counter.tmp
    Enqueued job <job_id> for flaky_task.
    This task is designed to fail 2 times and succeed on the 3rd attempt.
    Run the worker to process the job and observe retries:
      naq worker naq_default_queue
    Observe the logs for retry messages and delays.
    ```
3.  In a separate terminal, start a worker to process the job from the default queue:
    ```bash
    naq worker naq_default_queue
    ```
4.  Observe the worker logs. You will see the `flaky_task` being attempted multiple times, with delays between retries, until it finally succeeds (on the 3rd attempt in this example). The temporary file `retry_counter.tmp` in the example directory is used to track the attempt count across retries.