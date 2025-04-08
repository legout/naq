# Job Dependencies Example

This example demonstrates how to enqueue a job (`final_task`) that depends on the completion of other jobs (`task_a` and `task_b`) using the `depends_on` argument in `naq.enqueue_sync`.

## Running the Example

1.  Ensure your NATS server is running.
2.  Run the script to enqueue the jobs:
    ```bash
    python examples/job_dependencies/job_dependencies.py
    ```
    You will see output indicating the Job IDs for Task A, Task B, and the Final Task.
    ```
    Enqueuing Task A...
      -> Enqueued Task A with Job ID: <job_id_a>
    Enqueuing Task B...
      -> Enqueued Task B with Job ID: <job_id_b>
    Enqueuing Final Task (depends on Task A and Task B)...
      -> Enqueued Final Task with Job ID: <job_id_final>
    ```
3.  In a separate terminal, start a worker to process the jobs from the default queue:
    ```bash
    naq worker naq_default_queue
    ```
4.  Observe the worker logs. `final_task` will only execute after both `task_a` and `task_b` have finished successfully. If you enabled DEBUG logging (as shown in the script), you will see messages indicating the worker is checking dependencies for the final task until its prerequisites are met.