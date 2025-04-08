# Hello World Example

This example demonstrates the basic usage of `naq.enqueue_sync` to enqueue a simple job (`say_hello`) onto the default queue (`naq_default_queue`).

## Running the Example

1.  Ensure your NATS server is running.
2.  Run the script to enqueue the job:
    ```bash
    python examples/hello_world/hello_world.py
    ```
    You will see output similar to:
    ```
    Enqueuing hello world job...
    Job enqueued with ID: <job_id>
    ```
3.  In a separate terminal, start a worker to process the job from the default queue:
    ```bash
    naq worker naq_default_queue
    ```
4.  The worker will execute the `say_hello` function, printing greetings and returning a completion message.

*(Optional)* The commented-out code in the script shows how you might attempt to fetch the job's result if a result backend were configured.