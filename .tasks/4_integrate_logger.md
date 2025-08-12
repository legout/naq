# Task: Integrate Logger into Core Components

## Objective

Integrate the `AsyncJobEventLogger` into the core `naq` components (`Queue`, `Worker`, `Scheduler`) to automatically log key events during a job's lifecycle.

## Requirements

1.  **Worker Integration (`src/naq/worker.py`):**
    *   In the `Worker.__init__` method, instantiate the `AsyncJobEventLogger`.
        *   You will need to instantiate the `NATSJobEventStorage` first and pass it to the logger.
        *   The logger should be started and stopped along with the worker's lifecycle (`run` and `_close` methods).
    *   In `Worker.process_message`:
        *   Immediately after deserializing the job, call `logger.log_job_started()`.
        *   After successful execution, call `logger.log_job_completed()`, passing in the job, worker details, and execution duration.
        *   In the `except` block where job execution fails, call `logger.log_job_failed()`.
    *   In `Worker._handle_job_execution_error`:
        *   When a retry is scheduled (i.e., when `msg.nak()` is called with a delay), log a `RETRY_SCHEDULED` event.

2.  **Queue Integration (`src/naq/queue.py`):**
    *   The `Queue` class will need access to a logger instance. Since `Queue` is often instantiated for single operations, the logger can be created on-demand within the methods.
    *   In `Queue.enqueue`:
        *   After successfully publishing the job to JetStream, call `logger.log_job_enqueued()`.
    *   In `Queue.enqueue_at`, `enqueue_in`, and `schedule`:
        *   After successfully storing the job in the scheduled jobs KV store, log a `SCHEDULED` event.

3.  **Scheduler Integration (`src/naq/scheduler.py`):**
    *   In `ScheduledJobProcessor._process_single_job`:
        *   When a scheduled job is due and is being enqueued, log a `SCHEDULE_TRIGGERED` event.

## Implementation Notes

*   To avoid circular dependencies, you may need to instantiate the logger and storage within the methods where they are used, rather than in the `__init__` of every class.
*   Ensure that event logging is non-blocking and does not introduce significant latency to core operations like enqueuing or job execution. The logger's internal buffering is key to this.
*   Pass all necessary contextual information (job ID, worker ID, queue name, etc.) when creating the event.
