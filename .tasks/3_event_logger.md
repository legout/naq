# Task: Implement the Event Logger

## Objective

Create a high-performance, non-blocking event logger that buffers events in memory and flushes them to the NATS storage backend periodically. This component will be the primary interface for the application to log job events.

## Requirements

1.  **Create new file:**
    *   Create `src/naq/events/logger.py`.

2.  **Implement `AsyncJobEventLogger`:**
    *   **Initialization (`__init__`):**
        *   Accept a storage backend instance (e.g., `NATSJobEventStorage`).
        *   Accept configuration parameters: `batch_size`, `flush_interval`, `max_buffer_size`.
        *   Initialize an in-memory buffer (e.g., a `list`) and an `asyncio.Lock` for thread-safe buffer access.
    *   **Buffering (`log_event`):**
        *   Implement `log_event(event: JobEvent)` which adds an event to the internal buffer.
        *   This method should be fast and non-blocking.
        *   If the buffer size exceeds `batch_size`, it should trigger an asynchronous flush.
    *   **Background Flushing (`_flush_loop`):**
        *   Implement a background `asyncio.Task` that runs in a loop.
        *   The loop should wake up every `flush_interval` seconds and call `_flush_events`.
    *   **`_flush_events`:**
        *   This method should acquire the lock, copy the current buffer, and clear it.
        *   It then iterates over the copied events and calls `storage.store_event()` for each one.
        *   Implement retry logic here. If `store_event` fails, the events should be re-added to the buffer for the next flush attempt.
    *   **Convenience Methods:**
        *   Create public methods like `log_job_enqueued`, `log_job_started`, etc., that create the appropriate `JobEvent` object and pass it to `log_event`.
    *   **Lifecycle Management:**
        *   Implement `start()` and `stop()` methods to manage the background flush task.
        *   The `stop()` method must ensure any remaining events in the buffer are flushed before exiting.

3.  **Implement `JobEventLogger` (Synchronous Wrapper):**
    *   Create a synchronous wrapper class that follows the existing `_sync` pattern in the `naq` library.
    *   It will instantiate `AsyncJobEventLogger`.
    *   Its methods (`log_event`, etc.) will use `anyio.from_thread.run` or a similar utility to call the async methods of the `AsyncJobEventLogger` instance.
