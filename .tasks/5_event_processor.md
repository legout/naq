# Task: Implement the Event Processor

## Objective

Create a real-time event processor that can subscribe to the NATS event stream and dispatch events to registered handlers. This component allows users of the `naq` library to build reactive, event-driven logic based on job lifecycles.

## Requirements

1.  **Create new file:**
    *   Create `src/naq/events/processor.py`.

2.  **Implement `AsyncJobEventProcessor`:**
    *   **Initialization (`__init__`):**
        *   Accept a storage backend instance (`NATSJobEventStorage`).
        *   Initialize a dictionary to store event handlers, mapping `JobEventType` to a list of callable handlers.
    *   **Handler Registration:**
        *   Implement `add_handler(event_type: JobEventType, handler: Callable)` to register a function that will be called when an event of a specific type occurs.
        *   Implement `add_global_handler(handler: Callable)` to register a function that will be called for any event.
    *   **Event Processing Loop (`_process_events`):**
        *   Implement a background `asyncio.Task` that runs a processing loop.
        *   Inside the loop, use the `storage.stream_events()` method to asynchronously iterate over incoming events from NATS.
        *   For each event received, look up the corresponding handlers (both specific and global) and execute them.
        *   Handlers should be called concurrently (e.g., using `asyncio.gather`) to prevent one slow handler from blocking others.
    *   **Lifecycle Management:**
        *   Implement `start()` and `stop()` methods to manage the background processing task.

3.  **CLI Integration (Bonus):**
    *   In `src/naq/cli.py`, add a new command `naq events`.
    *   This command will instantiate and run the `AsyncJobEventProcessor`.
    *   Register a simple global handler that prints a formatted representation of each received event to the console using `rich`.
    *   This provides a powerful, built-in tool for users to monitor their job queues in real-time.
