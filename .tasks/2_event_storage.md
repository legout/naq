# Task: Implement NATS Event Storage

## Objective

Create a robust storage backend for job events using NATS JetStream. This component will be responsible for persisting events in a durable, time-ordered stream and providing methods to retrieve them.

## Requirements

1.  **Create new files:**
    *   Create `src/naq/events/storage.py`.

2.  **Define `BaseEventStorage` (Optional but Recommended):**
    *   In `storage.py`, define a `BaseEventStorage` abstract base class using `abc.ABC`. This promotes a clean interface for potential future storage backends.
    *   Define abstract methods like `store_event`, `get_events`, and `stream_events`.

3.  **Implement `NATSJobEventStorage`:**
    *   This class will implement the `BaseEventStorage` interface.
    *   **Initialization (`__init__`):**
        *   Accept NATS URL, stream name (e.g., `NAQ_JOB_EVENTS`), and a subject prefix (e.g., `naq.jobs.events`).
        *   Define a default stream configuration (storage type, retention policy, max age, etc.).
    *   **Connection Management:**
        *   Implement `_connect` and `_disconnect` methods to manage the NATS connection and JetStream context.
        *   Use the existing connection manager in `src/naq/connection.py` to get the NATS connection.
    *   **Stream Setup (`_setup_stream`):**
        *   On connection, check if the target JetStream stream exists. If not, create it using the default configuration.
    *   **`store_event(event: JobEvent)`:**
        *   Generate a NATS subject for the event based on its properties (e.g., `naq.jobs.events.{job_id}.{event_type}`).
        *   Serialize the `JobEvent` object using `msgspec.msgpack.encode()` for efficiency.
        *   Publish the serialized event to the generated subject.
    *   **`get_events(job_id: str) -> List[JobEvent]`:**
        *   Retrieve all events for a specific job ID by subscribing to `naq.jobs.events.{job_id}.>`.
        *   Use an ephemeral, pull-based consumer for this operation to efficiently get all historical messages.
    *   **`stream_events(...) -> AsyncIterator[JobEvent]`:**
        *   Provide a method to subscribe to new events in real-time.
        *   Use a push-based consumer that starts from the `NEW` deliver policy.

4.  **Subject Hierarchy:**
    *   Use the following subject structure: `naq.jobs.events.{job_id}.{context}.{event_type}`
    *   Example: `naq.jobs.events.job-123.worker.worker-abc.started`
    *   Example: `naq.jobs.events.job-456.queue.high-priority.enqueued`
