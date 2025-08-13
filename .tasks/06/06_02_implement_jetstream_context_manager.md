### Sub-task: Implement `jetstream_context` Context Manager
**Description:** Create an asynchronous context manager to obtain a JetStream context from an existing NATS connection, handling potential errors during context creation.
**Implementation Steps:**
- Implement the `jetstream_context` async context manager in `src/naq/connection/context_managers.py`.
- The context manager should accept a `nats.aio.client.Client` instance as input.
- Add error logging for JetStream context creation failures.
**Success Criteria:**
- `jetstream_context` context manager is correctly implemented in `src/naq/connection/context_managers.py`.
- The context manager successfully retrieves a `JetStreamContext` from a NATS connection.
- Error handling for JetStream context creation is in place and logs errors.
**Testing:**
- Unit tests for `jetstream_context` to verify:
    - Successful retrieval of a JetStream context.
    - Error handling when an invalid NATS connection is provided or JetStream is unavailable.
**Documentation:**
- Add a docstring to the `jetstream_context` context manager explaining its purpose, parameters, and usage.
- Update the API documentation for `src/naq/connection/context_managers.py` to include `jetstream_context`.