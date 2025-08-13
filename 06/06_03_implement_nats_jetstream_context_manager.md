### Sub-task: Implement `nats_jetstream` Combined Context Manager
**Description:** Create an asynchronous context manager that combines `nats_connection` and `jetstream_context` to provide both a NATS connection and a JetStream context in a single `async with` statement.
**Implementation Steps:**
- Implement the `nats_jetstream` async context manager in `src/naq/connection/context_managers.py`.
- This context manager should internally use `nats_connection` and `jetstream_context`.
- It should yield both the NATS connection and the JetStream context.
**Success Criteria:**
- `nats_jetstream` context manager is correctly implemented in `src/naq/connection/context_managers.py`.
- The context manager provides both a NATS connection and a JetStream context.
- Proper nesting and cleanup of underlying context managers are ensured.
**Testing:**
- Unit tests for `nats_jetstream` to verify:
    - Both connection and JetStream context are yielded.
    - The combined context manager handles lifecycle correctly.
    - Integration with `nats_connection` and `jetstream_context` works as expected.
**Documentation:**
- Add a docstring to the `nats_jetstream` context manager explaining its purpose, parameters, and usage.
- Update the API documentation for `src/naq/connection/context_managers.py` to include `nats_jetstream`.