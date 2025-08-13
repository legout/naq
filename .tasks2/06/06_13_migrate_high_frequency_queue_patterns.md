### Sub-task: Migrate High-Frequency Connection Patterns (Queue Module)
**Description:** Replace existing NATS connection patterns in the `src/naq/queue/` module with the newly created context managers (`nats_jetstream`, `nats_kv_store`).
**Implementation Steps:**
- For each file in `src/naq/queue/`:
    - Identify connection patterns.
    - Replace `get_nats_connection`/`get_jetstream_context` with `async with nats_jetstream(...) as (conn, js):` for JetStream operations.
    - Replace KV store patterns with `async with nats_kv_store(...) as kv:`.
    - Ensure `Config` objects are passed or `get_config()` is used.
    - Remove unused imports (`get_nats_connection`, `get_jetstream_context`, `close_nats_connection`).
**Success Criteria:**
- All connection patterns in `src/naq/queue/` are replaced with context managers.
- No functional regressions in queue operations (enqueue, scheduled job storage, etc.).
- Code in `src/naq/queue/` is cleaner and uses consistent connection patterns.
**Testing:**
- Run all existing unit and integration tests related to the `queue` module.
- Create new targeted integration tests if specific queue functionalities relying on connection management are not covered.
- Verify enqueue, dequeue, scheduling, and other queue operations work as expected.
**Documentation:**
- Update any internal documentation or comments within the `src/naq/queue/` files to reflect the new connection management approach.