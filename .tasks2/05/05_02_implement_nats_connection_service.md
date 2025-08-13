### Sub-task: Implement `connection.py` - NATS Connection Service
**Description:** Centralize NATS connection management, eliminating redundant connection patterns across the codebase. This service will handle connection pooling, lifecycle management, and provide JetStream context.
**Implementation Steps:**
- Create `src/naq/services/connection.py`.
- Implement `ConnectionService` inheriting from `BaseService`.
- Include methods for `get_connection` (with pooling), `get_jetstream`, and `connection_scope` (async context manager).
- Integrate configuration-driven connection parameters.
- Implement error recovery and reconnection logic for NATS connections.
**Success Criteria:**
- `src/naq/services/connection.py` is created.
- `ConnectionService` provides pooled NATS connections and JetStream contexts.
- The `connection_scope` context manager ensures safe resource handling.
- The service handles connection lifecycle, pooling, and reuse.
**Testing:**
- Unit tests for `ConnectionService` methods, including connection pooling, `get_jetstream`, and `connection_scope`.
- Integration tests to verify that `ConnectionService` correctly manages NATS connections and JetStream contexts.
- Test error recovery and reconnection scenarios.
**Documentation:**
- Document `src/naq/services/connection.py`, detailing connection pooling, `get_connection`, `get_jetstream`, and `connection_scope`.
- Update relevant sections in `docs/quickstart.qmd` or `docs/examples.qmd` to demonstrate `ConnectionService` usage.