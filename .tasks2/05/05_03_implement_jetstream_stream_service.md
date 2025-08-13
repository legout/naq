### Sub-task: Implement `streams.py` - JetStream Stream Service
**Description:** Provide a centralized service for JetStream stream creation, management, and operations, ensuring consistent configuration and lifecycle management for streams.
**Implementation Steps:**
- Create `src/naq/services/streams.py`.
- Implement `StreamService` inheriting from `BaseService` and accepting `ConnectionService` as a dependency.
- Include methods such as `ensure_stream`, `get_stream_info`, `delete_stream`, and `purge_stream`.
- Implement consistent stream creation and configuration logic.
**Success Criteria:**
- `src/naq/services/streams.py` is created.
- `StreamService` can ensure, get info, delete, and purge JetStream streams.
- Stream operations are consistent and leverage `ConnectionService`.
**Testing:**
- Unit tests for `StreamService` methods, ensuring correct interaction with JetStream via `ConnectionService`.
- Integration tests to verify stream creation, modification, and deletion.
**Documentation:**
- Document `src/naq/services/streams.py`, explaining methods for stream management.
- Provide examples of `StreamService` usage in `docs/examples.qmd`.