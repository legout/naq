### Sub-task: Implement `events.py` - Event Service
**Description:** Centralize event logging, processing, and monitoring for job events, providing high-performance logging, streaming, and history queries.
**Implementation Steps:**
- Create `src/naq/services/events.py`.
- Implement `EventService` inheriting from `BaseService`, accepting `ConnectionService` and `StreamService` as dependencies.
- Include methods like `log_event`, `log_job_started` (convenience), `stream_events`, and `get_event_history`.
- Implement high-performance event logging mechanisms.
**Success Criteria:**
- `src/naq/services/events.py` is created.
- `EventService` can log, stream, and retrieve job events efficiently.
- Event filtering and history queries are functional.
**Testing:**
- Unit tests for `EventService` methods, focusing on event logging and retrieval.
- Integration tests to verify event streaming and filtering.
**Documentation:**
- Document `src/naq/services/events.py`, explaining event logging, streaming, and history features.
- Add examples of using `EventService` for monitoring and debugging in `docs/examples.qmd`.