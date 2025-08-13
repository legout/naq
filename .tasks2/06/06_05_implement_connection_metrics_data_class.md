### Sub-task: Implement `ConnectionMetrics` Data Class
**Description:** Define a data class to store and track various metrics related to NATS connection usage and performance.
**Implementation Steps:**
- Create the file `src/naq/connection/utils.py`.
- Implement the `ConnectionMetrics` dataclass in `src/naq/connection/utils.py` with fields for `total_connections`, `active_connections`, `failed_connections`, and `average_connection_time`.
**Success Criteria:**
- `ConnectionMetrics` dataclass is correctly defined in `src/naq/connection/utils.py`.
- The dataclass fields are appropriate for tracking connection statistics.
**Testing:**
- Unit tests for `ConnectionMetrics` to ensure:
    - Fields are initialized correctly.
    - Values can be updated as expected.
**Documentation:**
- Add a docstring to the `ConnectionMetrics` dataclass explaining its purpose and fields.
- Update the API documentation for `src/naq/connection/utils.py` to include `ConnectionMetrics`.