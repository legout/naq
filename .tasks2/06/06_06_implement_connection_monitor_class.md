### Sub-task: Implement `ConnectionMonitor` Class
**Description:** Create a class to monitor NATS connection usage and performance metrics using the `ConnectionMetrics` dataclass.
**Implementation Steps:**
- Implement the `ConnectionMonitor` class in `src/naq/connection/utils.py`.
- The class should contain methods:
    - `__init__`: Initializes `metrics` and an internal list for connection durations.
    - `record_connection_start()`: Increments `total_connections` and `active_connections`.
    - `record_connection_end(duration: float)`: Decrements `active_connections`, records duration, and updates `average_connection_time`.
    - `record_connection_failure()`: Increments `failed_connections`.
- Instantiate a global `connection_monitor` instance.
**Success Criteria:**
- `ConnectionMonitor` class is correctly implemented in `src/naq/connection/utils.py`.
- The monitoring methods correctly update the `ConnectionMetrics`.
- The `average_connection_time` is calculated accurately.
- A global `connection_monitor` instance is available.
**Testing:**
- Unit tests for `ConnectionMonitor` to verify:
    - `record_connection_start` and `record_connection_end` correctly update connection counts and average time.
    - `record_connection_failure` correctly updates failed connection count.
    - Edge cases like zero connections or single connection are handled.
**Documentation:**
- Add a docstring to the `ConnectionMonitor` class explaining its purpose and methods.
- Update the API documentation for `src/naq/connection/utils.py` to include `ConnectionMonitor`.