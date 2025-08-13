### Sub-task: Implement `nats_connection` Context Manager
**Description:** Create an asynchronous context manager for NATS connections that handles connection establishment, configuration, error logging, and proper connection closure. This context manager will centralize NATS connection logic.
**Implementation Steps:**
- Create the file `src/naq/connection/context_managers.py`.
- Implement the `nats_connection` async context manager in `src/naq/connection/context_managers.py`.
- Ensure the context manager accepts an optional `Config` object and uses `get_config()` if none is provided.
- Integrate NATS connection parameters from the `Config` object (servers, client_name, max_reconnect_attempts, reconnect_time_wait, etc.).
- Add error logging for connection failures.
- Ensure `conn.close()` is called in the `finally` block for proper resource cleanup.
**Success Criteria:**
- `nats_connection` context manager is correctly implemented in `src/naq/connection/context_managers.py`.
- The context manager successfully establishes and closes NATS connections.
- Configuration parameters are correctly applied from the `Config` object.
- Error handling for connection issues is in place and logs errors.
**Testing:**
- Unit tests for `nats_connection` to verify:
    - Successful connection establishment and closure.
    - Correct application of configuration parameters.
    - Proper error handling (e.g., when NATS server is unavailable).
    - Resource cleanup even if errors occur within the `async with` block.
**Documentation:**
- Add a docstring to the `nats_connection` context manager explaining its purpose, parameters, and usage.
- Update the API documentation for `src/naq/connection/context_managers.py` to include `nats_connection`.