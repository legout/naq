### Sub-task: Implement `wait_for_nats_connection` Utility Function
**Description:** Create an asynchronous utility function that waits for the NATS connection to become available within a specified timeout.
**Implementation Steps:**
- Implement the `wait_for_nats_connection` async function in `src/naq/connection/utils.py`.
- This function should repeatedly call `test_nats_connection` until successful or the timeout is reached.
- It should include a delay between retries.
**Success Criteria:**
- `wait_for_nats_connection` function is correctly implemented in `src/naq/connection/utils.py`.
- The function successfully waits for NATS connectivity.
- It respects the specified timeout.
**Testing:**
- Unit tests for `wait_for_nats_connection` to verify:
    - Returns `True` when connection becomes available within timeout.
    - Returns `False` when connection does not become available within timeout.
    - Correct retry logic and delays.
**Documentation:**
- Add a docstring to the `wait_for_nats_connection` function explaining its purpose, parameters, and return value.
- Update the API documentation for `src/naq/connection/utils.py` to include `wait_for_nats_connection`.