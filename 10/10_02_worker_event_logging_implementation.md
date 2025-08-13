### Sub-task 2: Worker Event Logging Implementation
**Description:** Implement and integrate event logging for all critical worker lifecycle stages within the `Worker` class, including startup, shutdown, heartbeats, busy/idle status changes, and error conditions.
**Implementation Steps:**
- Modify `src/naq/worker.py` (assuming `Worker` class is defined here) to integrate `event_service` calls.
- In the `Worker.start` method, log a `worker_started` event upon worker initialization, including worker ID, hostname, PID, queue names, and concurrency limit.
- Implement or modify the worker heartbeat mechanism (`_heartbeat_loop`) to periodically log `worker_heartbeat` events, providing current active jobs, concurrency, and optionally the current job ID.
- Within the `_heartbeat_loop`'s exception handling, log a `worker_error` event if an error occurs during heartbeat processing.
- In the `_process_message` method, log a `worker_busy` event when the worker begins processing a job, specifying the job ID.
- In the `_process_message` method's `finally` block, log a `worker_idle` event if the worker becomes idle after completing a job.
- In the `Worker.stop` method, log a `worker_stopped` event upon worker shutdown.
**Success Criteria:**
- All specified worker lifecycle events (`worker_started`, `worker_stopped`, `worker_heartbeat`, `worker_busy`, `worker_idle`, `worker_error`) are consistently and correctly logged by the `Worker` class.
- Event data (worker ID, hostname, PID, active jobs, current job ID, queue names, concurrency, error details) accurately reflects the worker's state and activity.
**Testing:**
- Create or update unit/integration tests in `tests/unit/test_unit_worker.py` or `tests/integration/test_integration_worker.py`.
- Mock the `EventService` to capture and verify worker events for various scenarios (e.g., worker startup/shutdown, continuous operation with jobs, error conditions).
**Documentation:**
- Update the relevant API documentation, possibly in `docs/api/worker.qmd`, to describe the new worker event types, their triggers, and the data they contain.
- Provide code examples illustrating how worker events are logged and can be used for monitoring.