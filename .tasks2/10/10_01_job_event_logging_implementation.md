### Sub-task 1: Job Event Logging Implementation
**Description:** Implement and integrate event logging for all critical job lifecycle stages within the `JobService`, including enqueuing, starting, completing, failing, and scheduling retries.
**Implementation Steps:**
- Modify `src/naq/job.py` (assuming `JobService` is defined here) to integrate `event_service` calls.
- In the `enqueue_job` method, ensure a `job_enqueued` event is logged upon successful job enqueueing.
- In the `execute_job` method, log a `job_started` event at the beginning of job execution.
- In the `execute_job` method, log a `job_completed` event upon successful job completion, including duration and result size.
- In the `execute_job` method's exception handler, log a `job_failed` event when a job execution fails, including error type, message, and duration.
- Within the `execute_job` method's retry logic, log a `job_retry_scheduled` event when a job is scheduled for retry, including retry count and delay.
**Success Criteria:**
- All specified job lifecycle events (`job_enqueued`, `job_started`, `job_completed`, `job_failed`, `job_retry_scheduled`) are consistently and correctly logged by the `JobService`.
- Event data (job ID, queue name, worker ID, function name, args/kwargs count, duration, error details, retry specifics) accurately reflects the job's state.
**Testing:**
- Create or update unit/integration tests in `tests/unit/test_unit_job.py` or `tests/integration/test_integration_job.py`.
- Mock the `EventService` to capture logged events and assert their type, data, and correct invocation for various job scenarios (e.g., successful execution, immediate failure, retriable failure).
**Documentation:**
- Update the relevant API documentation, possibly in `docs/api/job.qmd`, to describe the new job event types, their triggers, and the data they carry.
- Provide code examples demonstrating how job events are logged and can be consumed by external services.