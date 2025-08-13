### Sub-task 3: Scheduler Event Logging Implementation
**Description:** Implement and integrate event logging for all critical scheduler operations within the `SchedulerService`, including scheduled job creation, job triggering, schedule management (pause, resume, cancel), and leader election events.
**Implementation Steps:**
- Modify `src/naq/scheduler.py` (assuming `SchedulerService` is defined here) to integrate `event_service` calls.
- In the `SchedulerService.schedule_job` method, log a `job_scheduled` event when a job is successfully scheduled, including job ID, queue name, scheduled timestamp, cron, interval, and repeat details.
- In the `SchedulerService.trigger_due_jobs` method, log a `schedule_triggered` event for each job triggered by the scheduler, including job ID, queue name, scheduled time, and actual trigger time.
- Within the `SchedulerService.trigger_due_jobs` method's exception handling for individual schedule processing, log a `scheduler_error` event, including schedule ID, error type, and message.
- If applicable to the current scheduler design, implement logging for `schedule_paused`, `schedule_resumed`, and `schedule_cancelled` events when these operations occur.
- If applicable, implement logging for `leader_elected` and `leader_revoked` events related to scheduler leader election.
**Success Criteria:**
- All specified scheduler events (`job_scheduled`, `schedule_triggered`, `scheduler_error`, and potentially management/leader election events) are consistently and correctly logged by the `SchedulerService`.
- Event data accurately reflects the scheduler's operations and any associated errors.
**Testing:**
- Create or update unit/integration tests in `tests/unit/test_unit_scheduler.py` or `tests/integration/test_integration_scheduler.py`.
- Mock the `EventService` to capture and verify scheduler events for various scenarios (e.g., job scheduling, triggering due jobs, error conditions).
**Documentation:**
- Update the relevant API documentation, possibly in `docs/api/scheduler.qmd`, to describe the new scheduler event types, their triggers, and the data they provide.
- Add code examples illustrating how scheduler events are logged and can be used for monitoring and auditing.