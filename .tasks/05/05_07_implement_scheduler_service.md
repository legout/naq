### Sub-task: Implement `scheduler.py` - Scheduler Service
**Description:** Centralize scheduler operations and scheduled job management, including job scheduling, triggering, cancellation, and pausing.
**Implementation Steps:**
- Create `src/naq/services/scheduler.py`.
- Implement `SchedulerService` inheriting from `BaseService`, accepting `ConnectionService`, `KVStoreService`, and `EventService` as dependencies.
- Include methods like `schedule_job`, `trigger_due_jobs`, `cancel_scheduled_job`, and `pause_scheduled_job`.
- Implement scheduled job lifecycle management and due job detection.
**Success Criteria:**
- `src/naq/services/scheduler.py` is created.
- `SchedulerService` can schedule, trigger, cancel, and pause jobs.
- Due job detection and triggering mechanisms are functional.
**Testing:**
- Unit tests for `SchedulerService` methods, covering scheduling, triggering, and managing scheduled jobs.
- Integration tests to verify the end-to-end scheduling process.
**Documentation:**
- Document `src/naq/services/scheduler.py`, detailing scheduled job management.
- Update `docs/examples.qmd` with usage examples for `SchedulerService`.