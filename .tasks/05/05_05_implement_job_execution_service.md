### Sub-task: Implement `jobs.py` - Job Execution Service
**Description:** Centralize job execution, result storage, and lifecycle management, including integrated event logging and failure handling.
**Implementation Steps:**
- Create `src/naq/services/jobs.py`.
- Implement `JobService` inheriting from `BaseService`, accepting `ConnectionService`, `KVStoreService`, and `EventService` as dependencies.
- Include methods like `execute_job`, `store_result`, `get_result`, and `handle_job_failure`.
- Implement full job lifecycle management, result storage, and retry logic.
**Success Criteria:**
- `src/naq/services/jobs.py` is created.
- `JobService` manages job execution, result storage, and failure handling.
- Integrated event logging and retry logic are functional.
**Testing:**
- Unit tests for `JobService` methods, covering job execution, result storage, and failure scenarios.
- Integration tests to verify end-to-end job processing, including event logging and result retrieval.
**Documentation:**
- Document `src/naq/services/jobs.py`, detailing job lifecycle management, result handling, and failure handling.
- Update `docs/examples.qmd` with examples of `JobService` usage in workers or queues.