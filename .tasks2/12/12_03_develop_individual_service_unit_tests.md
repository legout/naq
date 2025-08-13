### Sub-task: Develop Individual Service Unit Tests
**Description:** Implement comprehensive unit tests for each core service (`ConnectionService`, `JobService`, `EventService`, `StreamService`, `KVStoreService`, `SchedulerService`) to ensure their individual functionalities work as expected within the ServiceManager context.
**Implementation Steps:**
- Create `tests/test_services/test_connection_service.py` to test connection pooling, failover, and `jetstream_scope` context management.
- Create `tests/test_services/test_job_service.py` to test job execution orchestration, enqueuing, and dependency handling.
- Create `tests/test_services/test_event_service.py` to test event logging mechanisms.
- Create `tests/test_services/test_stream_service.py` to test JetStream operations.
- Create `tests/test_services/test_kv_service.py` to test KeyValue store operations.
- Create `tests/test_services/test_scheduler_service.py` to test scheduling operations.
- For each service, mock its dependencies using the patterns defined in `conftest.py` and the service test foundation.
- Cover core functionalities, edge cases, and error handling for each service.
**Success Criteria:**
- Dedicated unit test files exist for each primary service.
- Each service's critical public methods are covered by unit tests.
- Mocked dependencies correctly isolate the service under test.
- All service unit tests pass consistently.
- Code coverage for each service module meets the >95% target for new service layer.
**Testing:**
- Run `pytest tests/test_services/` to execute all service unit tests.
- Use a code coverage tool (e.g., `pytest-cov`) to verify coverage targets are met for each service.
**Documentation:**
- For each service, add a section in `docs/api/` (e.g., `docs/api/connection.qmd`) detailing its functionalities and how it integrates with the ServiceManager.
- Add specific examples of how to use each service in the `docs/examples.qmd`.