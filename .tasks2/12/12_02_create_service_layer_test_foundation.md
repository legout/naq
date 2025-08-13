### Sub-task: Create Service Layer Test Foundation
**Description:** Establish the foundational patterns and utilities required for testing individual services and their interactions within the ServiceManager architecture.
**Implementation Steps:**
- Establish a clear directory structure for service tests (e.g., `tests/test_services/`).
- Define common testing patterns for services, including how to set up and tear down service instances.
- Create utility functions or helper fixtures for mocking service dependencies consistently across tests.
- Set up utilities for testing service lifecycle management (initialization, cleanup) within test scenarios.
**Success Criteria:**
- A dedicated `tests/test_services/` directory is created.
- Basic test files for each core service (e.g., `test_connection_service.py`, `test_job_service.py`) are scaffolded.
- Common patterns for service testing are defined and consistently applied.
**Testing:**
- Write minimal, passing tests for the basic setup and teardown of a generic service using the new patterns.
- Verify that mock service dependencies can be successfully injected and behave as expected in a controlled test environment.
**Documentation:**
- Create a new section in `docs/testing.qmd` (or similar) detailing the established service testing patterns.
- Provide clear examples of how to write unit and integration tests for services, including dependency mocking.