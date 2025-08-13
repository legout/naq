### Sub-task: Migrate Existing Integration Tests (`test_integration_*.py`)
**Description:** Update existing integration tests (e.g., `tests/integration/test_integration_events.py`, `tests/integration/test_integration_job.py`, `tests/integration/test_integration_worker.py`) to leverage the ServiceManager for setting up and interacting with components.
**Implementation Steps:**
- For each integration test file, replace direct NATS URL usage and component instantiation (e.g., `AsyncJobEventLogger(nats_url=...)`) with ServiceManager-based setup.
- Utilize the `service_manager` fixture for obtaining service instances (e.g., `await service_manager.get_service(EventService)`).
- Ensure that the entire integrated workflow is correctly tested using the new service layer.
**Success Criteria:**
- All `test_integration_*.py` files are refactored to use `ServiceManager`.
- The integration tests accurately reflect the interactions within the new service architecture.
- All migrated integration tests pass consistently.
**Testing:**
- Run `pytest tests/integration/`.
- Confirm that the tests run successfully and validate the integration points.
**Documentation:**
- Update `docs/testing.qmd` with a section on how to write integration tests using the `ServiceManager` fixture.