### Sub-task: Update `conftest.py` for ServiceManager Architecture
**Description:** Refactor the existing `conftest.py` to support the new ServiceManager architecture, including new fixtures for ServiceManager, service-layer mock patterns, updated NATS mocks, and configuration fixture support. This is a critical prerequisite for all other testing phases.
**Implementation Steps:**
- Add `ServiceManager` fixtures, specifically `@pytest.fixture async def service_manager()` and `@pytest.fixture def mock_service_manager()`.
- Create service-layer mock patterns within `mock_service_manager` to properly mock `ConnectionService`, `JobService`, `EventService`, `StreamService`, and `KVStoreService`.
- Implement `jetstream_scope` context manager mocking for `ConnectionService`.
- Update existing NATS mocks (e.g., `mock_nats`) to work with the new service-aware version (`service_aware_nats_mock`).
- Add configuration fixture support, including `@pytest.fixture def service_test_config()` and `@pytest.fixture def temp_config_file()`.
- Replace `worker_dict` fixture with `service_test_config`.
- Update `service_test_job` fixture to align with the new `Job` model structure.
**Success Criteria:**
- `service_manager` fixture successfully initializes and cleans up `ServiceManager` instances.
- `mock_service_manager` fixture correctly mocks `ServiceManager` and its associated services, allowing for isolated unit testing.
- `service_aware_nats_mock` fixture provides a NATS mock compatible with the ServiceManager architecture.
- `service_test_config` and `temp_config_file` fixtures correctly provide and manage test configurations.
- All existing tests that relied on old `conftest.py` patterns are updated to use the new ServiceManager-compatible fixtures without breaking.
**Testing:**
- Create dedicated unit tests for the new `conftest.py` fixtures to ensure they initialize and behave as expected.
- Run a small subset of existing tests that depend on `conftest.py` to verify that the updates do not introduce regressions and that the new mocking patterns function correctly.
**Documentation:**
- Update `docs/contributing.qmd` or a new `docs/testing.qmd` with detailed instructions on how to use the new ServiceManager-compatible fixtures in tests.
- Add examples of mocking services and configuring tests using the new `conftest.py` patterns.