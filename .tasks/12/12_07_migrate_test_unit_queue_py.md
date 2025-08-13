### Sub-task: Migrate `test_unit_queue.py` to ServiceManager Architecture
**Description:** Update `tests/unit/test_unit_queue.py` to use the new ServiceManager-compatible fixtures and configuration patterns, replacing old NATS direct connection mocks.
**Implementation Steps:**
- Modify the `queue` fixture in `test_unit_queue.py` to use `service_aware_nats_mock` and `service_test_config`.
- Replace direct `get_nats_connection` and `get_jetstream_context` patching with ServiceManager mocking.
- Ensure `Queue` instantiation uses the new `config` parameter.
- Review all tests in the file and adjust them to the new ServiceManager-based environment, ensuring functionality remains correct.
**Success Criteria:**
- `test_unit_queue.py` successfully uses the new ServiceManager-compatible fixtures.
- All tests in `test_unit_queue.py` pass after migration.
- The queue module's unit test coverage is maintained or improved.
**Testing:**
- Run `pytest tests/unit/test_unit_queue.py`.
- Verify that the tests run without errors and all assertions hold true.
**Documentation:**
- No direct documentation updates related to this migration, as it's an internal test refactoring. However, `docs/testing.qmd` should cover the general approach to migrating existing tests.