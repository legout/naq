### Sub-task: Develop YAML Configuration Pipeline Tests
**Description:** Create tests to validate the entire configuration pipeline, from YAML file loading and environment variable overrides to the proper instantiation of the ServiceManager and its services based on the loaded configuration.
**Implementation Steps:**
- Create `tests/test_config/test_yaml_loading.py` to test:
    - Loading configuration from a YAML file using `load_config`.
    - Correct parsing of configuration sections (nats, workers, events, results).
    - Environment variable overrides (`NAQ_WORKERS_CONCURRENCY`, etc.).
    - The complete pipeline from YAML file -> `ConfigLoader` -> `TypedConfig` -> `ServiceManager` -> Services.
- Utilize the `temp_config_file` fixture from `conftest.py`.
**Success Criteria:**
- YAML configuration files are correctly parsed and loaded.
- Environment variables successfully override corresponding YAML settings.
- `ServiceManager` instances are correctly configured and initialized using loaded configurations.
- All YAML configuration pipeline tests pass.
**Testing:**
- Run `pytest tests/test_config/test_yaml_loading.py`.
- Manually verify that different configurations (valid and invalid) are handled as expected.
**Documentation:**
- Update `docs/configuration.qmd` (if exists, or create one) to explain YAML configuration and environment variable overrides.
- Document the expected configuration structure and parameters.