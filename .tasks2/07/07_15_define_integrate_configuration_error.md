### Sub-task 15: Define and Integrate `ConfigurationError`
**Description:** Ensure a custom `ConfigurationError` exception is defined and used consistently throughout the configuration system for error handling.
**Implementation Steps:**
- Define `ConfigurationError` in `src/naq/exceptions.py` or a dedicated exception file within the `config` module if it's specific to config. (The provided `loader.py` snippet implies it's already available or needs to be created).
- Ensure `loader.py` raises `ConfigurationError` for file loading issues.
- Ensure `schema.py` raises `ConfigurationError` for validation failures.
- Ensure `config/__init__.py` (CLI commands) catches and handles `ConfigurationError`.
**Success Criteria:**
- `ConfigurationError` is defined.
- All configuration-related errors are consistently raised as `ConfigurationError`.
- Error messages are clear and informative.
**Testing:**
- Include tests that specifically assert the raising of `ConfigurationError` under various error conditions (e.g., invalid YAML, schema violations, file not found).
**Documentation:**
- Document the `ConfigurationError` exception and its usage in the error handling section of the documentation.