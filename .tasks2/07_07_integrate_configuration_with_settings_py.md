### Sub-task 7: Integrate Configuration with `settings.py`
**Description:** Update the `settings.py` module to use the new configuration system while maintaining backward compatibility with existing environment variables.
**Implementation Steps:**
- Modify `src/naq/settings.py`.
- Implement the `_get_env_or_config` helper function to prioritize environment variables over the new configuration system for existing settings.
- Replace direct `os.getenv` calls for `DEFAULT_NATS_URL`, `DEFAULT_QUEUE_NAME`, etc., with calls to `_get_env_or_config`.
**Success Criteria:**
- `settings.py` successfully retrieves configuration values from the new system.
- Existing environment variables still correctly override values from the new configuration system.
- All existing settings in `settings.py` are updated to use the new mechanism.
**Testing:**
- Create integration tests to verify backward compatibility:
    - Set an environment variable and ensure `settings.py` uses it.
    - Provide a YAML config and ensure `settings.py` uses it when the environment variable is not set.
    - Test scenarios where both are present, confirming environment variable precedence.
**Documentation:**
- Update the migration guide with a detailed section on how existing environment variables map to the new YAML configuration paths.
- Document the `_get_env_or_config` helper function.