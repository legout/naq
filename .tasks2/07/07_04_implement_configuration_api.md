### Sub-task 4: Implement Configuration API
**Description:** Create the public API for the configuration system in `config/__init__.py`, providing functions to load, retrieve, and reload the configuration, along with a context manager for testing.
**Implementation Steps:**
- Create the file `src/naq/config/__init__.py`.
- Import `ConfigLoader`, `NAQConfig`, and `ConfigValidator`.
- Define a global `_config_instance` variable.
- Implement the `load_config` function to load and optionally validate the configuration, storing it in `_config_instance`.
- Implement the `get_config` function to return the current configuration instance, loading it if not already loaded.
- Implement the `reload_config` function to force a reload of the configuration.
- Implement the `temp_config` context manager for temporarily overriding the configuration for testing purposes.
**Success Criteria:**
- The `src/naq/config/__init__.py` file is created.
- `load_config` correctly initializes and validates the configuration.
- `get_config` provides the singleton configuration instance.
- `reload_config` effectively reloads the configuration.
- `temp_config` allows for temporary configuration overrides in tests.
**Testing:**
- Create unit tests for `config/__init__.py` covering:
    - Loading and retrieving configuration.
    - Reloading configuration.
    - Using the `temp_config` context manager to assert temporary overrides and proper restoration.
    - Ensuring validation is triggered when `validate=True` in `load_config`.
**Documentation:**
- Document the public API functions (`load_config`, `get_config`, `reload_config`) and the `temp_config` context manager in the API documentation.
- Provide examples of how to use the configuration API.