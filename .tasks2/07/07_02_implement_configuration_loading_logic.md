### Sub-task 2: Implement Configuration Loading Logic
**Description:** Develop the `ConfigLoader` class in `loader.py` to handle loading configuration from multiple sources (YAML files, environment variables, defaults) with a defined priority order and support for environment variable interpolation within YAML files.
**Implementation Steps:**
- Create the file `src/naq/config/loader.py`.
- Implement the `ConfigLoader` class with a `DEFAULT_CONFIG_PATHS` attribute.
- Implement the `__init__` method to accept an optional `config_path`.
- Implement the `load_config` method to orchestrate loading from defaults, YAML files (explicit then default paths), and environment variables, respecting the priority order: explicit config file > default config files > environment variables > defaults.
- Implement `_load_yaml_file` to read and parse YAML files, including error handling for `FileNotFoundError` and `yaml.YAMLError`.
- Implement `_interpolate_env_vars` to replace `${VAR:default}` patterns in YAML content with environment variable values.
- Implement `_load_env_variables` to map `NAQ_*` environment variables to the nested configuration structure.
- Implement `_convert_env_value` to correctly convert string environment variable values to boolean, integer, or float types.
- Implement `_merge_config` and `_apply_environment_overrides` methods. (These are implied by the `load_config` logic but not explicitly detailed as separate methods in the provided `loader.py` snippet. They will be needed for a complete implementation.)
- Ensure the `ConfigurationError` exception is properly raised.
**Success Criteria:**
- The `src/naq/config/loader.py` file is created.
- The `ConfigLoader` class correctly loads configuration from specified YAML files and environment variables.
- Configuration priority is respected: explicit config file > default config files > environment variables.
- Environment variable interpolation within YAML files functions as expected.
- Error handling for file operations and YAML parsing is robust.
**Testing:**
- Create unit tests for `loader.py` covering:
    - File discovery and priority (explicit path, current directory, user config, system config).
    - Environment variable override.
    - Environment variable interpolation in YAML.
    - Error handling for invalid YAML or non-existent files.
- Ensure tests cover different data types for environment variable conversion.
**Documentation:**
- Document the configuration loading process, including the priority order of sources and the environment variable interpolation feature.
- Explain the `ConfigLoader` class and its methods in the API documentation.