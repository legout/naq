### Sub-task 10: Create Example Configuration Files
**Description:** Provide example YAML configuration files for different environments.
**Implementation Steps:**
- Create the file `examples/configs/development.yaml`.
- Create the file `examples/configs/production.yaml`.
- Create the file `examples/configs/testing.yaml`.
- Populate these files with realistic configuration values as per the YAML configuration schema.
**Success Criteria:**
- All three example configuration files are created.
- The example files are syntactically correct YAML and adhere to the defined `CONFIG_SCHEMA`.
- The content of each file reflects typical settings for its respective environment.
**Testing:**
- Manually or programmatically validate the example YAML files against the `CONFIG_SCHEMA`.
- Ensure the `generate_config` CLI command can generate these examples.
**Documentation:**
- Reference these example files in the main configuration documentation.