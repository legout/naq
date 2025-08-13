### Sub-task: Update `src/naq/settings.py` for Connection Configuration
**Description:** Add the new NATS connection configuration schema to `src/naq/settings.py` to support configuration-driven connections.
**Implementation Steps:**
- Open `src/naq/settings.py`.
- Define a new Pydantic model or equivalent structure for NATS connection settings, reflecting the schema outlined in the "Configuration Integration" section of the task.
- Integrate this new configuration into the main `Config` class, allowing NATS settings to be loaded from environment variables and configuration files.
**Success Criteria:**
- NATS connection configuration schema is correctly added to `src/naq/settings.py`.
- The system can load NATS connection parameters from configuration.
- Environment variable overrides for NATS settings are supported.
**Testing:**
- Unit tests for `src/naq/settings.py` to verify:
    - NATS configuration can be loaded correctly from a dictionary or environment variables.
    - Default values are applied when not explicitly provided.
    - Invalid configuration raises appropriate errors.
**Documentation:**
- Update the documentation for `src/naq/settings.py` to describe the new NATS connection configuration options.
- Add a section to the project's main documentation (e.g., `docs/installation.qmd` or `docs/advanced.qmd`) detailing how to configure NATS connections.