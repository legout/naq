### Sub-task 3: Implement Configuration Schema and Validation
**Description:** Define the JSON Schema for NAQ configuration and implement a `ConfigValidator` class to validate loaded configuration data against this schema.
**Implementation Steps:**
- Create the file `src/naq/config/schema.py`.
- Define the `CONFIG_SCHEMA` dictionary using JSON Schema syntax for `nats`, `workers`, and `events` sections, including type, properties, required fields, and minimum values.
- Implement the `ConfigValidator` class.
- Implement the `__init__` method to accept an optional schema.
- Implement the `validate` method to perform schema validation using `jsonschema.validate`. Raise `ConfigurationError` on validation failure.
- Implement `validate_nats_servers` to specifically validate NATS server URLs.
**Success Criteria:**
- The `src/naq/config/schema.py` file is created.
- The `CONFIG_SCHEMA` accurately represents the expected structure and types of the NAQ configuration.
- The `ConfigValidator` successfully validates correct configurations and raises `ConfigurationError` for invalid ones.
- `validate_nats_servers` correctly identifies invalid NATS server URLs.
**Testing:**
- Create unit tests for `schema.py` covering:
    - Successful validation of a valid configuration.
    - Failure cases for invalid data types, missing required fields, and out-of-range values.
    - Specific tests for `validate_nats_servers` with valid and invalid URLs.
**Documentation:**
- Add a detailed section to the configuration documentation outlining the `CONFIG_SCHEMA` and its purpose.
- Document the `ConfigValidator` class and its usage.