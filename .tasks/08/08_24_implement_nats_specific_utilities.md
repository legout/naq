### Sub-task: Implement NATS-Specific Utilities
**Description:** Implement utility functions for common NATS operations that are not part of the connection management, such as subject building/parsing or stream existence checks.
**Implementation Steps:**
- Create the file `src/naq/utils/nats_helpers.py`.
- Implement a `build_subject` function to create NATS subjects from components.
- Implement a `parse_subject` function to parse NATS subjects into components.
- Implement a `stream_exists` function to check if a JetStream stream exists.
**Success Criteria:**
- The `nats_helpers.py` file is created and contains the specified utility functions.
- NATS utility functions correctly perform their operations.
**Testing:**
- Unit tests for `nats_helpers.py` to verify:
    - `build_subject` creates correct subjects.
    - `parse_subject` correctly extracts components from subjects.
    - `stream_exists` accurately reports stream existence.
**Documentation:**
- Add comprehensive docstrings to the NATS utility functions explaining their purpose and usage.
- Update the API documentation for `src/naq/utils/nats_helpers.py` to include these utilities.