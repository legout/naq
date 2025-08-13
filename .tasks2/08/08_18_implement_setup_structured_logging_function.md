### Sub-task: Implement `setup_structured_logging` Function
**Description:** Implement a `setup_structured_logging` function to configure the Python logging system to use the `StructuredLogger` and `JSONFormatter`.
**Implementation Steps:**
- Implement the `setup_structured_logging` function in `src/naq/utils/logging.py`.
- The function should accept `level`, `format_type` (e.g., "json", "text"), and `extra_fields`.
- It should configure the root logger, remove existing handlers, and add a new `StreamHandler` with the appropriate formatter (`JSONFormatter` or standard `Formatter`).
- It should return an instance of `StructuredLogger`.
**Success Criteria:**
- The `setup_structured_logging` function is correctly implemented in `src/naq/utils/logging.py`.
- It successfully configures structured logging with JSON output.
**Testing:**
- Unit tests for `setup_structured_logging` to verify:
    - Logger is configured with the correct level.
    - `JSONFormatter` is applied when `format_type` is "json".
    - Standard formatter is applied otherwise.
    - `StructuredLogger` instance is returned.
**Documentation:**
- Add a comprehensive docstring to the `setup_structured_logging` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/logging.py` to include `setup_structured_logging`.