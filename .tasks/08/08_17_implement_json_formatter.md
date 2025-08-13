### Sub-task: Implement JSON Formatter for Logging
**Description:** Implement a `JSONFormatter` class for Python's `logging` module to output logs in a structured JSON format.
**Implementation Steps:**
- Implement the `JSONFormatter` class in `src/naq/utils/logging.py`.
- Inherit from `logging.Formatter`.
- Override the `format` method to convert `LogRecord` objects into JSON strings, including standard attributes and any extra fields.
**Success Criteria:**
- The `JSONFormatter` class is correctly implemented in `src/naq/utils/logging.py`.
- It correctly formats log records into valid JSON strings.
**Testing:**
- Unit tests for `JSONFormatter` to verify:
    - Standard log attributes are correctly included in the JSON output.
    - Extra fields are included.
    - Exception information is correctly formatted when present.
**Documentation:**
- Add a comprehensive docstring to the `JSONFormatter` class explaining its purpose.
- Update the API documentation for `src/naq/utils/logging.py` to include `JSONFormatter`.