### Sub-task: Implement `StructuredLogger` Class
**Description:** Implement a `StructuredLogger` class for consistent, structured logging across the application.
**Implementation Steps:**
- Create the file `src/naq/utils/logging.py`.
- Implement the `StructuredLogger` class in `src/naq/utils/logging.py`.
- The class should wrap a standard Python `logging.Logger` instance.
- It should provide methods like `info`, `error`, `debug` that log with structured context.
- Implement an `operation_context` context manager for logging operation start, completion, and failures with timing and context.
**Success Criteria:**
- The `StructuredLogger` class is correctly implemented in `src/naq/utils/logging.py`.
- It logs messages with structured data.
- The `operation_context` manager correctly logs operation lifecycle events.
**Testing:**
- Unit tests for `StructuredLogger` to verify:
    - Messages are logged with correct level and extra fields.
    - `operation_context` logs start, complete, and failed events with accurate timing and context.
**Documentation:**
- Add a comprehensive docstring to the `StructuredLogger` class explaining its purpose, methods, and usage examples.
- Update the API documentation for `src/naq/utils/logging.py` to include `StructuredLogger`.