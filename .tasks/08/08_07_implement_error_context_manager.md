### Sub-task: Implement Error Context Manager
**Description:** Implement an asynchronous `error_context` manager for standardizing error handling in code blocks, allowing for suppression of specific exceptions and consistent logging.
**Implementation Steps:**
- Implement the `error_context` async context manager in `src/naq/utils/context_managers.py`.
- The context manager should accept `operation_name`, `logger`, and `suppress_exceptions`.
- It should log errors with traceback and re-raise by default, unless exceptions are in `suppress_exceptions`.
**Success Criteria:**
- The `error_context` manager is correctly implemented in `src/naq/utils/context_managers.py`.
- It logs errors consistently and suppresses specified exceptions.
- It correctly re-raises exceptions by default.
**Testing:**
- Unit tests for `error_context` to verify:
    - Exceptions are caught and logged.
    - Specific exceptions are suppressed without re-raising.
    - Exceptions are re-raised when not suppressed.
    - Behavior for both sync and async functions.
**Documentation:**
- Add a comprehensive docstring to the `error_context` manager explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/context_managers.py` to include `error_context`.