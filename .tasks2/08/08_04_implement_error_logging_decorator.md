### Sub-task: Implement Error Logging Decorator
**Description:** Implement a `log_errors` decorator for consistently logging exceptions that occur in decorated functions.
**Implementation Steps:**
- Implement the `log_errors` decorator in `src/naq/utils/decorators.py`.
- The decorator should accept parameters for a `logger` instance, `level` of logging, and a `reraise` flag.
- Catch `Exception` (or specific exceptions) and log the error message and traceback.
- Support both asynchronous and synchronous functions.
**Success Criteria:**
- The `log_errors` decorator is correctly implemented in `src/naq/utils/decorators.py`.
- It accurately logs exceptions including traceback.
- The `reraise` flag controls whether the exception is re-raised after logging.
**Testing:**
- Unit tests for `log_errors` decorator to verify:
    - Exceptions are caught and logged correctly.
    - Traceback is included in the log.
    - `reraise` flag functionality (exception is re-raised or suppressed).
    - Behavior for both sync and async functions.
**Documentation:**
- Add a comprehensive docstring to the `log_errors` decorator explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/decorators.py` to include the `log_errors` decorator.