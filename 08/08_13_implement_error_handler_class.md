### Sub-task: Implement `ErrorHandler` Class
**Description:** Implement a centralized `ErrorHandler` class for consistent error handling, logging, and dispatching errors to registered handlers.
**Implementation Steps:**
- Implement the `ErrorHandler` class in `src/naq/utils/error_handling.py`.
- The class should have an `__init__` method that accepts an optional `logger`.
- Implement `register_handler` to associate exception types with specific handler functions.
- Implement `handle_error` to:
    - Log the error with context and traceback.
    - Attempt to call a specific handler for the exception type.
    - Attempt to call handlers for parent exception types.
    - Re-raise the exception if `reraise` is `True` and no handler suppresses it.
- Implement a private helper `_call_handler` to correctly call async or sync handlers.
**Success Criteria:**
- The `ErrorHandler` class is correctly implemented in `src/naq/utils/error_handling.py`.
- It logs errors consistently and dispatches them to registered handlers.
- It correctly handles both specific and general exception types.
**Testing:**
- Unit tests for `ErrorHandler` to verify:
    - Errors are logged with correct details.
    - Registered handlers are invoked for matching exceptions.
    - Parent exception handlers are invoked correctly.
    - Exceptions are re-raised or suppressed based on `reraise` flag.
    - Async and sync handlers are called correctly.
**Documentation:**
- Add a comprehensive docstring to the `ErrorHandler` class explaining its purpose, methods, and usage examples.
- Update the API documentation for `src/naq/utils/error_handling.py` to include `ErrorHandler`.