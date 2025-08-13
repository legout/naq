### Sub-task: Implement `create_error_context` Function
**Description:** Implement a utility function `create_error_context` to generate a dictionary with contextual information about an operation for structured logging.
**Implementation Steps:**
- Implement the `create_error_context` function in `src/naq/utils/error_handling.py`.
- The function should accept an `operation_name` and include details like `timestamp`, `traceback` (using `traceback.format_exc()`), and `thread_id` (using `threading.get_ident()`).
**Success Criteria:**
- The `create_error_context` function is correctly implemented in `src/naq/utils/error_handling.py`.
- It generates a dictionary with accurate contextual information.
**Testing:**
- Unit tests for `create_error_context` to verify:
    - The returned dictionary contains all expected keys.
    - The values (timestamp, traceback, thread_id) are correctly captured.
**Documentation:**
- Add a comprehensive docstring to the `create_error_context` function explaining its purpose, parameters, and return value.
- Update the API documentation for `src/naq/utils/error_handling.py` to include `create_error_context`.