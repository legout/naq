### Sub-task: Implement `wrap_naq_exception` Function
**Description:** Implement a utility function `wrap_naq_exception` to wrap generic exceptions in NAQ-specific exceptions, providing consistent error types and preserving original tracebacks.
**Implementation Steps:**
- Implement the `wrap_naq_exception` function in `src/naq/utils/error_handling.py`.
- The function should accept an `Exception` object, an optional `context` string, and a `original_traceback` flag.
- It should map common exceptions (e.g., `ConnectionError`, `ValueError`, `TypeError`, `pickle.PicklingError`, `json.JSONDecodeError`) to specific NAQ exceptions (`NaqConnectionError`, `ConfigurationError`, `SerializationError`).
- Ensure the original exception is chained using `from` if `original_traceback` is `True`.
**Success Criteria:**
- The `wrap_naq_exception` function is correctly implemented in `src/naq/utils/error_handling.py`.
- It correctly maps generic exceptions to NAQ-specific types.
- Original tracebacks are preserved.
**Testing:**
- Unit tests for `wrap_naq_exception` to verify:
    - Correct mapping of different exception types.
    - Preservation of original exception chain.
    - Behavior when `original_traceback` is `False`.
**Documentation:**
- Add a comprehensive docstring to the `wrap_naq_exception` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/error_handling.py` to include `wrap_naq_exception`.