### Sub-task: Implement `retry_async` Helper
**Description:** Implement an asynchronous `retry_async` helper function to retry asynchronous operations with exponential backoff.
**Implementation Steps:**
- Implement the `retry_async` async function in `src/naq/utils/async_helpers.py`.
- The function should accept the async function to retry, `max_attempts`, `delay`, `backoff` factor, and `exceptions` to retry on.
- Include exponential backoff logic.
**Success Criteria:**
- The `retry_async` function is correctly implemented in `src/naq/utils/async_helpers.py`.
- It retries asynchronous operations with the specified parameters.
- Exponential backoff is correctly applied.
**Testing:**
- Unit tests for `retry_async` to verify:
    - Retries occur on specified exceptions.
    - `max_attempts` limit is respected.
    - Exponential backoff calculations are correct.
    - Function returns expected values or raises exceptions after retries.
**Documentation:**
- Add a comprehensive docstring to the `retry_async` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/async_helpers.py` to include `retry_async`.