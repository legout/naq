### Sub-task: Implement `sync_to_async` Helper
**Description:** Implement `sync_to_async` helper function to convert a synchronous function into an asynchronous one, allowing it to be awaited.
**Implementation Steps:**
- Implement the `sync_to_async` function in `src/naq/utils/async_helpers.py`.
- This function should return an async wrapper that uses `run_in_thread` to execute the original synchronous function.
**Success Criteria:**
- The `sync_to_async` function is correctly implemented in `src/naq/utils/async_helpers.py`.
- It successfully converts synchronous functions to awaitable asynchronous functions.
**Testing:**
- Unit tests for `sync_to_async` to verify:
    - A synchronous function wrapped by `sync_to_async` can be `await`ed.
    - The wrapped function executes correctly and returns the expected result.
**Documentation:**
- Add a comprehensive docstring to the `sync_to_async` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/async_helpers.py` to include `sync_to_async`.