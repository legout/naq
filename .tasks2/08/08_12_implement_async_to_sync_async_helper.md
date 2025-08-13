### Sub-task: Implement `async_to_sync` Helper
**Description:** Implement `async_to_sync` helper function to convert an asynchronous function into a synchronous one, running it in a new event loop.
**Implementation Steps:**
- Implement the `async_to_sync` function in `src/naq/utils/async_helpers.py`.
- This function should return a synchronous wrapper that uses `asyncio.run()` to execute the original asynchronous function in a new event loop.
**Success Criteria:**
- The `async_to_sync` function is correctly implemented in `src/naq/utils/async_helpers.py`.
- It successfully converts asynchronous functions to synchronous ones.
**Testing:**
- Unit tests for `async_to_sync` to verify:
    - An asynchronous function wrapped by `async_to_sync` can be called synchronously.
    - The wrapped function executes correctly and returns the expected result.
**Documentation:**
- Add a comprehensive docstring to the `async_to_sync` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/async_helpers.py` to include `async_to_sync`.