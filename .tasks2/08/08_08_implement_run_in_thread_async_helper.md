### Sub-task: Implement `run_in_thread` Async Helper
**Description:** Implement an asynchronous helper function `run_in_thread` to execute synchronous functions in a thread pool, preventing blocking of the event loop.
**Implementation Steps:**
- Create the file `src/naq/utils/async_helpers.py`.
- Implement the `run_in_thread` async function in `src/naq/utils/async_helpers.py`.
- The function should accept a synchronous callable `func` and its arguments.
- It should use `asyncio.get_event_loop().run_in_executor()` to run the function in a thread pool.
**Success Criteria:**
- The `run_in_thread` function is correctly implemented in `src/naq/utils/async_helpers.py`.
- It successfully executes synchronous functions without blocking the main event loop.
**Testing:**
- Unit tests for `run_in_thread` to verify:
    - Synchronous function execution in a separate thread.
    - Non-blocking behavior of the main event loop.
    - Correct return values and exception handling.
**Documentation:**
- Add a comprehensive docstring to the `run_in_thread` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/async_helpers.py` to include `run_in_thread`.