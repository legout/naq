### Sub-task: Implement `gather_with_concurrency` Async Helper
**Description:** Implement an asynchronous helper function `gather_with_concurrency` to execute a list of awaitable tasks with a limited concurrency.
**Implementation Steps:**
- Implement the `gather_with_concurrency` async function in `src/naq/utils/async_helpers.py`.
- The function should accept a list of `Awaitable` tasks and a `concurrency` limit.
- It should use `asyncio.Semaphore` to limit the number of concurrently running tasks.
**Success Criteria:**
- The `gather_with_concurrency` function is correctly implemented in `src/naq/utils/async_helpers.py`.
- It executes tasks concurrently while respecting the specified concurrency limit.
**Testing:**
- Unit tests for `gather_with_concurrency` to verify:
    - Tasks are executed with the correct concurrency limit.
    - All tasks complete successfully.
    - Behavior with various numbers of tasks and concurrency limits.
**Documentation:**
- Add a comprehensive docstring to the `gather_with_concurrency` function explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/async_helpers.py` to include `gather_with_concurrency`.