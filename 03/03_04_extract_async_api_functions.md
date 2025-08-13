### Sub-task 4: Extract Async API Functions to async_api.py
**Description:** Move all high-level asynchronous API functions from `src/naq/queue.py` to `src/naq/queue/async_api.py`.
**Implementation Steps:**
- Move the async functions (`enqueue`, `enqueue_at`, etc.) to `src/naq/queue/async_api.py`.
- Add necessary imports, including the `Queue` class from `naq.queue.core`.
- Ensure function signatures and behavior are preserved.
**Success Criteria:**
- All specified async functions are present in `src/naq/queue/async_api.py`.
- The functions correctly interact with the `Queue` class.
- Function signatures remain identical to the original implementation.
**Testing:** Write integration tests for each async API function to ensure they work correctly with the refactored `Queue` and `ScheduledJobManager`.
**Documentation:** Add docstrings to each function in `src/naq/queue/async_api.py` explaining its purpose, parameters, and return values.