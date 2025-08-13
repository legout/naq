### Sub-task 5: Extract Sync API Wrappers to sync_api.py
**Description:** Move all synchronous wrapper functions (`*_sync`) from `src/naq/queue.py` to `src/naq/queue/sync_api.py`.
**Implementation Steps:**
- Move all functions ending with `_sync` to `src/naq/queue/sync_api.py`.
- Import the corresponding async functions from `naq.queue.async_api`.
- Preserve the thread-local connection management logic.
**Success Criteria:**
- All synchronous wrapper functions are located in `src/naq/queue/sync_api.py`.
- The sync functions correctly call their async counterparts.
- Thread-local connection optimization is maintained.
**Testing:** Test each synchronous wrapper function to confirm it correctly wraps the async function and manages the event loop and connections properly.
**Documentation:** Add clear docstrings to each synchronous function in `src/naq/queue/sync_api.py`.