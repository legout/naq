### Sub-task: Extract WorkerStatusManager into status.py
**Description:** Move the `WorkerStatusManager` class from `src/naq/worker.py` into its own dedicated module, `src/naq/worker/status.py`. This module will be responsible for all worker status reporting and heartbeat functionality.
**Implementation Steps:**
- Identify and copy the `WorkerStatusManager` class and all its related imports and dependencies from `src/naq/worker.py`.
- Paste the copied code into the new `src/naq/worker/status.py` file.
- Ensure the class is self-contained and all necessary imports are included.
- Design a clean interface for the `Worker` class to use, including a `__init__` method that accepts a `worker` reference.
**Success Criteria:**
- The `WorkerStatusManager` class is fully contained within `src/naq/worker/status.py`.
- The module is free of linting errors and passes type checks.
- The module has no circular dependencies.
**Testing:** Add unit tests to verify the status management logic independently. Test status transitions, heartbeat scheduling, and KV store operations.
**Documentation:** Add a module-level docstring to `status.py` explaining its purpose. Update the class docstring for `WorkerStatusManager` to reflect its new location and role.