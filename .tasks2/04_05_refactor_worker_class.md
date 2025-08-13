### Sub-task: Refactor Worker Class into core.py
**Description:** Move the main `Worker` class into `src/naq/worker/core.py` and refactor it to use the new manager classes.
**Implementation Steps:**
- Copy the `Worker` class from `src/naq/worker.py` to `src/naq/worker/core.py`.
- Remove the `WorkerStatusManager`, `JobStatusManager`, and `FailedJobHandler` classes from `src/naq/worker/core.py`.
- Import the manager classes from ` .status`, ` .jobs`, and ` .failed`.
- In the `Worker`'s `__init__` method, instantiate the manager classes, passing the `worker` instance (`self`) to them.
- Update the `Worker` class logic to call the methods on the manager instances instead of its own methods (e.g., `self.status_manager.update_status(...)`).
**Success Criteria:**
- The `Worker` class resides in `src/naq/worker/core.py`.
- The `Worker` class correctly imports and uses the manager classes.
- All original public methods of the `Worker` class are maintained.
- The file does not exceed 400 lines.
**Testing:** Run integration tests to ensure the refactored `Worker` class coordinates correctly with the manager modules. Test the full job processing lifecycle, including success, failure, and status reporting.
**Documentation:** Update the `Worker` class docstring to reflect its new role as a coordinator and its dependencies on the manager modules.