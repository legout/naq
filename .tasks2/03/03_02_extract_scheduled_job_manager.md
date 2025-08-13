### Sub-task 2: Extract ScheduledJobManager to scheduled.py
**Description:** Move the `ScheduledJobManager` class and its related logic from `src/naq/queue.py` to the new `src/naq/queue/scheduled.py` file. This isolates the scheduled job management functionality.
**Implementation Steps:**
- Identify and copy the `ScheduledJobManager` class from `src/naq/queue.py`.
- Paste the class and its related helper functions/imports into `src/naq/queue/scheduled.py`.
- Ensure all necessary dependencies (e.g., models, settings) are correctly imported in `src/naq/queue/scheduled.py`.
**Success Criteria:**
- The `ScheduledJobManager` class is fully contained within `src/naq/queue/scheduled.py`.
- The module is self-contained and does not have unresolved dependencies.
- The functionality of `ScheduledJobManager` remains unchanged.
**Testing:** Add unit tests specifically for the `ScheduledJobManager` class to verify its functionality in isolation.
**Documentation:** Add a module-level docstring to `src/naq/queue/scheduled.py` and ensure the `ScheduledJobManager` class has comprehensive docstrings.