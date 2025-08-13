### Sub-task 3: Extract Queue Class to core.py
**Description:** Move the main `Queue` class from `src/naq/queue.py` to `src/naq/queue/core.py`. This will form the core of the new queue package.
**Implementation Steps:**
- Copy the `Queue` class from `src/naq/queue.py` to `src/naq/queue/core.py`.
- Update imports within `core.py` to import `ScheduledJobManager` from `naq.queue.scheduled`.
- Remove any logic that is now handled by `ScheduledJobManager`, delegating those calls to an instance of it.
**Success Criteria:**
- The `Queue` class is located in `src/naq/queue/core.py`.
- It correctly imports and uses `ScheduledJobManager` from the new `scheduled` module.
- All original `Queue` methods are present and functional.
**Testing:** Create or update unit tests for the `Queue` class to ensure all its methods work as expected with the new structure.
**Documentation:** Update the docstrings for the `Queue` class to reflect the new structure and its dependencies.