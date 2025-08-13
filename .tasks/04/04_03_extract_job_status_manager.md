### Sub-task: Extract JobStatusManager into jobs.py
**Description:** Move the `JobStatusManager` class from `src/naq/worker.py` into its own module, `src/naq/worker/jobs.py`. This module will handle job status tracking and dependency management.
**Implementation Steps:**
- Identify and copy the `JobStatusManager` class and its dependencies from `src/naq/worker.py`.
- Paste the code into `src/naq/worker/jobs.py`.
- Ensure all required imports are present.
- Define a clear interface, including an `__init__` method that accepts a `worker` reference.
**Success Criteria:**
- The `JobStatusManager` class is fully contained within `src/naq/worker/jobs.py`.
- The module is free of linting errors and passes type checks.
- The module has no circular dependencies.
**Testing:** Create unit tests for the `JobStatusManager` class. Test job status lifecycle management, dependency resolution logic, and KV store interactions.
**Documentation:** Add a module-level docstring to `jobs.py`. Update the `JobStatusManager` class docstring.