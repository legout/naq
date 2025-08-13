### Sub-task: Extract FailedJobHandler into failed.py
**Description:** Move the `FailedJobHandler` class from `src/naq/worker.py` into its dedicated module, `src/naq/worker/failed.py`. This module will be responsible for processing failed jobs and managing retry logic.
**Implementation Steps:**
- Identify and copy the `FailedJobHandler` class and its dependencies from `src/naq/worker.py`.
- Paste the code into `src/naq/worker/failed.py`.
- Add all necessary imports.
- Define a clear interface, including an `__init__` method that accepts a `worker` reference.
**Success Criteria:**
- The `FailedJobHandler` class is fully contained within `src/naq/worker/failed.py`.
- The module is free of linting errors and passes type checks.
- The module has no circular dependencies.
**Testing:** Write unit tests for the `FailedJobHandler`. Verify failed job classification, retry logic, and failed job stream operations.
**Documentation:** Add a module-level docstring to `failed.py`. Update the `FailedJobHandler` class docstring.