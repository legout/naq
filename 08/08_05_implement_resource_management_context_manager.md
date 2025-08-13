### Sub-task: Implement Resource Management Context Manager
**Description:** Implement a generic `managed_resource` context manager for acquiring and releasing resources, with optional error handling.
**Implementation Steps:**
- Create the file `src/naq/utils/context_managers.py`.
- Implement the `managed_resource` async context manager in `src/naq/utils/context_managers.py`.
- The context manager should accept `acquire_func` (coroutine for resource acquisition), `release_func` (coroutine for resource release), and an optional `on_error` callback.
- Ensure proper resource acquisition, yielding, and release, even if errors occur within the `async with` block.
**Success Criteria:**
- The `managed_resource` context manager is correctly implemented in `src/naq/utils/context_managers.py`.
- It properly acquires and releases resources.
- The `on_error` callback is invoked on exceptions.
**Testing:**
- Unit tests for `managed_resource` to verify:
    - Resource acquisition and release in success and failure scenarios.
    - `on_error` callback is triggered correctly.
    - Resource cleanup happens regardless of exceptions.
**Documentation:**
- Add a comprehensive docstring to the `managed_resource` context manager explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/context_managers.py` to include `managed_resource`.