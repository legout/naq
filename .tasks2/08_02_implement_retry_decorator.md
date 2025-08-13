### Sub-task: Implement Retry Decorator
**Description:** Implement a flexible `retry` decorator with configurable backoff strategies (linear, exponential, jitter) that can be applied to both synchronous and asynchronous functions.
**Implementation Steps:**
- Create the file `src/naq/utils/decorators.py`.
- Implement the `retry` decorator in `src/naq/utils/decorators.py`.
- The decorator should accept parameters for `max_attempts`, `delay`, `backoff` strategy, `exceptions` to retry on, and an optional `on_retry` callback.
- Support both asynchronous (`async def`) and synchronous (`def`) functions.
- Implement the logic for linear, exponential, and jitter backoff strategies.
**Success Criteria:**
- The `retry` decorator is correctly implemented in `src/naq/utils/decorators.py`.
- The decorator functions as expected for both sync and async functions.
- All configurable parameters (`max_attempts`, `delay`, `backoff`, `exceptions`, `on_retry`) are functional.
**Testing:**
- Unit tests for `retry` decorator to verify:
    - Function retries correctly on specified exceptions.
    - `max_attempts` limit is respected.
    - Different backoff strategies (`linear`, `exponential`, `jitter`) calculate delays correctly.
    - `on_retry` callback is invoked as expected.
    - Decorated functions (both sync and async) return expected values or raise exceptions after retries.
**Documentation:**
- Add a comprehensive docstring to the `retry` decorator explaining its purpose, parameters, and usage examples for both sync and async functions.
- Update the API documentation for `src/naq/utils/decorators.py` to include the `retry` decorator.