### Sub-task: Implement Timing Decorator
**Description:** Implement a `timing` decorator for measuring function execution time, with optional logging for slow operations.
**Implementation Steps:**
- Implement the `timing` decorator in `src/naq/utils/decorators.py`.
- The decorator should accept parameters for a `logger` instance, a `threshold_ms` for slow operation logging, and an optional `message`.
- Measure execution time using `time.perf_counter()`.
- Support both asynchronous and synchronous functions.
**Success Criteria:**
- The `timing` decorator is correctly implemented in `src/naq/utils/decorators.py`.
- It accurately measures and logs function execution times.
- Slow operations are logged when `threshold_ms` is exceeded.
**Testing:**
- Unit tests for `timing` decorator to verify:
    - Accurate time measurement for both sync and async functions.
    - Logging occurs correctly when `threshold_ms` is met or exceeded.
    - No logging occurs when `threshold_ms` is not met.
**Documentation:**
- Add a comprehensive docstring to the `timing` decorator explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/decorators.py` to include the `timing` decorator.