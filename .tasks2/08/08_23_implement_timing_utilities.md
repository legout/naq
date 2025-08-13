### Sub-task: Implement Timing Utilities
**Description:** Implement utility functions for performance timing, such as a simple stopwatch and a function for measuring execution time.
**Implementation Steps:**
- Create the file `src/naq/utils/timing.py`.
- Implement a `Stopwatch` class with `start`, `stop`, and `elapsed` methods.
- Implement a `measure_execution_time` decorator or context manager for ad-hoc timing.
**Success Criteria:**
- The `timing.py` file is created and contains the specified utility functions.
- Timing utilities accurately measure durations.
**Testing:**
- Unit tests for `timing.py` to verify:
    - `Stopwatch` correctly measures time intervals.
    - `measure_execution_time` accurately reports function execution time.
**Documentation:**
- Add comprehensive docstrings to the timing utility functions explaining their purpose and usage.
- Update the API documentation for `src/naq/utils/timing.py` to include these utilities.