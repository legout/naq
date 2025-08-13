### Sub-task 3: Move Event Models to `events.py`
**Description:** This sub-task involves moving the `JobEvent` and `WorkerEvent` model classes from the original `models.py` file to the new `src/naq/models/events.py` file.
**Implementation Steps:**
- Copy the `JobEvent` and `WorkerEvent` class definitions to `src/naq/models/events.py`.
- Update the import statements to reference `enums.py` for any required `Enum` definitions (e.g., `from .enums import ...`).
- Add comprehensive docstrings to both classes, including usage examples.
**Success Criteria:**
- The file `src/naq/models/events.py` contains the `JobEvent` and `WorkerEvent` classes.
- All internal imports from `enums.py` are correct.
- All class methods and properties are fully functional.
**Testing:** Add unit tests for the `JobEvent` and `WorkerEvent` classes to verify that all methods, including factory methods, work as expected.
**Documentation:** Update the API documentation for `JobEvent` and `WorkerEvent` to reflect their new module location.