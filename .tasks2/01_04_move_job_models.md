### Sub-task 4: Move Job Models to `jobs.py`
**Description:** This sub-task focuses on moving the `Job` and `JobResult` model classes, along with the `RetryDelayType` type hint, from the original `models.py` file to the new `src/naq/models/jobs.py` file.
**Implementation Steps:**
- Copy the `Job`, `JobResult`, and `RetryDelayType` definitions to `src/naq/models/jobs.py`.
- Update import statements to get `Enum` definitions from `enums.py` and event models from `events.py` as needed for type hints.
- Add comprehensive docstrings to both classes.
**Success Criteria:**
- The file `src/naq/models/jobs.py` contains the `Job` and `JobResult` classes and the `RetryDelayType` type hint.
- All internal imports from `enums.py` and `events.py` are correct.
- All class methods and properties function as expected.
**Testing:** Add or update unit tests for the `Job` and `JobResult` classes to ensure all methods and properties work correctly after the move.
**Documentation:** Update the API documentation for `Job` and `JobResult` to reflect their new module location.