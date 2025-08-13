### Sub-task 5: Move Schedule Model to `schedules.py`
**Description:** This sub-task involves moving the `Schedule` model class from the original `models.py` file to the new `src/naq/models/schedules.py` file.
**Implementation Steps:**
- Copy the `Schedule` class definition to `src/naq/models/schedules.py`.
- Update import statements to reference `enums.py` for any required `Enum` definitions.
- Add a comprehensive docstring to the `Schedule` class, including usage examples.
**Success Criteria:**
- The file `src/naq/models/schedules.py` contains the `Schedule` class.
- All internal imports from `enums.py` are correct.
- All class methods function as expected.
**Testing:** Add unit tests for the `Schedule` model to verify its methods work correctly in the new module.
**Documentation:** Update the API documentation for the `Schedule` model to reflect its new location.