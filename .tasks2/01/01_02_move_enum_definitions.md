### Sub-task 2: Move Enum Definitions to `enums.py`
**Description:** This sub-task focuses on moving all `Enum` definitions from the original `models.py` file to the new `src/naq/models/enums.py` file. This is the first step in separating concerns and has the fewest dependencies.
**Implementation Steps:**
- Copy the following `Enum` definitions from `src/naq/models.py` to `src/naq/models/enums.py`:
    - `JOB_STATUS`
    - `JobEventType`
    - `WorkerEventType`
    - The `RETRY_STRATEGY` import and `VALID_RETRY_STRATEGIES` constant.
- Add comprehensive docstrings to each `Enum`, including usage examples.
- Ensure all necessary type annotations are included.
**Success Criteria:**
- The file `src/naq/models/enums.py` contains all specified `Enum` definitions.
- Each `Enum` has a complete docstring with usage examples.
- All required imports are present, and the code is free of linting errors.
**Testing:** Run linting and type-checking on `src/naq/models/enums.py` to ensure correctness.
**Documentation:** Update any relevant developer documentation to reflect the new location of these `Enum` definitions.