### Sub-task: Implement Validation Utilities
**Description:** Implement utility functions for common validation patterns, including parameter validation, type checking, and data conversion.
**Implementation Steps:**
- Create the file `src/naq/utils/validation.py`.
- Implement a `validate_parameter` function to check if a parameter meets specified criteria (e.g., not None, within range, matches regex).
- Implement a `ensure_type` function to perform type checking and optionally convert data to the expected type, raising an error on mismatch.
**Success Criteria:**
- The `validation.py` file is created and contains the specified utility functions.
- Validation functions correctly identify invalid inputs and convert types as expected.
**Testing:**
- Unit tests for `validation.py` to verify:
    - `validate_parameter` works for various validation rules.
    - `ensure_type` correctly validates and converts types, and raises errors for invalid types.
**Documentation:**
- Add comprehensive docstrings to the validation utility functions explaining their purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/utils/validation.py` to include these utilities.