### Sub-task: Refactor CLI Modules to Use Utilities
**Description:** Replace repeated code patterns in the `src/naq/cli/` modules with the newly created utility functions and decorators.
**Implementation Steps:**
- For each file in `src/naq/cli/`:
    - Apply `timing` and `log_errors` decorators where appropriate to CLI commands.
    - Replace manual logging with `StructuredLogger`.
    - Replace manual serialization/deserialization with `SerializationHelper` (if applicable).
    - Replace validation logic with `validation.py` utilities.
    - Replace NATS operations with `nats_helpers.py` utilities (if applicable).
**Success Criteria:**
- Code in `src/naq/cli/` is refactored to use common utilities.
- Reduced code duplication in `src/naq/cli/`.
- Functionality of CLI modules remains unchanged.
**Testing:**
- Run all existing unit and integration tests for the `cli` module.
- Verify that error handling, logging, and serialization work as expected after refactoring.
**Documentation:**
- Update any internal documentation or comments within the `src/naq/cli/` files to reflect the new utility usage.