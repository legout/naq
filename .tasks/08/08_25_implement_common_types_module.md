### Sub-task: Implement Common Types Module
**Description:** Create a module for common type definitions used across the NAQ codebase to avoid redundancy and ensure consistency.
**Implementation Steps:**
- Create the file `src/naq/utils/types.py`.
- Add any common type aliases, `TypedDict` definitions, or small data classes that are frequently used across different modules and do not belong to a specific domain (like `models`).
**Success Criteria:**
- The `types.py` file is created and contains common type definitions.
- These types are accessible and reusable across the codebase.
**Testing:**
- Unit tests for `types.py` to verify:
    - Type aliases and definitions are correctly interpreted by type checkers.
**Documentation:**
- Document any new type definitions in `src/naq/utils/types.py`.
- Update the API documentation for `src/naq/utils/types.py`.