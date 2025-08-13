### Sub-task 7: Implement Import Compatibility Tests
**Description:** Create tests to ensure that existing import paths and patterns remain functional after the service layer refactoring, preventing breaking changes for users.
**Implementation Steps:**
- Create `tests/test_compatibility/test_imports.py`.
- Write tests that import key components and functions from `naq` and `naq.models` using their legacy import paths (e.g., `from naq import Queue`, `from naq.models import Job`).
- Assert that these imports succeed and the imported objects are of the expected type or are callable.
**Success Criteria:**
- All specified legacy imports (main package, models, events) function without errors.
- No breaking changes are introduced to the public API's import structure.
- All import compatibility tests pass.
**Testing:**
- Run `pytest tests/test_compatibility/test_imports.py`.
**Documentation:**
- In `docs/migration.qmd` (if exists, or create one) or `docs/quickstart.qmd`, explicitly state that backward compatibility for imports is maintained.