### Sub-task 2: Update Package `__init__.py` Files
**Description:** Update `__init__.py` files within specific sub-packages (`models`, `queue`, `worker`, `cli`, `services`, `utils`) to maintain backward compatibility for existing imports while properly exposing new modular components via `__all__`.
**Implementation Steps:**
- Modify `src/naq/models/__init__.py` to import from `enums`, `jobs`, `events`, and `schedules` sub-modules and explicitly define `__all__` for public exports.
- Modify `src/naq/queue/__init__.py` to import from `core`, `async_api`, and `sync_api` sub-modules and explicitly define `__all__`.
- Modify `src/naq/worker/__init__.py` to import from `core`, `status`, `jobs`, and `failed` sub-modules and explicitly define `__all__`.
- Modify `src/naq/cli/__init__.py` to import `app` and `main` from the `main` sub-module and explicitly define `__all__`.
- Modify `src/naq/services/__init__.py` to import from `base`, `connection`, `jobs`, `events`, `streams`, `kv_stores`, `scheduler` sub-modules and explicitly define `__all__`.
- Modify `src/naq/utils/__init__.py` to import from `decorators`, `context_managers`, `async_helpers`, `error_handling`, `logging`, `serialization`, `run_async_from_sync`, and `setup_logging` sub-modules and explicitly define `__all__`.
**Success Criteria:**
- All specified `__init__.py` files are updated with the correct imports from their respective sub-modules.
- The `__all__` variable in each `__init__.py` file correctly lists the public API elements for that package.
- Existing imports from these sub-packages (e.g., `from naq.models import Job`) continue to function without modification.
**Testing:**
- Create or update tests to explicitly verify that both legacy imports (`from naq.models import Job`) and new modular imports (`from naq.models.jobs import Job`) work correctly for each affected package.
- Run the full test suite to ensure no regressions are introduced due to these `__init__.py` changes.
**Documentation:**
- Update the API reference documentation for each of these sub-packages (e.g., `docs/api/models.qmd`, `docs/api/queue.qmd`) to reflect the new internal structure and clarify which imports are publicly exposed.
- Ensure docstring examples within these `__init__.py` files and their sub-modules are consistent with the new import patterns.