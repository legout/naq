### Sub-task 6: Implement Public API in `__init__.py`
**Description:** This sub-task is to create a public-facing API in `src/naq/models/__init__.py` that maintains backward compatibility with the old `models.py` module.
**Implementation Steps:**
- In `src/naq/models/__init__.py`, import all classes, `Enum` definitions, and type hints from the new sub-modules (`enums.py`, `events.py`, `jobs.py`, `schedules.py`).
- Define the `__all__` variable to explicitly export all symbols that were previously available from `naq.models`.
- Optionally, add `DeprecationWarning` for any symbols that may be changed in the future.
**Success Criteria:**
- The `__init__.py` file correctly imports and exports all required symbols.
- The public API of `naq.models` remains identical to what it was before the refactor.
- Code that previously imported from `naq.models` continues to work without modification.
**Testing:** Write tests that specifically import various symbols from `naq.models` to confirm that the public API is unchanged and that no circular import errors are introduced.
**Documentation:** Add a module-level docstring to `src/naq/models/__init__.py` explaining its purpose of maintaining backward compatibility.