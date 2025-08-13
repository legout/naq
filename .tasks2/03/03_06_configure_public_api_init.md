### Sub-task 6: Configure Public API in __init__.py
**Description:** Populate the `src/naq/queue/__init__.py` file to export all the necessary classes and functions, ensuring backward compatibility with the rest of the application.
**Implementation Steps:**
- In `src/naq/queue/__init__.py`, import all public-facing classes and functions from `core.py`, `scheduled.py`, `async_api.py`, and `sync_api.py`.
- Define the `__all__` list to explicitly declare the public API.
**Success Criteria:**
- `from naq.queue import ...` statements work as they did before the refactoring.
- The public API exposed by the `naq.queue` package is identical to the previous version.
**Testing:** Run a script that performs imports from `naq.queue` to ensure all public components are accessible.
**Documentation:** Add a module-level docstring to `src/naq/queue/__init__.py` explaining its role in exposing the public API.