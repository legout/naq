### Sub-task: Implement `base.py` - Base Service Classes
**Description:** Create the foundational classes for all NAQ services, including `BaseService` for consistent interfaces and `ServiceManager` for managing service instances and dependencies. This centralizes configuration, connection lifecycle, error handling, and resource cleanup.
**Implementation Steps:**
- Create the `src/naq/services/` directory.
- Create `src/naq/services/base.py`.
- Implement the `BaseService` abstract class with `initialize`, `_do_initialize` (abstract), `cleanup`, and `__aenter__`/`__aexit__` methods for lifecycle management and context manager support.
- Implement the `ServiceManager` class for managing service instances, dependency injection, and configuration.
- Ensure consistent error handling patterns are considered in the base design.
**Success Criteria:**
- `src/naq/services/base.py` is created.
- `BaseService` provides abstract methods for initialization and cleanup, and supports `async with` context management.
- `ServiceManager` can register, retrieve, and manage service instances based on configuration.
- The base classes enforce consistent patterns for service development.
**Testing:**
- Unit tests for `BaseService` lifecycle methods (`initialize`, `cleanup`, `__aenter__`, `__aexit__`).
- Unit tests for `ServiceManager` for service registration, retrieval, and dependency handling.
- Verify proper resource cleanup upon service exit.
**Documentation:**
- Add documentation for `src/naq/services/base.py`, explaining `BaseService` and `ServiceManager` usage.
- Update the overall architecture documentation in `docs/architecture.qmd` to reflect the new service layer.