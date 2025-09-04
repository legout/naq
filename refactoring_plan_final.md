# Final Refactoring Plan: Removing Compatibility Layers

## 1. Introduction

This document outlines the plan to complete the architectural refactoring of the `naq` library by removing the remaining compatibility layers and legacy components. The goal is to fully transition to the new, simplified architecture, resulting in a cleaner and more maintainable codebase.

## 2. Plan Details

### Phase 1: Deletion of Legacy and Compatibility Files

The first step is to remove the files that were kept for backward compatibility. This will enforce the use of the new architectural patterns across the entire codebase.

The following files will be deleted:
- `src/naq/settings.py`
- `src/naq/service_context.py`
- `src/naq/queue/async_api.py`
- `src/naq/queue/sync_api.py`

### Phase 2: Codebase Refactoring

With the legacy files removed, the codebase must be updated to remove all references to them.

1.  **Configuration**: All imports and usages of `naq.settings` will be replaced with direct calls to the new configuration system in `naq.config`.
2.  **API Unification**: All imports from `naq.queue.async_api` and `naq.queue.sync_api` will be updated to import from the unified `naq.queue.api` module. The `__init__.py` files in `naq` and `naq.queue` will be updated to reflect this change.
3.  **CLI Refactoring**: The command-line interface in `src/naq/cli/` will be refactored. The dependency on `service_context` will be removed. Each CLI command will be responsible for creating and managing the lifecycle of its own `NatsClient` instance.

### Phase 3: Documentation Update

The official documentation will be updated to reflect the final architecture.

1.  **Content Review**: All files in the `docs/` directory will be reviewed to remove mentions of the old `ServiceManager`, `settings.py`, and the separate `async_api`/`sync_api` modules.
2.  **Architecture Update**: The `architecture.qmd` file and any related diagrams will be updated to accurately represent the final, streamlined architecture.
3.  **Code Examples**: All code examples in the documentation (including the tutorial, quickstart, and API reference) will be verified and updated to use the new API and patterns.

### Phase 4: Test Suite Refactoring

The test suite will be updated to align with the new architecture and ensure full coverage.

1.  **Remove Obsolete Tests**: Tests for the deleted modules (e.g., `tests/test_sync_api.py`) will be removed.
2.  **Update Test Mocks**: Tests that mock the old `ServiceManager` or services will be updated to mock the `NatsClient` and its dependencies.
3.  **Refactor CLI Tests**: Tests for the CLI will be updated to reflect the new `NatsClient` management within each command.
4.  **Verification**: The entire test suite will be run to ensure all tests pass and that the refactoring has not introduced any regressions.
