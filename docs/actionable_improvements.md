# Actionable Improvement Plan for naq

## 1. Introduction

This document consolidates the findings from the documentation analysis and source code review into a single, actionable task list. It is intended to guide the development efforts to improve the `naq` library's security, performance, usability, and robustness. Tasks are prioritized from P0 (most critical) to P3 (lowest priority).

---

## P0: Critical Security Fixes

### Task 1: Address `pickle` Serialization Vulnerability

- **Goal**: Mitigate the Arbitrary Code Execution vulnerability caused by the default use of `pickle`.
- **Priority**: **CRITICAL**

- **Sub-task 1.1: Implement a Functional `JsonSerializer`**
    - **Action**: Complete the `JsonSerializer` class in `src/naq/job.py`. It must handle the serialization of callables (e.g., by storing their fully qualified import path as a string) and their arguments.
    - **Acceptance Criteria**: The `JsonSerializer` can successfully serialize and deserialize a job with common data types, allowing it to be executed by a worker.

- **Sub-task 1.2: Add Runtime Warning for `pickle`**
    - **Action**: In the `naq` startup sequence (e.g., in `__init__.py` or when a `Worker` or `Queue` is initialized), check the `JOB_SERIALIZER` setting. If it is `"pickle"`, log a prominent, high-level warning (e.g., `logger.warning(...)`) about the security risks.
    - **Files**: `src/naq/job.py` (in `get_serializer`), `src/naq/worker.py`.

- **Sub-task 1.3: Update Documentation**
    - **Action**: Create a new `docs/security.md` file explaining the `pickle` vulnerability in detail. Add warnings to `README.md` and `docs/api.md` about this.

- **Sub-task 1.4 (Future)**: Plan to switch the default serializer from `pickle` to `json` in the next major version release.

---

## P1: High-Impact Improvements

### Task 2: Consolidate Exception Handling

- **Goal**: Remove ambiguity in connection-related exceptions.
- **Action**: In `src/naq/exceptions.py`, remove the `ConnectionError` class and exclusively use `NaqConnectionError`. Update all modules (`src/naq/connection.py`, `src/naq/queue.py`, etc.) to import and raise `NaqConnectionError` consistently.
- **Files**: `src/naq/exceptions.py`, `src/naq/connection.py`, `src/naq/queue.py`.

### Task 3: Improve Synchronous API Efficiency

- **Goal**: Prevent the inefficient creation and teardown of NATS connections for every synchronous API call.
- **Action**: Create a new `SyncClient` class or context manager. This class should initialize and hold a NATS connection, and its methods (`enqueue`, `purge`, etc.) should reuse that connection.
- **Example Usage**:
  ```python
  with naq.SyncClient(nats_url="...") as client:
      client.enqueue(...)
      client.purge_queue(...)
  ```
- **Files**: `src/naq/queue.py` (or a new `client.py`).

### Task 4: Enhance Dashboard Functionality

- **Goal**: Make the dashboard a comprehensive monitoring and administration tool.
- **Sub-task 4.1: Expand Dashboard API**
    - **Action**: In `src/naq/dashboard/app.py`, add new API endpoints to fetch data for:
        - Queues (pending messages, consumer counts).
        - Scheduled jobs (listing, status).
        - Failed jobs.
- **Sub-task 4.2: Enhance Dashboard UI**
    - **Action**: Update `src/naq/dashboard/templates/dashboard.html` to consume the new API endpoints and display the information in new tables or sections.
- **Sub-task 4.3: Simplify Template Strategy**
    - **Action**: Remove the unused `htmy` components in `src/naq/dashboard/templates.py` to eliminate dead code and focus on the Jinja2/Datastar implementation.

---

## P2: Quality of Life & Future-Proofing

### Task 5: Improve "Poison Pill" Job Handling

- **Goal**: Prevent the loss of malformed job messages.
- **Action**: In `src/naq/worker.py`, modify the `process_message` exception handler. When a `SerializationError` is caught, instead of just terminating the message, publish the raw `msg.data` to a new, dedicated dead-letter stream (e.g., `naq_malformed_jobs`).
- **Files**: `src/naq/worker.py`.

### Task 6: Make Worker Shutdown Timeout Configurable

- **Goal**: Allow users to configure the graceful shutdown period for workers.
- **Action**: In `src/naq/worker.py`, change the hardcoded `timeout=30.0` in the `run` method's shutdown logic. Add a `shutdown_timeout` parameter to the `Worker`'s `__init__` method and use its value there.
- **Files**: `src/naq/worker.py`.

### Task 7: Add Confirmation for Destructive CLI Commands

- **Goal**: Prevent accidental data loss from the CLI.
- **Action**: In `src/naq/cli.py`, modify the `purge` command. Before executing the purge, use `typer.confirm()` to ask the user for confirmation. Add a `--force` option to bypass this check.
- **Files**: `src/naq/cli.py`.

---

## P3: Documentation & Code Cleanup

### Task 8: Consolidate Architecture Documentation

- **Goal**: Create a single, authoritative source for architectural information.
- **Action**: Merge the detailed content from `docs/comprehensive_architecture_plan.md` into `docs/architecture.md`. Afterwards, delete `docs/comprehensive_architecture_plan.md`.
- **Files**: `docs/architecture.md`, `docs/comprehensive_architecture_plan.md`.

### Task 9: Create Deployment & Tuning Guide

- **Goal**: Provide users with operational best practices.
- **Action**: Create a new `docs/deployment.md` file. Document topics such as:
    - NATS durability and clustering configuration.
    - Worker concurrency tuning.
    - Scheduler polling considerations and the scalability limitations of the KV scan.

### Task 10: General Code Cleanup

- **Goal**: Improve code health and remove unused code.
- **Action**:
    - In `src/naq/job.py`, refactor the `deserialize` method to simplify its logic.
    - In `src/naq/__init__.py`, remove the unused `_get_loop` function and related global variables.
- **Files**: `src/naq/job.py`, `src/naq/__init__.py`.
