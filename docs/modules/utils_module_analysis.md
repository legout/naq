# Analysis of [`src/naq/utils.py`](src/naq/utils.py)

## 1. Module Overview and Role

The [`src/naq/utils.py`](src/naq/utils.py) module provides essential utility functions for the `naq` library. Its primary responsibilities are:

- Facilitating the execution of asynchronous coroutines from synchronous code paths.
- Providing a standardized way to configure logging using the `loguru` library.

This module encapsulates common, reusable logic that supports various components within the `naq` ecosystem.

## 2. Public Interfaces

### A. `run_async_from_sync(coro: Coroutine[Any, Any, T]) -> T` ([`src/naq/utils.py:11`](src/naq/utils.py:11))

- **Description**: Executes an asynchronous coroutine from a synchronous context. Manages the asyncio event loop, running the coroutine in a new loop if possible. If an event loop is already running in the current thread, it raises a `RuntimeError` to prevent conflicts, guiding the user to use asynchronous calling patterns.
- **Arguments**:
  - `coro: Coroutine[Any, Any, T]`: The asynchronous coroutine to be executed.
- **Returns**: `T` — The result of the executed coroutine.
- **Raises**: `RuntimeError` if `asyncio.run()` is called while another event loop is already running in the same thread.

### B. `setup_logging(level: str = "INFO")` ([`src/naq/utils.py:57`](src/naq/utils.py:57))

- **Description**: Configures the `loguru` logger for the application. Removes any pre-existing default handlers and adds a new handler that outputs to `sys.stdout`. The logging level and colorization can be controlled.
- **Arguments**:
  - `level: str` (default: `"INFO"`): The minimum logging level (e.g., "DEBUG", "INFO", "WARNING", "ERROR").
- **Returns**: `None`.

## 3. Key Functionalities

- **Asynchronous Code Execution from Synchronous Contexts**:
  - `run_async_from_sync` ([`src/naq/utils.py:11`](src/naq/utils.py:11)) bridges `naq`'s asynchronous core with synchronous application code.
  - Uses `asyncio.run(coro)` ([`src/naq/utils.py:39`](src/naq/utils.py:39)) when no event loop is active.
  - If an event loop is already running, raises a `RuntimeError` with an informative message ([`src/naq/utils.py:41-48`](src/naq/utils.py:41-48)), promoting the use of native async calls in such environments.

- **Centralized Logging Configuration**:
  - `setup_logging` ([`src/naq/utils.py:57`](src/naq/utils.py:57)) provides a consistent way to initialize logging across the `naq` library or applications using it.
  - Leverages `loguru` for flexible and powerful logging.
  - Removes default handlers ([`src/naq/utils.py:59`](src/naq/utils.py:59)) and adds a new, colorized `stdout` sink ([`src/naq/utils.py:60-64`](src/naq/utils.py:60-64)).

## 4. Dependencies and Interactions

- **Internal `naq` Module Interactions**:
  - [`src/naq/job.py`](src/naq/job.py): `Job.fetch_result_sync()` uses `run_async_from_sync` ([`src/naq/utils.py:11`](src/naq/utils.py:11)) to execute the asynchronous `Job.fetch_result()` from a synchronous call.
  - Other modules or entry points (e.g., [`src/naq/cli.py`](src/naq/cli.py)) typically call `setup_logging` ([`src/naq/utils.py:57`](src/naq/utils.py:57)) at startup.

- **External Library Dependencies**:
  - `asyncio`: Used for event loop management in `run_async_from_sync`.
  - `sys`: Used by `setup_logging` to direct log output to standard output.
  - `loguru`: The logging library configured by `setup_logging`.
  - `typing`: Used for type hinting (`Coroutine`, `TypeVar`, `Any`).

## 5. Notable Implementation Details

- **Event Loop Management in `run_async_from_sync`**:
  - Checks for a running event loop using `asyncio.get_running_loop()` ([`src/naq/utils.py:26`](src/naq/utils.py:26)).
  - Uses `asyncio.run()` ([`src/naq/utils.py:39`](src/naq/utils.py:39)) for the basic case.
  - Raises a `RuntimeError` ([`src/naq/utils.py:45-48`](src/naq/utils.py:45-48)) if a loop is already running, avoiding complex or unsafe scheduling. Comments in the source code ([`src/naq/utils.py:27-34`](src/naq/utils.py:27-34), [`src/naq/utils.py:43-51`](src/naq/utils.py:43-51)) explain this design choice.

- **Loguru Configuration**:
  - `logger.remove()` ([`src/naq/utils.py:59`](src/naq/utils.py:59)) ensures a predictable logging setup.
  - `colorize=True` ([`src/naq/utils.py:64`](src/naq/utils.py:64)) enhances log readability in terminals.

- **Type Safety**: Use of `TypeVar("T")` ([`src/naq/utils.py:8`](src/naq/utils.py:8)) in `run_async_from_sync` maintains type correctness for coroutine return values.

## 6. Mermaid Diagram

```mermaid
graph TD
    subgraph naq.utils
        U_run_async["run_async_from_sync()"]
        U_setup_logging["setup_logging()"]
    end

    subgraph External Libraries
        Lib_asyncio[asyncio]
        Lib_loguru[loguru]
        Lib_sys[sys]
    end

    subgraph Other naq Modules / Callers
        M_Job_fetch_sync["naq.job.Job.fetch_result_sync()"]
        M_CLI_App["naq.cli / App Entry Point"]
    end

    U_run_async -->|uses| Lib_asyncio
    U_setup_logging -->|uses| Lib_loguru
    U_setup_logging -->|outputs to| Lib_sys

    M_Job_fetch_sync -->|calls| U_run_async
    M_CLI_App -->|calls at startup| U_setup_logging

    classDef utils fill:#D6EAF8,stroke:#3498DB,color:#000000
    classDef extLib fill:#E8DAEF,stroke:#8E44AD,color:#000000
    classDef naqModCaller fill:#D5F5E3,stroke:#2ECC71,color:#000000

    class U_run_async,U_setup_logging utils
    class Lib_asyncio,Lib_loguru,Lib_sys extLib
    class M_Job_fetch_sync,M_CLI_App naqModCaller