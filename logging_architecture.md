# NAQ Logging Architecture

## Overview

This document outlines the architecture for logging in the NAQ library. The goal is to provide flexible logging that can be easily configured via environment variables or programmatically.

## Configuration Flow

```mermaid
graph TD
    A[Application Start / Component Instantiation] --> B{Call setup_logging()?};
    B -->|Yes| C[naq.utils.setup_logging(level)];
    B -->|No| D[Use settings from environment];

    subgraph Settings Resolution
        C --> E[CLI 'level' argument provided?];
        E -->|Yes| F[Use CLI level];
        E -->|No| G[Read NAQ_LOG_LEVEL from naq.settings];
        F --> H[Effective Log Level];
        G --> H;
        H --> I[Read NAQ_LOG_TO_FILE_ENABLED from naq.settings];
        I --> J[Read NAQ_LOG_FILE_PATH from naq.settings];
    end

    subgraph Loguru Configuration
        H --> K[logger.remove()];
        K --> L[Add stdout handler with Effective Log Level];
        I -->|True| M[Add file handler with LOG_FILE_PATH and Effective Log Level];
        I -->|False| N[No file handler];
    end

    subgraph Logging Usage
        O[naq.worker, naq.queue, naq.scheduler, etc.] --> P[logger.debug(), logger.info(), etc.];
        P --> Q{Log level >= Handler level?};
        Q -->|Yes| R[Output to stdout];
        Q -->|Yes| S[Output to file (if enabled)];
        Q -->|No| T[Message dropped];
    end

    L --> R;
    M --> S;

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#f9f,stroke:#333,stroke-width:2px
    style K fill:#ccf,stroke:#333,stroke-width:2px
    style L fill:#ccf,stroke:#333,stroke-width:2px
    style M fill:#ccf,stroke:#333,stroke-width:2px
    style P fill:#cfc,stroke:#333,stroke-width:2px
```

## Key Components

1.  **Environment Variables (`src/naq/settings.py`):**
    *   `NAQ_LOG_LEVEL`: Sets the global log level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). Defaults to `CRITICAL` (effectively disabled).
    *   `NAQ_LOG_TO_FILE_ENABLED`: Boolean (`true`/`false`) to enable/disable logging to a file. Defaults to `false`.
    *   `NAQ_LOG_FILE_PATH`: Path for the log file. Can include placeholders like `{time}`. Defaults to `naq_{time}.log`.

2.  **`naq.utils.setup_logging(level)` (`src/naq/utils.py`):**
    *   This is the central function for configuring `loguru`.
    *   It first removes all existing `loguru` handlers to ensure a clean state.
    *   It determines the `effective_level`:
        *   If a `level` argument is passed to the function (e.g., from CLI), it uses that.
        *   Otherwise, it reads `NAQ_LOG_LEVEL` from `naq.settings`.
    *   It adds a handler for `sys.stdout` with the `effective_level`.
    *   If `NAQ_LOG_TO_FILE_ENABLED` is `true`, it adds a file handler using `NAQ_LOG_FILE_PATH` and the `effective_level`.

3.  **Entry Points (`cli.py`, `worker.py`, `scheduler.py`, `queue.py`):**
    *   `cli.py`: Calls `setup_logging(log_level)` where `log_level` comes from a Typer option. This allows users to override the environment variable setting via the command line.
    *   `worker.py`, `scheduler.py`, `queue.py`: Call `setup_logging()` without arguments within their `run` or `start` methods. This ensures logging is configured based on environment variables when these components are used directly without the CLI.

4.  **Loguru Usage:**
    *   All modules (`worker.py`, `queue.py`, `scheduler.py`, `connection.py`, etc.) use the global `logger` instance from `loguru`.
    *   Standard logging calls like `logger.debug()`, `logger.info()`, etc., are used.
    *   `loguru` handles filtering messages based on the levels configured in its handlers.

## Per-Instance/Per-Call Logging (Future Enhancement)

The current implementation focuses on global logging control. The user also expressed interest in per-instance or per-call logging (e.g., `Worker(..., log_level="info")` or `enqueue(..., log_level="info")`).

This is more complex with `loguru` due to its global nature. A potential approach involves:

1.  **Accept `log_level` in constructors/functions:** Modify `Worker.__init__()`, `Queue.__init__()`, `Scheduler.__init__()`, and `enqueue()` to accept an optional `log_level` parameter.
2.  **Store `log_level` on instances:** If provided, store this `log_level` on the instance (e.g., `self._instance_log_level`).
3.  **Conditional Logging:** Instead of direct `logger.info()` calls, use a helper function or a wrapper that checks the instance's `_instance_log_level` (if it exists) against the message's level before calling the global `logger`.
    *   Example: `self.log("info", "My message")` where `self.log` is a method on the class that performs the check.
    *   Alternatively, `logger.bind(instance_log_level=self._instance_log_level).opt(lazy=True).log("INFO", lambda: "My message")` could be explored, but requires careful handling of the level comparison.

This enhancement would be a significant refactor of all logging calls within the affected classes and is proposed as a future step.