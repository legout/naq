# Task: Add Configuration and Public API

## Objective

Finalize the event logging feature by adding necessary configuration settings and exposing the new functionality through the library's public API.

## Requirements

1.  **Update Settings (`src/naq/settings.py`):**
    *   Add new settings for the event logging system. These should be configurable via environment variables.
    *   `NAQ_EVENTS_ENABLED`: A boolean to easily enable or disable the entire feature.
    *   `NAQ_EVENT_STORAGE_TYPE`: The type of storage backend (defaults to `nats`).
    *   `NAQ_EVENT_STORAGE_URL`: The NATS URL for the event system (can default to `DEFAULT_NATS_URL`).
    *   `NAQ_EVENT_STREAM_NAME`: The name of the JetStream stream for events (e.g., `NAQ_JOB_EVENTS`).
    *   `NAQ_EVENT_SUBJECT_PREFIX`: The base subject for events (e.g., `naq.jobs.events`).

2.  **Update Public API (`src/naq/__init__.py`):**
    *   Create a new `src/naq/events/__init__.py` file.
    *   In this new `__init__.py`, import and expose the key classes and enums for users:
        *   `JobEvent`
        *   `JobEventType`
        *   `AsyncJobEventLogger`
        *   `AsyncJobEventProcessor`
        *   `NATSJobEventStorage`
    *   In the top-level `src/naq/__init__.py`, import the `events` module so users can access it via `naq.events`.
        *   Example: `from . import events`

3.  **Update `pyproject.toml`:**
    *   Add `msgspec` to the list of dependencies under `[project.dependencies]` as it is a core component of the new event model.

4.  **Update Documentation (Conceptual):**
    *   Although not a code change, the implementation plan should acknowledge the need to update `README.md` and other documentation to explain the new event logging feature, how to configure it, and how to use the `AsyncJobEventProcessor` to monitor jobs.
