
# GEMINI.md

## Project Overview

This project is a Python library named **naq** (NATS Asynchronous Queue). It provides a simple, asynchronous job queueing system built on top of NATS and its JetStream persistence layer. It is inspired by RQ (Redis Queue).

The project is structured as a standard Python library with a `src` directory containing the source code and a `tests` directory for tests. It uses `pyproject.toml` for project metadata and dependency management.

The core features of `naq` include:

*   Asynchronous job processing using `asyncio` and `nats-py`.
*   Job persistence with NATS JetStream.
*   Scheduled and recurring jobs.
*   Job dependencies and retries.
*   A web dashboard for monitoring.
*   A command-line interface (CLI) for managing workers, queues, and jobs.

## Building and Running

### Dependencies

The project's dependencies are listed in the `pyproject.toml` file. The main dependencies are:

*   `nats-py`
*   `cloudpickle`
*   `typer`
*   `loguru`
*   `croniter`
*   `rich`
*   `anyio`
*   `msgspec`

Optional dependencies for the dashboard are also defined.

### Running the Application

The project provides a command-line interface (CLI) through the `naq` command. The main commands are:

*   `naq worker <queues>`: Starts a worker to process jobs from the specified queues.
*   `naq scheduler`: Starts the scheduler to handle scheduled jobs.
*   `naq purge <queues>`: Removes all jobs from the specified queues.
*   `naq list-scheduled`: Lists all scheduled jobs.
*   `naq job-control <job_id> <action>`: Controls a scheduled job (cancel, pause, resume, reschedule).
*   `naq list-workers`: Lists all active workers.
*   `naq dashboard`: Starts the web dashboard.

### Running Tests

The project uses `pytest` for testing. The tests are located in the `tests` directory. To run the tests, you can use the following command:

```bash
pytest
```

## Development Conventions

*   **Code Style**: The code follows standard Python conventions (PEP 8).
*   **Logging**: The `loguru` library is used for logging.
*   **CLI**: The `typer` library is used for creating the command-line interface.
*   **Serialization**: `cloudpickle` is the default serializer, but `JsonSerializer` is recommended for security.
*   **Timezones**: All internal scheduling and time handling are based on UTC.
