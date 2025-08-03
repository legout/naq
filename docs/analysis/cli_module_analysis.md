# Analysis of `src/naq/cli.py`

## 1. Module Overview and Role

The [`src/naq/cli.py`](src/naq/cli.py) module serves as the command-line interface (CLI) for the `naq` library. It utilizes the `typer` library to define a set of commands that allow users to interact with and manage various components of the `naq` system. This includes starting and managing workers, controlling task queues, overseeing the scheduler and scheduled jobs, and launching a web-based dashboard for monitoring.

The primary responsibilities of this module are:
*   Providing user-friendly commands for common `naq` operations.
*   Parsing command-line arguments and options.
*   Invoking the appropriate functionalities within other `naq` modules (e.g., `Worker`, `Queue`, `Scheduler`).
*   Handling NATS connections for CLI operations that require them.
*   Formatting and presenting information to the user, often using the `rich` library for enhanced console output.
*   Configuring logging for CLI operations.

It acts as the main entry point for users to operate and introspect the `naq` distributed task queue system from the command line.

## 2. Public Interfaces

The primary public interface is the `typer.Typer()` application instance, `app` ([`src/naq/cli.py:46`](src/naq/cli.py:46)). The following commands are exposed:

### A. Main Application Callback

*   **`main(version: Optional[bool] = typer.Option(None, "--version", ...))` ([`src/naq/cli.py:815`](src/naq/cli.py:815))**
    *   The main callback for the `naq` Typer application.
    *   Primarily used to handle the global `--version` option via the `version_callback`.

### B. Command-Line Commands

*   **`worker(queues: List[str], nats_url: Optional[str], concurrency: int, name: Optional[str], log_level: str)` ([`src/naq/cli.py:68`](src/naq/cli.py:68))**
    *   Starts a `naq` worker process.
    *   `queues`: List of queue names for the worker to listen to.
    *   `nats_url`: URL of the NATS server. Defaults to `DEFAULT_NATS_URL` or `NAQ_NATS_URL` env var.
    *   `concurrency`: Maximum number of concurrent jobs to process.
    *   `name`: Optional name for the worker instance.
    *   `log_level`: Logging level for the worker.

*   **`purge(queues: List[str], nats_url: Optional[str], log_level: str)` ([`src/naq/cli.py:128`](src/naq/cli.py:128))**
    *   Removes all jobs from the specified queues.
    *   `queues`: List of queue names to purge.
    *   `nats_url`: URL of the NATS server. Defaults to `DEFAULT_NATS_URL` or `NAQ_NATS_URL` env var.
    *   `log_level`: Logging level for the command.

*   **`scheduler(nats_url: Optional[str], poll_interval: float, instance_id: Optional[str], disable_ha: bool, log_level: str)` ([`src/naq/cli.py:204`](src/naq/cli.py:204))**
    *   Starts a `naq` scheduler process.
    *   `nats_url`: URL of the NATS server. Defaults to `DEFAULT_NATS_URL` or `NAQ_NATS_URL` env var.
    *   `poll_interval`: Interval in seconds for checking due jobs.
    *   `instance_id`: Optional unique ID for the scheduler instance (for HA).
    *   `disable_ha`: If true, disables high availability mode.
    *   `log_level`: Logging level for the scheduler.

*   **`list_scheduled_jobs(nats_url: Optional[str], status: Optional[str], job_id: Optional[str], queue: Optional[str], detailed: bool, log_level: str)` (command: `list-scheduled`) ([`src/naq/cli.py:271`](src/naq/cli.py:271))**
    *   Lists scheduled jobs.
    *   `nats_url`: URL of the NATS server.
    *   `status`: Filter by job status (e.g., 'active', 'paused').
    *   `job_id`: Filter by a specific job ID.
    *   `queue`: Filter by queue name.
    *   `detailed`: Show more detailed job information.
    *   `log_level`: Logging level for the command.

*   **`job_control(job_id: str, action: str, nats_url: Optional[str], cron: Optional[str], interval: Optional[float], repeat: Optional[int], next_run: Optional[str], log_level: str)` (command: `job-control`) ([`src/naq/cli.py:492`](src/naq/cli.py:492))**
    *   Controls scheduled jobs (cancel, pause, resume, reschedule).
    *   `job_id`: The ID of the scheduled job to control.
    *   `action`: Action to perform ('cancel', 'pause', 'resume', 'reschedule').
    *   `nats_url`: URL of the NATS server.
    *   `cron`, `interval`, `repeat`, `next_run`: Parameters for the 'reschedule' action.
    *   `log_level`: Logging level for the command.

*   **`list_workers_command(nats_url: Optional[str], log_level: str)` (command: `list-workers`) ([`src/naq/cli.py:648`](src/naq/cli.py:648))**
    *   Lists all currently active workers.
    *   `nats_url`: URL of the NATS server.
    *   `log_level`: Logging level for the command.

*   **`dashboard(host: str, port: int, log_level: str)` ([`src/naq/cli.py:753`](src/naq/cli.py:753))**
    *   Starts the NAQ web dashboard.
    *   `host`: Host to bind the dashboard server to.
    *   `port`: Port for the dashboard server.
    *   `log_level`: Logging level for the dashboard server and uvicorn.

### C. Helper Functions (Internal to CLI logic)

*   **`version_callback(value: bool)` ([`src/naq/cli.py:58`](src/naq/cli.py:58))**
    *   Typer callback function triggered by the `--version` option. Prints the `naq` version and exits.

## 3. Key Functionalities

The `cli.py` module orchestrates several key functionalities of the `naq` system:

*   **Worker Lifecycle Management**:
    *   The `worker` command initializes and runs `Worker` instances from [`src/naq/worker.py`](src/naq/worker.py). It handles command-line arguments for specifying queues, concurrency, NATS URL, and worker naming. It also manages the asyncio event loop for the worker and handles graceful shutdown on `KeyboardInterrupt`.

*   **Queue Operations**:
    *   The `purge` command interacts with the `Queue` object from [`src/naq/queue.py`](src/naq/queue.py) to remove all jobs from one or more specified NATS streams (queues). It provides feedback on the number of jobs purged per queue.

*   **Scheduler Lifecycle Management**:
    *   The `scheduler` command initializes and runs a `Scheduler` instance from [`src/naq/scheduler.py`](src/naq/scheduler.py). It configures the scheduler's NATS connection, polling interval, and high-availability (HA) mode based on CLI arguments.

*   **Scheduled Job Management & Introspection**:
    *   `list-scheduled`: Retrieves and displays information about jobs stored in the NATS Key-Value store designated for scheduled jobs (`SCHEDULED_JOBS_KV_NAME`). It uses `cloudpickle` to deserialize job data and `rich` to present it in a table, with filtering options.
    *   `job-control`: Allows users to modify the state or schedule of existing scheduled jobs. It interacts with methods on the `Queue` object (e.g., `cancel_scheduled_job`, `pause_scheduled_job`, `resume_scheduled_job`, `modify_scheduled_job`) to effect these changes in the NATS KV store.

*   **Active Worker Introspection**:
    *   `list-workers`: Calls the static method `Worker.list_workers()` to fetch information about all active worker instances. It then formats this data into a `rich` table, showing worker ID, status, listened queues, current job (if busy), and last heartbeat time.

*   **Web Dashboard Launch**:
    *   The `dashboard` command uses `uvicorn` to serve the Sanic web application defined in [`src/naq/dashboard/app.py`](src/naq/dashboard/app.py), providing a web interface for monitoring `naq`.

*   **Configuration and Logging**:
    *   Most commands utilize `setup_logging()` from [`src/naq/utils.py`](src/naq/utils.py) to initialize `loguru`-based logging.
    *   NATS connection parameters and other settings are often configurable via CLI options with fallbacks to environment variables or defaults from [`src/naq/settings.py`](src/naq/settings.py).

*   **User Interface**:
    *   Leverages `typer` for robust command parsing and help generation.
    *   Employs the `rich` library extensively to provide clear, formatted, and user-friendly output in the terminal (e.g., tables for `list-scheduled` and `list-workers`, panels for summaries).

## 4. Dependencies and Interactions

The `cli.py` module interacts with various internal `naq` modules and external libraries:

### A. External Library Dependencies:

*   **`typer`**: Core library for building the CLI application structure, command definition, argument/option parsing, and help text generation.
*   **`rich`**: Used for creating rich text, tables, panels, and styled output in the console, enhancing user experience.
*   **`loguru`**: For application-level logging within the CLI commands and the processes they spawn.
*   **`cloudpickle`**: Used by `list_scheduled_jobs` ([`src/naq/cli.py:362`](src/naq/cli.py:362)) to deserialize scheduled job data retrieved from the NATS KV store.
*   **`uvicorn`**: Used by the `dashboard` command ([`src/naq/cli.py:780`](src/naq/cli.py:780)) to run the Sanic web application for the dashboard.
*   **`nats-py`**: (Indirectly) All NATS communications are facilitated by this library, via the abstractions in [`src/naq/connection.py`](src/naq/connection.py).

### B. Internal `naq` Module Interactions:

*   **[`src/naq/connection.py`](src/naq/connection.py)**:
    *   Functions like `get_nats_connection()` ([`src/naq/cli.py:318`](src/naq/cli.py:318), etc.), `get_jetstream_context()` ([`src/naq/cli.py:319`](src/naq/cli.py:319), etc.), and `close_nats_connection()` ([`src/naq/cli.py:192`](src/naq/cli.py:192), etc.) are used by most commands to establish, use, and terminate connections to the NATS server.
*   **[`src/naq/worker.py`](src/naq/worker.py)**:
    *   The `worker` command instantiates and runs the `Worker` class ([`src/naq/cli.py:108`](src/naq/cli.py:108)).
    *   The `list-workers` command calls the static method `Worker.list_workers()` ([`src/naq/cli.py:671`](src/naq/cli.py:671)) to retrieve data about active workers.
*   **[`src/naq/queue.py`](src/naq/queue.py)**:
    *   The `purge` command instantiates `Queue` ([`src/naq/cli.py:156`](src/naq/cli.py:156)) and calls its `purge()` method ([`src/naq/cli.py:162`](src/naq/cli.py:162)).
    *   The `job-control` command instantiates `Queue` ([`src/naq/cli.py:562`](src/naq/cli.py:562)) and calls its methods for managing scheduled jobs (e.g., `cancel_scheduled_job()` ([`src/naq/cli.py:566`](src/naq/cli.py:566)), `pause_scheduled_job()` ([`src/naq/cli.py:573`](src/naq/cli.py:573)), etc.).
*   **[`src/naq/scheduler.py`](src/naq/scheduler.py)**:
    *   The `scheduler` command instantiates and runs the `Scheduler` class ([`src/naq/cli.py:253`](src/naq/cli.py:253)).
*   **[`src/naq/settings.py`](src/naq/settings.py)**:
    *   Imports default values like `DEFAULT_NATS_URL` ([`src/naq/cli.py:32`](src/naq/cli.py:32)), `SCHEDULED_JOBS_KV_NAME` ([`src/naq/cli.py:36`](src/naq/cli.py:36)), various status constants (e.g., `SCHEDULED_JOB_STATUS_ACTIVE` ([`src/naq/cli.py:33`](src/naq/cli.py:33))), and `NAQ_PREFIX` ([`src/naq/cli.py:38`](src/naq/cli.py:38)). These are used for default configurations and consistent naming.
*   **[`src/naq/utils.py`](src/naq/utils.py)**:
    *   The `setup_logging()` function ([`src/naq/cli.py:40`](src/naq/cli.py:40)) is called by almost all commands to initialize logging ([`src/naq/cli.py:102`](src/naq/cli.py:102), [`src/naq/cli.py:147`](src/naq/cli.py:147), etc.).
*   **[`src/naq/__init__.py`](src/naq/__init__.py)**:
    *   The `__version__` string ([`src/naq/cli.py:22`](src/naq/cli.py:22)) is imported and used by the `version_callback` ([`src/naq/cli.py:60`](src/naq/cli.py:60)).
*   **[`src/naq/dashboard/app.py`](src/naq/dashboard/app.py)**:
    *   The Sanic application instance (`app`) is imported as a string path (`"naq.dashboard.app:app"`) by the `dashboard` command ([`src/naq/cli.py:791`](src/naq/cli.py:791)) to be run by `uvicorn`.

## 5. Notable Implementation Details

*   **Asynchronous Operations**: Many commands performing NATS operations (e.g., `purge`, `list-scheduled`, `job-control`, `list-workers`) define an inner `async def` helper function which is then executed using `asyncio.run()`. This allows for non-blocking I/O with NATS.
*   **Typer for CLI Structure**: The module heavily relies on `typer` decorators (`@app.command()`, `@app.callback()`) and type hints to define the CLI structure, arguments, options, and automatic help generation.
*   **Rich for Enhanced Output**: The `rich` library is consistently used to provide formatted and user-friendly console output, including tables (`list-scheduled`, `list-workers`), panels (`purge` summary), and styled text. A global `console = Console()` ([`src/naq/cli.py:53`](src/naq/cli.py:53)) instance is used.
*   **Configuration Flexibility**: CLI options often allow overriding default values and can frequently be set via environment variables (e.g., `nats_url` via `NAQ_NATS_URL` ([`src/naq/cli.py:77`](src/naq/cli.py:77))).
*   **Error Handling and Exit Codes**: Commands generally include `try...except` blocks to catch exceptions (like `KeyboardInterrupt` or general `Exception`). `typer.Exit(code=1)` is used to signal failure to the shell. `logger.exception()` is used to log errors with stack traces.
*   **NATS Connection Management**: NATS connections are typically established at the beginning of an async operation and closed in a `finally` block using `close_nats_connection()` from [`src/naq/connection.py`](src/naq/connection.py) to ensure resources are released.
*   **Modular Command Logic**: Each CLI command is encapsulated in its own function, promoting clarity and separation of concerns. Async logic is often further nested within these command functions.
*   **Conditional Import for Dashboard**: Although the `try...except ImportError` for `uvicorn` in the `dashboard` command is commented out ([`src/naq/cli.py:748-750`](src/naq/cli.py:748), [`src/naq/cli.py:800-808`](src/naq/cli.py:800)), the local import of `uvicorn` ([`src/naq/cli.py:780`](src/naq/cli.py:780)) within the command function suggests an intention for optional dependencies for this feature.

## 6. Mermaid Diagram

```mermaid
graph TD
    subgraph User Interaction
        CLI_User[User via Terminal]
    end

    subgraph NAQ CLI (`src/naq/cli.py`)
        CLI_App[Typer App (`app`)]
        Cmd_Worker["worker()"]
        Cmd_Purge["purge()"]
        Cmd_Scheduler["scheduler()"]
        Cmd_ListScheduled["list-scheduled()"]
        Cmd_JobControl["job-control()"]
        Cmd_ListWorkers["list-workers()"]
        Cmd_Dashboard["dashboard()"]
        Helper_Version["version_callback()"]
        Helper_Main["main() --version"]
    end

    subgraph NAQ Core Modules
        Mod_Worker["Worker (`src/naq/worker.py`)"]
        Mod_Queue["Queue (`src/naq/queue.py`)"]
        Mod_SchedulerMod["Scheduler (`src/naq/scheduler.py`)"]
        Mod_Connection["Connection Mgmt (`src/naq/connection.py`)"]
        Mod_Settings["Settings (`src/naq/settings.py`)"]
        Mod_Utils["Utils (`src/naq/utils.py`)"]
        Mod_DashboardApp["Dashboard App (`src/naq/dashboard/app.py`)"]
    end

    subgraph External Services / Libraries
        NATS_Service[NATS Server]
        Lib_Typer[Typer Lib]
        Lib_Rich[Rich Lib]
        Lib_Loguru[Loguru Lib]
        Lib_Uvicorn[Uvicorn Lib]
        Lib_Cloudpickle[Cloudpickle Lib]
    end

    CLI_User --> CLI_App

    CLI_App -- Invokes --> Cmd_Worker
    CLI_App -- Invokes --> Cmd_Purge
    CLI_App -- Invokes --> Cmd_Scheduler
    CLI_App -- Invokes --> Cmd_ListScheduled
    CLI_App -- Invokes --> Cmd_JobControl
    CLI_App -- Invokes --> Cmd_ListWorkers
    CLI_App -- Invokes --> Cmd_Dashboard
    CLI_App -- Invokes --> Helper_Main
    Helper_Main -- Calls --> Helper_Version

    Cmd_Worker --- Uses ---> Mod_Worker
    Cmd_Worker --- Uses ---> Mod_Connection
    Cmd_Worker --- Uses ---> Mod_Settings
    Cmd_Worker --- Uses ---> Mod_Utils

    Cmd_Purge --- Uses ---> Mod_Queue
    Cmd_Purge --- Uses ---> Mod_Connection
    Cmd_Purge --- Uses ---> Mod_Settings
    Cmd_Purge --- Uses ---> Mod_Utils

    Cmd_Scheduler --- Uses ---> Mod_SchedulerMod
    Cmd_Scheduler --- Uses ---> Mod_Connection
    Cmd_Scheduler --- Uses ---> Mod_Settings
    Cmd_Scheduler --- Uses ---> Mod_Utils

    Cmd_ListScheduled --- Uses ---> Mod_Connection
    Cmd_ListScheduled --- Uses ---> Mod_Settings
    Cmd_ListScheduled --- Uses ---> Lib_Cloudpickle
    Cmd_ListScheduled --- Uses ---> Mod_Utils

    Cmd_JobControl --- Uses ---> Mod_Queue
    Cmd_JobControl --- Uses ---> Mod_Connection
    Cmd_JobControl --- Uses ---> Mod_Settings
    Cmd_JobControl --- Uses ---> Mod_Utils

    Cmd_ListWorkers --- Uses ---> Mod_Worker
    Cmd_ListWorkers --- Uses ---> Mod_Connection
    Cmd_ListWorkers --- Uses ---> Mod_Settings
    Cmd_ListWorkers --- Uses ---> Mod_Utils

    Cmd_Dashboard --- Uses ---> Mod_DashboardApp
    Cmd_Dashboard --- Uses ---> Lib_Uvicorn
    Cmd_Dashboard --- Uses ---> Mod_Utils

    CLI_App --- Built with ---> Lib_Typer
    CLI_App --- Uses for Output ---> Lib_Rich
    CLI_App --- Uses for Logging ---> Lib_Loguru

    Mod_Connection --- Interacts with ---> NATS_Service
    Mod_Queue --- Interacts with ---> NATS_Service
    Mod_Worker --- Interacts with ---> NATS_Service
    Mod_SchedulerMod --- Interacts with ---> NATS_Service