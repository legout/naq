# Analysis of [`src/naq/settings.py`](src/naq/settings.py)

## 1. Module Overview and Role

The [`src/naq/settings.py`](src/naq/settings.py) module centralizes all configuration parameters for the `naq` library. It defines constants for connection details, queue and scheduler settings, job/result/worker status, and more. These settings are used throughout the library to ensure consistent behavior and allow for easy customization via environment variables. The module promotes maintainability and flexibility by grouping all configuration logic in a single location.

## 2. Public Interfaces

All public interfaces are module-level constants, grouped by function:

### A. General/NATS Connection

- [`DEFAULT_NATS_URL`](src/naq/settings.py:5):  
  `os.getenv("NAQ_NATS_URL", "nats://localhost:4222")`  
  Default NATS server URL.

- [`DEFAULT_QUEUE_NAME`](src/naq/settings.py:8):  
  `os.getenv("NAQ_DEFAULT_QUEUE", "naq_default_queue")`  
  Default queue name (maps to a NATS subject/stream).

- [`NAQ_PREFIX`](src/naq/settings.py:11):  
  `"naq"`  
  Prefix for NATS subjects/streams used by naq.

- [`JOB_SERIALIZER`](src/naq/settings.py:15):  
  `os.getenv("NAQ_JOB_SERIALIZER", "pickle")`  
  Serialization format for jobs (`'pickle'` or `'json'`).

### B. Scheduler Settings

- [`SCHEDULED_JOBS_KV_NAME`](src/naq/settings.py:19):  
  `f"{NAQ_PREFIX}_scheduled_jobs"`  
  KV bucket name for scheduled jobs.

- [`SCHEDULER_LOCK_KV_NAME`](src/naq/settings.py:21):  
  `f"{NAQ_PREFIX}_scheduler_lock"`  
  KV bucket name for scheduler leader election lock.

- [`SCHEDULER_LOCK_KEY`](src/naq/settings.py:23):  
  `"leader_lock"`  
  Key used within the lock KV store.

- [`SCHEDULER_LOCK_TTL_SECONDS`](src/naq/settings.py:25):  
  `int(os.getenv("NAQ_SCHEDULER_LOCK_TTL", "30"))`  
  TTL (in seconds) for the leader lock.

- [`SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS`](src/naq/settings.py:27):  
  `int(os.getenv("NAQ_SCHEDULER_LOCK_RENEW_INTERVAL", "15"))`  
  How often the leader tries to renew the lock.

- [`MAX_SCHEDULE_FAILURES`](src/naq/settings.py:32):  
  `os.getenv("NAQ_MAX_SCHEDULE_FAILURES")` (parsed as int, defaults to 5, disables on invalid)  
  Maximum number of times the scheduler will try to enqueue a job before marking it as failed.

### C. Job Status Settings

- [`JOB_STATUS_KV_NAME`](src/naq/settings.py:47):  
  `f"{NAQ_PREFIX}_job_status"`  
  KV bucket name for tracking job completion status.

- [`JOB_STATUS_COMPLETED`](src/naq/settings.py:49):  
  `"completed"`  
  Status value for completed jobs.

- [`JOB_STATUS_FAILED`](src/naq/settings.py:50):  
  `"failed"`  
  Status value for failed jobs.

- [`JOB_STATUS_TTL_SECONDS`](src/naq/settings.py:52):  
  `int(os.getenv("NAQ_JOB_STATUS_TTL", 86400))`  
  TTL for job status entries.

- [`SCHEDULED_JOB_STATUS_ACTIVE`](src/naq/settings.py:55):  
  `"active"`  
  Status value for active scheduled jobs.

- [`SCHEDULED_JOB_STATUS_PAUSED`](src/naq/settings.py:56):  
  `"paused"`  
  Status value for paused scheduled jobs.

- [`SCHEDULED_JOB_STATUS_FAILED`](src/naq/settings.py:57):  
  `"schedule_failed"`  
  Status value for scheduled jobs that failed to be enqueued.

### D. Failed Job Settings

- [`FAILED_JOB_SUBJECT_PREFIX`](src/naq/settings.py:62):  
  `f"{NAQ_PREFIX}.failed"`  
  Subject prefix for failed jobs.

- [`FAILED_JOB_STREAM_NAME`](src/naq/settings.py:64):  
  `f"{NAQ_PREFIX}_failed_jobs"`  
  Stream name for failed jobs.

### E. Result Backend Settings

- [`RESULT_KV_NAME`](src/naq/settings.py:68):  
  `f"{NAQ_PREFIX}_results"`  
  KV bucket name for storing job results/errors.

- [`DEFAULT_RESULT_TTL_SECONDS`](src/naq/settings.py:70):  
  `int(os.getenv("NAQ_DEFAULT_RESULT_TTL", 604800))`  
  Default TTL (in seconds) for job results.

### F. Worker Monitoring Settings

- [`WORKER_KV_NAME`](src/naq/settings.py:74):  
  `f"{NAQ_PREFIX}_workers"`  
  KV bucket name for storing worker status and heartbeats.

- [`DEFAULT_WORKER_TTL_SECONDS`](src/naq/settings.py:76):  
  `int(os.getenv("NAQ_WORKER_TTL", "60"))`  
  Default TTL for worker heartbeat entries.

- [`DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS`](src/naq/settings.py:78):  
  `int(os.getenv("NAQ_WORKER_HEARTBEAT_INTERVAL", "15"))`  
  Default interval for worker heartbeats.

- [`WORKER_STATUS_STARTING`](src/naq/settings.py:82):  
  `"starting"`  
  Worker status constant.

- [`WORKER_STATUS_IDLE`](src/naq/settings.py:83):  
  `"idle"`  
  Worker status constant.

- [`WORKER_STATUS_BUSY`](src/naq/settings.py:84):  
  `"busy"`  
  Worker status constant.

- [`WORKER_STATUS_STOPPING`](src/naq/settings.py:85):  
  `"stopping"`  
  Worker status constant.

### G. Dependency Checking

- [`DEPENDENCY_CHECK_DELAY_SECONDS`](src/naq/settings.py:87):  
  `5`  
  Delay (in seconds) between dependency checks.

## 3. Key Functionalities

- **Centralized Configuration:**  
  All settings for the `naq` system are defined in one place, promoting maintainability and discoverability.

- **Environment Variable Overrides:**  
  Most settings can be overridden by environment variables, allowing for flexible deployment and testing.

- **Type Safety and Defaults:**  
  Numeric settings are parsed with `int()`, and defaults are provided for all settings.  
  `MAX_SCHEDULE_FAILURES` includes error handling for invalid values.

- **Dynamic Construction:**  
  Many settings are constructed using the `NAQ_PREFIX` constant to ensure naming consistency across NATS subjects, streams, and KV buckets.

- **Logical Grouping:**  
  Settings are grouped by function (scheduler, job status, worker, etc.) for clarity.

## 4. Dependencies and Interactions

- **Internal Dependencies:**  
  - Uses Python's standard [`os`](https://docs.python.org/3/library/os.html) module for environment variable access.

- **Consumers in `naq`:**
  - [`src/naq/connection.py`](src/naq/connection.py): Uses `DEFAULT_NATS_URL`.
  - [`src/naq/queue.py`](src/naq/queue.py): Uses queue, job, result, and status settings.
  - [`src/naq/job.py`](src/naq/job.py): Uses serializer, result, and job status settings.
  - [`src/naq/worker.py`](src/naq/worker.py): Uses failed job and worker monitoring settings.
  - [`src/naq/scheduler.py`](src/naq/scheduler.py): Uses scheduler and scheduled job status settings.

- **External Dependencies:**  
  - None beyond the Python standard library.

## 5. Notable Implementation Details

- **Environment Variable Pattern:**  
  The use of `os.getenv("ENV_VAR", "default")` is consistent throughout, making it easy to override settings.

- **Type Conversion and Validation:**  
  Settings expected to be integers are explicitly cast, and `MAX_SCHEDULE_FAILURES` includes a warning and disables the limit if the environment variable is invalid.

- **Naming Consistency:**  
  The `NAQ_PREFIX` is used to construct all NATS and KV resource names, reducing the risk of naming collisions.

- **No Classes or Functions:**  
  The module is intentionally simple, containing only constants and no complex logic.

## 6. Mermaid Diagram

```mermaid
graph TD
    subgraph Settings_Module [src/naq/settings.py]
        direction LR
        NATS_Config["NATS Config (DEFAULT_NATS_URL, NAQ_PREFIX)"]
        Queue_Config["Queue Config (DEFAULT_QUEUE_NAME, JOB_SERIALIZER)"]
        Scheduler_Config["Scheduler Config (SCHEDULED_JOBS_KV_NAME, SCHEDULER_LOCK_KV_NAME)"]
        Job_Status_Config["Job Status Config (JOB_STATUS_KV_NAME, JOB_STATUS_COMPLETED)"]
        Result_Backend_Config["Result Backend Config (RESULT_KV_NAME, DEFAULT_RESULT_TTL_SECONDS)"]
        Worker_Config["Worker Config (WORKER_KV_NAME, DEFAULT_WORKER_TTL_SECONDS)"]
        Failed_Job_Config["Failed Job Config (FAILED_JOB_SUBJECT_PREFIX)"]
    end

    Connection_Module([src/naq/connection.py])
    Queue_Module([src/naq/queue.py])
    Job_Module([src/naq/job.py])
    Scheduler_Module([src/naq/scheduler.py])
    Worker_Module([src/naq/worker.py])

    NATS_Config --> Connection_Module
    Queue_Config --> Queue_Module
    Job_Status_Config --> Queue_Module
    Result_Backend_Config --> Queue_Module
    Scheduler_Config --> Scheduler_Module
    Job_Status_Config --> Job_Module
    Result_Backend_Config --> Job_Module
    Worker_Config --> Worker_Module
    Failed_Job_Config --> Worker_Module

    classDef settingsGroup fill:#f9f,stroke:#333,stroke-width:2px;
    class NATS_Config,Queue_Config,Scheduler_Config,Job_Status_Config,Result_Backend_Config,Worker_Config,Failed_Job_Config settingsGroup;