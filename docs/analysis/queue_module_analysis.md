# Analysis of `src/naq/queue.py`

## 1. Module Overview and Role

The [`src/naq/queue.py`](src/naq/queue.py) module is central to the `naq` library's ability to manage and process asynchronous tasks. Its primary responsibilities revolve around:
1.  Defining the `Queue` class ([`src/naq/queue.py:321`](src/naq/queue.py:321)), which provides the main interface for enqueuing jobs for immediate execution.
2.  Handling scheduled and recurring jobs through the `ScheduledJobManager` class ([`src/naq/queue.py:33`](src/naq/queue.py:33)) and associated methods within the `Queue` class. This includes storing, retrieving, and managing jobs that are set to run at specific times, intervals, or according to cron expressions.
3.  Interacting with NATS JetStream for durable job message persistence and NATS Key-Value (KV) store for managing metadata of scheduled jobs.
4.  Providing a suite of asynchronous and synchronous helper functions for common queue operations, making it easier for client applications to interact with the `naq` system.

The module ensures that jobs, represented by the `Job` class from [`src/naq/job.py`](src/naq/job.py), are correctly formatted, serialized, and published to the appropriate NATS subjects for workers to consume. It also manages the lifecycle of scheduled tasks, including their creation, cancellation, pausing, resuming, and modification.

## 2. Public Interfaces

### A. `Queue` Class ([`src/naq/queue.py:321`](src/naq/queue.py:321))

Represents a job queue backed by a NATS JetStream stream. It provides methods to enqueue jobs for immediate execution, schedule jobs for future execution, and manage the queue itself.

*   **Key Attributes (Initialized in `__init__` ([`src/naq/queue.py:324`](src/naq/queue.py:324))):**
    *   `name: str` ([`src/naq/queue.py:338`](src/naq/queue.py:338)): The name of the queue (e.g., `default`).
    *   `subject: str` ([`src/naq/queue.py:339`](src/naq/queue.py:339)): The NATS subject jobs are published to for this queue (e.g., `naq.queue.default`).
    *   `stream_name: str` ([`src/naq/queue.py:340`](src/naq/queue.py:340)): The NATS JetStream stream name where jobs are stored (e.g., `naq_jobs`).
    *   `_nats_url: Optional[str]` ([`src/naq/queue.py:341`](src/naq/queue.py:341)): Optional NATS server URL override.
    *   `_js: Optional[nats.js.JetStreamContext]` ([`src/naq/queue.py:342`](src/naq/queue.py:342)): Cached JetStream context.
    *   `_default_timeout: Optional[int]` ([`src/naq/queue.py:343`](src/naq/queue.py:343)): Optional default job timeout.
    *   `_scheduled_job_manager: ScheduledJobManager` ([`src/naq/queue.py:344`](src/naq/queue.py:344)): An instance of `ScheduledJobManager` for this queue.

*   **Key Public Methods:**
    *   `async enqueue(func: Callable, *args: Any, max_retries: Optional[int] = 0, retry_delay: RetryDelayType = 0, depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None, **kwargs: Any) -> Job` ([`src/naq/queue.py:363`](src/naq/queue.py:363)): Creates a `Job` from a function call and enqueues it for immediate processing.
    *   `async enqueue_at(dt: datetime.datetime, func: Callable, *args: Any, max_retries: Optional[int] = 0, retry_delay: RetryDelayType = 0, **kwargs: Any) -> Job` ([`src/naq/queue.py:423`](src/naq/queue.py:423)): Schedules a job to be enqueued at a specific datetime.
    *   `async enqueue_in(delta: timedelta, func: Callable, *args: Any, max_retries: Optional[int] = 0, retry_delay: RetryDelayType = 0, **kwargs: Any) -> Job` ([`src/naq/queue.py:475`](src/naq/queue.py:475)): Schedules a job to be enqueued after a specific time delta.
    *   `async schedule(func: Callable, *args: Any, cron: Optional[str] = None, interval: Optional[Union[timedelta, float, int]] = None, repeat: Optional[int] = None, max_retries: Optional[int] = 0, retry_delay: RetryDelayType = 0, **kwargs: Any) -> Job` ([`src/naq/queue.py:512`](src/naq/queue.py:512)): Schedules a job to run repeatedly based on a cron string or interval.
    *   `async purge() -> int` ([`src/naq/queue.py:608`](src/naq/queue.py:608)): Removes all jobs from this queue by purging messages from the NATS stream for the queue's subject. Returns the number of purged messages.
    *   `async cancel_scheduled_job(job_id: str) -> bool` ([`src/naq/queue.py:648`](src/naq/queue.py:648)): Cancels a scheduled job by deleting it from the KV store.
    *   `async pause_scheduled_job(job_id: str) -> bool` ([`src/naq/queue.py:663`](src/naq/queue.py:663)): Pauses a scheduled job.
    *   `async resume_scheduled_job(job_id: str) -> bool` ([`src/naq/queue.py:682`](src/naq/queue.py:682)): Resumes a paused scheduled job.
    *   `async modify_scheduled_job(job_id: str, **updates: Any) -> bool` ([`src/naq/queue.py:701`](src/naq/queue.py:701)): Modifies parameters (e.g., cron, interval, repeat) of a scheduled job.

### B. `ScheduledJobManager` Class ([`src/naq/queue.py:33`](src/naq/queue.py:33))

Manages the storage, retrieval, and lifecycle of scheduled jobs within a NATS Key-Value store. This class is primarily used internally by the `Queue` class but its functionalities are exposed through `Queue`'s methods and module-level helpers.

*   **Key Attributes (Initialized in `__init__` ([`src/naq/queue.py:39`](src/naq/queue.py:39))):**
    *   `queue_name: str` ([`src/naq/queue.py:40`](src/naq/queue.py:40)): The name of the queue this manager is associated with.
    *   `_nats_url: Optional[str]` ([`src/naq/queue.py:41`](src/naq/queue.py:41)): Optional NATS server URL.
    *   `_kv: Optional[KeyValue]` ([`src/naq/queue.py:42`](src/naq/queue.py:42)): Cached NATS Key-Value store instance.

*   **Key Public Methods (primarily for internal use by `Queue`):**
    *   `async get_kv() -> KeyValue` ([`src/naq/queue.py:44`](src/naq/queue.py:44)): Gets the KeyValue store for scheduled jobs, creating it if it doesn't exist.
    *   `async store_job(job: Job, scheduled_timestamp: float, cron: Optional[str] = None, interval_seconds: Optional[float] = None, repeat: Optional[int] = None) -> None` ([`src/naq/queue.py:74`](src/naq/queue.py:74)): Stores a job's scheduling details in the KV store.
    *   `async cancel_job(job_id: str) -> bool` ([`src/naq/queue.py:121`](src/naq/queue.py:121)): Deletes a scheduled job from the KV store.
    *   `async update_job_status(job_id: str, status: str) -> bool` ([`src/naq/queue.py:148`](src/naq/queue.py:148)): Updates the status (e.g., 'active', 'paused') of a scheduled job.
    *   `async modify_job(job_id: str, **updates: Any) -> bool` ([`src/naq/queue.py:197`](src/naq/queue.py:197)): Modifies parameters of a stored scheduled job.

### C. Module-Level Asynchronous Helper Functions

These functions provide a convenient, direct way to perform queue operations without explicitly instantiating a `Queue` object. They typically create a temporary `Queue` instance internally.

*   `async enqueue(...) -> Job` ([`src/naq/queue.py:725`](src/naq/queue.py:725)): Enqueues a job. (Mirrors `Queue.enqueue`)
*   `async enqueue_at(...) -> Job` ([`src/naq/queue.py:748`](src/naq/queue.py:748)): Schedules a job for a specific time. (Mirrors `Queue.enqueue_at`)
*   `async enqueue_in(...) -> Job` ([`src/naq/queue.py:765`](src/naq/queue.py:765)): Schedules a job after a delay. (Mirrors `Queue.enqueue_in`)
*   `async schedule(...) -> Job` ([`src/naq/queue.py:782`](src/naq/queue.py:782)): Schedules a recurring job. (Mirrors `Queue.schedule`)
*   `async purge_queue(queue_name: str = DEFAULT_QUEUE_NAME, nats_url: Optional[str] = None) -> int` ([`src/naq/queue.py:808`](src/naq/queue.py:808)): Purges a specified queue.
*   `async cancel_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool` ([`src/naq/queue.py:817`](src/naq/queue.py:817)): Cancels a scheduled job.
*   `async pause_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool` ([`src/naq/queue.py:823`](src/naq/queue.py:823)): Pauses a scheduled job.
*   `async resume_scheduled_job(job_id: str, nats_url: Optional[str] = None) -> bool` ([`src/naq/queue.py:829`](src/naq/queue.py:829)): Resumes a scheduled job.
*   `async modify_scheduled_job(job_id: str, nats_url: Optional[str] = None, **updates: Any) -> bool` ([`src/naq/queue.py:835`](src/naq/queue.py:835)): Modifies a scheduled job.

### D. Module-Level Synchronous Helper Functions

These are synchronous wrappers around their asynchronous counterparts, using `run_async_from_sync` from [`src/naq/utils.py`](src/naq/utils.py). They also handle closing the NATS connection after execution.

*   `enqueue_sync(...) -> Job` ([`src/naq/queue.py:845`](src/naq/queue.py:845))
*   `enqueue_at_sync(...) -> Job` ([`src/naq/queue.py:873`](src/naq/queue.py:873))
*   `enqueue_in_sync(...) -> Job` ([`src/naq/queue.py:901`](src/naq/queue.py:901))
*   `schedule_sync(...) -> Job` ([`src/naq/queue.py:929`](src/naq/queue.py:929))
*   `purge_queue_sync(...) -> int` ([`src/naq/queue.py:961`](src/naq/queue.py:961))
*   `cancel_scheduled_job_sync(...) -> bool` ([`src/naq/queue.py:974`](src/naq/queue.py:974))
*   `pause_scheduled_job_sync(...) -> bool` ([`src/naq/queue.py:984`](src/naq/queue.py:984))
*   `resume_scheduled_job_sync(...) -> bool` ([`src/naq/queue.py:994`](src/naq/queue.py:994))
*   `modify_scheduled_job_sync(...) -> bool` ([`src/naq/queue.py:1004`](src/naq/queue.py:1004))

## 3. Key Functionalities

The [`src/naq/queue.py`](src/naq/queue.py) module implements several key functionalities for the `naq` task queue system:

*   **Job Enqueueing:**
    *   Allows submission of Python functions (with arguments) as jobs to a named queue.
    *   Jobs are serialized using the `Job.serialize()` method (from [`src/naq/job.py`](src/naq/job.py)) and published to a NATS JetStream subject corresponding to the queue name.
    *   Supports job dependencies, where a job will only be processed after its dependent jobs are completed.

*   **Scheduled Job Management:**
    *   **One-time Scheduling:** Jobs can be scheduled to run at a specific future `datetime` (`enqueue_at`) or after a `timedelta` (`enqueue_in`).
    *   **Recurring Scheduling:** Jobs can be scheduled to run repeatedly based on a cron expression or a fixed time interval (`schedule`). The `croniter` library is used for cron expression parsing if available.
    *   **Storage:** Scheduled job metadata (including the original serialized job, schedule type, next run time, status, etc.) is stored in a NATS Key-Value store (defaulting to `naq_scheduled_jobs`). `cloudpickle` is used to serialize this metadata.
    *   **Lifecycle Management:** Scheduled jobs can be cancelled (removed from KV store), paused (status updated in KV store to prevent scheduler from enqueuing), resumed (status updated to active), and modified (schedule parameters like cron, interval, repeat count can be changed).

*   **Queue Operations:**
    *   **Purging:** Allows for the removal of all jobs currently pending in a specific queue by purging messages from the NATS JetStream stream associated with the queue's subject.

*   **NATS Integration:**
    *   Manages connections to NATS and obtains JetStream contexts.
    *   Ensures the necessary NATS JetStream stream (for job messages) and Key-Value store (for scheduled jobs) exist, creating them if they don't.
    *   Publishes job messages to JetStream and uses KV store operations (put, get, delete, update) for scheduled job management.

*   **Asynchronous and Synchronous APIs:**
    *   Provides a primary asynchronous API (using `async/await`) for all operations.
    *   Offers synchronous wrapper functions for all key operations, facilitating integration into synchronous Python applications. These wrappers use a utility to run the async code in a blocking manner and manage NATS connection closure.

*   **Configuration and Logging:**
    *   Uses settings from [`src/naq/settings.py`](src/naq/settings.py) for defaults like queue names, NATS prefixes, and KV store names.
    *   Integrates with `loguru` for logging queue operations and potential errors.

## 4. Dependencies and Interactions

The [`src/naq/queue.py`](src/naq/queue.py) module has several dependencies and interactions, both internal to the `naq` library and with external packages:

*   **Internal `naq` Modules:**
    *   **[`src/naq/job.py`](src/naq/job.py):**
        *   Instantiates `Job` objects ([`src/naq/job.py:184`](src/naq/job.py:184)) to represent tasks.
        *   Calls `job.serialize()` ([`src/naq/job.py:265`](src/naq/job.py:265)) to prepare jobs for NATS transmission.
        *   Uses `RetryDelayType` from this module.
    *   **[`src/naq/connection.py`](src/naq/connection.py):**
        *   Heavily relies on this module for all NATS communications.
        *   Uses `get_nats_connection()` ([`src/naq/connection.py:100`](src/naq/connection.py:100)), `get_jetstream_context()` ([`src/naq/connection.py:128`](src/naq/connection.py:128)), `ensure_stream()` ([`src/naq/connection.py:50`](src/naq/connection.py:50)), and `close_nats_connection()` ([`src/naq/connection.py:118`](src/naq/connection.py:118)).
    *   **[`src/naq/settings.py`](src/naq/settings.py):**
        *   Imports and uses various constants:
            *   `DEFAULT_QUEUE_NAME` ([`src/naq/settings.py:10`](src/naq/settings.py:10))
            *   `JOB_SERIALIZER` ([`src/naq/settings.py:13`](src/naq/settings.py:13)) (used in `ScheduledJobManager` to record how the original job was serialized)
            *   `NAQ_PREFIX` ([`src/naq/settings.py:7`](src/naq/settings.py:7)) (for NATS subject and stream naming)
            *   `SCHEDULED_JOB_STATUS_ACTIVE`, `SCHEDULED_JOB_STATUS_PAUSED` ([`src/naq/settings.py:30-31`](src/naq/settings.py:30))
            *   `SCHEDULED_JOBS_KV_NAME` ([`src/naq/settings.py:27`](src/naq/settings.py:27))
    *   **[`src/naq/exceptions.py`](src/naq/exceptions.py):**
        *   Raises custom exceptions: `ConfigurationError` ([`src/naq/exceptions.py:20`](src/naq/exceptions.py:20)), `NaqConnectionError` (as `ConnectionError` from [`src/naq/exceptions.py:15`](src/naq/exceptions.py:15)), `JobNotFoundError` ([`src/naq/exceptions.py:30`](src/naq/exceptions.py:30)), `NaqException` ([`src/naq/exceptions.py:10`](src/naq/exceptions.py:10)).
    *   **[`src/naq/utils.py`](src/naq/utils.py):**
        *   Uses `run_async_from_sync()` ([`src/naq/utils.py:20`](src/naq/utils.py:20)) for synchronous helper functions.
        *   Calls `setup_logging()` ([`src/naq/utils.py:9`](src/naq/utils.py:9)) in `Queue.__init__`.
    *   **[`src/naq/worker.py`](src/naq/worker.py) (Implicit):**
        *   Workers are the consumers of jobs enqueued by this module.
        *   A scheduler component (potentially part of the worker or a separate process, defined in [`src/naq/scheduler.py`](src/naq/scheduler.py)) would monitor the `SCHEDULED_JOBS_KV_NAME` and use the `Queue` class (or its helper functions) to enqueue jobs when their scheduled time arrives.

*   **External Libraries:**
    *   **`nats` (Python NATS client):**
        *   Directly used for all NATS JetStream and KV store operations (e.g., `js.publish`, `js.key_value`, `kv.put`, `kv.get`, `kv.delete`, `kv.update`, `js.purge_stream`).
        *   Handles NATS-specific errors like `nats.js.errors.KeyNotFoundError`, `nats.js.errors.APIError`, `nats.errors.Error`.
    *   **`cloudpickle`:**
        *   Used by `ScheduledJobManager` ([`src/naq/queue.py:114`](src/naq/queue.py:114), [`src/naq/queue.py:169`](src/naq/queue.py:169)) to serialize and deserialize the `schedule_data` dictionary stored in the NATS KV store. This dictionary contains metadata about the scheduled job, including the original job payload.
    *   **`loguru`:**
        *   Used for structured logging throughout the module.
    *   **`croniter` (Optional):**
        *   Imported and used by `ScheduledJobManager._calculate_next_run_time()` ([`src/naq/queue.py:306`](src/naq/queue.py:306)) and `Queue.schedule()` ([`src/naq/queue.py:553`](src/naq/queue.py:553)) if cron-based scheduling is requested. An `ImportError` is raised if not installed.
    *   **Standard Python Libraries:** `datetime`, `typing`.

## 5. Notable Implementation Details

*   **Dual NATS Usage (JetStream + KV):** The module leverages NATS JetStream for robust, persistent message queuing of actual jobs and NATS Key-Value (KV) store for managing the metadata and state of scheduled/recurring jobs. This separation allows for efficient querying and management of scheduled tasks without cluttering the job streams.
*   **Dynamic Resource Creation:** Both the JetStream stream for jobs (`naq_jobs`) and the KV store for scheduled jobs (`naq_scheduled_jobs`) are created on-demand if they don't already exist (e.g., in `Queue._get_js()` ([`src/naq/queue.py:356`](src/naq/queue.py:356)) and `ScheduledJobManager.get_kv()` ([`src/naq/queue.py:59`](src/naq/queue.py:59))).
*   **Optimistic Concurrency Control:** When updating scheduled job status (`ScheduledJobManager.update_job_status()` ([`src/naq/queue.py:178`](src/naq/queue.py:178))) or modifying job parameters (`ScheduledJobManager.modify_job()` ([`src/naq/queue.py:273`](src/naq/queue.py:273))), the NATS KV `update` operation is used with the `last=entry.revision` parameter. This ensures that updates are only applied if the entry hasn't been modified by another process since it was read, preventing race conditions.
*   **Async/Sync Parity:** A comprehensive set of synchronous helper functions are provided, mirroring the asynchronous ones. This is achieved using the `run_async_from_sync` utility, making the library accessible to both async-native and traditional synchronous codebases. Sync helpers also manage NATS connection closure.
*   **Scheduled Job Data Serialization:** The `ScheduledJobManager` stores a dictionary containing schedule metadata and the *original serialized job payload* in the KV store. This means the job itself is serialized once by `Job.serialize()` and that payload is then embedded within the `cloudpickle`d schedule data.
*   **Graceful `croniter` Dependency:** The `croniter` library is an optional dependency. If a user attempts to use cron-based scheduling without `croniter` installed, a clear `ImportError` is raised with installation instructions.
*   **Clear Separation of Concerns:** The `Queue` class handles the general "queue" concept and immediate enqueuing, while the `ScheduledJobManager` class encapsulates the more complex logic of storing, retrieving, and managing the state of scheduled and recurring jobs.
*   **UTC Timestamps:** All internal time handling for scheduling (e.g., `scheduled_timestamp_utc`, `next_run_utc`) is done using UTC timestamps to avoid timezone ambiguities. Input `datetime` objects are converted to UTC.

## 6. Mermaid Diagram

```mermaid
classDiagram
    class Queue {
        +name: str
        +subject: str
        +stream_name: str
        -_scheduled_job_manager: ScheduledJobManager
        +__init__(name, nats_url, default_timeout)
        +enqueue(func, ...): Job
        +enqueue_at(dt, func, ...): Job
        +enqueue_in(delta, func, ...): Job
        +schedule(func, cron/interval, ...): Job
        +purge(): int
        +cancel_scheduled_job(job_id): bool
        +pause_scheduled_job(job_id): bool
        +resume_scheduled_job(job_id): bool
        +modify_scheduled_job(job_id, ...): bool
        -_get_js(): JetStreamContext
    }

    class ScheduledJobManager {
        +queue_name: str
        -_kv: KeyValue
        +__init__(queue_name, nats_url)
        +get_kv(): KeyValue
        +store_job(job, scheduled_ts, cron, interval, repeat)
        +cancel_job(job_id): bool
        +update_job_status(job_id, status): bool
        +modify_job(job_id, ...): bool
        -_calculate_next_run_time(schedule_data): float
    }

    class Job {
        +job_id: str
        +function: Callable
        +serialize(): bytes
    }

    class NatsConnection {
        <<Module>>
        +get_nats_connection()
        +get_jetstream_context()
        +ensure_stream()
        +close_nats_connection()
    }
    
    class Settings {
        <<Module>>
        +DEFAULT_QUEUE_NAME
        +NAQ_PREFIX
        +SCHEDULED_JOBS_KV_NAME
        +JOB_SERIALIZER
    }

    class Utils {
        <<Module>>
        +run_async_from_sync()
        +setup_logging()
    }

    Queue o--> ScheduledJobManager : uses
    Queue ..> Job : creates & uses
    ScheduledJobManager ..> Job : uses (stores serialized job)
    
    Queue ..> NatsConnection : uses
    ScheduledJobManager ..> NatsConnection : uses (for KV)

    Queue ..> Settings : uses
    ScheduledJobManager ..> Settings : uses

    Queue ..> Utils : uses (setup_logging)
    
    %% Module Helper Functions (Conceptual)
    class ModuleHelpers {
        <<Conceptual>>
        +enqueue_async()
        +enqueue_sync()
        +schedule_async()
        +schedule_sync()
        %% ... and other helpers
    }
    ModuleHelpers ..> Queue : instantiates & uses

    ScheduledJobManager ..> cloudpickle : uses for KV data
    ScheduledJobManager ..> croniter : uses (optional)

    note for Job "From src/naq/job.py"
    note for NatsConnection "From src/naq/connection.py"
    note for Settings "From src/naq/settings.py"
    note for Utils "From src/naq/utils.py"