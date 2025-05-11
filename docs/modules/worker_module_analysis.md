# Analysis of `src/naq/worker.py`

## 1. Module Overview and Role

The [`src/naq/worker.py`](src/naq/worker.py) module is central to the `naq` library's execution capabilities. Its primary role is to define the `Worker` class, which is responsible for fetching, processing, and managing the lifecycle of jobs from one or more NATS-backed queues. This module orchestrates various aspects of job execution, including:

- Connecting to NATS and JetStream.
- Subscribing to job queues using JetStream pull consumers.
- Deserializing job data into `Job` objects.
- Managing concurrent job execution using `asyncio.Semaphore`.
- Handling job dependencies by checking their completion status.
- Executing job functions (potentially in a thread pool for synchronous code).
- Implementing retry logic for failed jobs based on `Job` configurations.
- Storing job results or failure information.
- Reporting worker status and heartbeats.
- Handling graceful shutdown upon receiving signals.

The module is composed of several key classes:
- `Worker` ([`src/naq/worker.py:470`](src/naq/worker.py:470)): The main class that pulls jobs and manages their execution.
- `WorkerStatusManager` ([`src/naq/worker.py:50`](src/naq/worker.py:50)): Manages the worker's lifecycle reporting (status, heartbeats) to a NATS KV store.
- `JobStatusManager` ([`src/naq/worker.py:252`](src/naq/worker.py:252)): Manages job completion status (for dependency tracking) and stores job results in NATS KV stores.
- `FailedJobHandler` ([`src/naq/worker.py:418`](src/naq/worker.py:418)): Handles the publication of terminally failed jobs to a dedicated NATS stream.

## 2. Public Interfaces

### A. Worker Class ([`src/naq/worker.py:470`](src/naq/worker.py:470))

The main orchestrator for fetching and executing jobs.

- **`__init__(self, queues: Sequence[str] | str, nats_url: Optional[str] = None, concurrency: int = 10, worker_name: Optional[str] = None, heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS, worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS)`** ([`src/naq/worker.py:477`](src/naq/worker.py:477)):
  - `queues`: A list of queue names (or a single queue name) the worker should listen to.
  - `nats_url`: URL of the NATS server.
  - `concurrency`: Maximum number of jobs to process concurrently.
  - `worker_name`: Optional base name for the worker; a unique ID is generated.
  - `heartbeat_interval`: Interval in seconds for sending heartbeats.
  - `worker_ttl`: Time-to-live in seconds for the worker's record in the status KV store.
- **`async run(self) -> None`** ([`src/naq/worker.py:719`](src/naq/worker.py:719)):
  - The main entry point to start the worker. It connects to NATS, starts heartbeating, subscribes to queues, and processes jobs until a shutdown signal is received.
- **`async process_message(self, msg: Msg) -> None`** ([`src/naq/worker.py:610`](src/naq/worker.py:610)):
  - Handles a raw NATS message. It deserializes the job, checks dependencies, executes the job, and handles success, retries, or terminal failure.
- **`signal_handler(self, sig, frame) -> None`** ([`src/naq/worker.py:802`](src/naq/worker.py:802)):
  - Handles OS signals (SIGINT, SIGTERM) to initiate a graceful shutdown.
- **`install_signal_handlers(self) -> None`** ([`src/naq/worker.py:808`](src/naq/worker.py:808)):
  - Sets up the signal handlers.
- **`@staticmethod async list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]`** ([`src/naq/worker.py:814`](src/naq/worker.py:814)):
  - Lists active workers by querying the worker status KV store. Delegates to `WorkerStatusManager.list_workers`.
- **`@staticmethod list_workers_sync(nats_url: Optional[str] = None) -> List[Dict[str, Any]]`** ([`src/naq/worker.py:819`](src/naq/worker.py:819)):
  - Synchronous version of `list_workers`.

### B. WorkerStatusManager Class ([`src/naq/worker.py:50`](src/naq/worker.py:50))

Manages worker status, heartbeats, and registration in a NATS KV store.

- **`__init__(self, worker_id: str, queue_names: List[str], heartbeat_interval: int, worker_ttl: int, concurrency: int, semaphore: asyncio.Semaphore)`** ([`src/naq/worker.py:55`](src/naq/worker.py:55)):
  - Initializes with worker-specific details and NATS configuration.
- **`async initialize(self, js: JetStreamContext) -> None`** ([`src/naq/worker.py:77`](src/naq/worker.py:77)):
  - Connects to or creates the `WORKER_KV_NAME` KV store.
- **`async update_status(self, status: Optional[str] = None, job_id: Optional[str] = None) -> None`** ([`src/naq/worker.py:111`](src/naq/worker.py:111)):
  - Updates the worker's status (e.g., idle, busy, starting, stopping) and current job ID in the KV store. This also serves as a heartbeat.
- **`async start_heartbeat_loop(self) -> None`** ([`src/naq/worker.py:144`](src/naq/worker.py:144)):
  - Starts a background task to periodically call `update_status`.
- **`async stop_heartbeat_loop(self) -> None`** ([`src/naq/worker.py:171`](src/naq/worker.py:171)):
  - Stops the heartbeat background task.
- **`async unregister_worker(self) -> None`** ([`src/naq/worker.py:182`](src/naq/worker.py:182)):
  - Deletes the worker's entry from the KV store upon shutdown.
- **`@staticmethod async list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]`** ([`src/naq/worker.py:195`](src/naq/worker.py:195)):
  - Retrieves a list of all registered workers and their statuses from the KV store.

### C. JobStatusManager Class ([`src/naq/worker.py:252`](src/naq/worker.py:252))

Manages job completion status for dependency tracking and stores job results.

- **`__init__(self)`** ([`src/naq/worker.py:257`](src/naq/worker.py:257)):
  - Initializes KV store references.
- **`async initialize(self, js: JetStreamContext) -> None`** ([`src/naq/worker.py:261`](src/naq/worker.py:261)):
  - Connects to or creates the `JOB_STATUS_KV_NAME` and `RESULT_KV_NAME` KV stores.
- **`async check_dependencies(self, job: Job) -> bool`** ([`src/naq/worker.py:331`](src/naq/worker.py:331)):
  - Checks if all `job.dependency_ids` have a status of `JOB_STATUS_COMPLETED` in the `JOB_STATUS_KV_NAME` store. Returns `False` if any dependency is not found, has failed, or has an unknown status.
- **`async update_job_status(self, job_id: str, status: str) -> None`** ([`src/naq/worker.py:373`](src/naq/worker.py:373)):
  - Writes the completion status (`JOB_STATUS_COMPLETED` or `JOB_STATUS_FAILED`) of a `job_id` to the `JOB_STATUS_KV_NAME` store.
- **`async store_result(self, job: Job) -> None`** ([`src/naq/worker.py:385`](src/naq/worker.py:385)):
  - Serializes and stores the job's result (if successful) or error information (if failed) into the `RESULT_KV_NAME` store, keyed by `job.job_id`.

### D. FailedJobHandler Class ([`src/naq/worker.py:418`](src/naq/worker.py:418))

Handles the processing of jobs that have terminally failed.

- **`__init__(self)`** ([`src/naq/worker.py:423`](src/naq/worker.py:423)):
  - Initializes JetStream context reference.
- **`async initialize(self, js: JetStreamContext) -> None`** ([`src/naq/worker.py:426`](src/naq/worker.py:426)):
  - Stores the JetStream context and ensures the `FAILED_JOB_STREAM_NAME` stream exists.
- **`async publish_failed_job(self, job: Job) -> None`** ([`src/naq/worker.py:450`](src/naq/worker.py:450)):
  - Serializes the failed `Job` object (using `job.serialize_failed_job()`) and publishes it to a subject within the `FAILED_JOB_STREAM_NAME` (e.g., `naq.failed_jobs.queue_name`).

## 3. Key Functionalities

- **Job Fetching and Subscription**:
  - The `Worker` connects to NATS and uses `_subscribe_to_queue` ([`src/naq/worker.py:542`](src/naq/worker.py:542)) for each specified queue.
  - It creates durable JetStream pull consumers (`js.pull_subscribe`) with an explicit acknowledgment policy (`AckPolicy.EXPLICIT`).
  - Messages are fetched in batches (`psub.fetch`) based on available concurrency slots.
- **Concurrency Management**:
  - An `asyncio.Semaphore` ([`src/naq/worker.py:509`](src/naq/worker.py:509)) is used to limit the number of concurrently processed jobs to the `concurrency` value specified during `Worker` initialization.
  - The semaphore is acquired before a job processing task is created and released when the task completes (via `task.add_done_callback`).
- **Job Deserialization and Execution**:
  - In `process_message` ([`src/naq/worker.py:610`](src/naq/worker.py:610)), raw message data is deserialized into a `Job` object using `Job.deserialize(msg.data)` ([`src/naq/worker.py:618`](src/naq/worker.py:618)).
  - The actual job function (`job.function`) is executed using `await asyncio.to_thread(job.execute)` ([`src/naq/worker.py:638`](src/naq/worker.py:638)), allowing synchronous job functions to run without blocking the worker's async event loop.
- **Dependency Checking**:
  - Before execution, `JobStatusManager.check_dependencies(job)` ([`src/naq/worker.py:621`](src/naq/worker.py:621)) is called.
  - If dependencies are not met, the message is NAK'd (Negative Acknowledgment) with a delay (`DEPENDENCY_CHECK_DELAY_SECONDS`), causing it to be redelivered later.
- **Status Reporting and Heartbeats**:
  - `WorkerStatusManager` is responsible for this.
  - `update_status` ([`src/naq/worker.py:111`](src/naq/worker.py:111)) periodically writes the worker's ID, hostname, PID, listened queues, current status (starting, idle, busy, stopping), active tasks, and a heartbeat timestamp to the `WORKER_KV_NAME` KV store.
  - This allows for monitoring worker health and activity.
- **Result and Failure Handling**:
  - **Success**: If `job.execute()` completes without error, `JobStatusManager.update_job_status` ([`src/naq/worker.py:642`](src/naq/worker.py:642)) marks the job as `JOB_STATUS_COMPLETED`, and `JobStatusManager.store_result` ([`src/naq/worker.py:643`](src/naq/worker.py:643)) saves the outcome. The NATS message is then ACK'd.
  - **Execution Error (`JobExecutionError`)**: Handled by `_handle_job_execution_error` ([`src/naq/worker.py:660`](src/naq/worker.py:660)).
    - **Retry Logic**: If `msg.metadata.num_delivered` is less than or equal to `job.max_retries`, the message is NAK'd with a delay calculated by `job.get_retry_delay()`.
    - **Terminal Failure**: If retries are exhausted, `JobStatusManager.update_job_status` marks the job as `JOB_STATUS_FAILED`, `JobStatusManager.store_result` saves the error, and `FailedJobHandler.publish_failed_job` ([`src/naq/worker.py:690`](src/naq/worker.py:690)) sends it to the failed jobs stream. The original message is then ACK'd.
  - **Serialization Error**: If `Job.deserialize` fails, the message is considered a "poison pill" and is TERM'd (Terminated) to prevent redelivery ([`src/naq/worker.py:651`](src/naq/worker.py:651)).
  - **Unexpected Errors**: Handled by `_handle_unexpected_error` ([`src/naq/worker.py:700`](src/naq/worker.py:700)). The job (if deserialized) is marked as failed, its status and error are stored, and the message is TERM'd.
- **Graceful Shutdown**:
  - `install_signal_handlers` ([`src/naq/worker.py:808`](src/naq/worker.py:808)) sets up handlers for `SIGINT` and `SIGTERM`.
  - `signal_handler` ([`src/naq/worker.py:802`](src/naq/worker.py:802)) sets `self._running = False` and `self._shutdown_event.set()`.
  - The main `run` loop waits for this event, then stops heartbeats, waits for active jobs to complete (with a timeout), cancels subscription tasks, and finally unregisters the worker and closes the NATS connection via `_close()` ([`src/naq/worker.py:793`](src/naq/worker.py:793)).

## 4. Dependencies and Interactions

The `worker.py` module interacts significantly with other `naq` components and NATS:

- **[`src/naq/job.py`](src/naq/job.py) (`Job` and `JobExecutionError`)**:
  - Workers deserialize incoming NATS messages into `Job` objects using `Job.deserialize()` ([`src/naq/worker.py:618`](src/naq/worker.py:618)).
  - They call `job.execute()` ([`src/naq/worker.py:638`](src/naq/worker.py:638)) to run the task.
  - They use `job.max_retries` and `job.get_retry_delay()` for retry logic ([`src/naq/worker.py:671`](src/naq/worker.py:671), [`src/naq/worker.py:674`](src/naq/worker.py:674)).
  - `job.error` and `job.traceback` are populated on failure and used by `JobStatusManager.store_result()` and `FailedJobHandler.publish_failed_job()`.
  - `job.dependency_ids` are used by `JobStatusManager.check_dependencies()` ([`src/naq/worker.py:331`](src/naq/worker.py:331)).
  - `JobExecutionError` ([`src/naq/job.py:238`](src/naq/job.py:238)) is caught to trigger retry/failure logic ([`src/naq/worker.py:647`](src/naq/worker.py:647)).
  - `job.serialize_failed_job()` is used by `FailedJobHandler` ([`src/naq/worker.py:458`](src/naq/worker.py:458)).
- **[`src/naq/connection.py`](src/naq/connection.py) (NATS Communication)**:
  - Functions like `get_nats_connection()` ([`src/naq/worker.py:533`](src/naq/worker.py:533)), `get_jetstream_context()` ([`src/naq/worker.py:534`](src/naq/worker.py:534)), `ensure_stream()` ([`src/naq/worker.py:438`](src/naq/worker.py:438), [`src/naq/worker.py:735`](src/naq/worker.py:735)), and `close_nats_connection()` ([`src/naq/worker.py:798`](src/naq/worker.py:798)) are used for managing the NATS connection and JetStream context.
  - The helper managers (`WorkerStatusManager`, `JobStatusManager`) use JetStream KV store operations (`js.key_value()`, `kv.put()`, `kv.get()`, `kv.delete()`, `js.create_key_value()`).
  - `FailedJobHandler` uses `js.publish()` ([`src/naq/worker.py:459`](src/naq/worker.py:459)).
  - The `Worker` uses `js.pull_subscribe()` ([`src/naq/worker.py:554`](src/naq/worker.py:554)) and message methods like `msg.ack()`, `msg.nak()`, `msg.term()`.
- **[`src/naq/settings.py`](src/naq/settings.py) (Configuration Constants)**:
  - Numerous constants are used for KV store names (`WORKER_KV_NAME`, `JOB_STATUS_KV_NAME`, `RESULT_KV_NAME`), stream names (`FAILED_JOB_STREAM_NAME`), subject prefixes (`NAQ_PREFIX`, `FAILED_JOB_SUBJECT_PREFIX`), status strings (`WORKER_STATUS_*`, `JOB_STATUS_*`), TTLs, and default values.
- **[`src/naq/exceptions.py`](src/naq/exceptions.py) (Error Handling)**:
  - `NaqException` ([`src/naq/worker.py:26`](src/naq/worker.py:26)) can be raised for general worker issues (e.g., JetStream context not available).
  - `SerializationError` ([`src/naq/worker.py:26`](src/naq/worker.py:26)) is caught if `Job.deserialize` fails ([`src/naq/worker.py:649`](src/naq/worker.py:649)).
  - `BucketNotFoundError` and `KeyNotFoundError` from `nats.js.errors` are handled during KV store operations.
- **[`src/naq/utils.py`](src/naq/utils.py) (Utilities)**:
  - `setup_logging()` ([`src/naq/worker.py:528`](src/naq/worker.py:528)) is called to configure logging.
  - `run_async_from_sync()` ([`src/naq/worker.py:822`](src/naq/worker.py:822)) is used for the `list_workers_sync` static method.
- **External Libraries**:
  - `nats-py`: For all NATS and JetStream communication.
  - `cloudpickle`: Used by `WorkerStatusManager` and `JobStatusManager` to serialize/deserialize data stored in KV stores (worker status, job results).
  - `loguru`: For logging.
  - Standard libraries: `asyncio`, `os`, `signal`, `socket`, `time`, `traceback`, `uuid`.

## 5. Notable Implementation Details

- **Asynchronous Design**: The entire worker is built on `asyncio`, enabling efficient I/O-bound operations (NATS communication, waiting for jobs) and concurrent job processing.
- **JetStream Pull Consumers**: Instead of push consumers, pull consumers (`js.pull_subscribe`) are used. This gives the worker more control over when and how many messages to fetch, integrating well with its concurrency management (`_semaphore`).
- **Modular Helper Classes**: Functionality is broken down into `WorkerStatusManager`, `JobStatusManager`, and `FailedJobHandler`. This improves organization and separation of concerns.
- **KV Stores for State Management**: NATS JetStream Key-Value stores are extensively used:
  - `WORKER_KV_NAME`: For worker heartbeats, status, and discovery.
  - `JOB_STATUS_KV_NAME`: For tracking job completion, crucial for dependency resolution.
  - `RESULT_KV_NAME`: For storing job results or error details.
- **Graceful Shutdown**: Signal handlers (`SIGINT`, `SIGTERM`) ensure that the worker attempts to finish processing active jobs and clean up resources (unregister, close NATS connection) before exiting. There's a timeout (`_wait_for_semaphore` with timeout in `run`) to prevent indefinite hanging.
- **Explicit Acknowledgement**: Jobs are explicitly ACK'd, NAK'd, or TERM'd, giving fine-grained control over message redelivery and handling of problematic messages.
- **Durable Consumers**: Consumers are created with durable names (e.g., `naq-worker-queue_name`) to ensure message delivery resumes correctly after worker restarts.
- **Thread Pool for Synchronous Jobs**: `asyncio.to_thread(job.execute)` is used to run potentially synchronous job functions in a separate thread, preventing them from blocking the main asyncio event loop.
- **Error Handling Granularity**: Different types of errors (job execution errors, serialization errors, NATS errors, unexpected errors) are caught and handled distinctly, with appropriate logging and message acknowledgment strategies.
- **Dynamic KV Store Creation**: The KV stores (`WORKER_KV_NAME`, `JOB_STATUS_KV_NAME`, `RESULT_KV_NAME`) and the `FAILED_JOB_STREAM_NAME` stream are created by the worker if they don't already exist, simplifying deployment.

## 6. Mermaid Diagram

```mermaid
classDiagram
    class Worker {
        +worker_id: str
        +queue_names: List[str]
        +concurrency: int
        -_nc: NatsConnection
        -_js: JetStreamContext
        -_semaphore: asyncio.Semaphore
        -_status_manager: WorkerStatusManager
        -_job_status_manager: JobStatusManager
        -_failed_job_handler: FailedJobHandler
        +__init__(...)
        +run()
        +process_message(msg: NatsMsg)
        +signal_handler(sig, frame)
        +list_workers() static
        +list_workers_sync() static
    }

    class WorkerStatusManager {
        +worker_id: str
        -_worker_kv: NatsKeyValueStore
        +initialize(js)
        +update_status(status, job_id)
        +start_heartbeat_loop()
        +stop_heartbeat_loop()
        +unregister_worker()
        +list_workers() static
    }

    class JobStatusManager {
        -_status_kv: NatsKeyValueStore
        -_result_kv: NatsKeyValueStore
        +initialize(js)
        +check_dependencies(job: Job): bool
        +update_job_status(job_id, status)
        +store_result(job: Job)
    }

    class FailedJobHandler {
        -_js: JetStreamContext
        +initialize(js)
        +publish_failed_job(job: Job)
    }

    class Job {
      +job_id: str
      +function: Callable
      +args: Tuple
      +kwargs: Dict
      +dependency_ids: List[str]
      +max_retries: int
      +result: Any
      +error: str
      +execute()
      +deserialize(data): Job
      +serialize_failed_job(): bytes
      +get_retry_delay(): float
    }

    class NatsConnection {
        <<External>>
        +connect()
        +publish()
        +subscribe()
        +key_value()
        +pull_subscribe()
    }
    class NatsMsg {
        <<External>>
        +data: bytes
        +ack()
        +nak()
        +term()
    }
    class NatsKeyValueStore {
        <<External>>
        +get(key)
        +put(key, value)
        +delete(key)
        +create_key_value(...)
    }
    class JetStreamContext {
        <<External>>
        +key_value()
        +pull_subscribe()
        +publish()
        +create_key_value(...)
    }

    Worker *-- WorkerStatusManager
    Worker *-- JobStatusManager
    Worker *-- FailedJobHandler
    Worker ..> Job : deserializes & executes
    Worker ..> NatsConnection : uses
    Worker ..> JetStreamContext : uses
    Worker ..> NatsMsg : processes

    WorkerStatusManager ..> NatsKeyValueStore : uses (WORKER_KV_NAME)
    JobStatusManager ..> NatsKeyValueStore : uses (JOB_STATUS_KV_NAME, RESULT_KV_NAME)
    JobStatusManager ..> Job : uses for info
    FailedJobHandler ..> JetStreamContext : uses for publish
    FailedJobHandler ..> Job : uses for info

    Job ..> cloudpickle : for serialization (indirectly via KV stores)
    WorkerStatusManager ..> cloudpickle : for KV data
    JobStatusManager ..> cloudpickle : for KV data