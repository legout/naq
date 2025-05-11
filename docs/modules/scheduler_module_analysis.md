# Analysis of [`src/naq/scheduler.py`](src/naq/scheduler.py)

## 1. Module Overview and Role

The [`src/naq/scheduler.py`](src/naq/scheduler.py) module is responsible for orchestrating the execution of scheduled jobs within the `naq` distributed task queue system. It manages both one-off and recurring jobs (interval or cron-based), ensuring they are enqueued at the correct time. The module supports high availability (HA) through leader election, so only one scheduler instance is active at a time in a clustered deployment. It leverages NATS JetStream Key-Value stores for both job scheduling metadata and distributed locking.

## 2. Public Interfaces

### A. `Scheduler` Class ([`src/naq/scheduler.py:450`](src/naq/scheduler.py:450))

- **Purpose:** Main entry point for running the scheduler loop, handling NATS connections, leader election, and job polling.
- **Key Attributes:**
  - `_nats_url: str` — NATS server URL.
  - `_poll_interval: float` — How often to poll for scheduled jobs.
  - `_instance_id: str` — Unique identifier for this scheduler instance.
  - `_enable_ha: bool` — Whether high availability/leader election is enabled.
  - `_leader_election: LeaderElection` — Handles distributed lock for HA.
  - `_job_processor: ScheduledJobProcessor` — Processes scheduled jobs.
- **Key Methods:**
  - `__init__(...)`
  - `async run()` ([`src/naq/scheduler.py:515`](src/naq/scheduler.py:515)): Main scheduler loop; manages leadership and job polling.
  - `is_leader` ([`src/naq/scheduler.py:595`](src/naq/scheduler.py:595)): Property indicating if this instance is currently the leader.

### B. `LeaderElection` Class ([`src/naq/scheduler.py:48`](src/naq/scheduler.py:48))

- **Purpose:** Provides distributed leader election using a NATS KV lock, ensuring only one active scheduler.
- **Key Attributes:**
  - `instance_id: str` — Unique ID for this instance.
  - `lock_ttl: int` — Lock time-to-live.
  - `lock_renew_interval: int` — How often to renew the lock.
  - `_is_leader: bool` — Whether this instance currently holds the lock.
- **Key Methods:**
  - `__init__(...)`
  - `async initialize(js: JetStreamContext)`
  - `async try_become_leader() -> bool`
  - `async start_renewal_task(running_flag: bool)`
  - `async stop_renewal_task()`
  - `async release_lock()`
  - `is_leader`: Property indicating leadership status.

### C. `ScheduledJobProcessor` Class ([`src/naq/scheduler.py:202`](src/naq/scheduler.py:202))

- **Purpose:** Handles polling and processing of scheduled jobs from the NATS KV store.
- **Key Attributes:**
  - `_js: JetStreamContext` — JetStream context for publishing jobs.
  - `_kv: KeyValue` — KV store for scheduled jobs.
- **Key Methods:**
  - `__init__(...)`
  - `async process_jobs(is_leader: bool) -> tuple[int, int]`: Polls and processes jobs if leader.
  - `_enqueue_job(...)`
  - `_calculate_next_runtime(...)`
  - `async _process_single_job(...)`

### D. Key Constants

- Imported from [`src/naq/settings.py`](src/naq/settings.py):
  - `SCHEDULED_JOBS_KV_NAME`, `SCHEDULER_LOCK_KV_NAME`, `SCHEDULER_LOCK_KEY`, `SCHEDULER_LOCK_TTL_SECONDS`, `SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS`, `MAX_SCHEDULE_FAILURES`, `SCHEDULED_JOB_STATUS_ACTIVE`, `SCHEDULED_JOB_STATUS_FAILED`, `SCHEDULED_JOB_STATUS_PAUSED`, `NAQ_PREFIX`, `DEFAULT_NATS_URL`.

## 3. Key Functionalities

- **Leader Election:**  
  The `LeaderElection` class uses a NATS KV store as a distributed lock. Each scheduler instance tries to acquire the lock; only the leader processes jobs. The lock is periodically renewed, and released on shutdown.
- **Job Storage and Retrieval:**  
  Scheduled jobs are stored as serialized payloads in a NATS KV bucket. Each entry contains metadata (schedule, status, repeat count, etc.).
- **Job Polling and Processing:**  
  The scheduler loop (`run()`) checks leadership, then calls `ScheduledJobProcessor.process_jobs()`. This method:
  - Scans all scheduled jobs.
  - For each job, checks if it is due (based on timestamp/status).
  - Enqueues the job to the appropriate NATS subject if ready.
  - Handles recurrence (interval or cron) and repeat counts.
  - Updates or deletes the KV entry as needed.
- **Recurring Jobs:**  
  Supports both interval-based and cron-based recurrence. Uses `croniter` if available for cron parsing. Handles infinite or limited repeats.
- **Failure Handling:**  
  Tracks scheduling failures per job. If a job fails to enqueue more than `MAX_SCHEDULE_FAILURES`, it is marked as failed and skipped in future cycles.
- **Graceful Shutdown:**  
  Installs signal handlers to allow clean shutdown, releasing locks and closing NATS connections.

## 4. Dependencies and Interactions

- **External:**
  - [`nats`](https://github.com/nats-io/nats.py): NATS client, JetStream, and KV store.
  - [`cloudpickle`](https://github.com/cloudpipe/cloudpickle): Serialization of job and lock data.
  - [`loguru`](https://github.com/Delgan/loguru): Logging.
  - [`croniter`](https://github.com/kiorky/croniter) (optional): Cron schedule parsing.
- **Internal:**
  - [`src/naq/connection.py`](src/naq/connection.py): NATS connection helpers.
  - [`src/naq/settings.py`](src/naq/settings.py): Configuration constants.
  - [`src/naq/exceptions.py`](src/naq/exceptions.py): Custom exceptions.
  - [`src/naq/utils.py`](src/naq/utils.py): Logging setup.
  - Interacts indirectly with [`src/naq/queue.py`](src/naq/queue.py) and [`src/naq/job.py`](src/naq/job.py) by enqueuing serialized jobs for worker consumption.

## 5. Notable Implementation Details

- **NATS KV for State:**  
  Uses two NATS KV buckets: one for scheduled jobs, one for leader election.
- **High Availability:**  
  Only the elected leader processes jobs, preventing duplicate scheduling in clustered deployments.
- **Graceful Croniter Handling:**  
  If `croniter` is not installed, cron jobs are skipped with a warning.
- **Signal Handling:**  
  Handles `SIGINT` and `SIGTERM` for clean shutdown.
- **Instance ID:**  
  Each scheduler instance generates a unique ID for lock ownership.
- **Job Rescheduling:**  
  Recurring jobs are rescheduled by updating their KV entry; one-off jobs are deleted after execution.

## 6. Mermaid Diagram

```mermaid
classDiagram
    class Scheduler {
        +instance_id: str
        +enable_ha: bool
        +_leader_election: LeaderElection
        +_job_processor: ScheduledJobProcessor
        +run()
        +is_leader(): bool
        +_connect()
        +_close()
    }

    class LeaderElection {
        +instance_id: str
        +lock_ttl: int
        +is_leader: bool
        +initialize(js: JetStreamContext)
        +try_become_leader(): bool
        +start_renewal_task()
        +release_lock()
    }

    class ScheduledJobProcessor {
        -_js: JetStreamContext
        -_kv: KeyValue
        +process_jobs(is_leader: bool): tuple
        -_enqueue_job(queue_name, subject, payload): bool
        -_calculate_next_runtime(schedule_data, scheduled_ts): float
        -_process_single_job(key_bytes, now_ts): tuple
    }

    class NatsKVStore {
        <<NATS Resource>>
        +bucket_name: str
        +get(key): Entry
        +put(key, value)
        +delete(key)
        +keys()
    }

    class NatsJetStream {
        <<NATS Resource>>
        +publish(subject, payload)
    }

    Scheduler "1" *-- "1" LeaderElection : uses
    Scheduler "1" *-- "1" ScheduledJobProcessor : uses
    LeaderElection ..> NatsKVStore : uses (for lock)
    ScheduledJobProcessor ..> NatsKVStore : uses (for job definitions)
    ScheduledJobProcessor ..> NatsJetStream : publishes to (queues)

    Scheduler ..> naq.connection : uses
    Scheduler ..> naq.settings : uses
    ScheduledJobProcessor ..> cloudpickle : uses for serialization
    LeaderElection ..> cloudpickle : uses for serialization
    ScheduledJobProcessor ..> croniter : uses (optional)

    note for NatsKVStore "Represents `SCHEDULER_LOCK_KV_NAME` and `SCHEDULED_JOBS_KV_NAME`"