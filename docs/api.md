# naq - NATS Asynchronous Queue

## Overview

naq is a simple, asynchronous job queueing library for Python, built on top of NATS.io and its JetStream persistence layer. It provides a similar API to RQ (Redis Queue) but uses NATS for message queuing and result storage.

## ⚠️ Critical Security Warning

**IMPORTANT**: NAQ uses Python's `pickle` serialization by default, which poses a serious security vulnerability. Malicious job data can execute arbitrary code on worker systems. 

**Strongly recommended**: Use the secure JSON serializer by setting:
```bash
export NAQ_JOB_SERIALIZER=json
```

See [Security Documentation](security.md) for detailed information and migration guide.

## Core Components

The naq library is structured around the following core components:

*   **connection**: Manages the NATS connection and JetStream context.
*   **exceptions**: Defines custom exception classes for handling various error scenarios.
*   **job**: Represents a job to be executed, including the function, arguments, and metadata.
*   **queue**: Represents a job queue, handling job enqueuing, scheduling, and purging.
*   **scheduler**: Polls the scheduled jobs KV store and enqueues jobs that are ready. Supports high availability through leader election.
*   **settings**: Defines various settings and constants used throughout the library.
*   **worker**: A worker process that fetches jobs from queues and executes them.
*   **cli**: Provides a command-line interface for interacting with the library, including starting workers, purging queues, managing the scheduler, and listing scheduled jobs.

## Key Classes and Functions

The `naq` package exposes the following key classes and functions for use:

*   `Queue`: The main class for interacting with queues.
*   `Job`: Represents a job to be executed.
*   `Scheduler`: Manages scheduled jobs.
*   `Worker`: Executes jobs from queues.
*   `connect()`: Establishes a default NATS connection.
*   `disconnect()`: Closes the default NATS connection.
*   `fetch_job_result(job_id)`: Fetches the result or error information for a completed job.
*   `fetch_job_result_sync(job_id)`: Synchronous version of `fetch_job_result`.
*   `list_workers()`: Lists active workers.
*   `list_workers_sync()`: Synchronous version of `list_workers`.
*   `enqueue()`: Enqueues a job onto a specific queue (async).
*   `enqueue_at()`: Schedules a job for a specific time (async).
*   `enqueue_in()`: Schedules a job after a delay (async).
*   `schedule()`: Schedules a recurring job (async).
*   `purge_queue()`: Purges jobs from a specific queue (async).
*   `cancel_scheduled_job()`: Cancels a scheduled job (async).
*   `pause_scheduled_job()`: Pauses a scheduled job (async).
*   `resume_scheduled_job()`: Resumes a scheduled job (async).
*   `modify_scheduled_job()`: Modifies a scheduled job (async).
*   `enqueue_sync()`: Enqueues a job onto a specific queue (synchronous).
*   `enqueue_at_sync()`: Schedules a job for a specific time (sync).
*   `enqueue_in_sync()`: Schedules a job after a delay (sync).
*   `schedule_sync()`: Schedules a recurring job (sync).
*   `purge_queue_sync()`: Purges jobs from a specific queue (synchronous).
*   `cancel_scheduled_job_sync()`: Cancels a scheduled job (sync).
*   `pause_scheduled_job_sync()`: Pauses a scheduled job (sync).
*   `resume_scheduled_job_sync()`: Resumes a scheduled job (sync).
*   `modify_scheduled_job_sync()`: Modifies a scheduled job (sync).

## Queue Class

The `Queue` class represents a job queue backed by a NATS JetStream stream.

### `__init__(self, name=DEFAULT_QUEUE_NAME, nats_url=None, default_timeout=None)`

Initializes a `Queue` instance.

*   `name`: The name of the queue (defaults to `DEFAULT_QUEUE_NAME`).
*   `nats_url`: Optional NATS server URL.
*   `default_timeout`: Placeholder for job timeout.

### `enqueue(self, func, *args, max_retries=0, retry_delay=0, depends_on=None, **kwargs) -> Job`

Enqueues a job onto the queue.

*   `func`: The function to execute.
*   `*args`: Positional arguments for the function.
*   `max_retries`: Maximum number of retries allowed.
*   `retry_delay`: Delay between retries (seconds).
*   `depends_on`: A job ID, Job instance, or list of IDs/instances this job depends on.
*   `**kwargs`: Keyword arguments for the function.
*   Returns: The enqueued `Job` instance.

### `enqueue_at(self, dt, func, *args, max_retries=0, retry_delay=0, **kwargs) -> Job`

Schedules a job to be enqueued at a specific datetime.

*   `dt`: The datetime (UTC recommended) when the job should be enqueued.
*   `func`: The function to execute.
*   `*args`: Positional arguments for the function.
*   `max_retries`: Maximum number of retries allowed.
*   `retry_delay`: Delay between retries (seconds).
*   `**kwargs`: Keyword arguments for the function.
*   Returns: The scheduled `Job` instance.

### `enqueue_in(self, delta, func, *args, max_retries=0, retry_delay=0, **kwargs) -> Job`

Schedules a job to be enqueued after a specific time delta.

*   `delta`: The timedelta after which the job should be enqueued.
*   `func`: The function to execute.
*   `*args`: Positional arguments for the function.
*   `max_retries`: Maximum number of retries allowed.
*   `retry_delay`: Delay between retries (seconds).
*   `**kwargs`: Keyword arguments for the function.
*   Returns: The scheduled `Job` instance.

### `schedule(self, func, *args, cron=None, interval=None, repeat=None, max_retries=0, retry_delay=0, **kwargs) -> Job`

Schedules a job to run repeatedly based on cron or interval.

*   `func`: The function to execute.
*   `*args`: Positional arguments for the function.
*   `cron`: A cron string (e.g., '*/5 * * * *') defining the schedule.
*   `interval`: A timedelta or seconds defining the interval between runs.
*   `repeat`: Number of times to repeat (None for indefinitely).
*   `max_retries`: Max retries for each job execution.
*   `retry_delay`: Delay between execution retries.
*   `**kwargs`: Keyword arguments for the function.
*   Returns: The scheduled `Job` instance (representing the first scheduled run).

### `purge(self) -> int`

Removes all jobs from this queue by purging messages with the queue's subject from the underlying JetStream stream.

*   Returns: The number of purged messages.
*   Raises: `NaqConnectionError`, `NaqException`.

### `cancel_scheduled_job(self, job_id: str) -> bool`

Cancels a scheduled job by deleting it from the KV store.

*   `job_id`: The ID of the job to cancel.
*   Returns: `True` if the job was found and deleted, `False` otherwise.
*   Raises: `NaqConnectionError`, `NaqException`.

### `pause_scheduled_job(self, job_id: str) -> bool`

Pauses a scheduled job.

*   `job_id`: The ID of the job to pause.
*   Returns: `True` if the job was paused, `False` otherwise.
*   Raises: `NaqConnectionError`, `NaqException`.

### `resume_scheduled_job(self, job_id: str) -> bool`

Resumes a paused scheduled job.

*   `job_id`: The ID of the job to resume.
*   Returns: `True` if the job was resumed, `False` otherwise.
*   Raises: `NaqConnectionError`, `NaqException`.

### `modify_scheduled_job(self, job_id: str, **updates: Any) -> bool`

Modifies parameters of a scheduled job (e.g., cron, interval, repeat).

*   `job_id`: The ID of the job to modify.
*   `**updates`: Keyword arguments for parameters to update. Supported: 'cron', 'interval', 'repeat', 'scheduled_timestamp_utc'.
*   Returns: `True` if modification was successful.
*   Raises: `JobNotFoundError`, `ConfigurationError`, `NaqException`.

### `__repr__(self) -> str`

Returns a string representation of the `Queue` instance.

## Scheduler Class

The `Scheduler` class is responsible for polling the scheduled jobs KV store and enqueuing jobs that are ready to run. It also supports high availability through leader election using the NATS KV store.

### `__init__(self, nats_url=DEFAULT_NATS_URL, poll_interval=1.0, instance_id=None, enable_ha=True)`

Initializes a `Scheduler` instance.

*   `nats_url`: NATS server URL.
*   `poll_interval`: Interval in seconds between checks for due jobs (defaults to 1.0).
*   `instance_id`: Optional unique ID for this scheduler instance (for high availability).
*   `enable_ha`: Whether to enable high availability leader election (defaults to True).

### `run(self)`

Starts the scheduler loop with leader election.

## Worker Class

The `Worker` class is responsible for fetching jobs from specified NATS queues (subjects) and executing them. It uses JetStream pull consumers for fetching jobs. Handles retries, dependencies, and reports its status via heartbeats.

### `__init__(self, queues, nats_url=None, concurrency=10, worker_name=None, heartbeat_interval=DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS, worker_ttl=DEFAULT_WORKER_TTL_SECONDS)`

Initializes a `Worker` instance.

*   `queues`: A sequence of queue names or a single queue name.
*   `nats_url`: Optional NATS server URL.
*   `concurrency`: Maximum number of concurrent jobs (defaults to 10).
*   `worker_name`: Optional name for this worker instance (for durable consumer names).
*   `heartbeat_interval`: Heartbeat interval in seconds (defaults to `DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS`).
*   `worker_ttl`: Worker TTL in seconds (defaults to `DEFAULT_WORKER_TTL_SECONDS`).

### `run(self)`

Starts the worker, connects to NATS, and begins processing jobs.

## Settings and Constants

The following settings and constants are used throughout the `naq` library:

*   `DEFAULT_NATS_URL`: The default NATS server URL (`nats://localhost:4222`).
*   `DEFAULT_QUEUE_NAME`: The default queue name (`naq_default_queue`).
*   `NAQ_PREFIX`: The prefix for NATS subjects/streams (`naq`).
*   `JOB_SERIALIZER`: How jobs are serialized (`pickle` or `json`, defaults to `pickle`).
*   `SCHEDULED_JOBS_KV_NAME`: KV bucket name for scheduled jobs (`naq_scheduled_jobs`).
*   `SCHEDULER_LOCK_KV_NAME`: KV bucket name for scheduler leader election lock (`naq_scheduler_lock`).
*   `SCHEDULER_LOCK_KEY`: Key used within the lock KV store (`leader_lock`).
*   `SCHEDULER_LOCK_TTL_SECONDS`: TTL (in seconds) for the leader lock (defaults to 30).
*   `SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS`: How often the leader tries to renew the lock (defaults to 15).
*   `MAX_SCHEDULE_FAILURES`: Maximum number of times the scheduler will try to enqueue a job before marking it as failed (defaults to 5).
*   `JOB_STATUS_KV_NAME`: KV bucket name for tracking job completion status (for dependencies) (`naq_job_status`).
*   `JOB_STATUS`: Status value for job status entries (`pending`, `running`, `completed`, `failed`, `scheduled`, `retry`, `paused`, `cancelled`).
*   `JOB_STATUS_TTL_SECONDS`: TTL for job status entries (defaults to 86400 seconds, or 1 day).
*   `SCHEDULED_JOB_STATUS`: Status value for scheduled jobs (`active`, `paused`, `failed`).
*   `FAILED_JOB_SUBJECT_PREFIX`: Prefix for the subject of failed jobs (`naq.failed`).
*   `FAILED_JOB_STREAM_NAME`: Stream name for failed jobs (`naq_failed_jobs`).
*   `RESULT_KV_NAME`: KV bucket name for storing job results/errors (`naq_results`).
*   `DEFAULT_RESULT_TTL_SECONDS`: Default TTL (in seconds) for job results (defaults to 604800, or 7 days).
*   `WORKER_KV_NAME`: KV bucket name for storing worker status and heartbeats (`naq_workers`).
*   `DEFAULT_WORKER_TTL_SECONDS`: Default TTL (in seconds) for worker heartbeat entries (defaults to 60).
*   `DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS`: Default interval (in seconds) for worker heartbeats (defaults to 15).
*   `WORKER_STATUS_STARTING`: Worker status: starting.
*   `WORKER_STATUS_IDLE`: Worker status: idle.
*   `WORKER_STATUS_BUSY`: Worker status: busy.
*   `WORKER_STATUS_STOPPING`: Worker status: stopping.
*   `DEPENDENCY_CHECK_DELAY_SECONDS`: Delay in seconds before retrying a job if dependencies are not met (defaults to 5).

## Utility Functions

The `utils.py` file contains utility functions used throughout the library.

### `run_async_from_sync(coro)`

Runs an async coroutine from a synchronous context. Handles event loop management.

## CLI Commands

The `naq` CLI provides the following commands:

### `worker`

Starts a naq worker process to listen for and execute jobs on the specified queues.

*   `queues`: The names of the queues to listen to (required).
*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--concurrency`, `-c`: Maximum number of concurrent jobs to process (optional, defaults to 10).
*   `--name`, `-n`: Optional name for this worker instance.
*   `--log-level`, `-l`: Set logging level (optional, defaults to `INFO`).

### `purge`

Removes all jobs from the specified queues.

*   `queues`: The names of the queues to purge (required).
*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--log-level`, `-l`: Set logging level (optional, defaults to `WARNING`).

### `scheduler`

Starts a naq scheduler process to execute scheduled jobs at their specified times.

*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--poll-interval`, `-p`: Interval in seconds between checks for due jobs (optional, defaults to 1.0).
*   `--instance-id`, `-i`: Optional unique ID for this scheduler instance (for high availability).
*   `--disable-ha`: Disable high availability mode (leader election) (optional, defaults to False).
*   `--log-level`, `-l`: Set logging level (optional, defaults to `INFO`).

### `list-scheduled`

Lists all scheduled jobs with their status and next run time.

*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--status`, `-s`: Filter by job status (`SCHEDULED_JOB_STATUS_ACTIVE`, `SCHEDULED_JOB_STATUS_PAUSED`, or `SCHEDULED_JOB_STATUS_FAILED`) (optional).
*   `--job-id`, `-j`: Filter by job ID (optional).
*   `--queue`, `-q`: Filter by queue name (optional).
*   `--detailed`, `-d`: Show detailed job information (optional).
*   `--log-level`, `-l`: Set logging level (optional, defaults to `WARNING`).

### `job-control`

Controls scheduled jobs: cancel, pause, resume, or modify scheduling parameters.

*   `job_id`: The ID of the scheduled job to control (required).
*   `action`: Action to perform: 'cancel', 'pause', 'resume', or 'reschedule' (required).
*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--cron`: New cron expression for reschedule action (optional).
*   `--interval`: New interval in seconds for reschedule action (optional).
*   `--repeat`: New repeat count for reschedule action (optional).
*   `--next-run`: Next run time (ISO format, e.g. '2023-01-01T12:00:00Z') for reschedule action (optional).
*   `--log-level`, `-l`: Set logging level (optional, defaults to `INFO`).

### `list-workers`

Lists all currently active workers registered in the system.

*   `--nats-url`, `-u`: URL of the NATS server (optional, defaults to `DEFAULT_NATS_URL`).
*   `--log-level`, `-l`: Set logging level (optional, defaults to `WARNING`).

### `dashboard`

Starts the NAQ web dashboard (requires 'dashboard' extras).

*   `--host`, `-h`: Host to bind the dashboard server to (optional, defaults to `127.0.0.1`).
*   `--port`, `-p`: Port to run the dashboard server on (optional, defaults to 8080).
*   `--log-level`, `-l`: Set logging level for the dashboard server (optional, defaults to `INFO`).