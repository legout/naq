# Task 1: Define Core Data Models

## Objective

Centralize and define all core data models for the `naq` system using `msgspec.Struct` for high-performance serialization and type safety. This includes models for events, job results, and schedules.

## Requirements

1.  **Create a Central Model File:**
    *   Create a new file `src/naq/models.py`.
    *   This file will house all core data structures (`Job`, `JobEvent`, `JobResult`, `Schedule`, etc.).

2.  **Refactor Existing Models:**
    *   Move the `Job` class definition from `src/naq/job.py` to `src/naq/models.py`.
    *   Move the `JOB_STATUS` enum from `src/naq/job.py` to `src/naq/models.py`.
    *   Update all necessary imports across the codebase (`job.py`, `queue.py`, `worker.py`, etc.) to point to the new location in `naq.models`.

3.  **Define New `msgspec.Struct` Models in `src/naq/models.py`:**

    *   **`JobEventType(str, Enum)`:**
        *   Define as previously planned, with all relevant lifecycle events (`ENQUEUED`, `STARTED`, `COMPLETED`, `FAILED`, `RETRY_SCHEDULED`, `SCHEDULED`, `SCHEDULE_TRIGGERED`).

    *   **`JobEvent(msgspec.Struct)`:**
        *   Define as previously planned, ensuring it uses `msgspec.Struct`.
        *   Include fields: `job_id`, `event_type`, `timestamp`, `worker_id`, `queue_name`, `message`, `details`, `error_type`, `error_message`, `duration_ms`, `nats_subject`, `nats_sequence`.
        *   Include the convenient classmethods (`.enqueued()`, `.started()`, etc.).

    *   **`JobResult(msgspec.Struct)`:**
        *   This will replace the dictionary currently used in `results.py`.
        *   Fields: `job_id: str`, `status: str`, `result: Any`, `error: Optional[str]`, `traceback: Optional[str]`, `start_time: float`, `finish_time: float`.

    *   **`Schedule(msgspec.Struct)`:**
        *   This will replace the dictionary used for scheduled jobs in the NATS KV store.
        *   Fields: `job_id: str`, `cron: Optional[str]`, `interval_seconds: Optional[float]`, `repeat: Optional[int]`, `scheduled_timestamp_utc: float`, `status: str`, `last_enqueued_utc: Optional[float]`, `schedule_failure_count: int`, `_orig_job_payload: bytes`.

4.  **Update Task Dependencies:**
    *   Acknowledge that other tasks (especially Task 2, 4, and 5) will now depend on these new models from `src/naq/models.py`. The integration part of those tasks will involve replacing dictionary-based logic with these new typed structs.