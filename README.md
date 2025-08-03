# NAQ - NATS Asynchronous Queue

[![PyPI version](https://badge.fury.io/py/naq.svg)](https://badge.fury.io/py/naq) <!-- Placeholder - Add actual badge once published -->
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/legout/naq)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/legout/naq/blob/main/LICENSE)
[![Python version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/release/python-3120/)


**NAQ** is a simple, asynchronous job queueing library for Python, inspired by [RQ (Redis Queue)](https://python-rq.org/), but built entirely on top of [NATS](https://nats.io/) and its JetStream persistence layer.

Think of it as **N**ATS **A**synchronous **Q**ueue - your simple way of *Naqin' on NATS's Door* for background job processing.

It allows you to easily enqueue Python functions to be executed asynchronously by worker processes, leveraging the power and resilience of NATS JetStream for message persistence and delivery.

## Security

**:warning: Important Security Notice :warning:**

By default, `naq` uses `cloudpickle` to serialize job data for maximum flexibility with Python objects. However, `cloudpickle` can execute arbitrary code and is **not secure** if the job producer cannot be trusted.

If you are accepting jobs from untrusted sources, **you must switch to the `JsonSerializer`**.

You can do this by setting the `NAQ_JOB_SERIALIZER` environment variable:
```bash
export NAQ_JOB_SERIALIZER=json
```

The `JsonSerializer` is safer as it only serializes data to and from basic JSON types and handles functions by referencing their import path, preventing arbitrary code execution. See the [`SECURITY.md`](SECURITY.md) file for more details.

## Timezone Handling

All internal scheduling and time handling within `naq` are based on UTC (Coordinated Universal Time). This is to ensure consistent and unambiguous behavior across different systems and timezones.

When scheduling jobs, it is highly recommended to use timezone-aware `datetime` objects, specifically those set to UTC.

### Scheduling with UTC

The best practice is to always use `datetime.datetime.now(datetime.timezone.utc)` for the current time or to create `datetime` objects with `tzinfo=datetime.timezone.utc`.

```python
# schedule_example_utc.py
import datetime
from naq import enqueue_at_sync, enqueue_in_sync
from tasks import count_words

# Get the current time in UTC
now_utc = datetime.datetime.now(datetime.timezone.utc)

# Schedule to run at a specific UTC time
run_at_utc = now_utc + datetime.timedelta(seconds=30)
job_at = enqueue_at_sync(run_at_utc, count_words, "Job scheduled with explicit UTC time.")
print(f"Job {job_at.job_id} scheduled for {run_at_utc} (UTC)")

# Scheduling with a timedelta is also implicitly UTC-based
# as the scheduler operates in UTC.
run_in_delta = datetime.timedelta(minutes=5)
job_in = enqueue_in_sync(run_in_delta, count_words, "Job scheduled with a delay from now (UTC).")
print(f"Job {job_in.job_id} scheduled to run in {run_in_delta}")
```

### Handling Timezone-Naive Datetimes

If you provide a timezone-naive `datetime` object (one without `tzinfo` set) to scheduling functions like `enqueue_at` or `enqueue_in`, `naq` will treat it as **UTC**.

**Warning:** Relying on timezone-naive datetimes can lead to unexpected behavior if your system or the environment where the scheduler/worker runs has a different local timezone, or if daylight saving time changes occur. It's always safer to be explicit.

```python
# schedule_example_naive.py
import datetime
from naq import enqueue_at_sync
from tasks import count_words

# This datetime is naive (no timezone info)
# naq will interpret this as UTC.
naive_run_time = datetime.datetime(2024, 12, 25, 10, 30, 0)

job_naive = enqueue_at_sync(naive_run_time, count_words, "Job scheduled with a naive datetime (treated as UTC).")
print(f"Job {job_naive.job_id} scheduled for {naive_run_time} (interpreted as UTC).")
```

### Best Practices

1.  **Always Use UTC for Scheduling:** When creating `datetime` objects for scheduling, always make them timezone-aware with UTC.
2.  **Convert Local Time to UTC:** If you have a local time that you want to schedule a job for, first convert it to UTC before passing it to `naq`.

    ```python
    import datetime
    from naq import enqueue_at_sync

    # Example: Scheduling for 9 AM Berlin time
    local_time_str = "2024-12-25 09:00:00"
    berlin_tz = datetime.timezone(datetime.timedelta(hours=1), name="CET") # CET is UTC+1 in winter

    # Parse the local time as timezone-aware
    local_dt = datetime.datetime.fromisoformat(local_time_str).replace(tzinfo=berlin_tz)

    # Convert to UTC before scheduling
    utc_dt = local_dt.astimezone(datetime.timezone.utc)

    # job = enqueue_at_sync(utc_dt, my_function, ...)
    print(f"Scheduled for {local_dt} (Berlin) which is {utc_dt} (UTC)")
    ```
3.  **Store and Display in Local Time (Optional):** If your application needs to display scheduled times to users in their local timezone, perform the conversion from UTC to the user's local timezone at the display layer, not during scheduling.

By following these guidelines, you can avoid common pitfalls related to timezones and ensure your jobs run exactly when you expect them to.

## Features

*   Simple API similar to RQ.
*   Asynchronous core using `asyncio` and `nats-py`.
*   Job persistence via NATS JetStream streams.
*   Support for scheduled jobs (run at a specific time or after a delay).
*   Support for recurring jobs (cron-style or interval-based).
*   Job dependencies (run a job only after others complete).
*   Job retries with configurable backoff.
*   Result backend using NATS KV store (with TTL).
*   Worker monitoring and heartbeating using NATS KV store.
*   High Availability for the scheduler process via leader election.
*   Optional web dashboard (requires `naq[dashboard]`).
*   Command-line interface (`naq`) for workers, scheduler, queue management, and dashboard.
## Installation

Install `naq` using pip:

```bash
pip install naq
```

To include the optional web dashboard dependencies (Sanic, Jinja2, Datastar):
```bash
pip install naq[dashboard]
```

You also need a running NATS server with JetStream enabled. You can easily start one using the provided Docker Compose file:
```bash
cd docker
docker-compose up -d
```

## Basic Usage
### 1. Define your function

```python
# tasks.py
import time

def count_words(text):
    print(f"Counting words in: '{text[:20]}...'")
    time.sleep(1) # Simulate work
    count = len(text.split())
    print(f"Word count: {count}")
    return count
```

### 2. Enqueue the job
```python
# main.py
from naq import enqueue_sync
from tasks import count_words

print("Enqueuing job...")
# Enqueue synchronously (blocks until published)
job = enqueue_sync(count_words, "This is a sample text with several words.")

print(f"Job {job.job_id} enqueued.")
print("Run 'naq worker default' to process it.")
```

### 3. Run the worker:

Open a terminal and run the `naq` worker, telling it which queue(s) to listen to (default is `naq_default_queue`, often shortened to `default` in examples):
```bash
naq worker default
```

The worker will pick up the job and execute the `count_words` function.

### 4. Scheduling Jobs:

Jobs can be scheduled to run later.

```python
# schedule_example.py
import datetime
from naq import enqueue_at_sync, enqueue_in_sync
from tasks import count_words

now = datetime.datetime.now(datetime.timezone.utc)
run_at = now + datetime.timedelta(seconds=10)
run_in = datetime.timedelta(minutes=1)

# Schedule to run at a specific time (UTC recommended)
job_at = enqueue_at_sync(run_at, count_words, "Job scheduled for a specific time.")
print(f"Job {job_at.job_id} scheduled for {run_at}")

# Schedule to run after a delay
job_in = enqueue_in_sync(run_in, count_words, "Job scheduled after a delay.")
print(f"Job {job_in.job_id} scheduled to run in {run_in}")

print("Run 'naq scheduler' and 'naq worker default' to process scheduled jobs.")
````

5. Run the Scheduler
For scheduled jobs (`enqueue_at`, `enqueue_in`, `schedule`), you also need to run the `naq` scheduler process:
```bash
naq scheduler
```
The scheduler monitors scheduled jobs and enqueues them onto the appropriate queue when they are due.

## Efficient connection handling and batching

Synchronous producers (CLI tools, scripts, web handlers) often enqueue many jobs in quick succession. Reconnecting to NATS for every call can severely degrade performance. naq provides optimized connection reuse for both async and sync paths.

- Async producers:
  - Reuse a Queue instance for batching:
    ```python
    import asyncio
    from naq.queue import Queue

    async def produce(url: str):
        q = Queue(nats_url=url)
        for i in range(1000):
            await q.enqueue(my_func, i)
        await q.close()
    ```
  - The async path uses a process-wide pooled connection per URL.

- Sync producers:
  - Use enqueue_sync for simple scripts. It reuses a thread-local connection in the calling thread automatically:
    ```python
    from naq.queue import enqueue_sync, close_sync_connections

    for i in range(1000):
        enqueue_sync(my_func, i)

    # Optionally close at the end of the batch
    close_sync_connections()
    ```
  - All other sync helpers (enqueue_at_sync, enqueue_in_sync, schedule_sync, purge_queue_sync, etc.) reuse the same thread-local connection for efficiency.

Trade-offs:
- Thread-local reuse provides excellent performance for repeated calls from the same thread.
- If you need maximal control or use multiple threads, manage Queue instances asynchronously and keep them alive across operations.

Cleanup:
- Thread-local connections are cleaned up on process exit.
- To end a batch sooner, call close_sync_connections() from the producing thread.


