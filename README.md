# NAQ - NATS Asynchronous Queue

[![PyPI version](https://badge.fury.io/py/naq.svg)](https://badge.fury.io/py/naq) <!-- Placeholder - Add actual badge once published -->
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/legout/naq)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/legout/naq/blob/main/LICENSE)
[![Python version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/release/python-3120/)


**NAQ** is a simple, asynchronous job queueing library for Python, inspired by [RQ (Redis Queue)](https://python-rq.org/), but built entirely on top of [NATS](https://nats.io/) and its JetStream persistence layer.

Think of it as **N**ATS **A**synchronous **Q**ueue - your simple way of *Naqin' on NATS's Door* for background job processing.

It allows you to easily enqueue Python functions to be executed asynchronously by worker processes, leveraging the power and resilience of NATS JetStream for message persistence and delivery.

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

## ⚠️ Security Warning

**CRITICAL**: NAQ uses Python's `pickle` serialization by default, which allows arbitrary code execution and poses a serious security risk. Malicious job data can execute arbitrary commands on worker machines.

**Recommended**: Switch to the secure JSON serializer:
```bash
export NAQ_JOB_SERIALIZER=json
```

For more details, see [Security Documentation](docs/security.md).


