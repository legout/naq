"""Synchronous queue API functions.

This module contains high-level synchronous API functions for queue operations.
These functions provide convenient access to queue functionality without requiring
direct instantiation of the Queue class.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

from ..connection import (
    close_nats_connection,
)
from ..models.jobs import Job, RetryDelayType
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from ..utils import run_async_from_sync
from .async_api import (
    enqueue,
    enqueue_at,
    enqueue_in,
    schedule,
    purge_queue,
    cancel_scheduled_job,
    pause_scheduled_job,
    resume_scheduled_job,
    modify_scheduled_job,
)


# --- Sync Helper Functions ---


def enqueue_sync(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to enqueue a job onto a specific queue (synchronous).

    Performance and connection reuse:
      - This sync wrapper reuses a thread-local NATS connection and JetStream context
        by calling the async path with prefer_thread_local=True.
      - Reuse avoids connect/close per call and significantly improves throughput in
        batch-style producers that call enqueue_sync repeatedly from the same thread.

    When to use:
      - Use enqueue_sync for simple synchronous producers or CLI tools.
      - For tight loops and high throughput, consider either:
          a) Repeatedly calling enqueue_sync (it reuses TLS connection automatically), or
          b) Managing a Queue instance asynchronously in your own event loop for maximal control.

    Explicit cleanup:
      - Thread-local connections can be explicitly closed when a batch is completed:
            from naq.queue.sync_api import close_sync_connections
            close_sync_connections()
        This is optional; the connection is also cleaned up on process exit.

    Equivalent async batching (for reference):
        async def produce(url):
            q = Queue(nats_url=url, prefer_thread_local=False)
            for i in range(1000):
                await q.enqueue(my_func, i)
            await q.close()

    """

    async def _main():
        job = await enqueue(
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            depends_on=depends_on,
            timeout=timeout,
            prefer_thread_local=True,
            **kwargs,
        )
        # Do not close thread-local connection here; allow reuse across sync calls.
        return job

    return run_async_from_sync(_main)


def enqueue_at_sync(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job for a specific time (sync).

    This sync wrapper reuses a thread-local NATS connection/JetStream context to
    avoid per-call connect/close. See enqueue_sync() docstring for details on
    performance characteristics and explicit cleanup via close_sync_connections().
    """

    async def _main():
        job = await enqueue_at(
            dt,
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            prefer_thread_local=True,
            **kwargs,
        )
        return job

    return run_async_from_sync(_main)


def enqueue_in_sync(
    delta: timedelta,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job after a delay (sync).

    Uses a thread-local NATS connection for efficient repeated calls from the
    same thread. See enqueue_sync() for batching guidance and cleanup options.
    """

    async def _main():
        job = await enqueue_in(
            delta,
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            prefer_thread_local=True,
            **kwargs,
        )
        return job

    return run_async_from_sync(_main)


def schedule_sync(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    cron: Optional[str] = None,
    interval: Optional[Union[timedelta, float, int]] = None,
    repeat: Optional[int] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a recurring job (sync).

    Reuses a thread-local NATS connection/JetStream context to minimize overhead
    in synchronous producers. Refer to enqueue_sync() for full guidance on reuse,
    batching patterns, and explicit cleanup.
    """

    async def _main():
        job = await schedule(
            func,
            *args,
            queue_name=queue_name,
            nats_url=nats_url,
            cron=cron,
            interval=interval,
            repeat=repeat,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            prefer_thread_local=True,
            **kwargs,
        )
        return job

    return run_async_from_sync(_main)


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
) -> int:
    """
    Helper to purge jobs from a specific queue (synchronous).

    Uses thread-local connection reuse to avoid repeated connect/close costs.
    """

    async def _main():
        count = await purge_queue(
            queue_name=queue_name, nats_url=nats_url, prefer_thread_local=True
        )
        return count

    return run_async_from_sync(_main)


def cancel_scheduled_job_sync(job_id: str, nats_url: str = DEFAULT_NATS_URL) -> bool:
    """
    Helper to cancel a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """

    async def _main():
        res = await cancel_scheduled_job(
            job_id, nats_url=nats_url, prefer_thread_local=True
        )
        return res

    return run_async_from_sync(_main)


def pause_scheduled_job_sync(job_id: str, nats_url: str = DEFAULT_NATS_URL) -> bool:
    """
    Helper to pause a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """

    async def _main():
        res = await pause_scheduled_job(
            job_id, nats_url=nats_url, prefer_thread_local=True
        )
        return res

    return run_async_from_sync(_main)


def resume_scheduled_job_sync(job_id: str, nats_url: str = DEFAULT_NATS_URL) -> bool:
    """
    Helper to resume a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """

    async def _main():
        res = await resume_scheduled_job(
            job_id, nats_url=nats_url, prefer_thread_local=True
        )
        return res

    return run_async_from_sync(_main)


def modify_scheduled_job_sync(
    job_id: str, nats_url: str = DEFAULT_NATS_URL, **updates: Any
) -> bool:
    """
    Helper to modify a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """

    async def _main():
        res = await modify_scheduled_job(
            job_id, nats_url=nats_url, prefer_thread_local=True, **updates
        )
        return res

    return run_async_from_sync(_main)


# Optional: public function to explicitly close thread-local connection for sync batches
def close_sync_connections(nats_url: str = DEFAULT_NATS_URL) -> None:
    """
    Close thread-local NATS connection/JS context used by sync helpers.

    Use this to explicitly end a synchronous batch when you know no further
    enqueue_sync (or other sync helpers) will be called from the current thread.
    This can release the connection resources earlier than process exit.
    """

    async def _main():
        await close_nats_connection(url=nats_url, thread_local=True)

    return run_async_from_sync(_main)
