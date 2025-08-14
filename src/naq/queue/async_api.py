# src/naq/queue/async_api.py
"""
Async API functions for naq queues.

This module provides high-level asynchronous API functions for working with naq queues.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

from ..exceptions import NaqException
from ..models import Job, RetryDelayType
from ..settings import DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME
from ..utils import run_async_from_sync
from .core import Queue


async def enqueue(
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    timeout: Optional[int] = None,
    prefer_thread_local: bool = False,
    **kwargs: Any,
) -> Job:
    """Helper to enqueue a job onto a specific queue (async)."""
    q = Queue(
        name=queue_name, nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )
    job = await q.enqueue(
        func,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        depends_on=depends_on,
        timeout=timeout,
        **kwargs,
    )
    return job


async def enqueue_at(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    prefer_thread_local: bool = False,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job for a specific time (async)."""
    q = Queue(
        name=queue_name, nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )
    return await q.enqueue_at(
        dt,
        func,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        **kwargs,
    )


async def enqueue_in(
    delta: timedelta,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    prefer_thread_local: bool = False,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job after a delay (async)."""
    q = Queue(
        name=queue_name, nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )
    return await q.enqueue_in(
        delta,
        func,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        **kwargs,
    )


async def schedule(
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
    prefer_thread_local: bool = False,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a recurring job (async)."""
    q = Queue(
        name=queue_name, nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )
    return await q.schedule(
        func,
        *args,
        cron=cron,
        interval=interval,
        repeat=repeat,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        **kwargs,
    )


async def purge_queue(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
) -> int:
    """Helper to purge jobs from a specific queue (async)."""
    q = Queue(
        name=queue_name, nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )
    return await q.purge()


async def cancel_scheduled_job(
    job_id: str, nats_url: str = DEFAULT_NATS_URL, prefer_thread_local: bool = False
) -> bool:
    """Helper to cancel a scheduled job (async)."""
    q = Queue(
        nats_url=nats_url, prefer_thread_local=prefer_thread_local
    )  # Queue name doesn't matter here
    return await q.cancel_scheduled_job(job_id)


async def pause_scheduled_job(
    job_id: str, nats_url: str = DEFAULT_NATS_URL, prefer_thread_local: bool = False
) -> bool:
    """Helper to pause a scheduled job (async)."""
    q = Queue(nats_url=nats_url, prefer_thread_local=prefer_thread_local)
    return await q.pause_scheduled_job(job_id)


async def resume_scheduled_job(
    job_id: str, nats_url: str = DEFAULT_NATS_URL, prefer_thread_local: bool = False
) -> bool:
    """Helper to resume a scheduled job (async)."""
    q = Queue(nats_url=nats_url, prefer_thread_local=prefer_thread_local)
    return await q.resume_scheduled_job(job_id)


async def modify_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    **updates: Any,
) -> bool:
    """Helper to modify a scheduled job (async)."""
    q = Queue(nats_url=nats_url, prefer_thread_local=prefer_thread_local)
    return await q.modify_scheduled_job(job_id, **updates)