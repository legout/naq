"""Asynchronous queue API functions.

This module contains high-level asynchronous API functions for queue operations.
These functions provide convenient access to queue functionality without requiring
direct instantiation of the Queue class.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

from ..models.jobs import Job, RetryDelayType
from .core import Queue
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from ..utils.decorators import retry
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter


async def _execute_queue_operation(
    operation_name: str,
    queue_name: str,
    nats_url: str,
    func: Callable,
    operation: Callable,
    prefer_thread_local: bool = False,
    logger_name: str = "naq.queue.async_api",
    **context_args: Any,
) -> Job:
    """Helper to execute queue operations with proper error handling and logging."""
    structured_logger = StructuredLogger(logger_name)

    # Add function name to context
    context_args["func_name"] = getattr(func, "__name__", str(func))

    with structured_logger.operation_context(
        operation_name,
        queue_name=queue_name,
        nats_url=nats_url,
        **context_args,
    ):
        try:
            q = Queue(
                name=queue_name,
                nats_url=nats_url,
                prefer_thread_local=prefer_thread_local,
            )
            return await operation(q)
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(e, context=f"{operation_name} operation")
            error_handler.handle_error(
                wrapped_error,
                context={
                    "queue_name": queue_name,
                    "function": getattr(func, "__name__", str(func)),
                },
            )
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
    return await _execute_queue_operation(
        operation_name="enqueue_job",
        queue_name=queue_name,
        nats_url=nats_url,
        func=func,
        operation=lambda q: q.enqueue(
            func,
            *args,
            max_retries=max_retries,
            retry_delay=retry_delay,
            depends_on=depends_on,
            timeout=timeout,
            **kwargs,
        ),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.enqueue",
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
    validate_parameter(dt, "dt", not_none=True)

    return await _execute_queue_operation(
        operation_name="enqueue_at",
        queue_name=queue_name,
        nats_url=nats_url,
        func=func,
        operation=lambda q: q.enqueue_at(
            dt,
            func,
            *args,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            **kwargs,
        ),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.enqueue_at",
        scheduled_time=dt.isoformat(),
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)
    validate_parameter(delta, "delta", not_none=True, min_value=0)

    return await _execute_queue_operation(
        operation_name="enqueue_in",
        queue_name=queue_name,
        nats_url=nats_url,
        func=func,
        operation=lambda q: q.enqueue_in(
            delta,
            func,
            *args,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            **kwargs,
        ),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.enqueue_in",
        delay_seconds=delta.total_seconds(),
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
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
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    interval_seconds = (
        interval.total_seconds() if isinstance(interval, timedelta) else interval
    )

    return await _execute_queue_operation(
        operation_name="schedule_job",
        queue_name=queue_name,
        nats_url=nats_url,
        func=func,
        operation=lambda q: q.schedule(
            func,
            *args,
            cron=cron,
            interval=interval,
            repeat=repeat,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            **kwargs,
        ),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.schedule",
        cron=cron,
        interval_seconds=interval_seconds,
        repeat=repeat,
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def purge_queue(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
) -> int:
    """Helper to purge jobs from a specific queue (async)."""
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    return await _execute_queue_operation(
        operation_name="purge_queue",
        queue_name=queue_name,
        nats_url=nats_url,
        func=None,
        operation=lambda q: q.purge(),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.purge_queue",
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def cancel_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
) -> bool:
    """Helper to cancel a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    return await _execute_queue_operation(
        operation_name="cancel_scheduled_job",
        queue_name="",  # Queue name doesn't matter here
        nats_url=nats_url,
        func=None,
        operation=lambda q: q.cancel_scheduled_job(job_id),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.cancel_scheduled_job",
        job_id=job_id,
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def pause_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
) -> bool:
    """Helper to pause a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    return await _execute_queue_operation(
        operation_name="pause_scheduled_job",
        queue_name="",  # Queue name doesn't matter here
        nats_url=nats_url,
        func=None,
        operation=lambda q: q.pause_scheduled_job(job_id),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.pause_scheduled_job",
        job_id=job_id,
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def resume_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
) -> bool:
    """Helper to resume a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    return await _execute_queue_operation(
        operation_name="resume_scheduled_job",
        queue_name="",  # Queue name doesn't matter here
        nats_url=nats_url,
        func=None,
        operation=lambda q: q.resume_scheduled_job(job_id),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.resume_scheduled_job",
        job_id=job_id,
    )


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def modify_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    **updates: Any,
) -> bool:
    """Helper to modify a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    return await _execute_queue_operation(
        operation_name="modify_scheduled_job",
        queue_name="",  # Queue name doesn't matter here
        nats_url=nats_url,
        func=None,
        operation=lambda q: q.modify_scheduled_job(job_id, **updates),
        prefer_thread_local=prefer_thread_local,
        logger_name="naq.queue.async_api.modify_scheduled_job",
        job_id=job_id,
        update_keys=list(updates.keys()),
    )
