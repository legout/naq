"""Synchronous queue API functions.

This module contains high-level synchronous API functions for queue operations.
These functions provide convenient access to queue functionality without requiring
direct instantiation of the Queue class.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

import anyio

from ..models.jobs import Job, RetryDelayType
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from ..utils.error_handling import wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter
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

# Create a structured logger for sync operations
_sync_logger = StructuredLogger(__name__)


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

    This function provides a synchronous wrapper around the async enqueue operation.

    When to use:
      - Use enqueue_sync for simple synchronous producers or CLI tools.
      - For tight loops and high throughput, consider either:
          a) Repeatedly calling enqueue_sync, or
          b) Managing a Queue instance asynchronously in your own event loop for maximal control.
    """
    with _sync_logger.operation_context(
        "enqueue_sync",
        queue_name=queue_name,
        nats_url=nats_url,
        func_name=getattr(func, "__name__", str(func)),
        max_retries=max_retries,
        timeout=timeout,
    ):
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(
            message="enqueue_sync_start",
            queue_name=queue_name,
            func_name=getattr(func, "__name__", str(func)),
        )

        try:
            job = anyio.run(
                enqueue,
                func,
                *args,
                queue_name=queue_name,
                nats_url=nats_url,
                max_retries=max_retries,
                retry_delay=retry_delay,
                depends_on=depends_on,
                timeout=timeout,
                **kwargs,
            )
            _sync_logger.info(
                "enqueue_sync_success",
                queue_name=queue_name,
                job_id=job.job_id,
                func_name=getattr(func, "__name__", str(func)),
            )
            return job
        except Exception as e:
            _sync_logger.error(
                "enqueue_sync_failed",
                queue_name=queue_name,
                func_name=getattr(func, "__name__", str(func)),
                error=str(e),
            )
            raise wrap_naq_exception(e, f"Failed to enqueue job synchronously: {e}")


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
    """
    with _sync_logger.operation_context(
        "enqueue_at_sync",
        queue_name=queue_name,
        nats_url=nats_url,
        func_name=getattr(func, "__name__", str(func)),
        scheduled_time=dt.isoformat(),
        max_retries=max_retries,
        timeout=timeout,
    ):
        validate_parameter(dt, "dt", datetime.datetime)
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(
            message="enqueue_at_sync_start",
            queue_name=queue_name,
            func_name=getattr(func, "__name__", str(func)),
            scheduled_time=dt.isoformat(),
        )

        try:
            job = anyio.run(
                enqueue_at,
                dt,
                func,
                *args,
                queue_name=queue_name,
                nats_url=nats_url,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
                **kwargs,
            )
            _sync_logger.info(
                "enqueue_at_sync_success",
                queue_name=queue_name,
                job_id=job.job_id,
                func_name=getattr(func, "__name__", str(func)),
                scheduled_time=dt.isoformat(),
            )
            return job
        except Exception as e:
            _sync_logger.error(
                "enqueue_at_sync_failed",
                queue_name=queue_name,
                func_name=getattr(func, "__name__", str(func)),
                scheduled_time=dt.isoformat(),
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to enqueue job at specific time synchronously: {e}"
            )


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
    """
    with _sync_logger.operation_context(
        "enqueue_in_sync",
        queue_name=queue_name,
        nats_url=nats_url,
        func_name=getattr(func, "__name__", str(func)),
        delay_seconds=delta.total_seconds(),
        max_retries=max_retries,
        timeout=timeout,
    ):
        validate_parameter(delta, "delta", timedelta)
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(
            message="enqueue_in_sync_start",
            queue_name=queue_name,
            func_name=getattr(func, "__name__", str(func)),
            delay_seconds=delta.total_seconds(),
        )

        try:
            job = anyio.run(
                enqueue_in,
                delta,
                func,
                *args,
                queue_name=queue_name,
                nats_url=nats_url,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
                **kwargs,
            )
            _sync_logger.info(
                "enqueue_in_sync_success",
                queue_name=queue_name,
                job_id=job.job_id,
                func_name=getattr(func, "__name__", str(func)),
                delay_seconds=delta.total_seconds(),
            )
            return job
        except Exception as e:
            _sync_logger.error(
                "enqueue_in_sync_failed",
                queue_name=queue_name,
                func_name=getattr(func, "__name__", str(func)),
                delay_seconds=delta.total_seconds(),
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to enqueue job with delay synchronously: {e}"
            )


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
    """
    interval_seconds = (
        interval.total_seconds() if isinstance(interval, timedelta) else interval
    )

    with _sync_logger.operation_context(
        "schedule_sync",
        queue_name=queue_name,
        nats_url=nats_url,
        func_name=getattr(func, "__name__", str(func)),
        cron=cron,
        interval_seconds=interval_seconds,
        repeat=repeat,
        max_retries=max_retries,
        timeout=timeout,
    ):
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(
            message="schedule_sync_start",
            queue_name=queue_name,
            func_name=getattr(func, "__name__", str(func)),
            cron=cron,
            interval_seconds=interval_seconds,
            repeat=repeat,
        )

        try:
            job = anyio.run(
                schedule,
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
                **kwargs,
            )
            _sync_logger.info(
                "schedule_sync_success",
                queue_name=queue_name,
                job_id=job.job_id,
                func_name=getattr(func, "__name__", str(func)),
                cron=cron,
                interval_seconds=interval_seconds,
                repeat=repeat,
            )
            return job
        except Exception as e:
            _sync_logger.error(
                "schedule_sync_failed",
                queue_name=queue_name,
                func_name=getattr(func, "__name__", str(func)),
                cron=cron,
                interval_seconds=interval_seconds,
                repeat=repeat,
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to schedule recurring job synchronously: {e}"
            )


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
) -> int:
    """
    Helper to purge jobs from a specific queue (synchronous).
    """
    with _sync_logger.operation_context(
        "purge_queue_sync", queue_name=queue_name, nats_url=nats_url
    ):
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="purge_queue_sync_start", queue_name=queue_name)

        try:
            count = anyio.run(
                purge_queue,
                queue_name=queue_name,
                nats_url=nats_url,
            )
            _sync_logger.info(
                "purge_queue_sync_success",
                queue_name=queue_name,
                purged_count=count,
            )
            return count
        except Exception as e:
            _sync_logger.error(
                "purge_queue_sync_failed",
                queue_name=queue_name,
                error=str(e),
            )
            raise wrap_naq_exception(e, f"Failed to purge queue synchronously: {e}")


def cancel_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
) -> bool:
    """
    Helper to cancel a scheduled job (sync).
    """
    with _sync_logger.operation_context(
        "cancel_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="cancel_scheduled_job_sync_start", job_id=job_id)

        try:
            res = anyio.run(
                cancel_scheduled_job,
                job_id,
                nats_url=nats_url,
            )
            _sync_logger.info(
                "cancel_scheduled_job_sync_success",
                job_id=job_id,
                result=res,
            )
            return res
        except Exception as e:
            _sync_logger.error(
                "cancel_scheduled_job_sync_failed",
                job_id=job_id,
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to cancel scheduled job synchronously: {e}"
            )


def pause_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
) -> bool:
    """
    Helper to pause a scheduled job (sync).
    """
    with _sync_logger.operation_context(
        "pause_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="pause_scheduled_job_sync_start", job_id=job_id)

        try:
            res = anyio.run(
                pause_scheduled_job,
                job_id,
                nats_url=nats_url,
            )
            _sync_logger.info(
                "pause_scheduled_job_sync_success",
                job_id=job_id,
                result=res,
            )
            return res
        except Exception as e:
            _sync_logger.error(
                "pause_scheduled_job_sync_failed",
                job_id=job_id,
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to pause scheduled job synchronously: {e}"
            )


def resume_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
) -> bool:
    """
    Helper to resume a scheduled job (sync).
    """
    with _sync_logger.operation_context(
        "resume_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="resume_scheduled_job_sync_start", job_id=job_id)

        try:
            res = anyio.run(
                resume_scheduled_job,
                job_id,
                nats_url=nats_url,
            )
            _sync_logger.info(
                "resume_scheduled_job_sync_success",
                job_id=job_id,
                result=res,
            )
            return res
        except Exception as e:
            _sync_logger.error(
                "resume_scheduled_job_sync_failed",
                job_id=job_id,
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to resume scheduled job synchronously: {e}"
            )


def modify_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    **updates: Any,
) -> bool:
    """
    Helper to modify a scheduled job (sync).
    """
    with _sync_logger.operation_context(
        "modify_scheduled_job_sync",
        job_id=job_id,
        nats_url=nats_url,
        updates=list(updates.keys()),
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(
            message="modify_scheduled_job_sync_start",
            job_id=job_id,
            updates=list(updates.keys()),
        )

        try:
            res = anyio.run(
                modify_scheduled_job,
                job_id,
                nats_url=nats_url,
                **updates,
            )
            _sync_logger.info(
                "modify_scheduled_job_sync_success",
                job_id=job_id,
                result=res,
                updates=list(updates.keys()),
            )
            return res
        except Exception as e:
            _sync_logger.error(
                "modify_scheduled_job_sync_failed",
                job_id=job_id,
                updates=list(updates.keys()),
                error=str(e),
            )
            raise wrap_naq_exception(
                e, f"Failed to modify scheduled job synchronously: {e}"
            )


# Optional: public function to explicitly close thread-local connection for sync batches
def close_sync_connections(nats_url: str = DEFAULT_NATS_URL) -> None:
    """
    Close thread-local NATS connection/JS context used by sync helpers.

    This function is kept for backward compatibility. With anyio.run,
    connections are automatically managed and cleaned up when the function exits.
    """
    with _sync_logger.operation_context("close_sync_connections", nats_url=nats_url):
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="close_sync_connections_start")
        
        # With anyio.run, connections are automatically managed
        # This function is kept for backward compatibility
        _sync_logger.info("close_sync_connections_success")
