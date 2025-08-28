"""Synchronous queue API functions.

This module contains high-level synchronous API functions for queue operations.
These functions provide convenient access to queue functionality without requiring
direct instantiation of the Queue class.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

from ..models.jobs import Job, RetryDelayType
from ..services.config import create_global_config, GlobalServiceConfig
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from ..utils.error_handling import wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter
from ..service_context import run_with_service_context
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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to enqueue a job onto a specific queue (synchronous).

    This function provides a synchronous wrapper around the async enqueue operation
    using the service context pattern for proper lifecycle management.

    When to use:
      - Use enqueue_sync for simple synchronous producers or CLI tools.
      - For tight loops and high throughput, consider either:
          a) Repeatedly calling enqueue_sync (it uses service context automatically), or
          b) Managing a Queue instance asynchronously in your own event loop for maximal control.

    Service management:
      - This sync wrapper uses the service context pattern to manage connections
        and services automatically, ensuring proper initialization and cleanup.
      - Services are created and managed by the ServiceManager, providing a
        consistent and reliable way to handle resources.
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

        async def _enqueue_with_services(service_manager):
            try:
                job = await enqueue(
                    func,
                    *args,
                    queue_name=queue_name,
                    nats_url=nats_url,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    depends_on=depends_on,
                    timeout=timeout,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                    **kwargs,
                )
                _sync_logger.info(
                    "enqueue_sync_success",
                    {
                        "queue_name": queue_name,
                        "job_id": job.job_id,
                        "func_name": getattr(func, "__name__", str(func)),
                    },
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

        return run_with_service_context(
            _enqueue_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.enqueue_sync",
        )


def enqueue_at_sync(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job for a specific time (sync).

    This sync wrapper uses the service context pattern to manage connections
    and services automatically, ensuring proper initialization and cleanup.
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

        async def _enqueue_at_with_services(service_manager):
            try:
                job = await enqueue_at(
                    dt,
                    func,
                    *args,
                    queue_name=queue_name,
                    nats_url=nats_url,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    timeout=timeout,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                    **kwargs,
                )
                _sync_logger.info(
                    "enqueue_at_sync_success",
                    {
                        "queue_name": queue_name,
                        "job_id": job.job_id,
                        "func_name": getattr(func, "__name__", str(func)),
                        "scheduled_time": dt.isoformat(),
                    },
                )
                return job
            except Exception as e:
                _sync_logger.error(
                    "enqueue_at_sync_failed",
                    {
                        "queue_name": queue_name,
                        "func_name": getattr(func, "__name__", str(func)),
                        "scheduled_time": dt.isoformat(),
                        "error": str(e),
                    },
                )
                raise wrap_naq_exception(
                    e, f"Failed to enqueue job at specific time synchronously: {e}"
                )

        return run_with_service_context(
            _enqueue_at_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.enqueue_at_sync",
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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job after a delay (sync).

    Uses the service context pattern to manage connections and services automatically.
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

        async def _enqueue_in_with_services(service_manager):
            try:
                job = await enqueue_in(
                    delta,
                    func,
                    *args,
                    queue_name=queue_name,
                    nats_url=nats_url,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    timeout=timeout,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                    **kwargs,
                )
                _sync_logger.info(
                    "enqueue_in_sync_success",
                    {
                        "queue_name": queue_name,
                        "job_id": job.job_id,
                        "func_name": getattr(func, "__name__", str(func)),
                        "delay_seconds": delta.total_seconds(),
                    },
                )
                return job
            except Exception as e:
                _sync_logger.error(
                    "enqueue_in_sync_failed",
                    {
                        "queue_name": queue_name,
                        "func_name": getattr(func, "__name__", str(func)),
                        "delay_seconds": delta.total_seconds(),
                        "error": str(e),
                    },
                )
                raise wrap_naq_exception(
                    e, f"Failed to enqueue job with delay synchronously: {e}"
                )

        return run_with_service_context(
            _enqueue_in_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.enqueue_in_sync",
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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a recurring job (sync).

    Uses the service context pattern to manage connections and services automatically.
    """
    with _sync_logger.operation_context(
        "schedule_sync",
        queue_name=queue_name,
        nats_url=nats_url,
        func_name=getattr(func, "__name__", str(func)),
        cron=cron,
        interval=interval.total_seconds()
        if isinstance(interval, timedelta)
        else interval,
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
            interval=interval.total_seconds()
            if isinstance(interval, timedelta)
            else interval,
            repeat=repeat,
        )

        async def _schedule_with_services(service_manager):
            try:
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
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                    **kwargs,
                )
                _sync_logger.info(
                    "schedule_sync_success",
                    {
                        "queue_name": queue_name,
                        "job_id": job.job_id,
                        "func_name": getattr(func, "__name__", str(func)),
                        "cron": cron,
                        "interval": interval.total_seconds()
                        if isinstance(interval, timedelta)
                        else interval,
                        "repeat": repeat,
                    },
                )
                return job
            except Exception as e:
                _sync_logger.error(
                    "schedule_sync_failed",
                    {
                        "queue_name": queue_name,
                        "func_name": getattr(func, "__name__", str(func)),
                        "cron": cron,
                        "interval": interval.total_seconds()
                        if isinstance(interval, timedelta)
                        else interval,
                        "repeat": repeat,
                        "error": str(e),
                    },
                )
                raise wrap_naq_exception(
                    e, f"Failed to schedule recurring job synchronously: {e}"
                )

        return run_with_service_context(
            _schedule_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.schedule_sync",
        )


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> int:
    """
    Helper to purge jobs from a specific queue (synchronous).

    Uses the service context pattern to manage connections and services automatically.
    """
    with _sync_logger.operation_context(
        "purge_queue_sync", queue_name=queue_name, nats_url=nats_url
    ):
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="purge_queue_sync_start", queue_name=queue_name)

        async def _purge_with_services(service_manager):
            try:
                count = await purge_queue(
                    queue_name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                _sync_logger.info(
                    "purge_queue_sync_success",
                    {"queue_name": queue_name, "purged_count": count},
                )
                return count
            except Exception as e:
                _sync_logger.error(
                    "purge_queue_sync_failed",
                    {"queue_name": queue_name, "error": str(e)},
                )
                raise wrap_naq_exception(e, f"Failed to purge queue synchronously: {e}")

        return run_with_service_context(
            _purge_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.purge_queue_sync",
        )


def cancel_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to cancel a scheduled job (sync).

    Uses the service context pattern to manage connections and services automatically.
    """
    with _sync_logger.operation_context(
        "cancel_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="cancel_scheduled_job_sync_start", job_id=job_id)

        async def _cancel_with_services(service_manager):
            try:
                res = await cancel_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                _sync_logger.info(
                    "cancel_scheduled_job_sync_success",
                    {"job_id": job_id, "result": res},
                )
                return res
            except Exception as e:
                _sync_logger.error(
                    "cancel_scheduled_job_sync_failed",
                    {"job_id": job_id, "error": str(e)},
                )
                raise wrap_naq_exception(
                    e, f"Failed to cancel scheduled job synchronously: {e}"
                )

        return run_with_service_context(
            _cancel_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.cancel_scheduled_job_sync",
        )


def pause_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to pause a scheduled job (sync).

    Uses the service context pattern to manage connections and services automatically.
    """
    with _sync_logger.operation_context(
        "pause_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="pause_scheduled_job_sync_start", job_id=job_id)

        async def _pause_with_services(service_manager):
            try:
                res = await pause_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                _sync_logger.info(
                    "pause_scheduled_job_sync_success",
                    {"job_id": job_id, "result": res},
                )
                return res
            except Exception as e:
                _sync_logger.error(
                    "pause_scheduled_job_sync_failed",
                    {"job_id": job_id, "error": str(e)},
                )
                raise wrap_naq_exception(
                    e, f"Failed to pause scheduled job synchronously: {e}"
                )

        return run_with_service_context(
            _pause_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.pause_scheduled_job_sync",
        )


def resume_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to resume a scheduled job (sync).

    Uses the service context pattern to manage connections and services automatically.
    """
    with _sync_logger.operation_context(
        "resume_scheduled_job_sync", job_id=job_id, nats_url=nats_url
    ):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="resume_scheduled_job_sync_start", job_id=job_id)

        async def _resume_with_services(service_manager):
            try:
                res = await resume_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                _sync_logger.info(
                    "resume_scheduled_job_sync_success",
                    {"job_id": job_id, "result": res},
                )
                return res
            except Exception as e:
                _sync_logger.error(
                    "resume_scheduled_job_sync_failed",
                    {"job_id": job_id, "error": str(e)},
                )
                raise wrap_naq_exception(
                    e, f"Failed to resume scheduled job synchronously: {e}"
                )

        return run_with_service_context(
            _resume_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.resume_scheduled_job_sync",
        )


def modify_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
    **updates: Any,
) -> bool:
    """
    Helper to modify a scheduled job (sync).

    Uses the service context pattern to manage connections and services automatically.
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

        async def _modify_with_services(service_manager):
            try:
                res = await modify_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                    **updates,
                )
                _sync_logger.info(
                    "modify_scheduled_job_sync_success",
                    {"job_id": job_id, "result": res, "updates": list(updates.keys())},
                )
                return res
            except Exception as e:
                _sync_logger.error(
                    "modify_scheduled_job_sync_failed",
                    {
                        "job_id": job_id,
                        "updates": list(updates.keys()),
                        "error": str(e),
                    },
                )
                raise wrap_naq_exception(
                    e, f"Failed to modify scheduled job synchronously: {e}"
                )

        return run_with_service_context(
            _modify_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.queue.sync_api.modify_scheduled_job_sync",
        )


# Optional: public function to explicitly close thread-local connection for sync batches
def close_sync_connections(nats_url: str = DEFAULT_NATS_URL) -> None:
    """
    Close thread-local NATS connection/JS context used by sync helpers.

    This function is kept for backward compatibility. With the service context pattern,
    connections are automatically managed and cleaned up when the context exits.
    """
    with _sync_logger.operation_context("close_sync_connections", nats_url=nats_url):
        validate_parameter(nats_url, "nats_url", str)

        _sync_logger.debug(message="close_sync_connections_start")

        async def _close_with_services(service_manager):
            try:
                # With context managers, connections are automatically managed
                # This function is kept for backward compatibility
                _sync_logger.info("close_sync_connections_success")
                pass
            except Exception as e:
                _sync_logger.error("close_sync_connections_failed", {"error": str(e)})
                raise wrap_naq_exception(e, f"Failed to close sync connections: {e}")

        return run_with_service_context(
            _close_with_services,
            nats_url=nats_url,
            logger_name="naq.queue.sync_api.close_sync_connections",
        )
