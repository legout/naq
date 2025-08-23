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
from ..utils import run_async_from_sync
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
    config: Optional[GlobalServiceConfig] = None,
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
    with _sync_logger.operation_context("enqueue_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("enqueue_sync_start", {
            "queue_name": queue_name,
            "func_name": getattr(func, "__name__", str(func))
        })

        async def _main():
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
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                    **kwargs,
                )
                # Do not close thread-local connection here; allow reuse across sync calls.
                _sync_logger.info("enqueue_sync_success", {
                    "queue_name": queue_name,
                    "job_id": job.job_id,
                    "func_name": getattr(func, "__name__", str(func))
                })
                return job
            except Exception as e:
                _sync_logger.error("enqueue_sync_failed", {
                    "queue_name": queue_name,
                    "func_name": getattr(func, "__name__", str(func)),
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to enqueue job synchronously: {e}")

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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job for a specific time (sync).

    This sync wrapper reuses a thread-local NATS connection/JetStream context to
    avoid per-call connect/close. See enqueue_sync() docstring for details on
    performance characteristics and explicit cleanup via close_sync_connections().
    """
    with _sync_logger.operation_context("enqueue_at_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "scheduled_time": dt.isoformat(),
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(dt, "dt", datetime.datetime)
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("enqueue_at_sync_start", {
            "queue_name": queue_name,
            "func_name": getattr(func, "__name__", str(func)),
            "scheduled_time": dt.isoformat()
        })

        async def _main():
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
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                    **kwargs,
                )
                _sync_logger.info("enqueue_at_sync_success", {
                    "queue_name": queue_name,
                    "job_id": job.job_id,
                    "func_name": getattr(func, "__name__", str(func)),
                    "scheduled_time": dt.isoformat()
                })
                return job
            except Exception as e:
                _sync_logger.error("enqueue_at_sync_failed", {
                    "queue_name": queue_name,
                    "func_name": getattr(func, "__name__", str(func)),
                    "scheduled_time": dt.isoformat(),
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to enqueue job at specific time synchronously: {e}")

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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a job after a delay (sync).

    Uses a thread-local NATS connection for efficient repeated calls from the
    same thread. See enqueue_sync() for batching guidance and cleanup options.
    """
    with _sync_logger.operation_context("enqueue_in_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "delay_seconds": delta.total_seconds(),
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(delta, "delta", timedelta)
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("enqueue_in_sync_start", {
            "queue_name": queue_name,
            "func_name": getattr(func, "__name__", str(func)),
            "delay_seconds": delta.total_seconds()
        })

        async def _main():
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
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                    **kwargs,
                )
                _sync_logger.info("enqueue_in_sync_success", {
                    "queue_name": queue_name,
                    "job_id": job.job_id,
                    "func_name": getattr(func, "__name__", str(func)),
                    "delay_seconds": delta.total_seconds()
                })
                return job
            except Exception as e:
                _sync_logger.error("enqueue_in_sync_failed", {
                    "queue_name": queue_name,
                    "func_name": getattr(func, "__name__", str(func)),
                    "delay_seconds": delta.total_seconds(),
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to enqueue job with delay synchronously: {e}")

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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """
    Helper to schedule a recurring job (sync).

    Reuses a thread-local NATS connection/JetStream context to minimize overhead
    in synchronous producers. Refer to enqueue_sync() for full guidance on reuse,
    batching patterns, and explicit cleanup.
    """
    with _sync_logger.operation_context("schedule_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "cron": cron,
        "interval": interval.total_seconds() if isinstance(interval, timedelta) else interval,
        "repeat": repeat,
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("schedule_sync_start", {
            "queue_name": queue_name,
            "func_name": getattr(func, "__name__", str(func)),
            "cron": cron,
            "interval": interval.total_seconds() if isinstance(interval, timedelta) else interval,
            "repeat": repeat
        })

        async def _main():
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
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                    **kwargs,
                )
                _sync_logger.info("schedule_sync_success", {
                    "queue_name": queue_name,
                    "job_id": job.job_id,
                    "func_name": getattr(func, "__name__", str(func)),
                    "cron": cron,
                    "interval": interval.total_seconds() if isinstance(interval, timedelta) else interval,
                    "repeat": repeat
                })
                return job
            except Exception as e:
                _sync_logger.error("schedule_sync_failed", {
                    "queue_name": queue_name,
                    "func_name": getattr(func, "__name__", str(func)),
                    "cron": cron,
                    "interval": interval.total_seconds() if isinstance(interval, timedelta) else interval,
                    "repeat": repeat,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to schedule recurring job synchronously: {e}")

        return run_async_from_sync(_main)


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> int:
    """
    Helper to purge jobs from a specific queue (synchronous).

    Uses thread-local connection reuse to avoid repeated connect/close costs.
    """
    with _sync_logger.operation_context("purge_queue_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url
    }):
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("purge_queue_sync_start", {
            "queue_name": queue_name
        })

        async def _main():
            try:
                count = await purge_queue(
                    queue_name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                )
                _sync_logger.info("purge_queue_sync_success", {
                    "queue_name": queue_name,
                    "purged_count": count
                })
                return count
            except Exception as e:
                _sync_logger.error("purge_queue_sync_failed", {
                    "queue_name": queue_name,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to purge queue synchronously: {e}")

        return run_async_from_sync(_main)


def cancel_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to cancel a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """
    with _sync_logger.operation_context("cancel_scheduled_job_sync", {
        "job_id": job_id,
        "nats_url": nats_url
    }):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("cancel_scheduled_job_sync_start", {
            "job_id": job_id
        })

        async def _main():
            try:
                res = await cancel_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                )
                _sync_logger.info("cancel_scheduled_job_sync_success", {
                    "job_id": job_id,
                    "result": res
                })
                return res
            except Exception as e:
                _sync_logger.error("cancel_scheduled_job_sync_failed", {
                    "job_id": job_id,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to cancel scheduled job synchronously: {e}")

        return run_async_from_sync(_main)


def pause_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to pause a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """
    with _sync_logger.operation_context("pause_scheduled_job_sync", {
        "job_id": job_id,
        "nats_url": nats_url
    }):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("pause_scheduled_job_sync_start", {
            "job_id": job_id
        })

        async def _main():
            try:
                res = await pause_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                )
                _sync_logger.info("pause_scheduled_job_sync_success", {
                    "job_id": job_id,
                    "result": res
                })
                return res
            except Exception as e:
                _sync_logger.error("pause_scheduled_job_sync_failed", {
                    "job_id": job_id,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to pause scheduled job synchronously: {e}")

        return run_async_from_sync(_main)


def resume_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Helper to resume a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """
    with _sync_logger.operation_context("resume_scheduled_job_sync", {
        "job_id": job_id,
        "nats_url": nats_url
    }):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("resume_scheduled_job_sync_start", {
            "job_id": job_id
        })

        async def _main():
            try:
                res = await resume_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                )
                _sync_logger.info("resume_scheduled_job_sync_success", {
                    "job_id": job_id,
                    "result": res
                })
                return res
            except Exception as e:
                _sync_logger.error("resume_scheduled_job_sync_failed", {
                    "job_id": job_id,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to resume scheduled job synchronously: {e}")

        return run_async_from_sync(_main)


def modify_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
    **updates: Any,
) -> bool:
    """
    Helper to modify a scheduled job (sync).

    Uses thread-local connection reuse for efficiency across multiple calls.
    """
    with _sync_logger.operation_context("modify_scheduled_job_sync", {
        "job_id": job_id,
        "nats_url": nats_url,
        "updates": list(updates.keys())
    }):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("modify_scheduled_job_sync_start", {
            "job_id": job_id,
            "updates": list(updates.keys())
        })

        async def _main():
            try:
                res = await modify_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=True,
                    config=config or create_global_config(),
                    **updates,
                )
                _sync_logger.info("modify_scheduled_job_sync_success", {
                    "job_id": job_id,
                    "result": res,
                    "updates": list(updates.keys())
                })
                return res
            except Exception as e:
                _sync_logger.error("modify_scheduled_job_sync_failed", {
                    "job_id": job_id,
                    "updates": list(updates.keys()),
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to modify scheduled job synchronously: {e}")

        return run_async_from_sync(_main)


# Optional: public function to explicitly close thread-local connection for sync batches
def close_sync_connections(nats_url: str = DEFAULT_NATS_URL) -> None:
    """
    Close thread-local NATS connection/JS context used by sync helpers.

    Use this to explicitly end a synchronous batch when you know no further
    enqueue_sync (or other sync helpers) will be called from the current thread.
    This can release the connection resources earlier than process exit.

    Note: With context managers, connections are automatically closed when the
    context exits. This function is kept for backward compatibility.
    """
    with _sync_logger.operation_context("close_sync_connections", {
        "nats_url": nats_url
    }):
        validate_parameter(nats_url, "nats_url", str)
        
        _sync_logger.debug("close_sync_connections_start")

        async def _main():
            try:
                # With context managers, connections are automatically managed
                # This function is kept for backward compatibility
                _sync_logger.info("close_sync_connections_success")
                pass
            except Exception as e:
                _sync_logger.error("close_sync_connections_failed", {
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to close sync connections: {e}")

        return run_async_from_sync(_main)
