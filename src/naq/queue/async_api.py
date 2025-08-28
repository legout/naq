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
from ..services.config import create_global_config, GlobalServiceConfig
from ..settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from ..service_context import service_context
from ..utils.decorators import retry
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.validation import validate_parameter


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
    config: Optional[GlobalServiceConfig] = None,
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
        config=config,
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
    config: Optional[GlobalServiceConfig] = None,
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
        config=config,
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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a job after a delay (async)."""
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)
    validate_parameter(delta, "delta", not_none=True, min_value=0)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context(
        "enqueue_in",
        queue_name=queue_name,
        function_name=func.__name__,
        delay_seconds=delta.total_seconds(),
    ):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.enqueue_in",
            ) as service_manager:
                q = Queue(
                    name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
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
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(e, context="enqueue_in operation")
            error_handler.handle_error(
                wrapped_error,
                context={"queue_name": queue_name, "function": func.__name__},
            )
            raise


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
    config: Optional[GlobalServiceConfig] = None,
    **kwargs: Any,
) -> Job:
    """Helper to schedule a recurring job (async)."""
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context(
        "schedule_job",
        queue_name=queue_name,
        function_name=func.__name__,
        cron=cron,
        interval_seconds=interval.total_seconds()
        if isinstance(interval, timedelta)
        else interval,
        repeat=repeat,
    ):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.schedule",
            ) as service_manager:
                q = Queue(
                    name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
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
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(e, context="schedule operation")
            error_handler.handle_error(
                wrapped_error,
                context={"queue_name": queue_name, "function": func.__name__},
            )
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def purge_queue(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    config: Optional[GlobalServiceConfig] = None,
) -> int:
    """Helper to purge jobs from a specific queue (async)."""
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context("purge_queue", queue_name=queue_name):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.purge_queue",
            ) as service_manager:
                q = Queue(
                    name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                return await q.purge()
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(e, context="purge_queue operation")
            error_handler.handle_error(
                wrapped_error, context={"queue_name": queue_name}
            )
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def cancel_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """Helper to cancel a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context("cancel_scheduled_job", job_id=job_id):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.cancel_scheduled_job",
            ) as service_manager:
                q = Queue(
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )  # Queue name doesn't matter here
                return await q.cancel_scheduled_job(job_id)
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(
                e, context="cancel_scheduled_job operation"
            )
            error_handler.handle_error(wrapped_error, context={"job_id": job_id})
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def pause_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """Helper to pause a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context("pause_scheduled_job", job_id=job_id):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.pause_scheduled_job",
            ) as service_manager:
                q = Queue(
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                return await q.pause_scheduled_job(job_id)
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(
                e, context="pause_scheduled_job operation"
            )
            error_handler.handle_error(wrapped_error, context={"job_id": job_id})
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def resume_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """Helper to resume a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context("resume_scheduled_job", job_id=job_id):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.resume_scheduled_job",
            ) as service_manager:
                q = Queue(
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                return await q.resume_scheduled_job(job_id)
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(
                e, context="resume_scheduled_job operation"
            )
            error_handler.handle_error(wrapped_error, context={"job_id": job_id})
            raise


@retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
async def modify_scheduled_job(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    prefer_thread_local: bool = False,
    config: Optional[GlobalServiceConfig] = None,
    **updates: Any,
) -> bool:
    """Helper to modify a scheduled job (async)."""
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)
    validate_parameter(nats_url, "nats_url", not_none=True)

    structured_logger = StructuredLogger("naq.queue.async_api")

    with structured_logger.operation_context(
        "modify_scheduled_job", job_id=job_id, update_keys=list(updates.keys())
    ):
        try:
            # Use service context for short-lived operation
            async with service_context(
                nats_url=nats_url,
                config=config,
                logger_name="naq.queue.async_api.modify_scheduled_job",
            ) as service_manager:
                q = Queue(
                    nats_url=nats_url,
                    prefer_thread_local=prefer_thread_local,
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                return await q.modify_scheduled_job(job_id, **updates)
        except Exception as e:
            error_handler = ErrorHandler()
            wrapped_error = wrap_naq_exception(
                e, context="modify_scheduled_job operation"
            )
            error_handler.handle_error(
                wrapped_error, context={"job_id": job_id, "updates": updates}
            )
            raise
