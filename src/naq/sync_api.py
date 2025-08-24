"""Synchronous API wrappers with service context support.

This module provides synchronous API functions that use the service context patterns
for compatibility with synchronous code while ensuring proper service lifecycle management.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, List, Optional, Union

from .models.jobs import Job, RetryDelayType
from .services.config import create_global_config, GlobalServiceConfig
from .settings import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_NATS_URL,
)
from .service_context import run_with_service_context
from .utils.error_handling import wrap_naq_exception
from .utils.logging import StructuredLogger
from .utils.validation import validate_parameter

# Create a structured logger for sync operations
_sync_logger = StructuredLogger("naq.sync_api")


def enqueue_job_sync(
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
    Synchronously enqueue a job using service context.
    
    This function provides a synchronous wrapper around the async enqueue operation
    using the service context pattern for proper lifecycle management.
    
    Args:
        func: The function to execute.
        *args: Positional arguments for the function.
        queue_name: The name of the queue to enqueue to.
        nats_url: NATS server URL.
        max_retries: Maximum number of retries.
        retry_delay: Delay between retries.
        depends_on: Job dependencies.
        timeout: Job timeout.
        config: Global service configuration.
        **kwargs: Keyword arguments for the function.
        
    Returns:
        The enqueued Job instance.
    """
    with _sync_logger.operation_context("enqueue_job_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        async def _enqueue_with_services(service_manager) -> Job:
            from .queue.async_api import enqueue
            
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
                _sync_logger.info("enqueue_job_sync_success", {
                    "queue_name": queue_name,
                    "job_id": job.job_id,
                    "func_name": getattr(func, "__name__", str(func))
                })
                return job
            except Exception as e:
                _sync_logger.error("enqueue_job_sync_failed", {
                    "queue_name": queue_name,
                    "func_name": getattr(func, "__name__", str(func)),
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to enqueue job synchronously: {e}")
        
        return run_with_service_context(
            _enqueue_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.enqueue_job"
        )


def enqueue_at_sync(
    dt: datetime,
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
    Synchronously schedule a job for a specific time using service context.
    
    Args:
        dt: The datetime when the job should run.
        func: The function to execute.
        *args: Positional arguments for the function.
        queue_name: The name of the queue to enqueue to.
        nats_url: NATS server URL.
        max_retries: Maximum number of retries.
        retry_delay: Delay between retries.
        timeout: Job timeout.
        config: Global service configuration.
        **kwargs: Keyword arguments for the function.
        
    Returns:
        The scheduled Job instance.
    """
    with _sync_logger.operation_context("enqueue_at_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url,
        "func_name": getattr(func, "__name__", str(func)),
        "scheduled_time": dt.isoformat(),
        "max_retries": max_retries,
        "timeout": timeout
    }):
        validate_parameter(dt, "dt", datetime)
        validate_parameter(func, "func", Callable)
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        async def _enqueue_at_with_services(service_manager) -> Job:
            from .queue.async_api import enqueue_at
            
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
        
        return run_with_service_context(
            _enqueue_at_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.enqueue_at"
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
    Synchronously schedule a job after a delay using service context.
    
    Args:
        delta: The delay before the job should run.
        func: The function to execute.
        *args: Positional arguments for the function.
        queue_name: The name of the queue to enqueue to.
        nats_url: NATS server URL.
        max_retries: Maximum number of retries.
        retry_delay: Delay between retries.
        timeout: Job timeout.
        config: Global service configuration.
        **kwargs: Keyword arguments for the function.
        
    Returns:
        The scheduled Job instance.
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
        
        async def _enqueue_in_with_services(service_manager) -> Job:
            from .queue.async_api import enqueue_in
            
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
        
        return run_with_service_context(
            _enqueue_in_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.enqueue_in"
        )


def purge_queue_sync(
    queue_name: str = DEFAULT_QUEUE_NAME,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> int:
    """
    Synchronously purge jobs from a queue using service context.
    
    Args:
        queue_name: The name of the queue to purge.
        nats_url: NATS server URL.
        config: Global service configuration.
        
    Returns:
        The number of purged jobs.
    """
    with _sync_logger.operation_context("purge_queue_sync", {
        "queue_name": queue_name,
        "nats_url": nats_url
    }):
        validate_parameter(queue_name, "queue_name", str)
        validate_parameter(nats_url, "nats_url", str)
        
        async def _purge_with_services(service_manager) -> int:
            from .queue.async_api import purge_queue
            
            try:
                count = await purge_queue(
                    queue_name=queue_name,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
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
        
        return run_with_service_context(
            _purge_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.purge_queue"
        )


def cancel_scheduled_job_sync(
    job_id: str,
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> bool:
    """
    Synchronously cancel a scheduled job using service context.
    
    Args:
        job_id: The ID of the job to cancel.
        nats_url: NATS server URL.
        config: Global service configuration.
        
    Returns:
        True if the job was cancelled, False otherwise.
    """
    with _sync_logger.operation_context("cancel_scheduled_job_sync", {
        "job_id": job_id,
        "nats_url": nats_url
    }):
        validate_parameter(job_id, "job_id", str)
        validate_parameter(nats_url, "nats_url", str)
        
        async def _cancel_with_services(service_manager) -> bool:
            from .queue.async_api import cancel_scheduled_job
            
            try:
                result = await cancel_scheduled_job(
                    job_id,
                    nats_url=nats_url,
                    prefer_thread_local=False,  # Use service context instead
                    config=config or create_global_config(),
                    service_manager=service_manager,
                )
                _sync_logger.info("cancel_scheduled_job_sync_success", {
                    "job_id": job_id,
                    "result": result
                })
                return result
            except Exception as e:
                _sync_logger.error("cancel_scheduled_job_sync_failed", {
                    "job_id": job_id,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to cancel scheduled job synchronously: {e}")
        
        return run_with_service_context(
            _cancel_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.cancel_scheduled_job"
        )


def list_workers_sync(
    nats_url: str = DEFAULT_NATS_URL,
    config: Optional[GlobalServiceConfig] = None,
) -> List[dict]:
    """
    Synchronously list active workers using service context.
    
    Args:
        nats_url: NATS server URL.
        config: Global service configuration.
        
    Returns:
        List of worker information dictionaries.
    """
    with _sync_logger.operation_context("list_workers_sync", {
        "nats_url": nats_url
    }):
        validate_parameter(nats_url, "nats_url", str)
        
        async def _list_with_services(service_manager) -> List[dict]:
            from .worker import Worker
            
            try:
                workers = await Worker.list_workers(nats_url=nats_url)
                _sync_logger.info("list_workers_sync_success", {
                    "nats_url": nats_url,
                    "worker_count": len(workers)
                })
                return workers
            except Exception as e:
                _sync_logger.error("list_workers_sync_failed", {
                    "nats_url": nats_url,
                    "error": str(e)
                })
                raise wrap_naq_exception(e, f"Failed to list workers synchronously: {e}")
        
        return run_with_service_context(
            _list_with_services,
            nats_url=nats_url,
            global_config=config,
            logger_name="naq.sync_api.list_workers"
        )