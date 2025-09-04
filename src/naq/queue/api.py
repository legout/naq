"""Unified queue API functions.

This module contains high-level API functions for queue operations, providing both
asynchronous and synchronous interfaces in a single module. These functions provide
convenient access to queue functionality without requiring direct instantiation of
the Queue class.
"""

import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union, overload

import anyio

from ..models.jobs import Job, RetryDelayType
from .core import Queue
from ..config import NAQConfig, get_config
from ..exceptions import NaqException
from ..utils.logging import get_logger
from ..utils.validation import validate_parameter

# Get logger for this module
logger = get_logger(__name__)


# --- Async API Functions ---


async def enqueue(
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Enqueue a job onto a specific queue (async).

    Args:
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        depends_on: Jobs this job depends on.
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    # Validate parameters
    validate_parameter(func, "func", not_none=True)
    validate_parameter(queue_name, "queue_name", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug(
        "Enqueuing job",
        queue_name=queue_name,
        func_name=getattr(func, "__name__", str(func)),
    )

    # Create queue and enqueue job
    queue = Queue(name=queue_name, nats_url=nats_url, config=config)
    return await queue.enqueue(
        func,
        *args,
        max_retries=max_retries,
        retry_delay=retry_delay,
        depends_on=depends_on,
        timeout=timeout,
        **kwargs,
    )


async def enqueue_at(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a job for a specific time (async).

    Args:
        dt: When to execute the job.
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    # Validate parameters
    validate_parameter(dt, "dt", not_none=True)
    validate_parameter(func, "func", not_none=True)
    validate_parameter(queue_name, "queue_name", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug(
        "Scheduling job for specific time",
        queue_name=queue_name,
        func_name=getattr(func, "__name__", str(func)),
        scheduled_time=dt.isoformat(),
    )

    # Create queue and enqueue job
    queue = Queue(name=queue_name, nats_url=nats_url, config=config)
    return await queue.enqueue_at(
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
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a job after a delay (async).

    Args:
        delta: Delay before executing the job.
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    # Validate parameters
    validate_parameter(delta, "delta", not_none=True, min_value=0)
    validate_parameter(func, "func", not_none=True)
    validate_parameter(queue_name, "queue_name", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug(
        "Scheduling job with delay",
        queue_name=queue_name,
        func_name=getattr(func, "__name__", str(func)),
        delay_seconds=delta.total_seconds(),
    )

    # Create queue and enqueue job
    queue = Queue(name=queue_name, nats_url=nats_url, config=config)
    return await queue.enqueue_in(
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
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    cron: Optional[str] = None,
    interval: Optional[Union[timedelta, float, int]] = None,
    repeat: Optional[int] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a recurring job (async).

    Args:
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        cron: Cron expression for scheduling.
        interval: Interval between executions (seconds or timedelta).
        repeat: Number of times to repeat the job.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
        ValueError: If neither cron nor interval is provided.
    """
    # Validate parameters
    validate_parameter(func, "func", not_none=True)
    validate_parameter(queue_name, "queue_name", not_none=True)
    if cron is None and interval is None:
        raise ValueError("Either cron or interval must be provided")

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug(
        "Scheduling recurring job",
        queue_name=queue_name,
        func_name=getattr(func, "__name__", str(func)),
        cron=cron,
        interval=interval.total_seconds() if isinstance(interval, timedelta) else interval,
        repeat=repeat,
    )

    # Create queue and enqueue job
    queue = Queue(name=queue_name, nats_url=nats_url, config=config)
    return await queue.schedule(
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
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> int:
    """Purge jobs from a specific queue (async).

    Args:
        queue_name: Name of the queue to purge.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        Number of purged jobs.

    Raises:
        NaqException: If the queue cannot be purged.
    """
    # Validate parameters
    validate_parameter(queue_name, "queue_name", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug("Purging queue", queue_name=queue_name)

    # Create queue and purge
    queue = Queue(name=queue_name, nats_url=nats_url, config=config)
    return await queue.purge()


async def cancel_scheduled_job(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Cancel a scheduled job (async).

    Args:
        job_id: ID of the job to cancel.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was cancelled, False otherwise.

    Raises:
        NaqException: If the job cannot be cancelled.
    """
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug("Cancelling scheduled job", job_id=job_id)

    # Create queue and cancel job
    queue = Queue(nats_url=nats_url, config=config)
    return await queue.cancel_scheduled_job(job_id)


async def pause_scheduled_job(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Pause a scheduled job (async).

    Args:
        job_id: ID of the job to pause.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was paused, False otherwise.

    Raises:
        NaqException: If the job cannot be paused.
    """
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats.servers[0]

    logger.debug("Pausing scheduled job", job_id=job_id)

    # Create queue and pause job
    queue = Queue(nats_url=nats_url, config=config)
    return await queue.pause_scheduled_job(job_id)


async def resume_scheduled_job(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Resume a scheduled job (async).

    Args:
        job_id: ID of the job to resume.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was resumed, False otherwise.

    Raises:
        NaqException: If the job cannot be resumed.
    """
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug("Resuming scheduled job", job_id=job_id)

    # Create queue and resume job
    queue = Queue(nats_url=nats_url, config=config)
    return await queue.resume_scheduled_job(job_id)


async def modify_scheduled_job(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
    **updates: Any,
) -> bool:
    """Modify a scheduled job (async).

    Args:
        job_id: ID of the job to modify.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.
        **updates: Updates to apply to the job.

    Returns:
        True if the job was modified, False otherwise.

    Raises:
        NaqException: If the job cannot be modified.
    """
    # Validate parameters
    validate_parameter(job_id, "job_id", not_none=True)

    # Get configuration
    config = config or get_config()
    nats_url = nats_url or config.nats_url

    logger.debug("Modifying scheduled job", job_id=job_id, updates=list(updates.keys()))

    # Create queue and modify job
    queue = Queue(nats_url=nats_url, config=config)
    return await queue.modify_scheduled_job(job_id, **updates)


# --- Sync API Functions ---


def enqueue_sync(
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Enqueue a job onto a specific queue (sync).

    Args:
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        depends_on: Jobs this job depends on.
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    return anyio.run(
        enqueue,
        func,
        *args,
        queue_name=queue_name,
        nats_url=nats_url,
        max_retries=max_retries,
        retry_delay=retry_delay,
        depends_on=depends_on,
        timeout=timeout,
        config=config,
        **kwargs,
    )


def enqueue_at_sync(
    dt: datetime.datetime,
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a job for a specific time (sync).

    Args:
        dt: When to execute the job.
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    return anyio.run(
        enqueue_at,
        dt,
        func,
        *args,
        queue_name=queue_name,
        nats_url=nats_url,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        config=config,
        **kwargs,
    )


def enqueue_in_sync(
    delta: timedelta,
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a job after a delay (sync).

    Args:
        delta: Delay before executing the job.
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
    """
    return anyio.run(
        enqueue_in,
        delta,
        func,
        *args,
        queue_name=queue_name,
        nats_url=nats_url,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        config=config,
        **kwargs,
    )


def schedule_sync(
    func: Callable,
    *args: Any,
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    cron: Optional[str] = None,
    interval: Optional[Union[timedelta, float, int]] = None,
    repeat: Optional[int] = None,
    max_retries: Optional[int] = 0,
    retry_delay: RetryDelayType = 0,
    timeout: Optional[int] = None,
    config: Optional[NAQConfig] = None,
    **kwargs: Any,
) -> Job:
    """Schedule a recurring job (sync).

    Args:
        func: The function to execute.
        *args: Positional arguments to pass to the function.
        queue_name: Name of the queue to enqueue the job to.
        nats_url: NATS server URL. If None, uses config default.
        cron: Cron expression for scheduling.
        interval: Interval between executions (seconds or timedelta).
        repeat: Number of times to repeat the job.
        max_retries: Maximum number of retries for the job.
        retry_delay: Delay between retries (seconds or timedelta).
        timeout: Job execution timeout in seconds.
        config: NAQConfiguration object. If None, uses global config.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The enqueued Job object.

    Raises:
        NaqException: If the job cannot be enqueued.
        ValueError: If neither cron nor interval is provided.
    """
    return anyio.run(
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
        config=config,
        **kwargs,
    )


def purge_queue_sync(
    queue_name: str = "default",
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> int:
    """Purge jobs from a specific queue (sync).

    Args:
        queue_name: Name of the queue to purge.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        Number of purged jobs.

    Raises:
        NaqException: If the queue cannot be purged.
    """
    return anyio.run(purge_queue, queue_name=queue_name, nats_url=nats_url, config=config)


def cancel_scheduled_job_sync(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Cancel a scheduled job (sync).

    Args:
        job_id: ID of the job to cancel.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was cancelled, False otherwise.

    Raises:
        NaqException: If the job cannot be cancelled.
    """
    return anyio.run(cancel_scheduled_job, job_id, nats_url=nats_url, config=config)


def pause_scheduled_job_sync(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Pause a scheduled job (sync).

    Args:
        job_id: ID of the job to pause.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was paused, False otherwise.

    Raises:
        NaqException: If the job cannot be paused.
    """
    return anyio.run(pause_scheduled_job, job_id, nats_url=nats_url, config=config)


def resume_scheduled_job_sync(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
) -> bool:
    """Resume a scheduled job (sync).

    Args:
        job_id: ID of the job to resume.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.

    Returns:
        True if the job was resumed, False otherwise.

    Raises:
        NaqException: If the job cannot be resumed.
    """
    return anyio.run(resume_scheduled_job, job_id, nats_url=nats_url, config=config)


def modify_scheduled_job_sync(
    job_id: str,
    nats_url: Optional[str] = None,
    config: Optional[NAQConfig] = None,
    **updates: Any,
) -> bool:
    """Modify a scheduled job (sync).

    Args:
        job_id: ID of the job to modify.
        nats_url: NATS server URL. If None, uses config default.
        config: NAQConfiguration object. If None, uses global config.
        **updates: Updates to apply to the job.

    Returns:
        True if the job was modified, False otherwise.

    Raises:
        NaqException: If the job cannot be modified.
    """
    return anyio.run(
        modify_scheduled_job, job_id, nats_url=nats_url, config=config, **updates
    )