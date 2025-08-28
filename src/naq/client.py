"""SyncClient implementation for efficient batch operations.

This module provides the SyncClient class that offers a convenient context manager
for batch operations with connection reuse, improving performance over individual
sync calls.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, List, Optional, Union

from .models.jobs import Job, RetryDelayType
from .queue.sync_api import (
    enqueue_sync,
    enqueue_at_sync,
    enqueue_in_sync,
    schedule_sync,
    purge_queue_sync,
    cancel_scheduled_job_sync,
    pause_scheduled_job_sync,
    resume_scheduled_job_sync,
    modify_scheduled_job_sync,
)
from .settings import DEFAULT_QUEUE_NAME, DEFAULT_NATS_URL
from .services.config import GlobalServiceConfig


class SyncClient:
    """A synchronous client for efficient batch operations with connection reuse.

    The SyncClient provides a context manager interface that allows for efficient
    batch operations by reusing NATS connections. This is significantly more
    performant than individual sync calls when enqueuing many jobs.

    Examples:
        >>> # Efficient batch operations
        >>> with SyncClient() as client:
        ...     jobs = []
        ...     for i in range(100):
        ...         job = client.enqueue(my_function, arg1=i)
        ...         jobs.append(job)

        >>> # Scheduled jobs
        >>> with SyncClient() as client:
        ...     # Schedule for specific time
        ...     job1 = client.enqueue_at(datetime.now() + timedelta(hours=1), my_function)
        ...     # Schedule with delay
        ...     job2 = client.enqueue_in(timedelta(minutes=30), my_function)
    """

    def __init__(
        self,
        nats_url: str = DEFAULT_NATS_URL,
        config: Optional[GlobalServiceConfig] = None,
    ):
        """Initialize the SyncClient.

        Args:
            nats_url: NATS server URL
            config: Global service configuration
        """
        self.nats_url = nats_url
        self.config = config
        self._jobs: List[Job] = []

    def __enter__(self) -> "SyncClient":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager."""
        # Clean up any resources if needed
        self._jobs.clear()

    def enqueue(
        self,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        """Enqueue a job using the sync API with connection reuse.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            queue_name: The name of the queue to enqueue to.
            max_retries: Maximum number of retries.
            retry_delay: Delay between retries.
            depends_on: Job dependencies.
            timeout: Job timeout.
            **kwargs: Keyword arguments for the function.

        Returns:
            The enqueued Job instance.
        """
        job = enqueue_sync(
            func,
            *args,
            queue_name=queue_name,
            nats_url=self.nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            depends_on=depends_on,
            timeout=timeout,
            config=self.config,
            **kwargs,
        )
        self._jobs.append(job)
        return job

    def enqueue_at(
        self,
        dt: datetime,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        """Schedule a job for a specific time.

        Args:
            dt: The datetime when the job should run.
            func: The function to execute.
            *args: Positional arguments for the function.
            queue_name: The name of the queue to enqueue to.
            max_retries: Maximum number of retries.
            retry_delay: Delay between retries.
            timeout: Job timeout.
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
        """
        job = enqueue_at_sync(
            dt,
            func,
            *args,
            queue_name=queue_name,
            nats_url=self.nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            config=self.config,
            **kwargs,
        )
        self._jobs.append(job)
        return job

    def enqueue_in(
        self,
        delta: timedelta,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        """Schedule a job after a delay.

        Args:
            delta: The delay before the job should run.
            func: The function to execute.
            *args: Positional arguments for the function.
            queue_name: The name of the queue to enqueue to.
            max_retries: Maximum number of retries.
            retry_delay: Delay between retries.
            timeout: Job timeout.
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
        """
        job = enqueue_in_sync(
            delta,
            func,
            *args,
            queue_name=queue_name,
            nats_url=self.nats_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            config=self.config,
            **kwargs,
        )
        self._jobs.append(job)
        return job

    def schedule(
        self,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        cron: Optional[str] = None,
        interval: Optional[Union[timedelta, float, int]] = None,
        repeat: Optional[int] = None,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> Job:
        """Schedule a recurring job.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            queue_name: The name of the queue to enqueue to.
            cron: Cron expression for scheduling.
            interval: Interval for scheduling.
            repeat: Number of times to repeat.
            max_retries: Maximum number of retries.
            retry_delay: Delay between retries.
            timeout: Job timeout.
            **kwargs: Keyword arguments for the function.

        Returns:
            The scheduled Job instance.
        """
        job = schedule_sync(
            func,
            *args,
            queue_name=queue_name,
            nats_url=self.nats_url,
            cron=cron,
            interval=interval,
            repeat=repeat,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            config=self.config,
            **kwargs,
        )
        self._jobs.append(job)
        return job

    def purge_queue(self, queue_name: str = DEFAULT_QUEUE_NAME) -> int:
        """Purge jobs from a queue.

        Args:
            queue_name: The name of the queue to purge.

        Returns:
            The number of purged jobs.
        """
        return purge_queue_sync(
            queue_name=queue_name,
            nats_url=self.nats_url,
            config=self.config,
        )

    def cancel_scheduled_job(self, job_id: str) -> bool:
        """Cancel a scheduled job.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            True if the job was cancelled, False otherwise.
        """
        return cancel_scheduled_job_sync(
            job_id=job_id,
            nats_url=self.nats_url,
            config=self.config,
        )

    def pause_scheduled_job(self, job_id: str) -> bool:
        """Pause a scheduled job.

        Args:
            job_id: The ID of the job to pause.

        Returns:
            True if the job was paused, False otherwise.
        """
        return pause_scheduled_job_sync(
            job_id=job_id,
            nats_url=self.nats_url,
            config=self.config,
        )

    def resume_scheduled_job(self, job_id: str) -> bool:
        """Resume a scheduled job.

        Args:
            job_id: The ID of the job to resume.

        Returns:
            True if the job was resumed, False otherwise.
        """
        return resume_scheduled_job_sync(
            job_id=job_id,
            nats_url=self.nats_url,
            config=self.config,
        )

    def modify_scheduled_job(self, job_id: str, **updates: Any) -> bool:
        """Modify a scheduled job.

        Args:
            job_id: The ID of the job to modify.
            **updates: Updates to apply to the job.

        Returns:
            True if the job was modified, False otherwise.
        """
        return modify_scheduled_job_sync(
            job_id=job_id,
            nats_url=self.nats_url,
            config=self.config,
            **updates,
        )

    @property
    def jobs(self) -> List[Job]:
        """Get the list of jobs created with this client.

        Returns:
            List of jobs created with this client.
        """
        return self._jobs.copy()
