# src/naq/client.py
import datetime
from datetime import timedelta
from typing import Any, Callable, List, Optional, Union

from .job import Job, RetryDelayType
from .queue import Queue
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .settings import DEFAULT_QUEUE_NAME, DEFAULT_NATS_URL


class SyncClient:
    """
    A synchronous client that maintains a persistent NATS connection 
    for efficient batched operations.
    
    This prevents the overhead of creating and tearing down NATS connections
    for every synchronous API call.
    
    Usage:
        with SyncClient(nats_url="nats://localhost:4222") as client:
            client.enqueue(my_function, arg1, arg2)
            client.purge_queue("my_queue")
            result = client.fetch_job_result(job_id)
    """
    
    def __init__(self, nats_url: Optional[str] = None):
        """
        Initialize the SyncClient.
        
        Args:
            nats_url: NATS server URL. If None, uses default.
        """
        self.nats_url = nats_url or DEFAULT_NATS_URL
        self._nc = None
        self._js = None
        self._connected = False
    
    async def _connect(self):
        """Establish NATS connection and JetStream context."""
        if not self._connected:
            self._nc = await get_nats_connection(url=self.nats_url)
            self._js = await get_jetstream_context(self._nc)
            self._connected = True
    
    async def _disconnect(self):
        """Close NATS connection."""
        if self._connected:
            await close_nats_connection()
            self._nc = None
            self._js = None
            self._connected = False
    
    def __enter__(self):
        """Context manager entry."""
        import asyncio
        
        # Handle different event loop scenarios
        try:
            loop = asyncio.get_running_loop()
            # If there's already a running loop, we need to use run_in_executor
            # to avoid "RuntimeError: asyncio.run() cannot be called from a running event loop"
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self._connect())
                future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            asyncio.run(self._connect())
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        import asyncio
        
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self._disconnect())
                future.result()
        except RuntimeError:
            asyncio.run(self._disconnect())
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._disconnect()
    
    def _run_async(self, coro):
        """Run an async coroutine synchronously."""
        import asyncio
        
        try:
            loop = asyncio.get_running_loop()
            # If there's already a running loop, use run_in_executor
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            return asyncio.run(coro)
    
    def enqueue(
        self,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
        **kwargs: Any,
    ) -> Job:
        """
        Enqueue a job onto a specific queue.
        
        Args:
            func: The function to execute
            *args: Positional arguments for the function
            queue_name: Name of the queue
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            depends_on: Job dependencies
            **kwargs: Keyword arguments for the function
            
        Returns:
            The enqueued Job object
        """
        async def _enqueue():
            queue = Queue(queue_name, nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.enqueue(
                func,
                *args,
                max_retries=max_retries,
                retry_delay=retry_delay,
                depends_on=depends_on,
                **kwargs,
            )
        
        return self._run_async(_enqueue())
    
    def enqueue_at(
        self,
        dt: datetime.datetime,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
        **kwargs: Any,
    ) -> Job:
        """
        Schedule a job to run at a specific time.
        
        Args:
            dt: When to run the job
            func: The function to execute
            *args: Positional arguments for the function
            queue_name: Name of the queue
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            depends_on: Job dependencies
            **kwargs: Keyword arguments for the function
            
        Returns:
            The scheduled Job object
        """
        async def _enqueue_at():
            queue = Queue(queue_name, nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.enqueue_at(
                dt,
                func,
                *args,
                max_retries=max_retries,
                retry_delay=retry_delay,
                depends_on=depends_on,
                **kwargs,
            )
        
        return self._run_async(_enqueue_at())
    
    def enqueue_in(
        self,
        delta: timedelta,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        depends_on: Optional[Union[str, List[str], Job, List[Job]]] = None,
        **kwargs: Any,
    ) -> Job:
        """
        Schedule a job to run after a delay.
        
        Args:
            delta: How long to wait before running the job
            func: The function to execute
            *args: Positional arguments for the function
            queue_name: Name of the queue
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            depends_on: Job dependencies
            **kwargs: Keyword arguments for the function
            
        Returns:
            The scheduled Job object
        """
        async def _enqueue_in():
            queue = Queue(queue_name, nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.enqueue_in(
                delta,
                func,
                *args,
                max_retries=max_retries,
                retry_delay=retry_delay,
                depends_on=depends_on,
                **kwargs,
            )
        
        return self._run_async(_enqueue_in())
    
    def schedule(
        self,
        func: Callable,
        *args: Any,
        queue_name: str = DEFAULT_QUEUE_NAME,
        cron: Optional[str] = None,
        interval: Optional[timedelta] = None,
        max_retries: Optional[int] = 0,
        retry_delay: RetryDelayType = 0,
        **kwargs: Any,
    ) -> Job:
        """
        Schedule a recurring job.
        
        Args:
            func: The function to execute
            *args: Positional arguments for the function
            queue_name: Name of the queue
            cron: Cron expression for scheduling
            interval: Interval for recurring execution
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            **kwargs: Keyword arguments for the function
            
        Returns:
            The scheduled Job object
        """
        async def _schedule():
            queue = Queue(queue_name, nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.schedule(
                func,
                *args,
                cron=cron,
                interval=interval,
                max_retries=max_retries,
                retry_delay=retry_delay,
                **kwargs,
            )
        
        return self._run_async(_schedule())
    
    def purge_queue(self, queue_name: str = DEFAULT_QUEUE_NAME) -> int:
        """
        Purge all jobs from a queue.
        
        Args:
            queue_name: Name of the queue to purge
            
        Returns:
            Number of messages purged
        """
        async def _purge():
            queue = Queue(queue_name, nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.purge()
        
        return self._run_async(_purge())
    
    def cancel_scheduled_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job.
        
        Args:
            job_id: ID of the job to cancel
            
        Returns:
            True if job was cancelled, False if not found
        """
        async def _cancel():
            queue = Queue("default", nats_url=self.nats_url)  # Queue name doesn't matter for scheduled jobs
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.cancel_scheduled_job(job_id)
        
        return self._run_async(_cancel())
    
    def pause_scheduled_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.
        
        Args:
            job_id: ID of the job to pause
            
        Returns:
            True if job was paused, False if not found
        """
        async def _pause():
            queue = Queue("default", nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.pause_scheduled_job(job_id)
        
        return self._run_async(_pause())
    
    def resume_scheduled_job(self, job_id: str) -> bool:
        """
        Resume a scheduled job.
        
        Args:
            job_id: ID of the job to resume
            
        Returns:
            True if job was resumed, False if not found
        """
        async def _resume():
            queue = Queue("default", nats_url=self.nats_url)
            queue._nc = self._nc  # Reuse existing connection
            queue._js = self._js
            return await queue.resume_scheduled_job(job_id)
        
        return self._run_async(_resume())
    
    def fetch_job_result(self, job_id: str) -> Any:
        """
        Fetch the result of a completed job.
        
        Args:
            job_id: ID of the job
            
        Returns:
            The job result
        """
        async def _fetch():
            return await Job.fetch_result(job_id, nats_url=self.nats_url)
        
        return self._run_async(_fetch())