"""
Synchronous API wrapper for NAQ.

This module provides synchronous wrappers around the async NAQ API,
making it easier to use in synchronous contexts.
"""

import asyncio
import threading
from typing import Any, Dict, List, Optional, Union

from .nats_client import NatsClient, NatsClientConfig
from .queue import Queue
from .worker import Worker
from .exceptions import NaqException


class SyncNatsClient:
    """
    Synchronous wrapper for NatsClient.
    
    This class provides a synchronous interface to the async NatsClient,
    allowing it to be used in synchronous code. It wraps all async methods
    and runs them in an internal event loop.
    
    Examples:
        >>> # Create a sync client
        >>> client = SyncNatsClient()
        >>> 
        >>> # Connect to NATS
        >>> client.connect()
        >>> 
        >>> # Publish a message
        >>> client.publish("subject", b"message")
        >>> 
        >>> # Use in a context manager
        >>> with SyncNatsClient() as client:
        ...     client.publish("subject", b"message")
    """
    
    
    def __init__(self, config: Optional[NatsClientConfig] = None) -> None:
        """
        Initialize the sync NATS client.
        
        Args:
            config: Optional configuration for the client.
        """
        self._async_client = NatsClient(config)
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to NATS."""
        return self._async_client.is_connected
    
    def connect(self) -> None:
        """Connect to NATS server."""
        self._run_async(self._async_client.connect())
    
    def disconnect(self) -> None:
        """Disconnect from NATS server."""
        self._run_async(self._async_client.disconnect())
    
    def ensure_stream(
        self,
        stream_name: str,
        subjects: List[str],
        **kwargs: Any,
    ) -> None:
        """
        Ensure a JetStream stream exists.
        
        Args:
            stream_name: Name of the stream.
            subjects: List of subjects for the stream.
            **kwargs: Additional stream configuration options.
        """
        self._run_async(
            self._async_client.ensure_stream(stream_name, subjects, **kwargs)
        )
    
    def publish(
        self,
        subject: str,
        payload: bytes,
        **kwargs: Any,
    ) -> str:
        """
        Publish a message to a subject.
        
        Args:
            subject: NATS subject to publish to.
            payload: Message payload as bytes.
            **kwargs: Additional publish options.
            
        Returns:
            str: The message ID.
        """
        return self._run_async(
            self._async_client.publish(subject, payload, **kwargs)
        )
    
    def jetstream_publish(
        self,
        subject: str,
        payload: bytes,
        **kwargs: Any,
    ) -> str:
        """
        Publish a message to JetStream.
        
        Args:
            subject: NATS subject to publish to.
            payload: Message payload as bytes.
            **kwargs: Additional publish options.
            
        Returns:
            str: The message ID.
        """
        return self._run_async(
            self._async_client.jetstream_publish(subject, payload, **kwargs)
        )
    
    def subscribe(
        self,
        subject: str,
        queue_group: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Subscribe to a subject.
        
        Args:
            subject: NATS subject to subscribe to.
            queue_group: Optional queue group name.
            **kwargs: Additional subscription options.
            
        Returns:
            The subscription.
        """
        return self._run_async(
            self._async_client.subscribe(subject, queue_group, **kwargs)
        )
    
    def pull_subscribe(
        self,
        subject: str,
        durable_name: str,
        **kwargs: Any,
    ) -> Any:
        """
        Create a pull subscription.
        
        Args:
            subject: NATS subject to subscribe to.
            durable_name: Durable consumer name.
            **kwargs: Additional subscription options.
            
        Returns:
            The pull subscription.
        """
        return self._run_async(
            self._async_client.pull_subscribe(subject, durable_name, **kwargs)
        )
    
    def fetch_messages(
        self,
        subscription: Any,
        batch_size: int = 1,
        timeout: float = 1.0,
    ) -> List[Any]:
        """
        Fetch messages from a pull subscription.
        
        Args:
            subscription: The pull subscription.
            batch_size: Number of messages to fetch.
            timeout: Fetch timeout in seconds.
            
        Returns:
            List of messages.
        """
        return self._run_async(
            self._async_client.fetch_messages(subscription, batch_size, timeout)
        )
    
    def purge_stream(
        self,
        stream_name: str,
        subject: Optional[str] = None,
    ) -> None:
        """
        Purge messages from a stream.
        
        Args:
            stream_name: Name of the stream.
            subject: Optional subject to filter messages.
        """
        self._run_async(
            self._async_client.purge_stream(stream_name, subject)
        )
    
    def get_kv(self, bucket_name: str) -> Any:
        """
        Get a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
            
        Returns:
            The KV bucket.
        """
        return self._run_async(self._async_client.get_kv(bucket_name))
    
    def create_kv(
        self,
        bucket_name: str,
        **kwargs: Any,
    ) -> Any:
        """
        Create a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
            **kwargs: Additional bucket configuration options.
            
        Returns:
            The KV bucket.
        """
        return self._run_async(
            self._async_client.create_kv(bucket_name, **kwargs)
        )
    
    def delete_kv(self, bucket_name: str) -> None:
        """
        Delete a Key-Value store bucket.
        
        Args:
            bucket_name: Name of the KV bucket.
        """
        self._run_async(self._async_client.delete_kv(bucket_name))
    
    def trigger_due_jobs(self) -> tuple[int, int]:
        """
        Trigger processing of due scheduled jobs.
        
        Returns:
            tuple[int, int]: A tuple of (processed_count, error_count)
        """
        return self._run_async(self._async_client.trigger_due_jobs())
    
    def _run_async(self, coro):
        """Run an async coroutine in the event loop."""
        return self._loop.run_until_complete(coro)
    """
    Synchronous wrapper for Queue.
    
    This class provides a synchronous interface to the async Queue,
    allowing it to be used in synchronous code. It wraps all async methods
    and runs them in an internal event loop.
    
    Examples:
        >>> # Create a sync queue
        >>> queue = SyncQueue("my_queue")
        >>> 
        >>> # Enqueue a job
        >>> job_id = queue.enqueue({"task": "my_task", "data": {...}})
        >>> 
        >>> # Dequeue jobs
        >>> jobs = queue.dequeue(batch_size=1)
        >>> 
        >>> # Use in a context manager
        >>> with SyncQueue("my_queue") as queue:
        ...     queue.enqueue({"task": "my_task"})
    """
    
    def __enter__(self) -> "SyncNatsClient":
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
    
    def __repr__(self) -> str:
        """String representation of the client."""
        return f"SyncNatsClient(async_client={self._async_client!r})"


class SyncQueue:
    
    
    def __init__(
        self,
        name: str,
        client: Optional[SyncNatsClient] = None,
        serializer: Optional[Any] = None,
    ) -> None:
        """
        Initialize the sync queue.
        
        Args:
            name: Name of the queue.
            client: Optional sync NATS client.
            serializer: Optional job serializer.
        """
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        if client is None:
            client = SyncNatsClient()
        
        self._async_queue = Queue(
            name=name,
            client=client._async_client,
            serializer=serializer,
        )
    
    def enqueue(
        self,
        job_data: Dict[str, Any],
        delay: Optional[float] = None,
    ) -> str:
        """
        Enqueue a job.
        
        Args:
            job_data: Job data dictionary.
            delay: Optional delay in seconds before job should be processed.
            
        Returns:
            str: The job ID.
        """
        return self._run_async(
            self._async_queue.enqueue(job_data, delay=delay)
        )
    
    def dequeue(
        self,
        batch_size: int = 1,
        timeout: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Dequeue jobs.
        
        Args:
            batch_size: Number of jobs to dequeue.
    """
    Synchronous wrapper for Worker.
    
    This class provides a synchronous interface to the async Worker,
    allowing it to be used in synchronous code. It wraps all async methods
    and runs them in an internal event loop.
    
    Examples:
        >>> # Create a sync worker
        >>> worker = SyncWorker(["my_queue"])
        >>> 
        >>> # Start the worker
        >>> worker.start()
        >>> 
        >>> # Stop the worker
        >>> worker.stop()
        >>> 
        >>> # Use in a context manager
        >>> with SyncWorker(["my_queue"]) as worker:
        ...     # Worker runs in the background
        ...     pass
    """
            timeout: Dequeue timeout in seconds.
            
        Returns:
            List of job data dictionaries.
        """
        return self._run_async(
            self._async_queue.dequeue(batch_size=batch_size, timeout=timeout)
        )
    
    def job_count(self) -> int:
        """
        Get the number of jobs in the queue.
        
        Returns:
            int: Number of jobs.
        """
        return self._run_async(self._async_queue.job_count())
    
    def purge(self) -> None:
        """Purge all jobs from the queue."""
        self._run_async(self._async_queue.purge())
    
    def close(self) -> None:
        """Close the queue."""
        self._run_async(self._async_queue.close())
    
    def _run_async(self, coro):
        """Run an async coroutine in the event loop."""
        return self._loop.run_until_complete(coro)


class SyncWorker:
    
    
    def __init__(
        self,
        queue_names: List[str],
        client: Optional[SyncNatsClient] = None,
        serializer: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the sync worker.
        
        Args:
            queue_names: List of queue names to process.
            client: Optional sync NATS client.
            serializer: Optional job serializer.
            **kwargs: Additional worker configuration options.
        """
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        if client is None:
            client = SyncNatsClient()
        
        self._async_worker = Worker(
            queue_names=queue_names,
            client=client._async_client,
            serializer=serializer,
            **kwargs,
        )
    
    def start(self) -> None:
        """Start the worker."""
        self._run_async(self._async_worker.start())
    
    def stop(self) -> None:
        """Stop the worker."""
        self._run_async(self._async_worker.stop())
    
    def run_once(self) -> None:
        """Run the worker once."""
        self._run_async(self._async_worker.run_once())
    
    def _run_async(self, coro):
        """Run an async coroutine in the event loop."""
        return self._loop.run_until_complete(coro)
    
    def __enter__(self) -> "SyncWorker":
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()