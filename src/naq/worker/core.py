"""Worker core module.

This module provides the main Worker class that fetches jobs from specified NATS queues
(subjects) and executes them. It uses JetStream pull consumers for fetching jobs and
coordinates with specialized manager classes for status tracking, job management, and
failed job handling.
"""

import asyncio
import os
import signal
import socket
import sys
import uuid
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

import nats
from nats.js.api import ConsumerConfig

from ..config import get_config
from ..exceptions import NaqException
from ..models.enums import WORKER_STATUS
from ..nats_client import NatsClient, NatsClientConfig
from ..utils import setup_logging
from ..utils.decorators import retry, timing
from ..utils.error_handling import ErrorHandler, wrap_naq_exception
from ..utils.logging import StructuredLogger
from ..utils.types import QueueName, WorkerID
from ..utils.validation import ensure_type, validate_parameter
from .controller import WorkerController
from .failed import FailedJobHandler
from .jobs import JobStatusManager
from .monitoring import WorkerMonitor
from .processing import JobProcessor
from .status import WorkerStatusManager
from .sync_interface import WorkerSyncInterface

if TYPE_CHECKING:
    # Import here to avoid circular imports
    pass


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    
    This class uses the unified NatsClient for all NATS operations, replacing the previous
    service layer approach. It provides a clean interface for job processing with support
    for multiple queues, concurrency control, and comprehensive error handling.
    
    The Worker coordinates with specialized manager classes for status tracking, job management,
    and failed job handling. It uses JetStream pull consumers for efficient job fetching.
    
    Examples:
        >>> # Create a worker for a single queue
        >>> worker = Worker(queues=["my_queue"])
        >>> 
        >>> # Start the worker
        >>> await worker.run()
        >>> 
        >>> # Create a worker with custom settings
        >>> worker = Worker(
        ...     queues=["queue1", "queue2"],
        ...     concurrency=5,
        ...     worker_name="my_worker"
        ... )
        >>> 
        >>> # Use with a custom NatsClient
        >>> client = NatsClient()
        >>> worker = Worker(queues=["my_queue"], nats_client=client)
    """

    def __init__(
        self,
        queues: Optional[Sequence[QueueName] | QueueName] = None,
        nats_url: Optional[str] = None,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: Optional[int] = None,
        worker_ttl: Optional[int] = None,
        ack_wait: Optional[
            int | Dict[QueueName, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
        nats_client: Optional[NatsClient] = None,
    ) -> None:
        """Initialize the worker with configuration and services.

        Args:
            queues: Optional sequence of queue names or single queue name to process.
                If None or empty, defaults to DEFAULT_QUEUE_NAME.
            nats_url: NATS server URL to connect to. If None, uses config.
            concurrency: Maximum number of concurrent jobs to process.
            worker_name: Optional base name for the worker ID. If None, generates
                a name based on hostname.
            heartbeat_interval: Interval in seconds between worker heartbeats. If None, uses config.
            worker_ttl: Time-to-live in seconds for worker registration. If None, uses config.
            ack_wait: Acknowledgment wait time in seconds. Can be a single value
                or a dictionary mapping queue names to their specific ack wait times.
            module_paths: Optional sequence of paths or single path to add to sys.path
                for job function imports.
            nats_client: Optional NatsClient instance to use. If None, creates a default one.
        """
        # Validate parameters
        self._validate_init_parameters(
            concurrency, heartbeat_interval, worker_ttl, nats_url
        )

        # Get configuration
        config = get_config()
        
        # Set defaults from config if not provided
        if nats_url is None:
            nats_url = config.nats.servers[0] if config.nats.servers else "nats://localhost:4222"
        if heartbeat_interval is None:
            heartbeat_interval = config.workers.heartbeat_interval
        if worker_ttl is None:
            worker_ttl = config.workers.ttl
        
        # Process queues parameter
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            queues = [config.queues.default_name if config.queues and "default_name" in config.queues else "naq_default_queue"]

        # Preserve order while ensuring uniqueness using dict.fromkeys()
        self.queue_names: List[QueueName] = list(dict.fromkeys(queues))
        self.subjects: List[str] = [
            f"{config.nats.prefix}.queue.{name}" for name in self.queue_names
        ]

        # Add current path to sys.path by default
        if os.getcwd() not in sys.path:
            sys.path.insert(0, os.getcwd())

        # Add custom module paths to sys.path
        if module_paths:
            if isinstance(module_paths, str):
                module_paths = [module_paths]
            for path in module_paths:
                if path not in sys.path:
                    sys.path.insert(0, path)

        self._nats_url = nats_url
        self._concurrency = concurrency

        # Generate a unique ID if name is not provided, otherwise use name as base
        base_name = worker_name or f"naq-worker-{socket.gethostname()}"
        self.worker_id: WorkerID = f"{base_name}-{os.getpid()}-{uuid.uuid4().hex[:6]}"

        self._heartbeat_interval = heartbeat_interval
        self._worker_ttl = worker_ttl
        # Ack wait configuration
        self._ack_wait_arg: Optional[int | Dict[QueueName, int]] = ack_wait

        # NATS client
        self._nats_client = nats_client or NatsClient(
            NatsClientConfig(nats_url=nats_url)
        )

        # Connection and state variables
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._consumers: Dict[QueueName, Any] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{config.nats.prefix}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{config.nats.prefix}-worker"

        # Initialize logging and error handling
        self._logger = StructuredLogger("worker_core")
        self._error_handler = ErrorHandler(self._logger)

        # Create component managers
        self.status_manager = WorkerStatusManager(self, nats_client=self._nats_client)
        self.job_manager = JobStatusManager(self, nats_client=self._nats_client)
        self.failed_handler = FailedJobHandler(self._nats_client)
        self.job_processor = JobProcessor(self)
        self.sync_interface = WorkerSyncInterface(self)

        setup_logging()  # Setup logging
        self._logger.info(
            "Worker initialized", worker_id=self.worker_id, queues=self.queue_names
        )

    @classmethod
    async def create(
        cls,
        queues: Optional[Sequence[QueueName] | QueueName] = None,
        nats_url: Optional[str] = None,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: Optional[int] = None,
        worker_ttl: Optional[int] = None,
        ack_wait: Optional[
            int | Dict[QueueName, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
        nats_client: Optional[NatsClient] = None,
    ) -> "Worker":
        """Create and initialize a Worker instance with services.

        This is the recommended way to create Worker instances as it ensures
        all services are properly initialized.

        Args:
            queues: Optional sequence of queue names or single queue name to process.
                If None or empty, defaults to DEFAULT_QUEUE_NAME.
            nats_url: NATS server URL to connect to. If None, uses config.
            concurrency: Maximum number of concurrent jobs to process.
            worker_name: Optional base name for the worker ID. If None, generates
                a name based on hostname.
            heartbeat_interval: Interval in seconds between worker heartbeats. If None, uses config.
            worker_ttl: Time-to-live in seconds for worker registration. If None, uses config.
            ack_wait: Acknowledgment wait time in seconds. Can be a single value
                or a dictionary mapping queue names to their specific ack wait times.
            module_paths: Optional sequence of paths or single path to add to sys.path
                for job function imports.
            nats_client: Optional NatsClient instance to use. If None, creates a default one.

        Returns:
            A fully initialized Worker instance.
        """
        # Create the worker instance
        worker = cls(
            queues=queues,
            nats_url=nats_url,
            concurrency=concurrency,
            worker_name=worker_name,
            heartbeat_interval=heartbeat_interval,
            worker_ttl=worker_ttl,
            ack_wait=ack_wait,
            module_paths=module_paths,
            nats_client=nats_client,
        )

        # Initialize NATS client
        await worker._nats_client.connect()

        return worker

    async def _initialize_components(self) -> None:
        """Initialize all worker components."""
        try:
            # Initialize component managers
            await self.status_manager.initialize()
            await self.job_manager.initialize()
            await self.failed_handler.initialize()

            self._logger.info("All worker components initialized successfully")
        except Exception as e:
            self._logger.error("Failed to initialize worker components", error=str(e))
            raise

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        with self._logger.operation_context("worker_connect", worker_id=self.worker_id):
            try:
                # Connect to NATS if not already connected
                if not self._nats_client.is_connected:
                    await self._nats_client.connect()

                self._logger.info(
                    "Connected to NATS and JetStream", worker_id=self.worker_id
                )

                # Initialize component managers
                await self._initialize_components()
                await self.status_manager.start_heartbeat_loop()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to connect worker")
                self._error_handler.handle_error(
                    wrapped_error, {"worker_id": self.worker_id}
                )
                raise

    def _resolve_ack_wait_seconds(self, queue_name: QueueName) -> int:
        """
        Resolve ack_wait seconds for a given queue based on precedence:
        1) Per-queue value provided via ack_wait dict argument
        2) Single ack_wait int argument
        3) Environment-driven per-queue settings.ACK_WAIT_PER_QUEUE
        4) settings.DEFAULT_ACK_WAIT_SECONDS
        Ensures a positive integer, falling back to default if invalid.

        Args:
            queue_name: The name of the queue to resolve ack_wait for.

        Returns:
            The ack_wait time in seconds for the specified queue.
        """
        # Get configuration
        config = get_config()
        default_ack_wait = config.queues.ack_wait if config.queues and "ack_wait" in config.queues else 60
        
        try:
            # 1) per-queue dict from constructor
            if (
                isinstance(self._ack_wait_arg, dict)
                and queue_name in self._ack_wait_arg
            ):
                v = ensure_type(self._ack_wait_arg[queue_name], int, "ack_wait_value")
                return v if v > 0 else default_ack_wait
            # 2) single int from constructor
            if isinstance(self._ack_wait_arg, int) and self._ack_wait_arg > 0:
                return int(self._ack_wait_arg)
            # 3) env per-queue
            if config.queues and "ack_wait_per_queue" in config.queues:
                ack_wait_per_queue = config.queues["ack_wait_per_queue"]
                if queue_name in ack_wait_per_queue:
                    v = ensure_type(
                        ack_wait_per_queue[queue_name], int, "env_ack_wait_value"
                    )
                    return v if v > 0 else default_ack_wait
        except Exception as e:
            self._logger.warning(
                "Error resolving ack_wait seconds, using default",
                queue_name=queue_name,
                error=str(e),
            )
        # 4) default
        return default_ack_wait

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
    async def _subscribe_to_queue(self, queue_name: QueueName) -> None:
        """Creates a durable consumer and starts fetching messages for a queue.

        Args:
            queue_name: The name of the queue to subscribe to.
        """
        with self._logger.operation_context(
            "subscribe_to_queue", queue_name=queue_name
        ):
            try:
                config = get_config()
                subject = f"{config.nats.prefix}.queue.{queue_name}"
                durable_name = f"{self.consumer_prefix}-{queue_name}"
                self._logger.info(
                    "Setting up consumer for queue",
                    queue_name=queue_name,
                    subject=subject,
                    durable_name=durable_name,
                    stream_name=self.stream_name,
                )

                # Resolve ack_wait seconds for this queue
                ack_wait_seconds = self._resolve_ack_wait_seconds(queue_name)
                self._logger.info(
                    "Creating consumer for queue",
                    queue_name=queue_name,
                    ack_wait_seconds=ack_wait_seconds,
                )
                
                # Use NatsClient to create pull subscription
                psub = await self._nats_client.pull_subscribe(
                    subject=subject,
                    durable_name=durable_name,
                    config=ConsumerConfig(
                        ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                        ack_wait=ack_wait_seconds,
                        max_ack_pending=self._concurrency * 2,
                    ),
                )
                self._consumers[queue_name] = psub
                self._logger.info(
                    "Pull consumer created for subject",
                    durable_name=durable_name,
                    subject=subject,
                )

                while self._running:
                    await self._process_messages_loop(psub, durable_name, queue_name)

            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to subscribe to queue")
                self._error_handler.handle_error(
                    wrapped_error, {"queue_name": queue_name}
                )
                raise

    async def _process_messages_loop(
        self, psub: Any, durable_name: str, queue_name: QueueName
    ) -> None:
        """Process messages in a loop for a given queue consumer.

        Args:
            psub: The pull subscription for the queue
            durable_name: The durable consumer name
            queue_name: The name of the queue being processed
        """
        try:
            # Use NatsClient to fetch messages
            msgs = await self._nats_client.fetch_messages(
                psub, batch_size=1, timeout=1.0
            )
            for msg in msgs:
                # Acquire semaphore to respect concurrency limits
                await self._semaphore.acquire()

                # Create a callback to release the semaphore when processing is done
                def release_semaphore(_):
                    self._semaphore.release()

                try:
                    # Process the message asynchronously
                    task = asyncio.create_task(
                        self.job_processor.process_message(
                            msg, queue_name, durable_name
                        )
                    )
                    # Add callback to release semaphore when task completes
                    task.add_done_callback(release_semaphore)
                except Exception as e:
                    self._semaphore.release()
                    wrapped_error = wrap_naq_exception(e, "Error processing message")
                    self._error_handler.handle_error(
                        wrapped_error,
                        {"queue_name": queue_name, "durable_name": durable_name},
                    )
        except asyncio.TimeoutError:
            # Timeout is expected when no messages are available, continue loop
            pass
        except Exception as e:
            wrapped_error = wrap_naq_exception(e, "Error fetching messages")
            self._error_handler.handle_error(
                wrapped_error, {"queue_name": queue_name, "durable_name": durable_name}
            )

    @timing(threshold_ms=1000)
    async def run(self) -> None:
        """Starts the worker, connects to NATS, and begins processing jobs."""
        with self._logger.operation_context("worker_run", worker_id=self.worker_id):
            try:
                self._running = True
                self._shutdown_event.clear()

                await self._connect()

                # Register worker initially
                await self.status_manager.update_status(status=WORKER_STATUS.STARTING)

                # Start heartbeat task
                await self.status_manager.start_heartbeat_loop()

                # Ensure the main work stream exists
                config = get_config()
                self._logger.info(
                    "Ensuring stream exists",
                    stream_name=self.stream_name,
                    subjects=[f"{config.nats.prefix}.queue.*"],
                )
                await self._nats_client.ensure_stream(
                    stream_name=self.stream_name,
                    subjects=[f"{config.nats.prefix}.queue.*"],
                )
                self._logger.info(
                    "Stream ensured successfully", stream_name=self.stream_name
                )

                # Start subscription tasks for each queue
                self._logger.info(
                    "Starting subscription tasks", queue_names=self.queue_names
                )
                subscription_tasks = [
                    asyncio.create_task(self._subscribe_to_queue(q_name))
                    for q_name in self.queue_names
                ]

                self._logger.info(
                    "Worker started",
                    worker_id=self.worker_id,
                    queues=self.queue_names,
                    concurrency=self._concurrency,
                )

                # Log worker_started event
                try:
                    import os
                    import socket

                    from ..models.events import WorkerEvent

                    event = WorkerEvent.started(
                        worker_id=self.worker_id,
                        queue_names=self.queue_names,
                        details={
                            "hostname": socket.gethostname(),
                            "pid": os.getpid(),
                            "concurrency": self._concurrency,
                            "nats_url": self._nats_url,
                        },
                    )
                    # Publish event to NATS
                    await self._nats_client.jetstream_publish(
                        f"{config.nats.prefix}.events.worker.started",
                        event.to_json().encode(),
                    )
                except Exception as e:
                    self._logger.warning("Failed to log worker_started event", error=str(e))

                # Set status to idle once subscriptions are ready
                await self.status_manager.update_status(status=WORKER_STATUS.IDLE)

                await self._shutdown_event.wait()

                self._logger.info(
                    "Shutdown signal received, waiting for tasks to complete"
                )
                await self.status_manager.update_status(status=WORKER_STATUS.STOPPING)

                # Stop heartbeat task
                await self.status_manager.stop_heartbeat_loop()

                # Wait for active processing tasks (respecting semaphore)
                active_tasks = self._concurrency - self._semaphore._value
                if active_tasks > 0:
                    self._logger.info(
                        "Waiting for active jobs to finish", active_tasks=active_tasks
                    )
                    # Wait for semaphore to be fully released, with a timeout
                    try:
                        await asyncio.wait_for(
                            self._wait_for_semaphore(), timeout=30.0
                        )  # Wait up to 30s
                    except asyncio.TimeoutError:
                        self._logger.warning(
                            "Timeout waiting for active jobs to finish"
                        )

                # Cancel subscription loops
                for task in subscription_tasks:
                    task.cancel()
                await asyncio.gather(*subscription_tasks, return_exceptions=True)

            except asyncio.CancelledError:
                self._logger.info("Run task cancelled")
            except Exception as e:
                await self._handle_worker_error(
                    e,
                    "Worker run loop encountered an error",
                    {"worker_id": self.worker_id},
                )
                await self.status_manager.update_status(status=WORKER_STATUS.STOPPING)
            finally:
                self._logger.info("Worker shutting down")
                await self._close()
                self._logger.info("Worker shutdown complete")

    @timing(threshold_ms=100)
    async def _wait_for_semaphore(self) -> None:
        """Helper to wait until the semaphore value reaches concurrency limit."""
        with self._logger.operation_context("wait_for_semaphore"):
            while self._semaphore._value < self._concurrency:
                await asyncio.sleep(0.1)

    @timing(threshold_ms=500)
    async def _close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        with self._logger.operation_context("worker_close"):
            # Set shutdown event first to prevent new message processing
            self._shutdown_event.set()
            self._running = False

            # Stop heartbeat and update status first
            try:
                await self.status_manager.stop_heartbeat_loop()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error stopping heartbeat")
                self._error_handler.handle_error(wrapped_error)

            # Cleanup all consumers before unregistering worker
            for queue_name, consumer in self._consumers.items():
                try:
                    self._logger.debug(
                        "Unsubscribing consumer for queue", queue_name=queue_name
                    )
                    await consumer.unsubscribe()
                    # Note: drain() method may not exist on PullSubscription in all NATS versions
                    if hasattr(consumer, "drain"):
                        self._logger.debug(
                            "Draining consumer for queue", queue_name=queue_name
                        )
                        await consumer.drain()
                except Exception as e:
                    wrapped_error = wrap_naq_exception(e, "Error cleaning up consumer")
                    self._error_handler.handle_error(
                        wrapped_error, {"queue_name": queue_name}
                    )
            self._consumers.clear()

            # Unregister worker after cleanup
            try:
                await self.status_manager.unregister_worker()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error unregistering worker")
                self._error_handler.handle_error(wrapped_error)

            # Log worker_stopped event
            try:
                import os
                import socket
                config = get_config()

                from ..models.events import WorkerEvent

                event = WorkerEvent.stopped(
                    worker_id=self.worker_id,
                    queue_names=self.queue_names,
                    details={
                        "hostname": socket.gethostname(),
                        "pid": os.getpid(),
                        "concurrency": self._concurrency,
                        "nats_url": self._nats_url,
                    },
                )
                # Publish event to NATS
                await self._nats_client.jetstream_publish(
                    f"{config.nats.prefix}.events.worker.stopped",
                    event.to_json().encode(),
                )
            except Exception as e:
                self._logger.warning("Failed to log worker_stopped event", error=str(e))

            # Finally close NATS connection
            try:
                await self._nats_client.disconnect()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error closing NATS connection")
                self._error_handler.handle_error(wrapped_error)

    async def _get_available_streams(self) -> List[str]:
        """Get list of available JetStream streams for debugging."""
        try:
            # This would need to be implemented in NatsClient
            # For now, return empty list
            return []
        except Exception as e:
            self._logger.warning("Failed to get stream names", error=str(e))
            return []

    def signal_handler(self, sig, frame) -> None:
        """Handles termination signals."""
        self._logger.warning(
            "Received termination signal, initiating graceful shutdown", signal=sig
        )
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self) -> None:
        """Installs signal handlers for graceful shutdown.

        Notes:
            - signal.signal() may only be called from the main thread of the main
              interpreter. If not in the main thread, this becomes a no-op with a warning.
        """
        try:
            import threading

            if threading.current_thread() is not threading.main_thread():
                self._logger.warning(
                    "Skipping installation of signal handlers because we are not in the main thread"
                )
                return
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except ValueError as e:
            # This can happen in environments that disallow setting signals (e.g., some notebooks)
            wrapped_error = wrap_naq_exception(e, "Could not install signal handlers")
            self._error_handler.handle_error(wrapped_error)
        except Exception as e:
            wrapped_error = wrap_naq_exception(
                e, "Unexpected error installing signal handlers"
            )
            self._error_handler.handle_error(wrapped_error)

    # --- Static methods for worker monitoring ---
    @staticmethod
    @timing(threshold_ms=1000)
    async def list_workers(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        if nats_url is None:
            config = get_config()
            nats_url = config.nats.servers[0] if config.nats.servers else "nats://localhost:4222"
        return await Worker._list_workers_internal(nats_url, async_mode=True)  # type: ignore

    @staticmethod
    @timing(threshold_ms=1000)
    def list_workers_sync(nats_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        if nats_url is None:
            config = get_config()
            nats_url = config.nats.servers[0] if config.nats.servers else "nats://localhost:4222"
        return Worker._list_workers_internal(nats_url, async_mode=False)  # type: ignore

    # --- Sync interface for long-running worker using anyio.BlockingPortal ---
    def _validate_init_parameters(
        self, concurrency: int, heartbeat_interval: Optional[int], worker_ttl: Optional[int], nats_url: Optional[str]
    ) -> None:
        """Validate initialization parameters."""
        validate_parameter(concurrency, "concurrency", not_none=True, min_value=1)
        if heartbeat_interval is not None:
            validate_parameter(heartbeat_interval, "heartbeat_interval", not_none=True, min_value=1)
        if worker_ttl is not None:
            validate_parameter(worker_ttl, "worker_ttl", not_none=True, min_value=1)
        if nats_url is not None:
            validate_parameter(nats_url, "nats_url", not_none=True)

    def run_sync(self) -> None:
        """Start the async worker in a clean AnyIO event loop using a BlockingPortal."""
        return self.sync_interface.run_sync()

    # --- Optional persistent lifecycle control for sync contexts ---
    def start_sync(self) -> "WorkerController":
        """Start the worker asynchronously and return a synchronous Controller."""
        return self.sync_interface.start_sync()

    def stop_sync(self) -> None:
        """Convenience synchronous stop for a worker that was started via start_sync()."""
        return self.sync_interface.stop_sync()

    async def _handle_worker_error(
        self,
        exception: Exception,
        context: str,
        error_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Handle worker errors with consistent logging and error wrapping."""
        wrapped_error = wrap_naq_exception(exception, context)
        self._error_handler.handle_error(wrapped_error, error_context or {})

    @staticmethod
    async def _list_workers_internal(
        nats_url: str, async_mode: bool
    ) -> List[Dict[str, Any]]:
        """Internal method to list workers, supporting both async and sync modes."""
        from ..utils.error_handling import ErrorHandler, wrap_naq_exception
        from ..utils.logging import StructuredLogger

        # Create a temporary NatsClient and WorkerMonitor
        nats_client = NatsClient(NatsClientConfig(nats_url=nats_url))
        monitor = WorkerMonitor(nats_client=nats_client, nats_url=nats_url)

        # Initialize logger and error handler for this static method
        logger_name = "worker_list" if async_mode else "worker_list_sync"
        logger = StructuredLogger(logger_name)
        error_handler = ErrorHandler(logger)
        operation_name = "list_workers" if async_mode else "list_workers_sync"

        try:
            with logger.operation_context(operation_name, nats_url=nats_url):
                # Connect to NATS
                await nats_client.connect()
                
                if async_mode:
                    return await monitor.list_workers(nats_url)
                else:
                    return monitor.list_workers_sync(nats_url)
        except Exception as e:
            error_msg = (
                "Error listing workers"
                if async_mode
                else "Error listing workers synchronously"
            )
            wrapped_error = wrap_naq_exception(e, error_msg)
            error_handler.handle_error(wrapped_error, {"nats_url": nats_url})
            raise
        finally:
            # Clean up the NATS client
            try:
                await nats_client.disconnect()
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Error disconnecting NATS client"
                )
                error_handler.handle_error(wrapped_error)
