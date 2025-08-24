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
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig

from ..exceptions import NaqException
from ..models.enums import WORKER_STATUS
from ..service_context import long_lived_service_context
from ..services import (ConnectionService, EventService, KVStoreService,
                        ServiceManager, StreamService)
from ..settings import (ACK_WAIT_PER_QUEUE, DEFAULT_ACK_WAIT_SECONDS,
                        DEFAULT_NATS_URL, DEFAULT_QUEUE_NAME,
                        DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
                        DEFAULT_WORKER_TTL_SECONDS, NAQ_PREFIX)
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
    from ..models.events import JobEvent, WorkerEvent
    from ..models.jobs import Job
    from ..queue.core import Queue


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs. Coordinates with specialized manager
    classes for status tracking, job management, and failed job handling.
    """

    def __init__(
        self,
        queues: Optional[Sequence[QueueName] | QueueName] = None,
        nats_url: str = DEFAULT_NATS_URL,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
        ack_wait: Optional[
            int | Dict[QueueName, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
        service_manager: Optional[ServiceManager] = None,
    ) -> None:
        """Initialize the worker with configuration and services.

        Args:
            queues: Optional sequence of queue names or single queue name to process.
                If None or empty, defaults to DEFAULT_QUEUE_NAME.
            nats_url: NATS server URL to connect to.
            concurrency: Maximum number of concurrent jobs to process.
            worker_name: Optional base name for the worker ID. If None, generates
                a name based on hostname.
            heartbeat_interval: Interval in seconds between worker heartbeats.
            worker_ttl: Time-to-live in seconds for worker registration.
            ack_wait: Acknowledgment wait time in seconds. Can be a single value
                or a dictionary mapping queue names to their specific ack wait times.
            module_paths: Optional sequence of paths or single path to add to sys.path
                for job function imports.
            service_manager: Optional ServiceManager instance to use. If None,
                creates a default one.
        """
        # Validate parameters
        validate_parameter(concurrency, "concurrency", not_none=True, min_value=1)
        validate_parameter(
            heartbeat_interval, "heartbeat_interval", not_none=True, min_value=1
        )
        validate_parameter(worker_ttl, "worker_ttl", not_none=True, min_value=1)
        validate_parameter(nats_url, "nats_url", not_none=True)

        # Process queues parameter
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            queues = [DEFAULT_QUEUE_NAME]

        # Preserve order while ensuring uniqueness using dict.fromkeys()
        self.queue_names: List[QueueName] = list(dict.fromkeys(queues))
        self.subjects: List[str] = [
            f"{NAQ_PREFIX}.queue.{name}" for name in self.queue_names
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

        # Service manager and services
        self._service_manager = service_manager
        self._connection_service: Optional[ConnectionService] = None
        self._stream_service: Optional[StreamService] = None
        self._kv_store_service: Optional[KVStoreService] = None
        self._event_service: Optional[EventService] = None

        # Connection and state variables
        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._consumers: Dict[
            QueueName, nats.js.api.PullSubscribe
        ] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Initialize logging and error handling
        self._logger = StructuredLogger("worker_core")
        self._error_handler = ErrorHandler(self._logger)

        # Create a default service manager if none provided
        if self._service_manager is None:
            from ..services import ServiceConfig, ServiceManager

            config = ServiceConfig(nats_url=self._nats_url)
            self._service_manager = ServiceManager(config)

        # Create component managers
        self.status_manager = WorkerStatusManager(
            self, service_manager=self._service_manager
        )
        self.job_manager = JobStatusManager(self, service_manager=self._service_manager)
        self.failed_handler = FailedJobHandler(self._service_manager)
        self.job_processor = JobProcessor(self)
        self.sync_interface = WorkerSyncInterface(self)

        setup_logging()  # Setup logging
        self._logger.info(
            "Worker initialized", worker_id=self.worker_id, queues=self.queue_names
        )

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, TimeoutError))
    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        with self._logger.operation_context("worker_connect", worker_id=self.worker_id):
            try:
                if self._nc is None or not self._nc.is_connected:
                    # Get or create services
                    if self._service_manager is None:
                        # Create a default service manager if none provided
                        from ..services import ServiceConfig, ServiceManager

                        config = ServiceConfig(nats_url=self._nats_url)
                        self._service_manager = ServiceManager(config)

                    # Use long-lived service context for worker lifecycle
                    async with long_lived_service_context(
                        self._service_manager,
                        logger_name=f"naq.worker.core.{self.worker_id}",
                    ) as service_manager:
                        # Get services from the service manager
                        self._connection_service = await service_manager.get_service(
                            "connection", ConnectionService
                        )
                        self._stream_service = await service_manager.get_service(
                            "stream", StreamService
                        )
                        self._kv_store_service = await service_manager.get_service(
                            "kv_store", KVStoreService
                        )
                        self._event_service = await service_manager.get_service(
                            "events", EventService
                        )

                        # Get connection and JetStream context
                        self._nc = await self._connection_service.get_connection()
                        self._js = await self._connection_service.get_jetstream()
                        self._logger.info(
                            "Connected to NATS and JetStream", worker_id=self.worker_id
                        )

                        # Initialize component managers
                        await self.status_manager.start_heartbeat_loop()
                        await self.job_manager.initialize(self._js)
                        await self.failed_handler.initialize()
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
        try:
            # 1) per-queue dict from constructor
            if (
                isinstance(self._ack_wait_arg, dict)
                and queue_name in self._ack_wait_arg
            ):
                v = ensure_type(self._ack_wait_arg[queue_name], int, "ack_wait_value")
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
            # 2) single int from constructor
            if isinstance(self._ack_wait_arg, int) and self._ack_wait_arg > 0:
                return int(self._ack_wait_arg)
            # 3) env per-queue
            if queue_name in ACK_WAIT_PER_QUEUE:
                v = ensure_type(
                    ACK_WAIT_PER_QUEUE[queue_name], int, "env_ack_wait_value"
                )
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
        except Exception as e:
            self._logger.warning(
                "Error resolving ack_wait seconds, using default",
                queue_name=queue_name,
                error=str(e),
            )
        # 4) default
        return DEFAULT_ACK_WAIT_SECONDS

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
                if not self._js:
                    raise NaqException("JetStream context not available.")

                subject = f"{NAQ_PREFIX}.queue.{queue_name}"
                durable_name = f"{self.consumer_prefix}-{queue_name}"
                self._logger.info(
                    "Setting up consumer for queue",
                    queue_name=queue_name,
                    subject=subject,
                    durable_name=durable_name,
                )

                # Resolve ack_wait seconds for this queue
                ack_wait_seconds = self._resolve_ack_wait_seconds(queue_name)
                self._logger.info(
                    "Creating consumer for queue",
                    queue_name=queue_name,
                    ack_wait_seconds=ack_wait_seconds,
                )
                psub = await self._js.pull_subscribe(
                    subject=subject,
                    durable=durable_name,
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
                    if self._semaphore.locked():  # Check semaphore before fetching
                        await asyncio.sleep(0.1)  # Wait if concurrency limit reached
                        continue

                    try:
                        # Calculate how many messages we can fetch based on available concurrency slots
                        available_slots = self._concurrency - (
                            self._concurrency - self._semaphore._value
                        )
                        if available_slots <= 0:
                            await asyncio.sleep(0.1)  # Wait if no slots free
                            continue

                        # Fetch up to the number of available slots, with a timeout
                        msgs = await psub.fetch(batch=available_slots, timeout=1)
                        if msgs:
                            self._logger.debug(
                                "Fetched messages from consumer",
                                durable_name=durable_name,
                                message_count=len(msgs),
                            )

                        for msg in msgs:
                            # Acquire semaphore before starting processing task
                            await self._semaphore.acquire()
                            # Create task to process the message
                            task = asyncio.create_task(
                                self.job_processor.process_message(msg)
                            )
                            # Add a callback to release the semaphore when the task completes (success or failure)
                            task.add_done_callback(lambda t: self._semaphore.release())
                            # Keep track of tasks (optional, for clean shutdown)
                            self._tasks.append(task)
                            self._tasks = [
                                t for t in self._tasks if not t.done()
                            ]  # Basic cleanup

                    except nats.errors.TimeoutError:
                        # No messages available, or timeout hit, loop continues
                        await asyncio.sleep(0.1)  # Small sleep to prevent busy-wait
                        continue
                    except nats.js.errors.ConsumerNotFoundError:
                        self._logger.warning(
                            "Consumer not found, stopping fetch loop",
                            durable_name=durable_name,
                        )
                        break
                    except Exception as e:
                        wrapped_error = wrap_naq_exception(
                            e, "Error fetching from consumer"
                        )
                        self._error_handler.handle_error(
                            wrapped_error,
                            {"durable_name": durable_name, "queue_name": queue_name},
                        )
                        await asyncio.sleep(1)  # Wait before retrying fetch

            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Failed to subscribe to queue")
                self._error_handler.handle_error(
                    wrapped_error, {"queue_name": queue_name}
                )
                raise

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
                await self._stream_service.ensure_stream(
                    stream_name=self.stream_name,
                    subjects=[f"{NAQ_PREFIX}.queue.*"],
                )

                # Start subscription tasks for each queue
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
                if self._event_service:
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
                    await self._event_service.log_worker_event(event)

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
                wrapped_error = wrap_naq_exception(
                    e, "Worker run loop encountered an error"
                )
                self._error_handler.handle_error(
                    wrapped_error, {"worker_id": self.worker_id}
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
                if self._event_service:
                    import os
                    import socket

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
                    await self._event_service.log_worker_event(event)
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Error logging worker_stopped event"
                )
                self._error_handler.handle_error(wrapped_error)

            # Finally close NATS connection
            try:
                if self._connection_service:
                    await self._connection_service.close_connection()
            except Exception as e:
                wrapped_error = wrap_naq_exception(e, "Error closing NATS connection")
                self._error_handler.handle_error(wrapped_error)

            self._nc = None
            self._js = None

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
    async def list_workers(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        from ..services import ServiceConfig, ServiceManager
        from ..utils.error_handling import ErrorHandler, wrap_naq_exception
        from ..utils.logging import StructuredLogger

        # Create a temporary WorkerMonitor with ServiceManager
        config = ServiceConfig(nats_url=nats_url)
        service_manager = ServiceManager(config)
        monitor = WorkerMonitor(service_manager=service_manager, nats_url=nats_url)

        # Initialize logger and error handler for this static method
        logger = StructuredLogger("worker_list")
        error_handler = ErrorHandler(logger)

        try:
            with logger.operation_context("list_workers", nats_url=nats_url):
                return await monitor.list_workers(nats_url)
        except Exception as e:
            wrapped_error = wrap_naq_exception(e, "Error listing workers")
            error_handler.handle_error(wrapped_error, {"nats_url": nats_url})
            raise
        finally:
            # Clean up the service manager
            try:
                await service_manager.cleanup_all()
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Error cleaning up service manager"
                )
                error_handler.handle_error(wrapped_error)

    @staticmethod
    @timing(threshold_ms=1000)
    def list_workers_sync(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        from ..services import ServiceConfig, ServiceManager
        from ..utils.error_handling import ErrorHandler, wrap_naq_exception
        from ..utils.logging import StructuredLogger

        # Create a temporary WorkerMonitor with ServiceManager
        config = ServiceConfig(nats_url=nats_url)
        service_manager = ServiceManager(config)
        monitor = WorkerMonitor(service_manager=service_manager, nats_url=nats_url)

        # Initialize logger and error handler for this static method
        logger = StructuredLogger("worker_list_sync")
        error_handler = ErrorHandler(logger)

        try:
            with logger.operation_context("list_workers_sync", nats_url=nats_url):
                return monitor.list_workers_sync(nats_url)
        except Exception as e:
            wrapped_error = wrap_naq_exception(e, "Error listing workers synchronously")
            error_handler.handle_error(wrapped_error, {"nats_url": nats_url})
            raise
        finally:
            # Clean up the service manager
            import asyncio

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Create a task for cleanup if loop is running
                    asyncio.create_task(service_manager.cleanup_all())
                else:
                    # Run cleanup directly if loop is not running
                    loop.run_until_complete(service_manager.cleanup_all())
            except Exception as e:
                wrapped_error = wrap_naq_exception(
                    e, "Error cleaning up service manager"
                )
                error_handler.handle_error(wrapped_error)

    # --- Sync interface for long-running worker using anyio.BlockingPortal ---
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
