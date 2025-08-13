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
import traceback
import uuid
from typing import Any, Dict, List, Optional, Sequence

import nats
from loguru import logger
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig

from ..connection import (
    close_nats_connection,
    ensure_stream,
    get_jetstream_context,
    get_nats_connection,
)
from ..exceptions import NaqException
from ..models.jobs import Job
from ..models.enums import JOB_STATUS, WORKER_STATUS
from ..settings import (
    DEFAULT_NATS_URL,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_WORKER_TTL_SECONDS,
    NAQ_PREFIX,
    DEFAULT_ACK_WAIT_SECONDS,
    ACK_WAIT_PER_QUEUE,
    DEFAULT_QUEUE_NAME,
)
from ..utils import run_async_from_sync, setup_logging
from .status import WorkerStatusManager
from .jobs import JobStatusManager
from .failed import FailedJobHandler
from .controller import WorkerController
from .processing import JobProcessor
from .monitoring import WorkerMonitor
from .sync_interface import WorkerSyncInterface


class Worker:
    """
    A worker that fetches jobs from specified NATS queues (subjects) and executes them.
    Uses JetStream pull consumers for fetching jobs. Coordinates with specialized manager
    classes for status tracking, job management, and failed job handling.
    """

    def __init__(
        self,
        queues: Optional[Sequence[str] | str] = None,
        nats_url: str = DEFAULT_NATS_URL,
        concurrency: int = 10,  # Max concurrent jobs
        worker_name: Optional[str] = None,  # For durable consumer names
        heartbeat_interval: int = DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECONDS,
        worker_ttl: int = DEFAULT_WORKER_TTL_SECONDS,
        ack_wait: Optional[
            int | Dict[str, int]
        ] = None,  # seconds; can be per-queue dict
        module_paths: Optional[Sequence[str] | str] = None,
    ):
        if isinstance(queues, str):
            queues = [queues]
        if not queues:
            queues = [DEFAULT_QUEUE_NAME]

        # Preserve order while ensuring uniqueness using dict.fromkeys()
        self.queue_names: List[str] = list(dict.fromkeys(queues))
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
        self.worker_id = f"{base_name}-{os.getpid()}-{uuid.uuid4().hex[:6]}"

        self._heartbeat_interval = heartbeat_interval
        self._worker_ttl = worker_ttl
        # Ack wait configuration
        self._ack_wait_arg: Optional[int | Dict[str, int]] = ack_wait

        # Connection and state variables
        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._consumers: Dict[
            str, nats.js.api.PullSubscribe
        ] = {}  # Track queue consumers

        # JetStream stream name
        self.stream_name = f"{NAQ_PREFIX}_jobs"
        # Durable consumer name prefix
        self.consumer_prefix = f"{NAQ_PREFIX}-worker"

        # Create component managers
        self.status_manager = WorkerStatusManager(self)
        self.job_manager = JobStatusManager(self)
        self.failed_handler = FailedJobHandler(self)
        self.job_processor = JobProcessor(self)
        self.sync_interface = WorkerSyncInterface(self)

        setup_logging()  # Setup logging

    async def _connect(self) -> None:
        """Establish NATS connection, JetStream context, and initialize components."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            logger.info(f"Worker '{self.worker_id}' connected to NATS and JetStream.")

            # Initialize component managers
            await self.status_manager.start_heartbeat_loop()
            await self.job_manager.initialize(self._js)
            await self.failed_handler.initialize(self._js)

    def _resolve_ack_wait_seconds(self, queue_name: str) -> int:
        """
        Resolve ack_wait seconds for a given queue based on precedence:
        1) Per-queue value provided via ack_wait dict argument
        2) Single ack_wait int argument
        3) Environment-driven per-queue settings.ACK_WAIT_PER_QUEUE
        4) settings.DEFAULT_ACK_WAIT_SECONDS
        Ensures a positive integer, falling back to default if invalid.
        """
        try:
            # 1) per-queue dict from constructor
            if (
                isinstance(self._ack_wait_arg, dict)
                and queue_name in self._ack_wait_arg
            ):
                v = int(self._ack_wait_arg[queue_name])
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
            # 2) single int from constructor
            if isinstance(self._ack_wait_arg, int) and self._ack_wait_arg > 0:
                return int(self._ack_wait_arg)
            # 3) env per-queue
            if queue_name in ACK_WAIT_PER_QUEUE:
                v = int(ACK_WAIT_PER_QUEUE[queue_name])
                return v if v > 0 else DEFAULT_ACK_WAIT_SECONDS
        except Exception:
            pass
        # 4) default
        return DEFAULT_ACK_WAIT_SECONDS

    async def _subscribe_to_queue(self, queue_name: str) -> None:
        """Creates a durable consumer and starts fetching messages for a queue."""
        if not self._js:
            raise NaqException("JetStream context not available.")

        subject = f"{NAQ_PREFIX}.queue.{queue_name}"
        durable_name = f"{self.consumer_prefix}-{queue_name}"
        logger.info(
            f"Setting up consumer for queue '{queue_name}' (subject: {subject}, durable: {durable_name})"
        )

        try:
            # Resolve ack_wait seconds for this queue
            ack_wait_seconds = self._resolve_ack_wait_seconds(queue_name)
            logger.info(
                f"Creating consumer for queue '{queue_name}' with ack_wait={ack_wait_seconds}s"
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
            logger.info(
                f"Pull consumer '{durable_name}' created for subject '{subject}'."
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
                        logger.debug(
                            f"Fetched {len(msgs)} messages from '{durable_name}'"
                        )

                    for msg in msgs:
                        # Acquire semaphore before starting processing task
                        await self._semaphore.acquire()
                        # Create task to process the message
                        task = asyncio.create_task(self.job_processor.process_message(msg))
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
                    logger.warning(
                        f"Consumer '{durable_name}' not found. Stopping fetch loop."
                    )
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching from consumer '{durable_name}': {e}",
                        exc_info=True,
                    )
                    await asyncio.sleep(1)  # Wait before retrying fetch

        except Exception as e:
            logger.error(
                f"Failed to subscribe or run fetch loop for queue '{queue_name}': {e}",
                exc_info=True,
            )


    async def run(self) -> None:
        """Starts the worker, connects to NATS, and begins processing jobs."""
        self._running = True
        self._shutdown_event.clear()

        try:
            await self._connect()

            # Register worker initially
            await self.status_manager.update_status(
                status=WORKER_STATUS.STARTING
            )

            # Start heartbeat task
            await self.status_manager.start_heartbeat_loop()

            # Ensure the main work stream exists
            await ensure_stream(
                js=self._js,
                stream_name=self.stream_name,
                subjects=[f"{NAQ_PREFIX}.queue.*"],
            )

            # Start subscription tasks for each queue
            subscription_tasks = [
                asyncio.create_task(self._subscribe_to_queue(q_name))
                for q_name in self.queue_names
            ]

            logger.info(
                f"Worker '{self.worker_id}' started. Listening on queues: {self.queue_names}. "
                f"Concurrency: {self._concurrency}"
            )

            # Set status to idle once subscriptions are ready
            await self.status_manager.update_status(status=WORKER_STATUS.IDLE)

            await self._shutdown_event.wait()

            logger.info("Shutdown signal received. Waiting for tasks to complete...")
            await self.status_manager.update_status(
                status=WORKER_STATUS.STOPPING
            )

            # Stop heartbeat task
            await self.status_manager.stop_heartbeat_loop()

            # Wait for active processing tasks (respecting semaphore)
            active_tasks = self._concurrency - self._semaphore._value
            if active_tasks > 0:
                logger.info(f"Waiting for {active_tasks} active job(s) to finish...")
                # Wait for semaphore to be fully released, with a timeout
                try:
                    await asyncio.wait_for(
                        self._wait_for_semaphore(), timeout=30.0
                    )  # Wait up to 30s
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for active jobs to finish.")

            # Cancel subscription loops
            for task in subscription_tasks:
                task.cancel()
            await asyncio.gather(*subscription_tasks, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Run task cancelled.")
        except Exception as e:
            logger.error(f"Worker run loop encountered an error: {e}", exc_info=True)
            await self.status_manager.update_status(
                status=WORKER_STATUS.STOPPING
            )
        finally:
            logger.info("Worker shutting down...")
            await self._close()
            logger.info("Worker shutdown complete.")

    async def _wait_for_semaphore(self) -> None:
        """Helper to wait until the semaphore value reaches concurrency limit."""
        while self._semaphore._value < self._concurrency:
            await asyncio.sleep(0.1)

    async def _close(self) -> None:
        """Closes NATS connection and cleans up resources."""
        # Set shutdown event first to prevent new message processing
        self._shutdown_event.set()
        self._running = False

        # Stop heartbeat and update status first
        try:
            await self.status_manager.stop_heartbeat_loop()
        except Exception as e:
            logger.error(f"Error stopping heartbeat: {e}")

        # Cleanup all consumers before unregistering worker
        for queue_name, consumer in self._consumers.items():
            try:
                logger.debug(f"Unsubscribing consumer for queue {queue_name}")
                await consumer.unsubscribe()
                logger.debug(f"Draining consumer for queue {queue_name}")
                await consumer.drain()
            except Exception as e:
                logger.warning(
                    f"Error cleaning up consumer for queue {queue_name}: {e}"
                )
        self._consumers.clear()

        # Unregister worker after cleanup
        try:
            await self.status_manager.unregister_worker()
        except Exception as e:
            logger.error(f"Error unregistering worker: {e}")

        # Finally close NATS connection
        try:
            await close_nats_connection()
        except Exception as e:
            logger.error(f"Error closing NATS connection: {e}")

        self._nc = None
        self._js = None

    def signal_handler(self, sig, frame) -> None:
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
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
                logger.warning(
                    "Skipping installation of signal handlers because we are not in the main thread."
                )
                return
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except ValueError as e:
            # This can happen in environments that disallow setting signals (e.g., some notebooks)
            logger.warning(f"Could not install signal handlers: {e}")
        except Exception as e:
            logger.error(
                f"Unexpected error installing signal handlers: {e}", exc_info=True
            )

    # --- Static methods for worker monitoring ---
    @staticmethod
    async def list_workers(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Lists active workers by querying the worker status KV store."""
        return await WorkerMonitor.list_workers(nats_url)

    @staticmethod
    def list_workers_sync(nats_url: str = DEFAULT_NATS_URL) -> List[Dict[str, Any]]:
        """Synchronous version of list_workers."""
        return WorkerMonitor.list_workers_sync(nats_url)

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
