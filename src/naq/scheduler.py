# src/naq/scheduler.py
import asyncio
import datetime
import signal
import socket
import time
import uuid
from datetime import timezone
from typing import Any, Dict, Optional

from loguru import logger

from .utils import setup_logging

# Attempt to import croniter only if needed later
try:
    from croniter import croniter
except ImportError:
    croniter = None  # type: ignore

from .exceptions import NaqConnectionError
from .settings import (
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
)
from .services import ServiceManager
from .services import ConnectionService
from .services import KVStoreService
from .services import EventService
from .services import SchedulerService
from .service_context import long_lived_service_context


class LeaderElection:
    """
    Handles leader election for high availability schedulers using NATS KV store.
    """

    def __init__(
        self,
        instance_id: str,
        lock_ttl: int = SCHEDULER_LOCK_TTL_SECONDS,
        lock_renew_interval: int = SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
        kv_store_service: Optional[KVStoreService] = None,
    ):
        self.instance_id = instance_id
        self.lock_ttl = lock_ttl
        self.lock_renew_interval = lock_renew_interval
        self._shutdown_event = asyncio.Event()
        self._is_leader = False
        self._lock_renewal_task: Optional[asyncio.Task] = None
        self._kv_store_service = kv_store_service

    async def initialize(self) -> None:
        """Initialize the leader election system."""
        try:
            logger.info(
                f"Initialized leader election for KV store '{SCHEDULER_LOCK_KV_NAME}'"
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize leader election with KV store "
                f"'{SCHEDULER_LOCK_KV_NAME}': {e}"
            )
            raise NaqConnectionError(f"Failed to access lock KV store: {e}") from e

    async def try_become_leader(self) -> bool:
        """
        Attempt to acquire the leader lock.

        Returns:
            True if this instance is now the leader, False otherwise
        """
        if not self._kv_store_service:
            logger.error("KVStoreService not available for leader election")
            return False

        try:
            # Try to get the current lock
            try:
                entry = await self._kv_store_service.get(
                    SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY, deserialize=True
                )
                if entry:
                    lock_data = entry
                    # Lock exists - see if it's expired
                    lock_time = lock_data.get("timestamp", 0)
                    lock_owner = lock_data.get("instance_id", "unknown")

                    # If lock is still valid and owned by someone else,
                    # can't become leader
                    if (
                        time.time() - lock_time < self.lock_ttl
                        and lock_owner != self.instance_id
                    ):
                        logger.debug(
                            f"Lock already held by '{lock_owner}', cannot become leader"
                        )
                        return False
            except Exception:
                # No existing lock, we can try to take it
                pass

            # Attempt to set the lock with our instance ID
            lock_data = {
                "instance_id": self.instance_id,
                "timestamp": time.time(),
                "hostname": socket.gethostname(),
            }
            await self._kv_store_service.put(
                SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY, lock_data
            )
            logger.info(
                f"Acquired scheduler leader lock. This instance ({self.instance_id}) "
                f"is now the leader."
            )
            
            # Log leader_elected event
            if self._kv_store_service:
                try:
                    # Try to get event service from the KV store service
                    # This is a bit of a hack since we don't have direct access to event service
                    # but we need to log the leader election event
                    logger.info(
                        f"Leader elected: {self.instance_id} at {time.time()}"
                    )
                except Exception as e:
                    logger.debug(f"Could not log leader_elected event: {e}")
            
            return True

        except Exception as e:
            logger.error(f"Error during leader election: {e}")
            return False

    async def start_renewal_task(self, running_flag: bool) -> None:
        """Start a background task to renew the leader lock."""
        self._shutdown_event.clear()
        self._is_leader = True
        self._lock_renewal_task = asyncio.create_task(
            self._renew_leader_lock(running_flag)
        )

    async def _renew_leader_lock(self, running_flag: bool) -> None:
        """
        Periodically renew the leader lock to maintain leadership.
        Runs as a background task while scheduler is active.
        """
        while running_flag and self._is_leader:
            try:
                if not self._kv_store_service:
                    logger.error("KVStoreService not available for lock renewal")
                    self._is_leader = False
                    break

                # Update the lock with fresh timestamp
                lock_data = {
                    "instance_id": self.instance_id,
                    "timestamp": time.time(),
                    "hostname": socket.gethostname(),
                }
                await self._kv_store_service.put(
                    SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY, lock_data
                )
                logger.debug(
                    f"Renewed leader lock. Next renewal in {self.lock_renew_interval}s"
                )

                # Wait for renewal interval or until shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=self.lock_renew_interval
                    )
                    break  # Shutdown was triggered
                except asyncio.TimeoutError:
                    # This is expected - continue the loop
                    pass

            except Exception as e:
                logger.error(f"Failed to renew leader lock: {e}")
                # Lost leadership
                self._is_leader = False
                break

        logger.info("Leader lock renewal task exiting")
        
        # Log leader_revoked event if we were previously the leader
        if self._is_leader:
            logger.info(
                f"Leader revoked: {self.instance_id} at {time.time()}"
            )
        
        self._is_leader = False

    async def stop_renewal_task(self) -> None:
        """Stop the lock renewal task and signal that we're no longer leader."""
        self._shutdown_event.set()
        if self._lock_renewal_task and not self._lock_renewal_task.done():
            self._lock_renewal_task.cancel()
            try:
                await self._lock_renewal_task
            except asyncio.CancelledError:
                pass
        self._is_leader = False

    async def release_lock(self) -> None:
        """Explicitly release the leader lock when shutting down."""
        if self._is_leader:
            try:
                if self._kv_store_service:
                    await self._kv_store_service.delete(
                        SCHEDULER_LOCK_KV_NAME, SCHEDULER_LOCK_KEY, purge=True
                    )
                    logger.info("Released scheduler leader lock")
                    # Log leader_revoked event
                    logger.info(
                        f"Leader revoked: {self.instance_id} at {time.time()}"
                    )
            except Exception as e:
                logger.error(f"Error releasing leader lock: {e}")
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        """Returns True if this instance is currently the leader."""
        return self._is_leader


class ScheduledJobProcessor:
    """
    Handles the processing of scheduled jobs using context managers.
    """

    def __init__(
        self,
        connection_service: Optional[ConnectionService] = None,
        kv_store_service: Optional[KVStoreService] = None,
        event_service: Optional[EventService] = None,
    ):
        # Services are kept for compatibility but not used in context manager approach
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service
        self._event_service = event_service

    async def _enqueue_job(self, queue_name: str, subject: str, payload: bytes) -> bool:
        """
        Enqueue a job payload to the specified queue subject.

        Returns:
            True if enqueuing was successful, False otherwise
        """
        if not self._connection_service:
            logger.error("ConnectionService not available for enqueuing job")
            return False

        try:
            # Use the connection service to publish the job
            js = self._connection_service.js
            if not js:
                logger.error("JetStream not available for enqueuing job")
                return False

            ack = await js.publish(subject=subject, payload=payload)
            logger.debug(
                f"Enqueued job to {subject}. Stream: {ack.stream}, Seq: {ack.seq}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue job payload to subject '{subject}': {e}")
            return False

    def _calculate_next_runtime(
        self, schedule_data: Dict[str, Any], scheduled_ts: float
    ) -> Optional[float]:
        """
        Calculate the next runtime for a recurring job.

        Args:
            schedule_data: The job schedule data
            scheduled_ts: The previous scheduled timestamp

        Returns:
            Next runtime timestamp or None if not recurring
        """
        cron = schedule_data.get("cron")
        interval_seconds = schedule_data.get("interval_seconds")
        next_scheduled_ts = None

        if cron:
            if croniter is None:
                logger.error(
                    "Cannot reschedule cron job: 'croniter' library not installed."
                )
                return None

            # Calculate next run time based on the previous scheduled time
            base_dt = datetime.datetime.fromtimestamp(scheduled_ts, timezone.utc)
            cron_iter = croniter(cron, base_dt)
            next_scheduled_ts = cron_iter.get_next(datetime.datetime).timestamp()

        elif interval_seconds:
            # Calculate next run time based on the previous scheduled time + interval
            next_scheduled_ts = scheduled_ts + interval_seconds

        return next_scheduled_ts

    async def process_jobs(self, is_leader: bool) -> tuple[int, int]:
        """
        Check the KV store for jobs ready to run and process them.

        Args:
            is_leader: Whether this instance is the leader

        Returns:
            Tuple of (processed_count, error_count)
        """
        if not is_leader:
            return 0, 0

        processed_count = 0
        error_count = 0

        try:
            # Note: KVStoreService doesn't have a direct keys() method,
            # so we'll need to handle this differently
            # For now, we'll rely on the SchedulerService to handle this
            logger.debug("Scheduled job processing delegated to SchedulerService")
            return 0, 0

        except Exception as e:
            logger.exception(f"Unexpected error during scheduler check: {e}")
            error_count += 1

        return processed_count, error_count

    async def _process_single_job(
        self, key_bytes: bytes, now_ts: float
    ) -> tuple[int, int]:
        """
        Process a single scheduled job.

        Note: This method is now simplified as most processing is handled by
        SchedulerService.

        Args:
            key_bytes: The KV store key
            now_ts: Current timestamp

        Returns:
            Tuple of (processed_count, error_count)
        """
        # This method is now a no-op as processing is delegated to SchedulerService
        # The SchedulerService.trigger_due_jobs() method handles all the logic
        return 0, 0


class Scheduler:
    """
    Scheduler for NAQ jobs. Polls the scheduled jobs KV store and enqueues jobs
    that are ready.
    Supports high availability through leader election using NATS KV store.
    """

    def __init__(
        self,
        nats_url: Optional[str] = None,
        service_manager: Optional[ServiceManager] = None,
        poll_interval: float = 1.0,  # Check for jobs every second
        instance_id: Optional[str] = None,  # For HA leader election
        enable_ha: bool = True,  # Whether to enable HA leader election
        config: Optional[object] = None,  # GlobalServiceConfig for backward compatibility
    ):
        # For backward compatibility, support both nats_url and service_manager
        if nats_url is not None:
            self._nats_url = nats_url
            self._service_manager = None  # Will be created in _connect()
            self._config = config
        elif service_manager is not None:
            self._service_manager = service_manager
            self._nats_url = None
            self._config = None
        else:
            raise ValueError("Either nats_url or service_manager must be provided")
            
        self._poll_interval = poll_interval
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Generate unique instance ID if none provided
        self._instance_id = (
            instance_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        )

        # Create components
        self._enable_ha = enable_ha
        self._leader_election = LeaderElection(
            instance_id=self._instance_id,
            lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
            lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
            kv_store_service=None,  # Will be set during _connect()
        )
        self._job_processor: Optional[ScheduledJobProcessor] = None

        # Services will be initialized during _connect()
        self._connection_service: Optional[ConnectionService] = None
        self._kv_store_service: Optional[KVStoreService] = None
        self._event_service: Optional[EventService] = None
        self._scheduler_service: Optional[SchedulerService] = None

        setup_logging()  # Set up logging

    async def _connect(self) -> None:
        """Establish service connections and initialize components."""
        try:
            # Use long-lived service context for scheduler lifecycle
            if self._nats_url:
                # Create service manager from nats_url
                async with long_lived_service_context(
                    nats_url=self._nats_url,
                    global_config=self._config,
                    logger_name=f"naq.scheduler.{self._instance_id}"
                ) as service_manager:
                    self._service_manager = service_manager
                    await self._initialize_services(service_manager)
            else:
                # Use provided service manager
                async with long_lived_service_context(
                    self._service_manager,
                    logger_name=f"naq.scheduler.{self._instance_id}"
                ) as service_manager:
                    await self._initialize_services(service_manager)

        except Exception as e:
            logger.error(f"Failed to connect to services: {e}")
            raise NaqConnectionError(f"Failed to connect to services: {e}") from e

    async def _initialize_services(self, service_manager: ServiceManager) -> None:
        """Initialize services from service manager."""
        # Get services from service manager
        self._connection_service = await service_manager.get_service(
            "connection", ConnectionService
        )
        self._kv_store_service = await service_manager.get_service(
            "kv_store", KVStoreService
        )
        self._event_service = await service_manager.get_service(
            "event", EventService
        )
        self._scheduler_service = await service_manager.get_service(
            "scheduler", SchedulerService
        )

        logger.info(
            f"Scheduler connected to services and KV store "
            f"'{SCHEDULED_JOBS_KV_NAME}'."
        )

        # Initialize components
        if self._enable_ha:
            # Set the KV store service for leader election
            self._leader_election._kv_store_service = self._kv_store_service
            await self._leader_election.initialize()

        # Create job processor
        self._job_processor = ScheduledJobProcessor(
            self._connection_service, self._kv_store_service, self._event_service
        )

    async def run(self) -> None:
        """Starts the scheduler loop with leader election."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()

            logger.info(
                f"Scheduler instance {self._instance_id} started. "
                f"Polling interval: {self._poll_interval}s"
            )
            logger.info(
                f"High availability mode: "
                f"{'enabled' if self._enable_ha else 'disabled'}"
            )

            # Drift-free loop: each cycle aims to start every poll_interval seconds
            while self._running:
                cycle_start = time.time()

                # Check leadership status (if HA is enabled)
                was_leader = self.is_leader

                if self._enable_ha:
                    if not self.is_leader:
                        # Try to become leader
                        if await self._leader_election.try_become_leader():
                            # Just became leader, start renewal task
                            await self._leader_election.start_renewal_task(
                                self._running
                            )
                else:
                    # If HA is disabled, always consider self as leader
                    if not was_leader:
                        self._leader_election._is_leader = True

                # Process jobs only if leader and scheduler service exists
                if self.is_leader and self._scheduler_service:
                    processed, errors = await self._scheduler_service.trigger_due_jobs()
                    # Log summary only if something happened
                    if processed > 0 or errors > 0:
                        logger.info(
                            f"Scheduler processed {processed} ready jobs, "
                            f"encountered {errors} errors."
                        )
                else:
                    logger.debug("Not the leader, waiting...")

                # Compute remaining sleep to align next cycle start
                elapsed = time.time() - cycle_start
                remaining = self._poll_interval - elapsed
                # If shutdown was triggered, exit promptly
                if self._shutdown_event.is_set():
                    break

                if remaining <= 0:
                    # Processing took longer than poll interval; start next
                    # cycle immediately
                    continue

                try:
                    # Wait only for the remaining time or until shutdown is triggered
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=remaining
                    )
                    # If wait() finishes without timeout, shutdown was triggered
                    break
                except asyncio.TimeoutError:
                    # Timeout is expected, continue the loop on next tick
                    pass

        except Exception as e:
            logger.exception(f"Scheduler run loop encountered an error: {e}")
        finally:
            logger.info("Scheduler shutting down...")

            # Stop leadership processes
            if self._enable_ha:
                await self._leader_election.stop_renewal_task()
                await self._leader_election.release_lock()

            # Close connections
            await self._close()
            logger.info("Scheduler shutdown complete.")

    async def _close(self) -> None:
        """Cleans up resources."""
        # Services are managed by the ServiceManager, so we don't close them here
        self._connection_service = None
        self._kv_store_service = None
        self._event_service = None
        self._scheduler_service = None
        self._job_processor = None

    def signal_handler(self, sig, frame) -> None:
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self) -> None:
        """Installs signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    @property
    def is_leader(self) -> bool:
        """Returns True if this scheduler instance is currently the leader."""
        # If HA is disabled, we're always the leader
        if not self._enable_ha:
            return True
        return self._leader_election.is_leader

    async def __aenter__(self):
        """Enter the async context manager."""
        # Initialize the service manager if needed
        if self._nats_url:
            # Create service manager from nats_url
            self._service_manager = await long_lived_service_context(
                nats_url=self._nats_url,
                global_config=self._config,
                logger_name=f"naq.scheduler.{self._instance_id}"
            ).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager."""
        if self._nats_url and self._service_manager:
            await self._service_manager.__aexit__(exc_type, exc_val, exc_tb)
        await self._close()
