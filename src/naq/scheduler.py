# src/naq/scheduler.py
import asyncio
import datetime
import signal
import socket
import time
import uuid
from datetime import timezone
from typing import Any, Dict, Optional

import cloudpickle
import nats
from loguru import logger
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue

from .utils import setup_logging

# Attempt to import croniter only if needed later
try:
    from croniter import croniter
except ImportError:
    croniter = None  # type: ignore

from .services import ServiceManager
from .services.kv_stores import KVStoreService
from .services.connection import ConnectionService
from .services.events import EventService
from .exceptions import NaqConnectionError, SerializationError
from .settings import DEFAULT_NATS_URL
from .settings import (
    MAX_SCHEDULE_FAILURES,
    NAQ_PREFIX,
    SCHEDULED_JOB_STATUS,
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
)


class LeaderElection:
    """
    Handles leader election for high availability schedulers using NATS Key-Value store.
    """

    def __init__(
        self,
        instance_id: str,
        service_manager: ServiceManager,
        lock_ttl: int = SCHEDULER_LOCK_TTL_SECONDS,
        lock_renew_interval: int = SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    ):
        self.instance_id = instance_id
        self.service_manager = service_manager
        self.lock_ttl = lock_ttl
        self.lock_renew_interval = lock_renew_interval
        self._lock_kv: Optional[KeyValue] = None
        self._shutdown_event = asyncio.Event()
        self._is_leader = False
        self._lock_renewal_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initialize the leader election system using services."""
        try:
            kv_service = await self.service_manager.get_service(KVStoreService)
            self._lock_kv = await kv_service.get_kv_store(
                SCHEDULER_LOCK_KV_NAME,
                description="Scheduler leader election lock",
                history=1,  # Only need latest value
                create_if_not_exists=True
            )
            logger.info(
                f"Initialized leader election KV store '{SCHEDULER_LOCK_KV_NAME}'"
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize lock KV store '{SCHEDULER_LOCK_KV_NAME}': {e}"
            )
            raise NaqConnectionError(
                f"Failed to access lock KV store: {e}"
            ) from e

    async def try_become_leader(self) -> bool:
        """
        Attempt to acquire the leader lock.

        Returns:
            True if this instance is now the leader, False otherwise
        """
        if not self._lock_kv:
            logger.error("Lock KV store not initialized")
            return False

        try:
            # Try to get the current lock
            try:
                entry = await self._lock_kv.get(SCHEDULER_LOCK_KEY)
                if entry:
                    # Lock exists - see if it's expired
                    lock_data = cloudpickle.loads(entry.value)
                    lock_time = lock_data.get("timestamp", 0)
                    lock_owner = lock_data.get("instance_id", "unknown")

                    # If lock is still valid and owned by someone else, can't become leader
                    if (
                        time.time() - lock_time < self.lock_ttl
                        and lock_owner != self.instance_id
                    ):
                        logger.debug(
                            f"Lock already held by '{lock_owner}', cannot become leader"
                        )
                        return False
            except KeyNotFoundError:
                # No existing lock, we can try to take it
                pass

            # Attempt to set the lock with our instance ID
            lock_data = {
                "instance_id": self.instance_id,
                "timestamp": time.time(),
                "hostname": socket.gethostname(),
            }
            serialized_lock = cloudpickle.dumps(lock_data)
            await self._lock_kv.put(SCHEDULER_LOCK_KEY, serialized_lock)
            logger.info(
                f"Acquired scheduler leader lock. This instance ({self.instance_id}) is now the leader."
            )
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
                if self._lock_kv:
                    # Update the lock with fresh timestamp
                    lock_data = {
                        "instance_id": self.instance_id,
                        "timestamp": time.time(),
                        "hostname": socket.gethostname(),
                    }
                    serialized_lock = cloudpickle.dumps(lock_data)
                    await self._lock_kv.put(SCHEDULER_LOCK_KEY, serialized_lock)
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
        if self._is_leader and self._lock_kv:
            try:
                await self._lock_kv.delete(SCHEDULER_LOCK_KEY)
                logger.info("Released scheduler leader lock")
            except Exception as e:
                logger.error(f"Error releasing leader lock: {e}")
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        """Returns True if this instance is currently the leader."""
        return self._is_leader


class ScheduledJobProcessor:
    """
    Handles the processing of scheduled jobs from the NATS KV store.
    """

    def __init__(self, service_manager: ServiceManager, kv: KeyValue):
        self.service_manager = service_manager
        self._kv = kv

    async def _enqueue_job(self, queue_name: str, subject: str, payload: bytes) -> bool:
        """
        Enqueue a job payload to the specified queue subject using services.

        Returns:
            True if enqueuing was successful, False otherwise
        """
        try:
            connection_service = await self.service_manager.get_service(ConnectionService)
            async with connection_service.jetstream_scope() as js:
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

        now_ts = time.time()
        processed_count = 0
        error_count = 0

        try:
            keys = await self._kv.keys()

            if not keys:
                logger.debug("No scheduled jobs found in KV store.")
                return 0, 0

            logger.debug(f"Found {len(keys)} potential scheduled jobs.")

            for key_bytes in keys:
                # Abort if we're no longer leader
                if not is_leader:
                    logger.info(
                        "Lost leadership during job processing loop, stopping check."
                    )
                    break

                # Process this job
                processed, had_error = await self._process_single_job(key_bytes, now_ts)
                processed_count += processed
                error_count += had_error

        except nats.errors.Error as e:
            # Handle the "no keys found" error as a normal case
            error_message = str(e).lower()
            if "no keys found" in error_message:
                logger.debug(
                    "No scheduled jobs found (received NATS 'no keys found' message)."
                )
            else:
                logger.error(f"NATS error during scheduler check: {e}")
                error_count += 1
        except Exception as e:
            logger.exception(f"Unexpected error during scheduler check: {e}")
            error_count += 1

        return processed_count, error_count

    async def _process_single_job(
        self, key_bytes: bytes, now_ts: float
    ) -> tuple[int, int]:
        """
        Process a single scheduled job.

        Args:
            key_bytes: The KV store key
            now_ts: Current timestamp

        Returns:
            Tuple of (processed_count, error_count)
        """
        processed = 0
        errors = 0
        key = key_bytes.decode("utf-8") if isinstance(key_bytes, bytes) else key_bytes

        try:
            entry = await self._kv.get(key_bytes)
            if entry is None:
                return 0, 0

            schedule_data: Dict[str, Any] = cloudpickle.loads(entry.value)

            # Skip paused jobs
            status = schedule_data.get("status")
            if status == SCHEDULED_JOB_STATUS.PAUSED:
                logger.debug(f"Skipping paused job '{key}'")
                return 0, 0

            # Skip failed jobs that exceeded retry attempts
            if status == SCHEDULED_JOB_STATUS.FAILED:
                logger.debug(f"Skipping failed job '{key}' that exceeded retry limits")
                return 0, 0

            # Check if job is ready to run
            scheduled_ts = schedule_data.get("scheduled_timestamp_utc")
            if scheduled_ts is None or scheduled_ts > now_ts:
                return 0, 0  # Not ready yet

            # Job is ready to run
            job_id = schedule_data.get("job_id", "unknown")
            queue_name = schedule_data.get("queue_name")
            original_payload = schedule_data.get("_orig_job_payload")
            cron = schedule_data.get("cron")
            interval_seconds = schedule_data.get("interval_seconds")
            repeat = schedule_data.get("repeat")  # None means infinite

            # Validate job data
            if not queue_name or not original_payload:
                logger.error(
                    f"Invalid schedule data for key '{key}' (missing queue_name or payload). Deleting."
                )
                await self._kv.delete(key_bytes)
                return 0, 1

            # Enqueue the job
            logger.info(f"Job {job_id} is ready. Enqueueing to queue '{queue_name}'.")
            target_subject = f"{NAQ_PREFIX}.queue.{queue_name}"

            enqueue_success = await self._enqueue_job(
                queue_name, target_subject, original_payload
            )

            # Track success/failure
            if enqueue_success:
                processed += 1
                # Reset failure count on success
                schedule_data["schedule_failure_count"] = 0
                schedule_data["last_enqueued_utc"] = now_ts
                
                # Log schedule triggered event using service
                try:
                    event_service = await self.service_manager.get_service(EventService)
                    await event_service.log_schedule_triggered(
                        job_id=job_id,
                        queue_name=queue_name,
                        trigger_timestamp=now_ts,
                    )
                except Exception as e:
                    logger.warning(f"Failed to log schedule triggered event for job {job_id}: {e}")
            else:
                errors += 1
                # Track failures for potential retry limiting
                failure_count = schedule_data.get("schedule_failure_count", 0) + 1
                schedule_data["schedule_failure_count"] = failure_count

                # Check if we should mark the job as permanently failed
                if MAX_SCHEDULE_FAILURES and failure_count >= MAX_SCHEDULE_FAILURES:
                    logger.warning(
                        f"Job {job_id} has failed scheduling {failure_count} times, marking as failed"
                    )
                    schedule_data["status"] = SCHEDULED_JOB_STATUS.FAILED
                    serialized_data = cloudpickle.dumps(schedule_data)
                    await self._kv.put(key_bytes, serialized_data)
                    return 0, 1
                else:
                    # Just log and continue - will retry on next check cycle
                    logger.warning(
                        f"Failed to enqueue job {job_id} (attempt {failure_count}). "
                        f"Will retry on next cycle."
                    )
                    serialized_data = cloudpickle.dumps(schedule_data)
                    await self._kv.put(key_bytes, serialized_data)
                    return 0, 1

            # Handle recurrence or deletion
            if not enqueue_success:
                return processed, errors

            # Calculate next run time if this is a recurring job
            next_scheduled_ts = self._calculate_next_runtime(
                schedule_data, scheduled_ts
            )
            delete_entry = True  # Assume deletion unless rescheduled

            if next_scheduled_ts is not None:
                # Check repeat count
                if repeat is not None:
                    if repeat > 1:
                        schedule_data["repeat"] = repeat - 1
                        schedule_data["scheduled_timestamp_utc"] = next_scheduled_ts
                        schedule_data["next_run_utc"] = next_scheduled_ts
                        delete_entry = False  # Reschedule
                        logger.debug(
                            f"Rescheduling job {job_id} for {next_scheduled_ts}. Repeats left: {repeat - 1}"
                        )
                    else:
                        # Last repetition
                        logger.debug(f"Job {job_id} finished its repetitions.")
                        delete_entry = True
                else:
                    # Infinite repeat
                    schedule_data["scheduled_timestamp_utc"] = next_scheduled_ts
                    schedule_data["next_run_utc"] = next_scheduled_ts
                    delete_entry = False  # Reschedule
                    logger.debug(
                        f"Rescheduling job {job_id} for {next_scheduled_ts} (infinite)."
                    )

            # Update or delete the KV entry
            if delete_entry:
                logger.debug(f"Deleting schedule entry for job {job_id}.")
                await self._kv.delete(key_bytes)
            else:
                logger.debug(f"Updating schedule entry for job {job_id}.")
                updated_payload = cloudpickle.dumps(schedule_data)
                await self._kv.put(key_bytes, updated_payload)

            return processed, errors

        except SerializationError as e:
            logger.error(
                f"Failed to deserialize schedule data for key '{key}': {e}. Deleting entry."
            )
            try:
                await self._kv.delete(key_bytes)
            except Exception as del_e:
                logger.error(
                    f"Failed to delete corrupted schedule entry '{key}': {del_e}"
                )
            return 0, 1
        except Exception as e:
            logger.exception(f"Error processing schedule key '{key}': {e}")
            return 0, 1


class Scheduler:
    """
    Scheduler for NAQ jobs. Polls the scheduled jobs KV store and enqueues jobs that are ready.
    Supports high availability through leader election using NATS KV store.
    """

    def __init__(
        self,
        config: Optional[Any] = None,
        nats_url: str = DEFAULT_NATS_URL,  # Legacy parameter
        poll_interval: float = 1.0,  # Check for jobs every second
        instance_id: Optional[str] = None,  # For HA leader election
        enable_ha: bool = True,  # Whether to enable HA leader election
    ):
        # Handle configuration - prefer passed config over legacy parameters
        if config is not None:
            self._config = config
            # Extract values from config if available
            self._nats_url = config.get('nats_url', nats_url)
            if 'poll_interval' in config:
                self._poll_interval = config['poll_interval']
            else:
                self._poll_interval = poll_interval
        else:
            # Fallback to legacy parameters for backward compatibility
            self._config = {
                'nats_url': nats_url,
                'poll_interval': poll_interval,
                'enable_ha': enable_ha,
            }
            self._nats_url = nats_url
            self._poll_interval = poll_interval
        
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Initialize service manager
        self._service_manager = ServiceManager(self._config)

        # Generate unique instance ID if none provided
        self._instance_id = (
            instance_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        )

        # Create components
        self._enable_ha = enable_ha
        self._leader_election = LeaderElection(
            instance_id=self._instance_id,
            service_manager=self._service_manager,
            lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
            lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
        )
        self._job_processor: Optional[ScheduledJobProcessor] = None
        self._scheduled_jobs_kv = None

        setup_logging()  # Set up logging

    async def _connect(self) -> None:
        """Initialize services and components."""
        try:
            # Initialize the service manager
            await self._service_manager.initialize_all()
            
            # Get the KV store service and initialize scheduled jobs KV
            kv_service = await self._service_manager.get_service(KVStoreService)
            self._scheduled_jobs_kv = await kv_service.get_kv_store(
                SCHEDULED_JOBS_KV_NAME,
                description="Scheduler job schedule storage",
                create_if_not_exists=True
            )
            logger.info(
                f"Scheduler connected to services and KV store '{SCHEDULED_JOBS_KV_NAME}'."
            )

            # Initialize components
            if self._enable_ha:
                await self._leader_election.initialize()

            # Create job processor
            if self._scheduled_jobs_kv:
                self._job_processor = ScheduledJobProcessor(self._service_manager, self._scheduled_jobs_kv)
                
        except Exception as e:
            logger.error(
                f"Failed to initialize scheduler services: {e}"
            )
            raise NaqConnectionError(
                f"Failed to initialize services: {e}"
            ) from e

    async def run(self) -> None:
        """Starts the scheduler loop with leader election."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()

            logger.info(
                f"Scheduler instance {self._instance_id} started. Polling interval: {self._poll_interval}s"
            )
            logger.info(
                f"High availability mode: {'enabled' if self._enable_ha else 'disabled'}"
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

                # Process jobs only if leader and job processor exists
                if self.is_leader and self._job_processor:
                    processed, errors = await self._job_processor.process_jobs(
                        self.is_leader
                    )
                    # Log summary only if something happened
                    if processed > 0 or errors > 0:
                        logger.info(
                            f"Scheduler processed {processed} ready jobs, encountered {errors} errors."
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
                    # Processing took longer than poll interval; start next cycle immediately
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
        """Clean up service manager and resources."""
        await self._service_manager.cleanup_all()
        self._scheduled_jobs_kv = None
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
