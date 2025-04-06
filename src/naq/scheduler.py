import asyncio
import signal
import time
import datetime
import uuid
import socket # For getting hostname
from datetime import timezone
from typing import Optional, Dict, Any

import nats
from nats.js import JetStreamContext
from nats.js.kv import KeyValue, KeyValueEntry
from nats.js.errors import KeyNotFoundError
from loguru import logger
import cloudpickle

# Attempt to import croniter only if needed later
try:
    from croniter import croniter
except ImportError:
    croniter = None # type: ignore

from .settings import (
    DEFAULT_NATS_URL, 
    NAQ_PREFIX, 
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_TTL_SECONDS,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    MAX_SCHEDULE_FAILURES,
    SCHEDULED_JOB_STATUS_ACTIVE,
    SCHEDULED_JOB_STATUS_PAUSED,
    SCHEDULED_JOB_STATUS_FAILED
)
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .exceptions import NaqException, ConnectionError as NaqConnectionError, SerializationError

class Scheduler:
    """
    Polls the scheduled jobs KV store and enqueues jobs that are ready.
    Supports high availability through leader election using NATS KV store.
    """
    def __init__(
        self,
        nats_url: str = DEFAULT_NATS_URL,
        poll_interval: float = 1.0, # Check for jobs every second
        instance_id: Optional[str] = None, # For HA leader election
        enable_ha: bool = True, # Whether to enable HA leader election
    ):
        self._nats_url = nats_url
        self._poll_interval = poll_interval
        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._kv: Optional[KeyValue] = None
        self._lock_kv: Optional[KeyValue] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._is_leader = False
        self._lock_renewal_task: Optional[asyncio.Task] = None
        
        # For HA leader election
        self._enable_ha = enable_ha
        # Generate unique instance ID if none provided
        self._instance_id = instance_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._lock_ttl = SCHEDULER_LOCK_TTL_SECONDS
        self._lock_renew_interval = SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS

    async def _connect(self):
        """Establish NATS connection, JetStream context, and KV handles."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            
            # Connect to the scheduled jobs KV store
            try:
                self._kv = await self._js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
                logger.info(f"Scheduler connected to NATS and KV store '{SCHEDULED_JOBS_KV_NAME}'.")
            except Exception as e:
                try:
                    # Try to create the KV bucket if it doesn't exist
                    self._kv = await self._js.create_key_value(
                        bucket=SCHEDULED_JOBS_KV_NAME,
                        description="Scheduler job schedule storage"
                    )
                    logger.info(f"Created KV store '{SCHEDULED_JOBS_KV_NAME}'")
                except Exception as create_e:
                    logger.error(f"Failed to get or create KV store '{SCHEDULED_JOBS_KV_NAME}': {create_e}")
                    raise NaqConnectionError(f"Failed to access KV store: {create_e}") from create_e
            
            # Set up the lock KV store for leader election if HA is enabled
            if self._enable_ha:
                try:
                    self._lock_kv = await self._js.key_value(bucket=SCHEDULER_LOCK_KV_NAME)
                    logger.info(f"Connected to leader election KV store '{SCHEDULER_LOCK_KV_NAME}'")
                except Exception as e:
                    try:
                        # Try to create the lock KV bucket
                        self._lock_kv = await self._js.create_key_value(
                            bucket=SCHEDULER_LOCK_KV_NAME,
                            description="Scheduler leader election lock",
                            history=1,  # Only need latest value
                        )
                        logger.info(f"Created leader election KV store '{SCHEDULER_LOCK_KV_NAME}'")
                    except Exception as create_e:
                        logger.error(f"Failed to get or create lock KV store '{SCHEDULER_LOCK_KV_NAME}': {create_e}")
                        raise NaqConnectionError(f"Failed to access lock KV store: {create_e}") from create_e

    async def _try_become_leader(self) -> bool:
        """
        Attempt to acquire the leader lock. Returns True if successful.
        """
        if not self._enable_ha or not self._lock_kv:
            # If HA is not enabled, always consider self as leader
            return True
        
        try:
            # Try to get the current lock
            try:
                entry = await self._lock_kv.get(SCHEDULER_LOCK_KEY)
                if entry:
                    # Lock exists - see if it's expired
                    lock_data = cloudpickle.loads(entry.value)
                    lock_time = lock_data.get('timestamp', 0)
                    lock_owner = lock_data.get('instance_id', 'unknown')
                    
                    # If lock is still valid and owned by someone else, can't become leader
                    if time.time() - lock_time < self._lock_ttl and lock_owner != self._instance_id:
                        logger.debug(f"Lock already held by '{lock_owner}', cannot become leader")
                        return False
                    
                    # If we already own the lock or it's expired, we can take/renew it
            except KeyNotFoundError:
                # No existing lock, we can try to take it
                pass
            
            # Attempt to set the lock with our instance ID
            lock_data = {
                'instance_id': self._instance_id,
                'timestamp': time.time(),
                'hostname': socket.gethostname(),
            }
            serialized_lock = cloudpickle.dumps(lock_data)
            await self._lock_kv.put(SCHEDULER_LOCK_KEY, serialized_lock)
            logger.info(f"Acquired scheduler leader lock. This instance ({self._instance_id}) is now the leader.")
            return True
            
        except Exception as e:
            logger.error(f"Error during leader election: {e}")
            return False

    async def _renew_leader_lock(self):
        """
        Periodically renew the leader lock to maintain leadership.
        Runs as a background task while scheduler is active.
        """
        while self._running and self._is_leader:
            try:
                if self._lock_kv:
                    # Update the lock with fresh timestamp
                    lock_data = {
                        'instance_id': self._instance_id,
                        'timestamp': time.time(),
                        'hostname': socket.gethostname(),
                    }
                    serialized_lock = cloudpickle.dumps(lock_data)
                    await self._lock_kv.put(SCHEDULER_LOCK_KEY, serialized_lock)
                    logger.debug(f"Renewed leader lock. Next renewal in {self._lock_renew_interval}s")
                
                # Wait for renewal interval or until shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=self._lock_renew_interval
                    )
                    # If wait completes without timeout, shutdown was triggered
                    break
                except asyncio.TimeoutError:
                    # This is expected - continue the loop
                    pass
                    
            except Exception as e:
                logger.error(f"Failed to renew leader lock: {e}")
                # Lost leadership
                self._is_leader = False
                break
        
        logger.info("Leader lock renewal task exiting")
        # If we reach here normally, we're no longer leader or shutting down
        self._is_leader = False

    async def _enqueue_job(self, queue_name: str, subject: str, payload: bytes):
        """Internal helper to publish a job payload to the correct work queue subject."""
        if not self._js:
            raise NaqException("JetStream context not available for enqueueing.")
        try:
            ack = await self._js.publish(subject=subject, payload=payload)
            logger.debug(f"Enqueued job to {subject}. Stream: {ack.stream}, Seq: {ack.seq}")
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue job payload to subject '{subject}': {e}")
            return False

    async def _check_and_process_jobs(self):
        """Check the KV store for jobs ready to run."""
        if not self._kv or not self._js:
            logger.warning("KV store or JetStream context not available, skipping check.")
            return

        now_ts = time.time()
        logger.debug(f"Scheduler checking for jobs ready before {now_ts}...")
        processed_count = 0
        error_count = 0

        try:
            keys = await self._kv.keys()
            for key_bytes in keys:
                # Only process if we're still the leader
                if not self._is_leader:
                    logger.info("No longer the leader, stopping job processing")
                    break
                    
                key = key_bytes.decode('utf-8')
                try:
                    entry = await self._kv.get(key_bytes)
                    if entry is None: continue # Should not happen if key exists, but check

                    schedule_data: Dict[str, Any] = cloudpickle.loads(entry.value)
                    
                    # Skip paused jobs
                    status = schedule_data.get('status')
                    if status == SCHEDULED_JOB_STATUS_PAUSED:
                        logger.debug(f"Skipping paused job '{key}'")
                        continue
                        
                    # Skip failed jobs that exceeded retry attempts
                    if status == SCHEDULED_JOB_STATUS_FAILED:
                        logger.debug(f"Skipping failed job '{key}' that exceeded retry limits")
                        continue

                    scheduled_ts = schedule_data.get('scheduled_timestamp_utc')
                    if scheduled_ts is None or scheduled_ts > now_ts:
                        continue # Not ready yet

                    # --- Job is ready, process it ---
                    job_id = schedule_data.get('job_id', 'unknown')
                    queue_name = schedule_data.get('queue_name')
                    original_payload = schedule_data.get('_orig_job_payload')
                    cron = schedule_data.get('cron')
                    interval_seconds = schedule_data.get('interval_seconds')
                    repeat = schedule_data.get('repeat') # None means infinite
                    
                    # Ensure the job has required fields
                    if not queue_name or not original_payload:
                        logger.error(f"Invalid schedule data for key '{key}' (missing queue_name or payload). Deleting.")
                        await self._kv.delete(key_bytes)
                        continue

                    logger.info(f"Job {job_id} is ready. Enqueueing to queue '{queue_name}'.")
                    target_subject = f"{NAQ_PREFIX}.queue.{queue_name}"
                    
                    # Attempt to enqueue the job
                    enqueue_success = await self._enqueue_job(queue_name, target_subject, original_payload)
                    
                    # Track success/failure for this job
                    if enqueue_success:
                        processed_count += 1
                        # Reset failure count on success if it exists
                        schedule_data['schedule_failure_count'] = 0 
                        schedule_data['last_enqueued_utc'] = now_ts
                    else:
                        error_count += 1
                        # Track failures for potential retry limiting
                        failure_count = schedule_data.get('schedule_failure_count', 0) + 1
                        schedule_data['schedule_failure_count'] = failure_count
                        
                        # Check if we should mark the job as permanently failed
                        if MAX_SCHEDULE_FAILURES and failure_count >= MAX_SCHEDULE_FAILURES:
                            logger.warning(f"Job {job_id} has failed scheduling {failure_count} times, marking as failed")
                            schedule_data['status'] = SCHEDULED_JOB_STATUS_FAILED
                            # Keep the entry for inspection but don't try to schedule again
                            serialized_data = cloudpickle.dumps(schedule_data)
                            await self._kv.put(key_bytes, serialized_data)
                            continue
                        else:
                            # Just log and continue - will retry on next check cycle
                            logger.warning(
                                f"Failed to enqueue job {job_id} (attempt {failure_count}). "
                                f"Will retry on next cycle."
                            )
                            serialized_data = cloudpickle.dumps(schedule_data)
                            await self._kv.put(key_bytes, serialized_data)
                            continue

                    # --- Handle recurrence or deletion ---
                    # (Only if enqueue was successful or we've given up)
                    next_scheduled_ts: Optional[float] = None
                    delete_entry = True # Assume deletion unless rescheduled

                    if cron:
                        if croniter is None:
                             logger.error("Cannot reschedule cron job: 'croniter' library not installed.")
                             # Keep the entry? Delete it? Log error and delete for now.
                        else:
                            # Calculate next run time based on the *original* scheduled time
                            # to avoid drift. Use the last scheduled time as the base for next calculation.
                            base_dt = datetime.datetime.fromtimestamp(scheduled_ts, timezone.utc)
                            cron_iter = croniter(cron, base_dt)
                            next_scheduled_ts = cron_iter.get_next(datetime.datetime).timestamp()

                    elif interval_seconds:
                        # Calculate next run time based on the *original* scheduled time + interval
                        next_scheduled_ts = scheduled_ts + interval_seconds

                    if next_scheduled_ts is not None:
                        # Check repeat count
                        if repeat is not None:
                            if repeat > 1:
                                schedule_data['repeat'] = repeat - 1
                                schedule_data['scheduled_timestamp_utc'] = next_scheduled_ts
                                schedule_data['next_run_utc'] = next_scheduled_ts
                                delete_entry = False # Reschedule instead of deleting
                                logger.debug(f"Rescheduling job {job_id} for {next_scheduled_ts}. Repeats left: {repeat - 1}")
                            else:
                                # Last repetition
                                logger.debug(f"Job {job_id} finished its repetitions.")
                                delete_entry = True
                        else:
                            # Infinite repeat
                            schedule_data['scheduled_timestamp_utc'] = next_scheduled_ts
                            schedule_data['next_run_utc'] = next_scheduled_ts
                            delete_entry = False # Reschedule instead of deleting
                            logger.debug(f"Rescheduling job {job_id} for {next_scheduled_ts} (infinite).")

                    # --- Update or Delete KV Entry ---
                    if delete_entry:
                        logger.debug(f"Deleting schedule entry for job {job_id}.")
                        await self._kv.delete(key_bytes)
                    else:
                        # Update the entry with new timestamp and repeat count
                        logger.debug(f"Updating schedule entry for job {job_id}.")
                        updated_payload = cloudpickle.dumps(schedule_data)
                        await self._kv.put(key_bytes, updated_payload) # Use put to update

                except SerializationError as e:
                    logger.error(f"Failed to deserialize schedule data for key '{key}': {e}. Deleting entry.")
                    await self._kv.delete(key_bytes)
                except Exception as e:
                    logger.exception(f"Error processing schedule key '{key}': {e}")
                    error_count += 1
                    # Continue to next job even if this one fails

        except nats.errors.NatsError as e:
            logger.error(f"NATS error during scheduler check: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during scheduler check: {e}")

        if processed_count > 0 or error_count > 0:
            logger.info(f"Scheduler processed {processed_count} ready jobs, encountered {error_count} errors.")

    async def run(self):
        """Starts the scheduler loop with leader election."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()
            
            logger.info(f"Scheduler instance {self._instance_id} started. Polling interval: {self._poll_interval}s")
            logger.info(f"High availability mode: {'enabled' if self._enable_ha else 'disabled'}")

            while self._running:
                # Check leadership status first
                was_leader = self._is_leader
                if not self._is_leader:
                    self._is_leader = await self._try_become_leader()
                    
                    # If just became leader, start the lock renewal task
                    if self._is_leader and not was_leader:
                        logger.info(f"Instance {self._instance_id} became the leader.")
                        self._lock_renewal_task = asyncio.create_task(self._renew_leader_lock())
                
                # Process jobs only if leader
                if self._is_leader:
                    await self._check_and_process_jobs()
                else:
                    logger.debug(f"Not the leader, waiting...")
                
                try:
                    # Wait for the poll interval or until shutdown is triggered
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=self._poll_interval)
                    # If wait() finished without timeout, shutdown was triggered
                    break
                except asyncio.TimeoutError:
                    # Timeout occurred, continue the loop
                    pass

        except Exception as e:
            logger.exception(f"Scheduler run loop encountered an error: {e}")
        finally:
            logger.info("Scheduler shutting down...")
            # Cancel lock renewal task if running
            if self._lock_renewal_task and not self._lock_renewal_task.done():
                self._lock_renewal_task.cancel()
                try:
                    await self._lock_renewal_task
                except asyncio.CancelledError:
                    pass
            
            await self._close()
            logger.info("Scheduler shutdown complete.")

    async def _close(self):
        """Closes NATS connection."""
        # Release leadership if we have it
        if self._is_leader and self._lock_kv:
            try:
                # Try to delete our lock to let another scheduler take over faster
                await self._lock_kv.delete(SCHEDULER_LOCK_KEY)
                logger.info("Released scheduler leader lock")
            except Exception as e:
                logger.error(f"Error releasing leader lock: {e}")
        
        await close_nats_connection() # Use the shared close function
        self._nc = None
        self._js = None
        self._kv = None
        self._lock_kv = None
        self._is_leader = False

    def signal_handler(self, sig, frame):
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self):
        """Installs signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    @property
    def is_leader(self) -> bool:
        """Returns True if this scheduler instance is currently the leader."""
        return self._is_leader
