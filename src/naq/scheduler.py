import asyncio
import signal
import time
import datetime
from datetime import timezone
from typing import Optional, Dict, Any

import nats
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from loguru import logger
import cloudpickle

# Attempt to import croniter only if needed later
try:
    from croniter import croniter
except ImportError:
    croniter = None # type: ignore

from .settings import DEFAULT_NATS_URL, NAQ_PREFIX
from .connection import get_nats_connection, get_jetstream_context, close_nats_connection
from .exceptions import NaqException, ConnectionError as NaqConnectionError, SerializationError
from .queue import SCHEDULED_JOBS_KV_NAME # Import KV name

class Scheduler:
    """
    Polls the scheduled jobs KV store and enqueues jobs that are ready.
    """
    def __init__(
        self,
        nats_url: str = DEFAULT_NATS_URL,
        poll_interval: float = 1.0, # Check for jobs every second
    ):
        self._nats_url = nats_url
        self._poll_interval = poll_interval
        self._nc: Optional[nats.aio.client.Client] = None
        self._js: Optional[JetStreamContext] = None
        self._kv: Optional[KeyValue] = None
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def _connect(self):
        """Establish NATS connection, JetStream context, and KV handle."""
        if self._nc is None or not self._nc.is_connected:
            self._nc = await get_nats_connection(url=self._nats_url)
            self._js = await get_jetstream_context(nc=self._nc)
            try:
                self._kv = await self._js.key_value(bucket=SCHEDULED_JOBS_KV_NAME)
                logger.info(f"Scheduler connected to NATS and KV store '{SCHEDULED_JOBS_KV_NAME}'.")
            except Exception as e:
                 # If KV doesn't exist, maybe create it? Or rely on Queue methods to create it?
                 # For now, assume Queue methods create it on first schedule.
                 logger.error(f"Failed to get KV store '{SCHEDULED_JOBS_KV_NAME}': {e}. Ensure jobs have been scheduled first.")
                 raise NaqConnectionError(f"Failed to access KV store: {e}") from e

    async def _enqueue_job(self, queue_name: str, subject: str, payload: bytes):
        """Internal helper to publish a job payload to the correct work queue subject."""
        if not self._js:
            raise NaqException("JetStream context not available for enqueueing.")
        try:
            ack = await self._js.publish(subject=subject, payload=payload)
            logger.debug(f"Enqueued job to {subject}. Stream: {ack.stream}, Seq: {ack.seq}")
        except Exception as e:
            logger.error(f"Failed to enqueue job payload to subject '{subject}': {e}")
            # Decide how to handle enqueue failure (retry? log and move on?)

    async def _check_and_process_jobs(self):
        """Check the KV store for jobs ready to run."""
        if not self._kv or not self._js:
            logger.warning("KV store or JetStream context not available, skipping check.")
            return

        now_ts = time.time()
        logger.debug(f"Scheduler checking for jobs ready before {now_ts}...")
        processed_count = 0

        try:
            keys = await self._kv.keys()
            for key_bytes in keys:
                key = key_bytes.decode('utf-8')
                try:
                    entry = await self._kv.get(key_bytes)
                    if entry is None: continue # Should not happen if key exists, but check

                    schedule_data: Dict[str, Any] = cloudpickle.loads(entry.value)

                    scheduled_ts = schedule_data.get('scheduled_timestamp_utc')
                    if scheduled_ts is None or scheduled_ts > now_ts:
                        continue # Not ready yet

                    # --- Job is ready, process it ---
                    processed_count += 1
                    job_id = schedule_data.get('job_id', 'unknown')
                    queue_name = schedule_data.get('queue_name')
                    original_payload = schedule_data.get('_orig_job_payload')
                    cron = schedule_data.get('cron')
                    interval_seconds = schedule_data.get('interval_seconds')
                    repeat = schedule_data.get('repeat') # None means infinite

                    if not queue_name or not original_payload:
                        logger.error(f"Invalid schedule data for key '{key}' (missing queue_name or payload). Deleting.")
                        await self._kv.delete(key_bytes)
                        continue

                    logger.info(f"Job {job_id} is ready. Enqueueing to queue '{queue_name}'.")
                    target_subject = f"{NAQ_PREFIX}.queue.{queue_name}"
                    await self._enqueue_job(queue_name, target_subject, original_payload)

                    # --- Handle recurrence or deletion ---
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
                                delete_entry = False # Reschedule instead of deleting
                                logger.debug(f"Rescheduling job {job_id} for {next_scheduled_ts}. Repeats left: {repeat - 1}")
                            else:
                                # Last repetition
                                logger.debug(f"Job {job_id} finished its repetitions.")
                                delete_entry = True
                        else:
                            # Infinite repeat
                            schedule_data['scheduled_timestamp_utc'] = next_scheduled_ts
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
                    # Decide whether to delete the problematic entry or leave it for retry

        except nats.errors.NatsError as e:
            logger.error(f"NATS error during scheduler check: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during scheduler check: {e}")

        if processed_count > 0:
            logger.info(f"Scheduler processed {processed_count} ready jobs.")


    async def run(self):
        """Starts the scheduler loop."""
        self._running = True
        self._shutdown_event.clear()
        self.install_signal_handlers()

        try:
            await self._connect()

            logger.info(f"Scheduler started. Polling interval: {self._poll_interval}s")

            while self._running:
                await self._check_and_process_jobs()
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
            await self._close()
            logger.info("Scheduler shutdown complete.")

    async def _close(self):
        """Closes NATS connection."""
        await close_nats_connection() # Use the shared close function
        self._nc = None
        self._js = None
        self._kv = None

    def signal_handler(self, sig, frame):
        """Handles termination signals."""
        logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def install_signal_handlers(self):
        """Installs signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
