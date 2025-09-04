# src/naq/scheduler.py
import asyncio
import os
import signal
import socket
import time
import uuid
from typing import Optional, Dict, Any

import anyio
import msgspec
from loguru import logger

from .nats_client import NatsClient
from .circuit_breaker import get_circuit_breaker
from .exceptions import (
    NaqConnectionError,
    LeaderElectionError,
    LockAcquisitionError,
    LockRenewalError,
    LockReleaseError,
    LockTimeoutError,
    LockConflictError,
    LockDataError,
    LockUpdateError,
)
from .metrics import EventType, record_event
from .schemas import (
    SCHEDULED_JOBS_KV_NAME,
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_KV_NAME,
    SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
    SCHEDULER_LOCK_TTL_SECONDS,
)
from .utils import setup_logging
from .utils.error_handling import ErrorHandler, create_error_context, wrap_naq_exception

# Attempt to import croniter only if needed later
try:
    from croniter import croniter
except ImportError:
    croniter = None  # type: ignore

# Check if croniter is available and log a warning if it's not
if croniter is None:
    logger.warning(
        "croniter library not installed. Cron-based scheduling will not work. "
        "Install croniter to enable cron-based scheduling: pip install croniter"
    )


class LockData(msgspec.Struct):
    """Data structure for the leader lock."""

    instance_id: str
    timestamp: float
    hostname: str
    pid: int
    start_time: float


class LeaderElection:
    """
    Handles leader election for high availability schedulers using NATS KV store.

    This class implements a leader election mechanism using a distributed lock
    stored in a NATS key-value store. Only one scheduler instance can be the
    leader at a time, ensuring that scheduled jobs are processed by exactly
    one instance in a high-availability setup.
    """

    def __init__(
        self,
        instance_id: str,
        lock_ttl: int = SCHEDULER_LOCK_TTL_SECONDS,
        lock_renew_interval: int = SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
        client: Optional[NatsClient] = None,
    ) -> None:
        """Initialize the leader election system.

        Args:
            instance_id: Unique identifier for this scheduler instance
            lock_ttl: Time-to-live for the leader lock in seconds
            lock_renew_interval: Interval at which to renew the leader lock in seconds
            client: NATS client for distributed locking
        """
        self.instance_id = instance_id
        self.lock_ttl = lock_ttl
        self.lock_renew_interval = lock_renew_interval
        self._shutdown_event = anyio.Event()
        self._is_leader = False
        self._lock_renewal_task: Optional[anyio.Task[None]] = None
        self._client = client
        self._error_handler = ErrorHandler(logger)
        self._last_lock_renewal = 0.0
        self.start_time = time.time()
        self._operation_lock = (
            anyio.Lock()
        )  # Shared lock for leader election operations
        self._circuit_breaker: Optional[Any] = (
            None  # Circuit breaker for KV store operations
        )

    async def initialize(self) -> None:
        """Initialize the leader election system."""
        if not self._client:
            raise NaqConnectionError("NatsClient is required for leader election")

        # Validate that the client is properly initialized with retry
        await self._validate_client_with_retry()

        # Initialize circuit breaker for KV store operations
        self._circuit_breaker = await get_circuit_breaker(
            name=f"leader-election-kv-store-{self.instance_id}",
            failure_threshold=3,
            recovery_timeout=30.0,
            expected_exception=NaqConnectionError,
        )

        logger.info(
            "Initialized leader election for instance {} using KV store '{}'",
            self.instance_id,
            SCHEDULER_LOCK_KV_NAME,
        )
        logger.debug(
            "Leader election configuration: instance_id={}, lock_ttl={}, lock_renew_interval={}",
            self.instance_id,
            self.lock_ttl,
            self.lock_renew_interval,
        )

    async def _validate_client(self) -> None:
        """Validate that the NATS client is available and accessible.

        Raises:
            NaqConnectionError: If NATS client is not available or accessible
        """
        if not self._client:
            raise NaqConnectionError("NatsClient is required for leader election")

        if not self._circuit_breaker:
            raise NaqConnectionError("Circuit breaker is required for leader election")

        try:
            # Use circuit breaker to protect KV store access
            async def validate_client():
                kv = await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)
                if not kv:
                    raise NaqConnectionError(
                        f"KV store '{SCHEDULER_LOCK_KV_NAME}' is not accessible"
                    )
                return kv

            await self._circuit_breaker.call(validate_client)
        except Exception as e:
            raise NaqConnectionError(
                f"Failed to access KV store '{SCHEDULER_LOCK_KV_NAME}': {e}"
            ) from e

    async def _validate_client_with_retry(self, max_retries: int = 3) -> None:
        """Validate that the NATS client is available and accessible with retries.

        Args:
            max_retries: Maximum number of retries before giving up

        Raises:
            NaqConnectionError: If NATS client is not available or accessible after retries
        """
        if not self._client:
            raise NaqConnectionError("NatsClient is required for leader election")

        if not self._circuit_breaker:
            raise NaqConnectionError("Circuit breaker is required for leader election")

        last_exception = None
        for attempt in range(max_retries):
            try:
                # Use circuit breaker to protect KV store access
                async def validate_client():
                    kv = await self._client.get_kv_store(
                        SCHEDULER_LOCK_KV_NAME
                    )
                    if not kv:
                        raise NaqConnectionError(
                            f"KV store '{SCHEDULER_LOCK_KV_NAME}' is not accessible"
                        )
                    return kv

                await self._circuit_breaker.call(validate_client)
                # If we get here, the validation succeeded
                return
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        "NATS client validation attempt {} failed, retrying: {}",
                        attempt + 1,
                        e,
                    )
                    # Exponential backoff with jitter
                    backoff_time = (2**attempt) + (time.time() % 1)  # Add jitter
                    await anyio.sleep(min(backoff_time, 5.0))  # Cap at 5 seconds
                else:
                    logger.error(
                        "NATS client validation failed after {} attempts: {}",
                        max_retries,
                        e,
                    )

        # If we get here, all retries failed
        raise NaqConnectionError(
            f"Failed to access KV store '{SCHEDULER_LOCK_KV_NAME}' after {max_retries} attempts: {last_exception}"
        ) from last_exception

    async def try_become_leader(self) -> bool:
        """
        Attempt to acquire the leader lock.

        Returns:
            True if this instance is now the leader, False otherwise
        """
        start_time = time.time()
        record_event(EventType.LOCK_ACQUISITION_ATTEMPT, self.instance_id)

        # Validate KV store service
        if not await self._validate_service_for_leader_acquisition(start_time):
            return False

        logger.debug("Instance {} attempting to acquire leader lock", self.instance_id)

        # Use the shared operation lock to prevent race conditions
        async with self._operation_lock:
            return await self._perform_leader_acquisition_with_timeout(start_time)

    async def _validate_service_for_leader_acquisition(self, start_time: float) -> bool:
        """Validate NATS client for leader acquisition.

        Args:
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            True if validation succeeded, False otherwise
        """
        try:
            await self._validate_client_with_retry()
            return True
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for instance {}: {}",
                self.instance_id,
                e,
            )
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"error": str(e)},
            )
            return False

    async def _perform_leader_acquisition_with_timeout(self, start_time: float) -> bool:
        """Perform leader acquisition with timeout.

        Args:
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            True if acquisition succeeded, False otherwise
        """
        # Check if lock is already held by another instance
        if await self._is_lock_held_by_other():
            logger.debug(
                "Instance {} cannot become leader: lock held by another instance",
                self.instance_id,
            )
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "lock_held_by_other"},
            )
            return False

        # Check lock health before attempting to acquire
        if not await self._check_lock_health_before_acquisition(start_time):
            return False

        # Attempt to acquire the lock with a timeout
        try:
            with anyio.move_on_after(5.0):  # 5 second timeout
                return await self._acquire_and_verify_leadership(start_time)

            # If we get here, the operation timed out
            logger.warning(
                "Leader lock acquisition for instance {} timed out", self.instance_id
            )
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "timeout"},
            )
            return False
        except Exception as e:
            return await self._handle_acquisition_exception(e, start_time)

    async def _check_lock_health_before_acquisition(self, start_time: float) -> bool:
        """Check lock health before attempting to acquire.

        Args:
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            True if lock health is good, False otherwise
        """
        health = await self.check_leader_lock_health()
        logger.debug("Lock health before acquisition attempt: {}", health)

        # If there's an error with the lock, don't try to acquire
        if health.get("status") == "error":
            logger.warning(
                "Cannot attempt to become leader due to lock health error: {}",
                health.get("message", "Unknown error"),
            )
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={
                    "reason": "lock_health_error",
                    "status": health.get("status"),
                },
            )
            return False

        return True

    async def _acquire_and_verify_leadership(self, start_time: float) -> bool:
        """Acquire lock and verify leadership.

        Args:
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            True if acquisition and verification succeeded, False otherwise
        """
        result = await self._acquire_lock(already_locked=True)
        if not result:
            logger.debug("Instance {} failed to acquire leader lock", self.instance_id)
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "acquisition_failed"},
            )
            return False

        logger.info("Instance {} successfully acquired leader lock", self.instance_id)

        # Verify leadership after acquisition
        return await self._verify_leadership_after_acquisition(start_time)

    async def _verify_leadership_after_acquisition(self, start_time: float) -> bool:
        """Verify leadership after acquisition.

        Args:
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            True if verification succeeded, False otherwise
        """
        verification_health = await self.check_leader_lock_health()
        if verification_health.get("is_owned_by_us"):
            logger.info(
                "Leadership verification successful for instance {}", self.instance_id
            )
            record_event(
                EventType.LOCK_ACQUISITION_SUCCESS,
                self.instance_id,
                duration=time.time() - start_time,
            )
            record_event(EventType.LEADERSHIP_GAINED, self.instance_id)
            record_event(EventType.LEADERSHIP_VERIFICATION_SUCCESS, self.instance_id)
            return True
        else:
            logger.warning(
                "Leadership verification failed for instance {} after acquisition",
                self.instance_id,
            )
            await self._clear_leadership_status("verification failed after acquisition")
            record_event(
                EventType.LOCK_ACQUISITION_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "verification_failed"},
            )
            record_event(EventType.LEADERSHIP_VERIFICATION_FAILURE, self.instance_id)
            return False

    async def _handle_acquisition_exception(
        self, e: Exception, start_time: float
    ) -> bool:
        """Handle exception during leader acquisition.

        Args:
            e: The exception that occurred
            start_time: When the acquisition attempt started (for metrics)

        Returns:
            Always False (exception handling)

        Raises:
            LockAcquisitionError: Always raised with details about the original exception
        """
        logger.exception(
            "Unexpected error during leader acquisition for instance {}",
            self.instance_id,
        )
        context = create_error_context("try_become_leader")
        self._error_handler.handle_error(
            wrap_naq_exception(e, context="trying to become leader"),
            context=context,
            reraise=False,
        )
        record_event(
            EventType.LOCK_ACQUISITION_FAILURE,
            self.instance_id,
            duration=time.time() - start_time,
            metadata={"reason": "exception", "error": str(e)},
        )
        raise LockAcquisitionError(
            f"Unexpected error during leader acquisition for instance {self.instance_id}: {e}"
        ) from e

    def _is_lock_expired(self, lock_data: LockData) -> bool:
        """Check if a lock entry is expired.

        Args:
            lock_data: The lock data struct

        Returns:
            True if the lock is expired, False otherwise
        """
        lock_time = lock_data.timestamp
        current_time = time.time()
        return current_time - lock_time >= self.lock_ttl

    async def _is_lock_held_by_other(self) -> bool:
        """Check if the lock is held by another instance.

        Returns:
            True if lock is held by another instance and is still valid, False otherwise.
            In case of errors, conservatively returns True to prevent multiple leaders.
        """
        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.warning(
                "NatsClient validation failed for checking lock status on instance {}: {}",
                self.instance_id,
                e,
            )
            return True

        # Get the KV store object using circuit breaker
        try:

            async def get_kv_store():
                return await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)

            kv = await self._circuit_breaker.call(get_kv_store)
        except NaqConnectionError as e:
            logger.warning(
                "Connection error getting KV store for lock status check, assuming lock is held: {}",
                str(e),
            )
            return True
        except Exception as e:
            logger.exception(
                "Unexpected error getting KV store for lock status check, assuming lock is held"
            )
            context = create_error_context("is_lock_held_by_other_get_kv")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="getting KV store for lock status check"),
                context=context,
                reraise=False,
            )
            return True

        # Get the current lock entry using circuit breaker
        try:

            async def get_lock_entry():
                return await kv.get(SCHEDULER_LOCK_KEY)

            current_entry = await self._circuit_breaker.call(get_lock_entry)
        except NaqConnectionError as e:
            logger.warning(
                "Connection error retrieving lock entry, assuming lock is held: {}",
                str(e),
            )
            return True
        except Exception as e:
            logger.exception(
                "Unexpected error retrieving lock entry, assuming lock is held"
            )
            context = create_error_context("is_lock_held_by_other_get_entry")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="retrieving lock entry"),
                context=context,
                reraise=False,
            )
            return True

        # No existing lock
        if current_entry is None:
            logger.debug("No existing leader lock found")
            return False

        # Process the lock data
        return await self._process_lock_data(current_entry)

    async def _process_lock_data(self, current_entry) -> bool:
        """Process the lock data to determine if it's held by another instance.

        Args:
            current_entry: Current lock entry from KV store

        Returns:
            True if lock is held by another instance and is still valid, False otherwise
        """
        try:
            # Deserialize the lock data
            try:
                lock_data = msgspec.msgpack.decode(current_entry.value, type=LockData)
            except msgspec.msgpack.DecodeError as e:
                logger.warning(
                    "Failed to deserialize lock data, treating as no lock: {}", e
                )
                return False
            except Exception as e:
                logger.exception(
                    "Unexpected error deserializing lock data, treating as no lock"
                )
                context = create_error_context("process_lock_data_deserialize")
                self._error_handler.handle_error(
                    wrap_naq_exception(e, context="deserializing lock data"),
                    context=context,
                    reraise=False,
                )
                return False

            # Validate required fields
            if not lock_data.instance_id or lock_data.timestamp <= 0:
                logger.warning(
                    "Invalid lock data with missing or invalid fields, treating as no lock"
                )
                return False

            # Check if lock is expired
            current_time = time.time()
            is_expired = current_time - lock_data.timestamp >= self.lock_ttl
            time_until_expiry = self.lock_ttl - (current_time - lock_data.timestamp)

            # Check if we own the lock
            is_owned_by_us = lock_data.instance_id == self.instance_id

            # If lock is still valid and owned by someone else
            if not is_expired and not is_owned_by_us:
                logger.debug(
                    "Lock already held by '{}' (hostname: {}) with {} seconds remaining, cannot become leader",
                    lock_data.instance_id,
                    lock_data.hostname,
                    max(0, time_until_expiry),
                )
                return True

            # If lock is expired
            if is_expired:
                logger.debug(
                    "Lock held by '{}' (hostname: {}) expired {} seconds ago",
                    lock_data.instance_id,
                    lock_data.hostname,
                    abs(time_until_expiry),
                )
                return False

            # Lock is owned by us
            logger.debug(
                "Lock owned by us (instance: {}) with {} seconds remaining",
                self.instance_id,
                max(0, time_until_expiry),
            )
            return False
        except Exception as e:
            # Failed to process lock data
            logger.exception(
                "Unexpected error processing lock data, treating as no lock"
            )
            context = create_error_context("process_lock_data")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="processing lock data"),
                context=context,
                reraise=False,
            )
            return False

    async def check_leader_lock_health(self) -> Dict[str, Any]:
        """Check the health of the leader lock and return status information.

        Returns:
            Dictionary containing lock health information
        """
        # Validate NATS client
        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            return {"status": "error", "message": f"NatsClient not available: {e}"}

        # Get the KV store object
        kv = await self._get_kv_store_for_health_check()
        if not kv:
            return {"status": "error", "message": "Failed to get KV store"}

        # Get the current lock entry
        current_entry = await self._get_lock_entry_for_health_check(kv)
        if current_entry is None:
            return {
                "status": "no_lock",
                "message": "No leader lock exists",
                "is_leader": self._is_leader,
                "instance_id": self.instance_id,
            }

        # Process the lock data and return status
        return await self._process_lock_data_for_health_check(current_entry)

    async def _get_kv_store_for_health_check(self):
        """Get the KV store for health check.

        Returns:
            KV store instance or None if failed
        """
        try:

            async def get_kv_store():
                return await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)

            return await self._circuit_breaker.call(get_kv_store)
        except NaqConnectionError as e:
            logger.warning(
                "Connection error getting KV store for health check: {}", str(e)
            )
            return None
        except Exception as e:
            logger.exception("Unexpected error getting KV store for health check")
            context = create_error_context("check_leader_lock_health_get_kv")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="getting KV store for health check"),
                context=context,
                reraise=False,
            )
            return None

    async def _get_lock_entry_for_health_check(self, kv):
        """Get the lock entry for health check.

        Args:
            kv: KV store instance

        Returns:
            Lock entry or None if failed
        """
        try:

            async def get_lock_entry():
                return await kv.get(SCHEDULER_LOCK_KEY)

            return await self._circuit_breaker.call(get_lock_entry)
        except NaqConnectionError as e:
            logger.warning(
                "Connection error getting lock entry for health check: {}", str(e)
            )
            return None
        except Exception as e:
            logger.exception("Unexpected error getting lock entry for health check")
            context = create_error_context("check_leader_lock_health_get_entry")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="getting lock entry for health check"),
                context=context,
                reraise=False,
            )
            return None

    async def _process_lock_data_for_health_check(
        self, current_entry
    ) -> Dict[str, Any]:
        """Process lock data for health check.

        Args:
            current_entry: Current lock entry from KV store

        Returns:
            Dictionary containing lock health information
        """
        try:
            # Deserialize the lock data
            lock_data = await self._deserialize_lock_data_for_health_check(
                current_entry
            )
            if not lock_data:
                return {
                    "status": "error",
                    "message": "Failed to deserialize lock data",
                    "is_leader": self._is_leader,
                    "instance_id": self.instance_id,
                }

            # Calculate lock status
            return self._calculate_lock_status(lock_data)
        except Exception as e:
            # Failed to process lock data
            logger.exception("Unexpected error processing lock data for health check")
            context = create_error_context("check_leader_lock_health_process")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="processing lock data for health check"),
                context=context,
                reraise=False,
            )
            return {
                "status": "error",
                "message": f"Unexpected error processing lock data: {str(e)}",
                "is_leader": self._is_leader,
                "instance_id": self.instance_id,
            }

    async def _deserialize_lock_data_for_health_check(self, current_entry):
        """Deserialize lock data for health check.

        Args:
            current_entry: Current lock entry from KV store

        Returns:
            LockData instance or None if failed
        """
        try:
            return msgspec.msgpack.decode(current_entry.value, type=LockData)
        except msgspec.msgpack.DecodeError as e:
            logger.warning("Invalid lock data structure in health check: {}", e)
            return None
        except Exception as e:
            logger.exception(
                "Unexpected error deserializing lock data for health check"
            )
            context = create_error_context("check_leader_lock_health_deserialize")
            self._error_handler.handle_error(
                wrap_naq_exception(
                    e, context="deserializing lock data for health check"
                ),
                context=context,
                reraise=False,
            )
            return None

    def _calculate_lock_status(self, lock_data: LockData) -> Dict[str, Any]:
        """Calculate lock status from lock data.

        Args:
            lock_data: Lock data instance

        Returns:
            Dictionary containing lock status information
        """
        current_time = time.time()
        is_expired = current_time - lock_data.timestamp >= self.lock_ttl
        time_until_expiry = self.lock_ttl - (current_time - lock_data.timestamp)
        is_owned_by_us = lock_data.instance_id == self.instance_id

        status = {
            "status": "healthy" if not is_expired else "expired",
            "is_leader": self._is_leader,
            "is_owned_by_us": is_owned_by_us,
            "lock_owner": lock_data.instance_id,
            "lock_hostname": lock_data.hostname,
            "lock_pid": lock_data.pid,
            "lock_time": lock_data.timestamp,
            "current_time": current_time,
            "time_until_expiry": max(0, time_until_expiry),
            "lock_ttl": self.lock_ttl,
            "instance_id": self.instance_id,
            "last_lock_renewal": getattr(self, "_last_lock_renewal", 0),
        }

        # Set appropriate message based on lock status
        if is_expired:
            status["message"] = (
                f"Lock held by '{lock_data.instance_id}' expired {abs(time_until_expiry):.2f} seconds ago"
            )
        elif is_owned_by_us:
            status["message"] = (
                f"We hold the lock with {time_until_expiry:.2f} seconds remaining"
            )
        else:
            status["message"] = (
                f"Lock held by '{lock_data.instance_id}' with {time_until_expiry:.2f} seconds remaining"
            )

        return status

    async def _acquire_lock(self, already_locked: bool = False) -> bool:
        """Attempt to acquire the leader lock using atomic compare-and-swap.

        Args:
            already_locked: Whether the operation lock is already held by the caller

        Returns:
            True if lock was successfully acquired, False otherwise
        """
        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for lock acquisition on instance {}: {}",
                self.instance_id,
                e,
            )
            raise LockAcquisitionError(
                f"Connection error acquiring leader lock for instance {self.instance_id}: {e}"
            ) from e

        try:
            logger.debug(
                "Instance {} attempting to acquire leader lock", self.instance_id
            )

            # Use the shared operation lock to prevent race conditions if not already locked
            if already_locked:
                return await self._perform_lock_acquisition()
            else:
                async with self._operation_lock:
                    return await self._perform_lock_acquisition()
        except NaqConnectionError as e:
            logger.warning(
                "Connection error acquiring leader lock for instance {}: {}",
                self.instance_id,
                e,
            )
            raise LockAcquisitionError(
                f"Connection error acquiring leader lock for instance {self.instance_id}: {e}"
            ) from e
        except Exception as e:
            logger.exception(
                "Unexpected error acquiring leader lock for instance {}",
                self.instance_id,
            )
            context = create_error_context("acquire_leader_lock")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="acquiring leader lock"),
                context=context,
                reraise=False,
            )
            raise LockAcquisitionError(
                f"Unexpected error acquiring leader lock for instance {self.instance_id}: {e}"
            ) from e

    async def _perform_lock_acquisition(self) -> bool:
        """Perform the actual lock acquisition operation.

        Returns:
            True if lock was successfully acquired, False otherwise
        """

        # Get the KV store object using circuit breaker
        async def get_kv_store():
            return await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)

        kv = await self._circuit_breaker.call(get_kv_store)

        # Prepare new lock data
        new_lock_data = self._create_lock_data()
        serialized_new_lock_data = msgspec.msgpack.encode(new_lock_data)

        # Use a timeout to prevent deadlocks
        with anyio.move_on_after(3.0):  # 3 second timeout
            # Get the current lock entry and its revision using circuit breaker
            async def get_lock_entry():
                return await kv.get(SCHEDULER_LOCK_KEY)

            current_entry = await self._circuit_breaker.call(get_lock_entry)

            if current_entry is None:
                # No lock exists, try to create it atomically
                return await self._create_new_lock(kv, serialized_new_lock_data)
            else:
                # Lock exists, check if it's expired
                return await self._handle_existing_lock(
                    kv, current_entry, serialized_new_lock_data
                )

        # If we get here, the operation timed out
        logger.warning(
            "Leader lock acquisition for instance {} timed out", self.instance_id
        )
        await self._clear_leadership_status("lock acquisition timed out")
        raise LockTimeoutError(
            f"Leader lock acquisition for instance {self.instance_id} timed out"
        )

    def _create_lock_data(self) -> LockData:
        """Create new lock data for this instance.

        Returns:
            New LockData instance
        """
        return LockData(
            instance_id=self.instance_id,
            timestamp=time.time(),
            hostname=socket.gethostname(),
            pid=os.getpid(),
            start_time=getattr(self, "start_time", time.time()),
        )

    async def _create_new_lock(self, kv, serialized_new_lock_data: bytes) -> bool:
        """Create a new lock in the KV store.

        Args:
            kv: KV store instance
            serialized_new_lock_data: Serialized lock data

        Returns:
            True if lock was created successfully, False otherwise
        """
        try:
            await kv.create(SCHEDULER_LOCK_KEY, serialized_new_lock_data, self.lock_ttl)
            logger.info(
                "Acquired scheduler leader lock (created). Instance {} (PID: {}) is now the leader at {}",
                self.instance_id,
                os.getpid(),
                time.time(),
            )
            self._is_leader = True
            self._last_lock_renewal = time.time()
            return True
        except Exception as e:  # nats.js.errors.KeyExistsError or similar
            logger.debug(
                "Instance {} failed to create lock, it was created by another instance: {}",
                self.instance_id,
                e,
            )
            return False

    async def _handle_existing_lock(
        self, kv, current_entry, serialized_new_lock_data: bytes
    ) -> bool:
        """Handle an existing lock in the KV store.

        Args:
            kv: KV store instance
            current_entry: Current lock entry
            serialized_new_lock_data: Serialized lock data

        Returns:
            True if lock was acquired successfully, False otherwise
        """
        try:
            # Assuming current_entry.value is bytes and needs deserialization
            # And current_entry.revision holds the revision number
            existing_lock_data = msgspec.msgpack.decode(
                current_entry.value, type=LockData
            )
            if self._is_lock_expired(existing_lock_data):
                # Lock is expired, try to update it atomically
                # Update lock using circuit breaker
                async def update_lock():
                    return await kv.update(
                        SCHEDULER_LOCK_KEY,
                        serialized_new_lock_data,
                        current_entry.revision,
                        self.lock_ttl,
                    )

                await self._circuit_breaker.call(update_lock)
                logger.info(
                    "Acquired scheduler leader lock (updated expired). Instance {} (PID: {}) is now the leader at {}",
                    self.instance_id,
                    os.getpid(),
                    time.time(),
                )
                self._is_leader = True
                self._last_lock_renewal = time.time()
                return True
            else:
                # Lock is held by another instance and is not expired
                logger.debug(
                    "Instance {} cannot acquire lock: lock held by another instance and is not expired",
                    self.instance_id,
                )
                return False
        except Exception as e:  # nats.js.errors.WrongLastRevisionError or similar
            logger.debug(
                "Instance {} failed to update lock, it was updated by another instance: {}",
                self.instance_id,
                e,
            )
            return False

    async def start_renewal_task(self, shutdown_event: anyio.Event) -> None:
        """Start a background task to renew the leader lock.

        Args:
            shutdown_event: Event that signals when to stop renewal

        Raises:
            NaqConnectionError: If NatsClient is not available
        """
        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for starting renewal task on instance {}: {}",
                self.instance_id,
                e,
            )
            raise NaqConnectionError(
                f"NatsClient is required for lock renewal: {e}"
            ) from e

        # Use the shared operation lock to prevent concurrent task creation
        async with self._operation_lock:
            # Check if a renewal task is already running
            if self._lock_renewal_task and not self._lock_renewal_task.done():
                logger.warning(
                    "Lock renewal task is already running for instance {}",
                    self.instance_id,
                )
                return

            # Ensure shutdown event is cleared before starting the task
            shutdown_event.clear()

            # Set leadership flag before creating the task to ensure consistency
            was_leader = self._is_leader
            self._is_leader = True

            try:
                # Create the renewal task
                logger.debug(
                    "Creating lock renewal task for instance {}", self.instance_id
                )
                self._lock_renewal_task = anyio.create_task(
                    self._renew_leader_lock(shutdown_event),
                    name=f"lock-renewal-{self.instance_id}",
                )

                # Verify the task was created successfully
                if not self._lock_renewal_task:
                    logger.error(
                        "Failed to create lock renewal task for instance {}",
                        self.instance_id,
                    )
                    self._is_leader = was_leader  # Restore previous state
                    raise RuntimeError("Failed to create lock renewal task")

                logger.info(
                    "Successfully started lock renewal task for instance {}",
                    self.instance_id,
                )
            except Exception as e:
                # If task creation failed, restore the previous leadership state
                logger.exception(
                    "Error creating lock renewal task for instance {}", self.instance_id
                )
                self._is_leader = was_leader
                raise

    async def _renew_leader_lock(self, shutdown_event: anyio.Event) -> None:
        """
        Periodically renew the leader lock to maintain leadership.
        Runs as a background task while scheduler is active.

        Args:
            shutdown_event: Event that signals when to stop renewal
        """
        consecutive_failures = 0
        max_consecutive_failures = 3

        logger.info(
            "Starting leader lock renewal task for instance {}", self.instance_id
        )

        while not shutdown_event.is_set() and self._is_leader:
            try:
                # Check if we should continue as leader
                if not await self._should_continue_as_leader(consecutive_failures):
                    break

                # Renew the lock and handle failures
                if not await self._renew_lock(already_locked=True):
                    consecutive_failures = await self._handle_renewal_failure(
                        consecutive_failures, max_consecutive_failures
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        break
                else:
                    # Reset failure counter on successful renewal
                    consecutive_failures = 0

                # Wait for the renewal interval or until shutdown is triggered
                if not await self._wait_for_renewal_interval(shutdown_event):
                    break

            except Exception as e:
                consecutive_failures = await self._handle_renewal_exception(
                    e, consecutive_failures, max_consecutive_failures
                )
                if consecutive_failures >= max_consecutive_failures:
                    break

        logger.info("Leader lock renewal task exiting")
        await self._clear_leadership_status("renewal task exiting")

    async def _should_continue_as_leader(self, consecutive_failures: int) -> bool:
        """Check if we should continue as leader based on health status.

        Args:
            consecutive_failures: Number of consecutive failures so far

        Returns:
            True if we should continue as leader, False otherwise
        """
        # Periodically check lock health for debugging
        if consecutive_failures > 0 or (
            time.time() - getattr(self, "_last_health_check", 0) > 30
        ):
            health = await self.check_leader_lock_health()
            logger.debug("Leader lock health check: {}", health)
            self._last_health_check = time.time()

            # If health check shows we're not the leader, exit
            if health.get("status") == "error" or (
                not health.get("is_owned_by_us", False)
                and health.get("status") != "no_lock"
            ):
                logger.warning(
                    "Health check indicates we're not the leader, stopping renewal"
                )
                await self._clear_leadership_status("health check failed")
                return False

        return True

    async def _handle_renewal_failure(
        self, consecutive_failures: int, max_consecutive_failures: int
    ) -> int:
        """Handle a lock renewal failure.

        Args:
            consecutive_failures: Current number of consecutive failures
            max_consecutive_failures: Maximum allowed consecutive failures

        Returns:
            Updated number of consecutive failures
        """
        consecutive_failures += 1
        logger.warning(
            "Failed to renew leader lock (attempt {}/{})",
            consecutive_failures,
            max_consecutive_failures,
        )

        # If we've failed too many times, give up leadership
        if consecutive_failures >= max_consecutive_failures:
            logger.error(
                "Too many consecutive lock renewal failures ({}), relinquishing leadership",
                consecutive_failures,
            )
            await self._clear_leadership_status("too many renewal failures")

        return consecutive_failures

    async def _handle_renewal_exception(
        self, e: Exception, consecutive_failures: int, max_consecutive_failures: int
    ) -> int:
        """Handle an exception in the renewal loop.

        Args:
            e: The exception that occurred
            consecutive_failures: Current number of consecutive failures
            max_consecutive_failures: Maximum allowed consecutive failures

        Returns:
            Updated number of consecutive failures
        """
        consecutive_failures += 1
        logger.exception(
            "Error in leader lock renewal loop (attempt {}/{})",
            consecutive_failures,
            max_consecutive_failures,
        )
        context = create_error_context("renew_leader_lock_loop")
        self._error_handler.handle_error(
            wrap_naq_exception(e, context="renewing leader lock in loop"),
            context=context,
            reraise=False,
        )

        # If we've failed too many times, give up leadership
        if consecutive_failures >= max_consecutive_failures:
            logger.error(
                "Too many consecutive exceptions in renewal loop ({}), relinquishing leadership",
                consecutive_failures,
            )
            await self._clear_leadership_status("too many renewal exceptions")

        return consecutive_failures

    async def _wait_for_renewal_interval(self, shutdown_event: anyio.Event) -> bool:
        """Wait for the renewal interval or until shutdown is triggered.

        Args:
            shutdown_event: Event that signals when to stop renewal

        Returns:
            True if should continue renewal, False if should exit
        """
        # Wait for the renewal interval or until shutdown is triggered
        with anyio.move_on_after(self.lock_renew_interval):
            await shutdown_event.wait()

        # If shutdown was triggered (not a timeout), exit the loop
        if shutdown_event.is_set():
            logger.debug("Shutdown event detected, exiting renewal loop")
            return False

        return True

    async def _renew_lock(self, already_locked: bool = False) -> bool:
        """Renew the leader lock with a fresh timestamp using atomic compare-and-swap.

        Args:
            already_locked: Whether the operation lock is already held by the caller

        Returns:
            True if lock was successfully renewed, False otherwise
        """
        start_time = time.time()
        record_event(EventType.LOCK_RENEWAL_ATTEMPT, self.instance_id)

        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for lock renewal on instance {}: {}",
                self.instance_id,
                e,
            )
            await self._clear_leadership_status("NatsClient validation failed")
            record_event(
                EventType.LOCK_RENEWAL_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"error": str(e)},
            )
            return False

        try:
            logger.debug(
                "Instance {} attempting to renew leader lock", self.instance_id
            )

            # Use the shared operation lock to prevent race conditions if not already locked
            if already_locked:
                return await self._perform_lock_renewal(start_time)
            else:
                async with self._operation_lock:
                    return await self._perform_lock_renewal(start_time)
        except NaqConnectionError as e:
            logger.warning(
                "Connection error renewing leader lock for instance {}: {}",
                self.instance_id,
                e,
            )
            await self._clear_leadership_status("connection error renewing lock")
            record_event(
                EventType.LOCK_RENEWAL_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "connection_error", "error": str(e)},
            )
            raise LockRenewalError(
                f"Connection error renewing leader lock for instance {self.instance_id}: {e}"
            ) from e
        except Exception as e:
            logger.exception(
                "Unexpected error renewing leader lock for instance {}",
                self.instance_id,
            )
            await self._clear_leadership_status("unexpected error renewing lock")
            context = create_error_context("renew_leader_lock")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="renewing leader lock"),
                context=context,
                reraise=False,
            )
            record_event(
                EventType.LOCK_RENEWAL_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "exception", "error": str(e)},
            )
            raise LockRenewalError(
                f"Unexpected error renewing leader lock for instance {self.instance_id}: {e}"
            ) from e

    async def _perform_lock_renewal(self, start_time: float) -> bool:
        """Perform the actual lock renewal operation.

        Args:
            start_time: When the renewal operation started (for metrics)

        Returns:
            True if lock was successfully renewed, False otherwise
        """

        # Get the KV store object using circuit breaker
        async def get_kv_store():
            return await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)

        kv = await self._circuit_breaker.call(get_kv_store)

        # Prepare updated lock data
        new_lock_data = self._create_lock_data()
        serialized_new_lock_data = msgspec.msgpack.encode(new_lock_data)

        # Use a timeout to prevent deadlocks
        with anyio.move_on_after(3.0):  # 3 second timeout
            # Get the current lock entry and its revision using circuit breaker
            async def get_lock_entry():
                return await kv.get(SCHEDULER_LOCK_KEY)

            current_entry = await self._circuit_breaker.call(get_lock_entry)

            if current_entry is None:
                # Lock doesn't exist, we can't renew it
                return await self._handle_missing_lock_on_renewal_with_metrics(
                    start_time
                )
            else:
                # Lock exists, check if we still hold it
                return await self._handle_existing_lock_on_renewal_with_metrics(
                    kv, current_entry, serialized_new_lock_data, start_time
                )

        # If we get here, the operation timed out
        return await self._handle_lock_renewal_timeout(start_time)

    async def _handle_missing_lock_on_renewal_with_metrics(
        self, start_time: float
    ) -> bool:
        """Handle the case where the lock doesn't exist during renewal with metrics.

        Args:
            start_time: When the renewal operation started (for metrics)

        Returns:
            Always False since we can't renew a non-existent lock
        """
        result = await self._handle_missing_lock_on_renewal()
        if result:
            record_event(
                EventType.LOCK_RENEWAL_SUCCESS,
                self.instance_id,
                duration=time.time() - start_time,
            )
        else:
            record_event(
                EventType.LOCK_RENEWAL_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "missing_lock"},
            )
        return result

    async def _handle_existing_lock_on_renewal_with_metrics(
        self, kv, current_entry, serialized_new_lock_data: bytes, start_time: float
    ) -> bool:
        """Handle an existing lock during renewal with metrics.

        Args:
            kv: KV store instance
            current_entry: Current lock entry
            serialized_new_lock_data: Serialized lock data
            start_time: When the renewal operation started (for metrics)

        Returns:
            True if lock was renewed successfully, False otherwise
        """
        result = await self._handle_existing_lock_on_renewal(
            kv, current_entry, serialized_new_lock_data
        )
        if result:
            record_event(
                EventType.LOCK_RENEWAL_SUCCESS,
                self.instance_id,
                duration=time.time() - start_time,
            )
        else:
            record_event(
                EventType.LOCK_RENEWAL_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"reason": "not_owned_by_us"},
            )
        return result

    async def _handle_lock_renewal_timeout(self, start_time: float) -> bool:
        """Handle lock renewal timeout.

        Args:
            start_time: When the renewal operation started (for metrics)

        Returns:
            Never returns, always raises LockTimeoutError
        """
        logger.warning(
            "Leader lock renewal for instance {} timed out", self.instance_id
        )
        await self._clear_leadership_status("lock renewal timed out")
        record_event(
            EventType.LOCK_RENEWAL_FAILURE,
            self.instance_id,
            duration=time.time() - start_time,
            metadata={"reason": "timeout"},
        )
        raise LockTimeoutError(
            f"Leader lock renewal for instance {self.instance_id} timed out"
        )

    async def _handle_missing_lock_on_renewal(self) -> bool:
        """Handle the case where the lock doesn't exist during renewal.

        Returns:
            Always False since we can't renew a non-existent lock
        """
        logger.warning(
            "Instance {} cannot renew lock: lock no longer exists", self.instance_id
        )
        await self._clear_leadership_status("lock no longer exists")
        return False

    async def _handle_existing_lock_on_renewal(
        self, kv, current_entry, serialized_new_lock_data: bytes
    ) -> bool:
        """Handle an existing lock during renewal.

        Args:
            kv: KV store instance
            current_entry: Current lock entry
            serialized_new_lock_data: Serialized lock data

        Returns:
            True if lock was renewed successfully, False otherwise
        """
        try:
            # Assuming current_entry.value is bytes and needs deserialization
            # And current_entry.revision holds the revision number
            existing_lock_data = msgspec.msgpack.decode(
                current_entry.value, type=LockData
            )
            if existing_lock_data.instance_id == self.instance_id:
                # We hold the lock, try to renew it atomically using circuit breaker
                async def update_lock():
                    return await kv.update(
                        SCHEDULER_LOCK_KEY,
                        serialized_new_lock_data,
                        current_entry.revision,
                        self.lock_ttl,
                    )

                await self._circuit_breaker.call(update_lock)
                self._last_lock_renewal = time.time()
                logger.debug(
                    "Instance {} renewed leader lock. Next renewal in {}s",
                    self.instance_id,
                    self.lock_renew_interval,
                )
                return True
            else:
                # Lock is held by another instance
                logger.warning(
                    "Instance {} cannot renew lock: lock is now held by another instance {}",
                    self.instance_id,
                    existing_lock_data.instance_id,
                )
                await self._clear_leadership_status("lock held by another instance")
                return False
        except Exception as e:  # nats.js.errors.WrongLastRevisionError or similar
            logger.warning(
                "Instance {} failed to renew lock, it was updated by another instance: {}",
                self.instance_id,
                e,
            )
            await self._clear_leadership_status("lock updated by another instance")
            return False

    def _log_leader_revoked(self) -> None:
        """Log leader_revoked event if we were previously the leader."""
        if self._is_leader:
            logger.info("Leader revoked: {} at {}", self.instance_id, time.time())

    async def _clear_leadership_status(self, reason: str) -> None:
        """Clear leadership status consistently across all shutdown scenarios.

        Args:
            reason: The reason for clearing leadership status (for logging)
        """
        if self._is_leader:
            logger.info(
                "Instance {} relinquishing leadership due to {}",
                self.instance_id,
                reason,
            )
            self._log_leader_revoked()
            record_event(
                EventType.LEADERSHIP_LOST, self.instance_id, metadata={"reason": reason}
            )
        self._is_leader = False

    async def stop_renewal_task(self) -> None:
        """Stop the lock renewal task and signal that we're no longer leader."""
        # Use shared lock to prevent concurrent operations
        async with self._operation_lock:
            # Set shutdown event first to signal the renewal loop to exit
            self._shutdown_event.set()

            if self._lock_renewal_task and not self._lock_renewal_task.done():
                logger.debug(
                    "Cancelling lock renewal task for instance {}", self.instance_id
                )
                self._lock_renewal_task.cancel()

                try:
                    # Use a timeout to avoid hanging during shutdown
                    with anyio.move_on_after(5.0):  # 5 second timeout
                        await self._lock_renewal_task
                    # If the task is still not done, log a warning
                    if not self._lock_renewal_task.done():
                        logger.warning(
                            "Lock renewal task did not complete gracefully within timeout"
                        )
                except anyio.get_cancelled_exc_class():
                    logger.debug("Lock renewal task cancelled successfully")
                    pass  # Expected when task is cancelled
                except Exception as e:
                    logger.exception(
                        "Error stopping lock renewal task for instance {}",
                        self.instance_id,
                    )
                    context = create_error_context("stop_renewal_task")
                    self._error_handler.handle_error(
                        wrap_naq_exception(e, context="stopping lock renewal task"),
                        context=context,
                        reraise=False,
                    )

            # Always ensure leadership flag is cleared, even if task cancellation failed
            await self._clear_leadership_status("renewal task stopped")

    async def release_lock(self) -> None:
        """Explicitly release the leader lock when shutting down."""
        start_time = time.time()
        record_event(EventType.LOCK_RELEASE_ATTEMPT, self.instance_id)

        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for releasing lock on instance {}: {}",
                self.instance_id,
                e,
            )
            record_event(
                EventType.LOCK_RELEASE_FAILURE,
                self.instance_id,
                duration=time.time() - start_time,
                metadata={"error": str(e)},
            )
            raise LockReleaseError(
                f"Connection error releasing leader lock for instance {self.instance_id}: {e}"
            ) from e

        # Use shared lock to prevent concurrent operations
        async with self._operation_lock:
            # Check leadership status once to avoid race conditions
            is_current_leader = self._is_leader

            if is_current_leader:
                try:
                    # First check if we actually still hold the lock
                    health = await self.check_leader_lock_health()
                    logger.debug("Lock health before release: {}", health)

                    if health.get("is_owned_by_us", False):
                        await self._perform_lock_release()
                        record_event(
                            EventType.LOCK_RELEASE_SUCCESS,
                            self.instance_id,
                            duration=time.time() - start_time,
                        )
                    else:
                        logger.info(
                            "Instance {} no longer holds the leader lock, nothing to release",
                            self.instance_id,
                        )
                        await self._clear_leadership_status("no longer holds lock")
                        record_event(
                            EventType.LOCK_RELEASE_FAILURE,
                            self.instance_id,
                            duration=time.time() - start_time,
                            metadata={"reason": "not_owned_by_us"},
                        )
                except Exception as e:
                    logger.exception(
                        "Error releasing leader lock for instance {}", self.instance_id
                    )
                    context = create_error_context("release_leader_lock")
                    self._error_handler.handle_error(
                        wrap_naq_exception(e, context="releasing leader lock"),
                        context=context,
                        reraise=False,
                    )
                    record_event(
                        EventType.LOCK_RELEASE_FAILURE,
                        self.instance_id,
                        duration=time.time() - start_time,
                        metadata={"reason": "exception", "error": str(e)},
                    )
                    raise LockReleaseError(
                        f"Error releasing leader lock for instance {self.instance_id}: {e}"
                    ) from e
            else:
                logger.debug(
                    "Instance {} is not the leader, no lock to release",
                    self.instance_id,
                )
                record_event(
                    EventType.LOCK_RELEASE_FAILURE,
                    self.instance_id,
                    duration=time.time() - start_time,
                    metadata={"reason": "not_leader"},
                )

            # Always ensure leadership flag is cleared using consistent method
            await self._clear_leadership_status("lock release completed")

    async def _perform_lock_release(self) -> None:
        """Perform the actual lock release operation."""
        try:
            await self._validate_client_with_retry()
        except NaqConnectionError as e:
            logger.error(
                "NatsClient validation failed for releasing lock on instance {}: {}",
                self.instance_id,
                e,
            )
            raise LockReleaseError(
                f"Connection error releasing leader lock for instance {self.instance_id}: {e}"
            ) from e

        logger.info("Instance {} releasing leader lock", self.instance_id)

        # Get the KV store object
        kv = await self._client.get_kv_store(SCHEDULER_LOCK_KV_NAME)

        # Use a timeout to avoid hanging during shutdown
        with anyio.move_on_after(5.0):  # 5 second timeout
            # Get the current lock entry and its revision using circuit breaker
            async def get_lock_entry():
                return await kv.get(SCHEDULER_LOCK_KEY)

            current_entry = await self._circuit_breaker.call(get_lock_entry)

            if current_entry is not None:
                # Delete the lock with the revision to ensure atomicity using circuit breaker
                async def delete_lock():
                    return await kv.delete(SCHEDULER_LOCK_KEY, current_entry.revision)

                await self._circuit_breaker.call(delete_lock)

                # Verify the lock was actually released
                verification = await self.check_leader_lock_health()
                if verification.get("status") == "no_lock":
                    logger.info("Successfully released scheduler leader lock")
                    self._log_leader_revoked()
                else:
                    logger.warning(
                        "Lock release verification failed. Lock status: {}",
                        verification.get("status"),
                    )
            else:
                # Lock doesn't exist, nothing to release
                logger.info("Lock no longer exists, nothing to release")
                self._log_leader_revoked()

        # If the operation timed out, log a warning
        if self._is_leader:  # Still leader means the operation didn't complete
            logger.warning("Leader lock release operation timed out")
            await self._clear_leadership_status("lock release timed out")

    @property
    def is_leader(self) -> bool:
        """Returns True if this instance is currently the leader."""
        return self._is_leader


class Scheduler:
    """
    Scheduler for NAQ jobs. Polls the scheduled jobs KV store and enqueues jobs
    that are ready.
    Supports high availability through leader election using NATS KV store.
    """

    def __init__(
        self,
        nats_url: Optional[str] = None,
        client: Optional[NatsClient] = None,
        poll_interval: float = 1.0,  # Check for jobs every second
        instance_id: Optional[str] = None,  # For HA leader election
        enable_ha: bool = True,  # Whether to enable HA leader election
        config: Optional[object] = None,  # Configuration for backward compatibility
    ) -> None:
        """Initialize the scheduler.

        Args:
            nats_url: NATS server URL for connection
            client: NATS client for accessing NAQ services
            poll_interval: Interval in seconds to check for scheduled jobs
            instance_id: Unique identifier for this scheduler instance
            enable_ha: Whether to enable high availability mode with leader election
            config: Configuration for backward compatibility

        Raises:
            ValueError: If neither nats_url nor client is provided
        """
        self._validate_init_params(nats_url, client)
        self._initialize_connection_params(nats_url, client, config)

        self._poll_interval = poll_interval
        self._running = False
        self._shutdown_event = anyio.Event()
        self._run_lock = (
            anyio.Lock()
        )  # Initialize run lock to prevent concurrent run calls

        self._instance_id = self._generate_instance_id(instance_id)
        self._enable_ha = enable_ha

        self._initialize_components()

        setup_logging()  # Set up logging

    def _validate_init_params(
        self, nats_url: Optional[str], client: Optional[NatsClient]
    ) -> None:
        """Validate initialization parameters.

        Args:
            nats_url: NATS server URL for connection
            client: NATS client for accessing NAQ services

        Raises:
            ValueError: If neither nats_url nor client is provided
        """
        if nats_url is None and client is None:
            raise ValueError("Either nats_url or client must be provided")

    def _initialize_connection_params(
        self,
        nats_url: Optional[str],
        client: Optional[NatsClient],
        config: Optional[object],
    ) -> None:
        """Initialize connection parameters.

        Args:
            nats_url: NATS server URL for connection
            client: NATS client for accessing NAQ services
            config: Configuration for backward compatibility
        """
        if nats_url is not None:
            self._nats_url = nats_url
            self._client = None  # Will be created in _connect()
            self._config = config
        else:
            self._client = client
            self._nats_url = None
            self._config = None

    def _generate_instance_id(self, instance_id: Optional[str]) -> str:
        """Generate a unique instance ID if none provided.

        Args:
            instance_id: Optional instance ID to use

        Returns:
            Unique instance identifier
        """
        return instance_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    def _initialize_components(self) -> None:
        """Initialize scheduler components."""
        self._leader_election = LeaderElection(
            instance_id=self._instance_id,
            lock_ttl=SCHEDULER_LOCK_TTL_SECONDS,
            lock_renew_interval=SCHEDULER_LOCK_RENEW_INTERVAL_SECONDS,
            client=None,  # Will be set during _connect()
        )

    def _initialize_services(self) -> None:
        """Initialize service references (will be populated during _connect)."""
        self._client: Optional[NatsClient] = None
        self._error_handler = ErrorHandler(logger)

    async def _connect(self) -> None:
        """Establish service connections and initialize components."""
        try:
            # Use a task group for connection operations
            async with anyio.create_task_group() as tg:
                if self._nats_url:
                    connect_task = tg.start_soon(self._connect_with_nats_url)
                else:
                    connect_task = tg.start_soon(self._connect_with_client)

                # Wait for connection to complete
                await connect_task
        except Exception as e:
            context = create_error_context("scheduler_connect")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="connecting to services"),
                context=context,
                reraise=True,
            )

    async def _connect_with_nats_url(self) -> None:
        """Connect using NATS URL."""
        self._client = NatsClient(nats_url=self._nats_url)
        await self._client.connect()
        await self._initialize_services(self._client)

    async def _connect_with_client(self) -> None:
        """Connect using provided client."""
        await self._initialize_services(self._client)

    async def _initialize_services(self, client: NatsClient) -> None:
        """Initialize services from client."""
        try:
            self._client = client
            
            logger.info(
                f"Scheduler connected to services and KV store '{SCHEDULED_JOBS_KV_NAME}'."
            )

            if self._enable_ha:
                self._leader_election._client = self._client
                await self._leader_election.initialize()
        except Exception as e:
            context = create_error_context("initialize_services")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="initializing services"),
                context=context,
                reraise=True,
            )

    async def run(self) -> None:
        """Starts the scheduler loop with leader election."""
        # Use a lock to prevent concurrent run calls
        if self._run_lock.locked():
            logger.warning("Scheduler is already running, ignoring duplicate run call")
            return

        async with self._run_lock:
            logger.info("Starting scheduler instance {}", self._instance_id)
            self._running = True
            self._shutdown_event.clear()
            self.install_signal_handlers()

            try:
                # Use a timeout for connection to prevent hanging
                logger.debug("Connecting to services...")
                connected = False
                with anyio.move_on_after(30.0):  # 30 second timeout
                    await self._connect()
                    self._log_scheduler_startup()
                    connected = True

                if not connected:
                    logger.error("Scheduler connection timed out after 30 seconds")
                    return

                # Drift-free loop: each cycle aims to start every poll_interval seconds
                cycle_count = 0
                while self._running:
                    cycle_count += 1
                    cycle_start = time.time()
                    logger.debug("Starting scheduler cycle #{}", cycle_count)

                    # Use a task group for concurrent operations with a timeout
                    cycle_completed = False
                    with anyio.move_on_after(
                        self._poll_interval + 5.0
                    ):  # Add buffer time
                        async with anyio.create_task_group() as tg:
                            # Run leadership transition and job processing concurrently
                            leadership_task = tg.start_soon(
                                self._handle_leadership_transition
                            )
                            jobs_task = tg.start_soon(self._process_scheduled_jobs)

                            # Wait for both tasks to complete
                            await leadership_task
                            await jobs_task
                        cycle_completed = True

                    if not cycle_completed:
                        logger.warning(
                            "Scheduler cycle #{} timed out after {} seconds",
                            cycle_count,
                            self._poll_interval + 5.0,
                        )

                    # Wait for the next cycle
                    if not await self._wait_for_next_cycle(cycle_start):
                        logger.info("Scheduler loop exiting normally")
                        break

            except Exception as e:
                logger.exception("Unexpected error in scheduler run loop")
                context = create_error_context("scheduler_run_loop")
                self._error_handler.handle_error(
                    wrap_naq_exception(e, context="scheduler run loop"),
                    context=context,
                    reraise=False,
                )
            finally:
                logger.info("Scheduler run method completing, initiating shutdown")
                await self._shutdown()

    def _log_scheduler_startup(self) -> None:
        """Log scheduler startup information."""
        logger.info(
            "Scheduler instance {} started. Polling interval: {}s",
            self._instance_id,
            self._poll_interval,
        )
        logger.info(
            "High availability mode: {}", "enabled" if self._enable_ha else "disabled"
        )
        logger.debug(
            "Scheduler configuration: instance_id={}, poll_interval={}, enable_ha={}",
            self._instance_id,
            self._poll_interval,
            self._enable_ha,
        )

    async def _handle_leadership_transition(self) -> None:
        """Handle leadership transition logic."""
        # Check leadership status once to avoid race conditions
        was_leader = self.is_leader
        is_current_leader = self.is_leader

        logger.debug(
            "Handling leadership transition. Was leader: {}, Is leader: {}, HA enabled: {}",
            was_leader,
            is_current_leader,
            self._enable_ha,
        )

        if self._enable_ha:
            await self._handle_ha_leadership(was_leader)
        elif not was_leader:
            # If HA is disabled, always consider self as leader
            # Use a lock to prevent concurrent modifications
            async with anyio.create_lock():
                self._leader_election._is_leader = True
                logger.info(
                    "HA disabled, instance {} assuming leadership", self._instance_id
                )

    async def _handle_ha_leadership(self, was_leader: bool) -> None:
        """Handle high availability leadership logic."""
        # Check leadership status once to avoid race conditions
        is_current_leader = self.is_leader

        if not is_current_leader:
            logger.debug("Instance {} attempting to become leader", self._instance_id)
            # Try to become leader
            if await self._leader_election.try_become_leader():
                logger.info("Instance {} successfully became leader", self._instance_id)
                # Just became leader, start renewal task
                # Use a timeout to prevent deadlocks
                renewal_started = False
                with anyio.move_on_after(5.0):  # 5 second timeout
                    await self._leader_election.start_renewal_task(self._shutdown_event)
                    renewal_started = True

                if not renewal_started:
                    logger.warning(
                        "Leader renewal task start timed out for instance {}",
                        self._instance_id,
                    )
            else:
                logger.debug("Instance {} failed to become leader", self._instance_id)

    async def _process_scheduled_jobs(self) -> None:
        """Process scheduled jobs if this instance is the leader."""
        # Check leadership status once to avoid race conditions
        is_current_leader = self.is_leader

        if is_current_leader and self._client:
            logger.debug(
                "Processing scheduled jobs as leader instance {}", self._instance_id
            )
            try:
                # Use a timeout to prevent deadlocks
                with anyio.move_on_after(10.0):  # 10 second timeout
                    processed, errors = await self._client.trigger_due_jobs()
                    # Log summary only if something happened
                    if processed > 0 or errors > 0:
                        logger.info(
                            "Scheduler processed {} ready jobs, encountered {} errors",
                            processed,
                            errors,
                        )
                    else:
                        logger.debug("No scheduled jobs ready for processing")
                    return

                # If we get here, the operation timed out
                logger.warning("Scheduled jobs processing timed out after 10 seconds")
            except Exception as e:
                logger.exception("Error processing scheduled jobs")
                context = create_error_context("process_scheduled_jobs")
                self._error_handler.handle_error(
                    wrap_naq_exception(e, context="processing scheduled jobs"),
                    context=context,
                    reraise=False,
                )
        else:
            logger.debug(
                "Instance {} is not the leader, skipping job processing",
                self._instance_id,
            )

    async def _wait_for_next_cycle(self, cycle_start: float) -> bool:
        """Wait for the next scheduler cycle.

        Args:
            cycle_start: Timestamp when the current cycle started

        Returns:
            True if should continue to next cycle, False if shutting down
        """
        # If shutdown was triggered, exit promptly
        if self._shutdown_event.is_set():
            logger.debug("Shutdown event detected, exiting scheduler loop")
            return False

        elapsed = time.time() - cycle_start
        remaining = self._poll_interval - elapsed

        # Processing took longer than poll interval; start next cycle immediately
        if remaining <= 0:
            logger.debug(
                "Processing took longer than poll interval, starting next cycle immediately"
            )
            return True

        try:
            logger.debug("Waiting {} seconds for next scheduler cycle", remaining)
            # Wait only for the remaining time or until shutdown is triggered
            with anyio.move_on_after(remaining):
                await self._shutdown_event.wait()

            # If wait() finishes without timeout, shutdown was triggered
            should_continue = self._shutdown_event.is_set() is False
            if not should_continue:
                logger.debug(
                    "Shutdown event detected during wait, exiting scheduler loop"
                )
            return should_continue
        except TimeoutError:
            # Timeout is expected, continue the loop on next tick
            logger.debug("Wait timeout reached, continuing to next cycle")
            return True
        except Exception as e:
            logger.exception("Error while waiting for next scheduler cycle")
            context = create_error_context("wait_for_next_cycle")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="waiting for next scheduler cycle"),
                context=context,
                reraise=False,
            )
            # Continue to next cycle despite errors
            return True

    async def _shutdown(self) -> None:
        """Perform graceful shutdown of the scheduler."""
        logger.info("Scheduler instance {} shutting down...", self._instance_id)

        try:
            # Use a task group for concurrent shutdown operations
            async with anyio.create_task_group() as tg:
                # Stop leadership processes if enabled
                if self._enable_ha:
                    logger.debug(
                        "Stopping leader election processes for instance {}",
                        self._instance_id,
                    )
                    stop_task = tg.start_soon(self._leader_election.stop_renewal_task)
                    release_task = tg.start_soon(self._leader_election.release_lock)

                    # Wait for leadership tasks to complete
                    await stop_task
                    await release_task

                # Close connections
                logger.debug("Closing connections for instance {}", self._instance_id)
                close_task = tg.start_soon(self._close)
                await close_task

            logger.info("Scheduler instance {} shutdown complete.", self._instance_id)
        except Exception as e:
            logger.exception(
                "Error during scheduler shutdown for instance {}", self._instance_id
            )
            context = create_error_context("scheduler_shutdown")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="shutting down scheduler"),
                context=context,
                reraise=False,
            )
            logger.info(
                "Scheduler instance {} shutdown completed with errors.",
                self._instance_id,
            )

    async def _close(self) -> None:
        """Cleans up resources."""
        try:
            # Use a task group for concurrent cleanup operations
            async with anyio.create_task_group() as tg:
                # Close client connection
                if self._client:
                    await self._client.close()
                    self._client = None

                # Ensure shutdown event is set
                self._shutdown_event.set()

                # Clear running flag
                self._running = False
        except Exception as e:
            context = create_error_context("scheduler_close")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="closing scheduler resources"),
                context=context,
                reraise=False,
            )

    def signal_handler(self, sig: int, frame) -> None:
        """Handles termination signals.

        Args:
            sig: The signal number
            frame: The current stack frame
        """
        signal_name = (
            "SIGINT"
            if sig == signal.SIGINT
            else "SIGTERM"
            if sig == signal.SIGTERM
            else f"UNKNOWN({sig})"
        )
        logger.warning(
            "Received signal {} for instance {}. Initiating graceful shutdown...",
            signal_name,
            self._instance_id,
        )
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
        return not self._enable_ha or self._leader_election.is_leader

    async def __aenter__(self):
        """Enter the async context manager."""
        try:
            # Initialize the client if needed
            if self._nats_url:
                self._client = NatsClient(nats_url=self._nats_url)
                await self._client.connect()
            return self
        except Exception as e:
            context = create_error_context("scheduler_context_enter")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="entering scheduler context"),
                context=context,
                reraise=True,
            )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager."""
        try:
            await self._close()
        except Exception as e:
            context = create_error_context("scheduler_context_exit")
            self._error_handler.handle_error(
                wrap_naq_exception(e, context="exiting scheduler context"),
                context=context,
                reraise=False,
            )
