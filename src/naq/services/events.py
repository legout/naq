"""
Event Service for NAQ

This module provides a centralized service for managing job and worker events,
including event logging, storage, and retrieval functionality.
"""

import time
from typing import Any, Dict, List, Optional, Union

import cloudpickle
import msgspec
from loguru import logger

from ..exceptions import NaqException
from ..models.events import JobEvent, WorkerEvent
from ..models.enums import JobEventType, WorkerEventType
from .base import BaseService, ServiceConfig, ServiceInitializationError, ServiceRuntimeError
from .connection import ConnectionService
from .kv_stores import KVStoreService


class EventServiceConfig(msgspec.Struct):
    """
    Configuration for the EventService.

    Attributes:
        events_bucket_name: Name of the KV bucket for storing events
        max_events_per_job: Maximum number of events to store per job
        event_retention_seconds: Time to retain events in seconds
        enable_event_logging: Whether to enable event logging
        auto_create_bucket: Whether to automatically create the events bucket
    """

    events_bucket_name: str = "naq_events"
    max_events_per_job: int = 100
    event_retention_seconds: Optional[int] = None
    enable_event_logging: bool = True
    auto_create_bucket: bool = True

    def as_dict(self) -> Dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "events_bucket_name": self.events_bucket_name,
            "max_events_per_job": self.max_events_per_job,
            "event_retention_seconds": self.event_retention_seconds,
            "enable_event_logging": self.enable_event_logging,
            "auto_create_bucket": self.auto_create_bucket,
        }


class EventService(BaseService):
    """
    Centralized event management service.

    This service provides functionality for logging, storing, and retrieving
    job and worker events. It uses the KVStoreService for persistent storage
    and provides both sync and async interfaces for event operations.
    """

    def __init__(
        self,
        config: Optional[ServiceConfig] = None,
        connection_service: Optional[ConnectionService] = None,
        kv_store_service: Optional[KVStoreService] = None,
    ) -> None:
        """
        Initialize the event service.

        Args:
            config: Optional configuration for the service.
            connection_service: Optional ConnectionService dependency.
            kv_store_service: Optional KVStoreService dependency.
        """
        super().__init__(config)
        self._event_config = self._extract_event_config()
        self._connection_service = connection_service
        self._kv_store_service = kv_store_service

    def _extract_event_config(self) -> EventServiceConfig:
        """
        Extract event-specific configuration from the service config.

        Returns:
            EventServiceConfig instance with event parameters.
        """
        # Start with default config
        event_config = EventServiceConfig()

        # Override with service config if provided
        if self._config and self._config.custom_settings:
            custom_settings = self._config.custom_settings

            if "events_bucket_name" in custom_settings:
                event_config.events_bucket_name = custom_settings["events_bucket_name"]

            if "max_events_per_job" in custom_settings:
                event_config.max_events_per_job = custom_settings["max_events_per_job"]

            if "event_retention_seconds" in custom_settings:
                event_config.event_retention_seconds = custom_settings["event_retention_seconds"]

            if "enable_event_logging" in custom_settings:
                event_config.enable_event_logging = custom_settings["enable_event_logging"]

            if "auto_create_bucket" in custom_settings:
                event_config.auto_create_bucket = custom_settings["auto_create_bucket"]

        return event_config

    async def _do_initialize(self) -> None:
        """
        Initialize the event service.

        This method validates the configuration and ensures the required
        services are available.

        Raises:
            ServiceInitializationError: If initialization fails.
        """
        try:
            self._logger.info("Initializing EventService")

            # Validate configuration
            if self._event_config.max_events_per_job <= 0:
                raise ServiceInitializationError("max_events_per_job must be positive")

            # Ensure connection service is available if KV store service is not provided
            if self._kv_store_service is None and self._connection_service is None:
                raise ServiceInitializationError("ConnectionService or KVStoreService is required")

            # Ensure connection service is initialized if provided
            if self._connection_service is not None and not self._connection_service.is_initialized:
                await self._connection_service.initialize()

            # Create KV store service if not provided
            if self._kv_store_service is None:
                from .kv_stores import KVStoreService, KVStoreServiceConfig
                
                kv_config = KVStoreServiceConfig(
                    auto_create_buckets=self._event_config.auto_create_bucket
                )
                self._kv_store_service = KVStoreService(
                    config=ServiceConfig(custom_settings=kv_config.as_dict()),
                    connection_service=self._connection_service
                )
                await self._kv_store_service.initialize()

            self._logger.info("EventService initialized successfully")

        except Exception as e:
            error_msg = f"Failed to initialize EventService: {e}"
            self._logger.error(error_msg)
            raise ServiceInitializationError(error_msg) from e

    async def _do_cleanup(self) -> None:
        """
        Clean up event service resources.

        This method cleans up the KV store service if it was created by this service.
        """
        try:
            self._logger.info("Cleaning up EventService")

            # Note: We don't clean up externally provided services
            # Only clean up if we created the KV store service
            if self._kv_store_service is not None and self._connection_service is not None:
                await self._kv_store_service.cleanup()

            self._logger.info("EventService cleaned up successfully")

        except Exception as e:
            error_msg = f"Failed to cleanup EventService: {e}"
            self._logger.error(error_msg)
            raise ServiceRuntimeError(error_msg) from e

    async def log_job_event(self, event: JobEvent) -> None:
        """
        Log a job event.

        Args:
            event: The JobEvent to log.

        Raises:
            NaqException: If logging the event fails.
        """
        if not self._event_config.enable_event_logging:
            return

        try:
            # Create event key
            event_key = f"job:{event.job_id}:events"
            
            # Get current events for this job
            try:
                current_events = await self._kv_store_service.get(
                    self._event_config.events_bucket_name,
                    event_key,
                    deserialize=True
                )
                if not isinstance(current_events, list):
                    current_events = []
            except NaqException:
                current_events = []

            # Add new event
            current_events.append(event)

            # Apply retention policy
            if len(current_events) > self._event_config.max_events_per_job:
                # Keep only the most recent events
                current_events = current_events[-self._event_config.max_events_per_job:]

            # Apply time-based retention if configured
            if self._event_config.event_retention_seconds:
                cutoff_time = time.time() - self._event_config.event_retention_seconds
                current_events = [
                    e for e in current_events 
                    if e.timestamp >= cutoff_time
                ]

            # Store updated events
            await self._kv_store_service.put(
                self._event_config.events_bucket_name,
                event_key,
                current_events,
                serialize=True
            )

            self._logger.debug(f"Logged job event {event.event_type.value} for job {event.job_id}")

        except Exception as e:
            error_msg = f"Failed to log job event for job {event.job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def log_worker_event(self, event: WorkerEvent) -> None:
        """
        Log a worker event.

        Args:
            event: The WorkerEvent to log.

        Raises:
            NaqException: If logging the event fails.
        """
        if not self._event_config.enable_event_logging:
            return

        try:
            # Create event key
            event_key = f"worker:{event.worker_id}:events"
            
            # Get current events for this worker
            try:
                current_events = await self._kv_store_service.get(
                    self._event_config.events_bucket_name,
                    event_key,
                    deserialize=True
                )
                if not isinstance(current_events, list):
                    current_events = []
            except NaqException:
                current_events = []

            # Add new event
            current_events.append(event)

            # Apply retention policy
            if len(current_events) > self._event_config.max_events_per_job:
                # Keep only the most recent events
                current_events = current_events[-self._event_config.max_events_per_job:]

            # Apply time-based retention if configured
            if self._event_config.event_retention_seconds:
                cutoff_time = time.time() - self._event_config.event_retention_seconds
                current_events = [
                    e for e in current_events 
                    if e.timestamp >= cutoff_time
                ]

            # Store updated events
            await self._kv_store_service.put(
                self._event_config.events_bucket_name,
                event_key,
                current_events,
                serialize=True
            )

            self._logger.debug(f"Logged worker event {event.event_type.value} for worker {event.worker_id}")

        except Exception as e:
            error_msg = f"Failed to log worker event for worker {event.worker_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def get_job_events(
        self, 
        job_id: str, 
        event_type: Optional[JobEventType] = None,
        limit: Optional[int] = None
    ) -> List[JobEvent]:
        """
        Get events for a specific job.

        Args:
            job_id: ID of the job to get events for.
            event_type: Optional event type to filter by.
            limit: Optional maximum number of events to return.

        Returns:
            List of JobEvent objects.

        Raises:
            NaqException: If retrieving events fails.
        """
        try:
            event_key = f"job:{job_id}:events"
            
            # Get events for this job
            try:
                events = await self._kv_store_service.get(
                    self._event_config.events_bucket_name,
                    event_key,
                    deserialize=True
                )
                if not isinstance(events, list):
                    events = []
            except NaqException:
                events = []

            # Filter by event type if specified
            if event_type is not None:
                events = [e for e in events if e.event_type == event_type]

            # Apply limit if specified
            if limit is not None:
                events = events[-limit:]

            return events

        except Exception as e:
            error_msg = f"Failed to get job events for job {job_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    async def get_worker_events(
        self, 
        worker_id: str, 
        event_type: Optional[WorkerEventType] = None,
        limit: Optional[int] = None
    ) -> List[WorkerEvent]:
        """
        Get events for a specific worker.

        Args:
            worker_id: ID of the worker to get events for.
            event_type: Optional event type to filter by.
            limit: Optional maximum number of events to return.

        Returns:
            List of WorkerEvent objects.

        Raises:
            NaqException: If retrieving events fails.
        """
        try:
            event_key = f"worker:{worker_id}:events"
            
            # Get events for this worker
            try:
                events = await self._kv_store_service.get(
                    self._event_config.events_bucket_name,
                    event_key,
                    deserialize=True
                )
                if not isinstance(events, list):
                    events = []
            except NaqException:
                events = []

            # Filter by event type if specified
            if event_type is not None:
                events = [e for e in events if e.event_type == event_type]

            # Apply limit if specified
            if limit is not None:
                events = events[-limit:]

            return events

        except Exception as e:
            error_msg = f"Failed to get worker events for worker {worker_id}: {e}"
            self._logger.error(error_msg)
            raise NaqException(error_msg) from e

    @property
    def event_config(self) -> EventServiceConfig:
        """Get the event service configuration."""
        return self._event_config

    @property
    def is_logging_enabled(self) -> bool:
        """Check if event logging is enabled."""
        return self._event_config.enable_event_logging