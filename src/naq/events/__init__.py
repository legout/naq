# src/naq/events/__init__.py
"""
NAQ Event Logging System

This module provides comprehensive job lifecycle event logging and processing
capabilities for the NAQ job queue system.

Key Components:
- JobEvent: Data structure representing job lifecycle events
- JobEventType: Enumeration of all possible event types
- AsyncJobEventLogger: High-performance, non-blocking event logger
- JobEventLogger: Synchronous wrapper for the async event logger
- AsyncJobEventProcessor: Real-time event stream processor
- NATSJobEventStorage: NATS JetStream-based event storage backend
- BaseEventStorage: Abstract interface for storage backends

Quick Start:

    # Basic event logging
    from naq.events import AsyncJobEventLogger
    
    logger = AsyncJobEventLogger()
    await logger.start()
    await logger.log_job_started("job-123", "worker-1", "high-priority")
    await logger.stop()

    # Real-time event monitoring
    from naq.events import AsyncJobEventProcessor
    
    processor = AsyncJobEventProcessor()
    
    # Register event handlers
    def on_job_completed(event):
        print(f"Job {event.job_id} completed!")
    
    processor.on_job_completed(on_job_completed)
    
    await processor.start()
    # Events will be processed in real-time
    await processor.stop()

Configuration:
    Event logging can be configured via environment variables:
    
    - NAQ_EVENTS_ENABLED: Enable/disable event logging (default: true)
    - NAQ_EVENT_STREAM_NAME: NATS stream name (default: NAQ_JOB_EVENTS)
    - NAQ_EVENT_SUBJECT_PREFIX: Subject prefix (default: naq.jobs.events)
    - NAQ_EVENT_LOGGER_BATCH_SIZE: Logger batch size (default: 100)
    - NAQ_EVENT_LOGGER_FLUSH_INTERVAL: Flush interval in seconds (default: 5.0)
"""

# Import key models from the parent models module
from ..models import JobEvent, JobEventType, WorkerEvent, WorkerEventType, JobResult, Schedule

# Import storage backends
from .storage import BaseEventStorage, NATSJobEventStorage

# Import loggers
from .logger import AsyncJobEventLogger, JobEventLogger

# Import processors
from .processor import AsyncJobEventProcessor

# Define public API
__all__ = [
    # Data models
    "JobEvent",
    "JobEventType",
    "WorkerEvent", 
    "WorkerEventType",
    "JobResult",
    "Schedule",
    
    # Storage backends
    "BaseEventStorage",
    "NATSJobEventStorage",
    
    # Event loggers
    "AsyncJobEventLogger",
    "JobEventLogger",
    
    # Event processors
    "AsyncJobEventProcessor",
]

# Version info
__version__ = "1.0.0"