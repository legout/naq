"""Type definitions for NAQ utils.

This module contains common type definitions used throughout the NAQ codebase
to avoid redundancy and ensure consistency.
"""

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import msgspec
from typing_extensions import TypedDict

# Basic type aliases for commonly used types
JSONValue = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
JSONDict = Dict[str, JSONValue]
JSONList = List[JSONValue]

# Function-related types
SyncCallable = Callable[..., Any]
AsyncCallable = Callable[..., Any]
AnyCallable = Union[SyncCallable, AsyncCallable]

# Retry-related types
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]

# Time-related types
Timestamp = float
DurationSeconds = float
DurationMilliseconds = float

# ID-related types
JobID = str
WorkerID = str
QueueName = str
StreamName = str
SubjectName = str

# Status-related types
StatusValue = str
ErrorMessage = str
TracebackStr = str

# Configuration-related types
ServerURL = str
ServerURLs = List[ServerURL]
ConfigPath = List[str]

# NATS-related types
NATSSequence = int
NATSSubject = str

# Serialization-related types
SerializedData = bytes
SerializerType = str  # "pickle" or "json"


# Dictionary structures for common data patterns
class JobMetadata(TypedDict):
    """TypedDict for job metadata."""

    job_id: JobID
    queue_name: QueueName
    enqueue_time: Timestamp
    status: StatusValue
    worker_id: Optional[WorkerID]
    retry_count: int
    max_retries: int
    timeout: Optional[int]
    depends_on: Optional[List[JobID]]


class WorkerMetadata(TypedDict):
    """TypedDict for worker metadata."""

    worker_id: WorkerID
    queue_names: List[QueueName]
    status: StatusValue
    last_heartbeat: Timestamp
    cpu_usage: Optional[float]
    memory_usage: Optional[float]


class ConnectionMetrics(TypedDict):
    """TypedDict for connection metrics."""

    connection_count: int
    total_connections: int
    active_connections: int
    failed_connections: int
    reconnect_count: int
    last_error: Optional[ErrorMessage]


class EventMetadata(TypedDict):
    """TypedDict for event metadata."""

    event_type: str
    timestamp: Timestamp
    source: Optional[str]
    version: Optional[str]
    serializer: Optional[SerializerType]


# Small data classes for common data structures
class PointInTime(msgspec.Struct):
    """Represents a point in time with timestamp and optional metadata.

    This class is useful for tracking events, measurements, or any other
    time-based data points throughout the system.

    Attributes:
        timestamp: Unix timestamp when the event occurred
        metadata: Optional dictionary of additional information
    """

    timestamp: float = msgspec.field(default_factory=lambda: __import__("time").time())
    metadata: Optional[Dict[str, Any]] = None


class ResourceUsage(msgspec.Struct):
    """Represents resource usage metrics.

    This class provides a standardized way to track CPU, memory, and other
    resource usage metrics throughout the system.

    Attributes:
        cpu_percent: CPU usage percentage (0-100)
        memory_percent: Memory usage percentage (0-100)
        memory_bytes: Memory usage in bytes
        custom_metrics: Optional dictionary for custom metrics
    """

    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    memory_bytes: Optional[int] = None
    custom_metrics: Optional[Dict[str, Union[int, float, str]]] = None


class RetryConfig(msgspec.Struct):
    """Configuration for retry behavior.

    This class encapsulates all retry-related configuration parameters
    in a single, reusable structure.

    Attributes:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for exponential backoff
        jitter: Whether to add random jitter to retry delays
        retry_on_exception_names: Tuple of exception class names to retry on
    """

    max_attempts: int = 3
    delay: float = 1.0
    backoff_factor: float = 2.0
    jitter: bool = True
    retry_on_exception_names: Tuple[str, ...] = ("Exception",)


class QueueStats(msgspec.Struct):
    """Statistics for a job queue.

    This class provides a standardized way to track queue statistics
    such as pending, running, completed, and failed job counts.

    Attributes:
        queue_name: Name of the queue
        pending_jobs: Number of jobs waiting to be processed
        running_jobs: Number of jobs currently being processed
        completed_jobs: Number of successfully completed jobs
        failed_jobs: Number of failed jobs
        total_jobs: Total number of jobs in the queue
        last_updated: Timestamp when these stats were last updated
    """

    queue_name: str
    pending_jobs: int = 0
    running_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_jobs: int = 0
    last_updated: float = msgspec.field(
        default_factory=lambda: __import__("time").time()
    )


# Type aliases for common callback patterns
JobCallback = Callable[[JobID, StatusValue, Optional[Any]], None]
WorkerCallback = Callable[[WorkerID, StatusValue, Optional[Dict[str, Any]]], None]
EventCallback = Callable[[str, EventMetadata, Optional[Dict[str, Any]]], None]
ErrorCallback = Callable[[Exception, Optional[Dict[str, Any]]], None]

# Type aliases for common data structures
JobDict = Dict[str, Any]
WorkerDict = Dict[str, Any]
EventDict = Dict[str, Any]
ConfigDict = Dict[str, Any]

# Type aliases for sequences
JobIDs = List[JobID]
WorkerIDs = List[WorkerID]
QueueNames = List[QueueName]
Subjects = List[SubjectName]

# Type aliases for optional values
OptionalJobID = Optional[JobID]
OptionalWorkerID = Optional[WorkerID]
OptionalQueueName = Optional[QueueName]
OptionalTimestamp = Optional[Timestamp]

# Type aliases for mappings
JobToResult = Dict[JobID, Any]
JobToError = Dict[JobID, ErrorMessage]
WorkerToStatus = Dict[WorkerID, StatusValue]
QueueToStats = Dict[QueueName, QueueStats]

# Type aliases for tuples
JobStatusTuple = Tuple[JobID, StatusValue, Optional[Timestamp]]
WorkerStatusTuple = Tuple[WorkerID, StatusValue, Optional[Timestamp]]
EventTuple = Tuple[str, str, Timestamp]  # event_type, source, timestamp

# Type aliases for function parameters
JobFunction = SyncCallable
JobArgs = Tuple[Any, ...]
JobKwargs = Dict[str, Any]

# Type aliases for return types
JobResultType = Any
WorkerResultType = Dict[str, Any]
EventResultType = Dict[str, Any]

# Type aliases for iterators
JobIterator = Any  # Could be more specific based on actual usage
WorkerIterator = Any
EventIterator = Any

# Type aliases for async types
AsyncJobIterator = Any
AsyncWorkerIterator = Any
AsyncEventIterator = Any

# Type aliases for context managers
ConnectionContext = Any
JobContext = Any
WorkerContext = Any

# Type aliases for decorators
JobDecorator = Callable[[JobFunction], JobFunction]
WorkerDecorator = Callable[[SyncCallable], SyncCallable]
EventDecorator = Callable[[Callable], Callable]

# Export all types for easy import
__all__ = [
    # Basic type aliases
    "JSONValue",
    "JSONDict",
    "JSONList",
    # Function-related types
    "SyncCallable",
    "AsyncCallable",
    "AnyCallable",
    # Retry-related types
    "RetryDelayType",
    # Time-related types
    "Timestamp",
    "DurationSeconds",
    "DurationMilliseconds",
    # ID-related types
    "JobID",
    "WorkerID",
    "QueueName",
    "StreamName",
    "SubjectName",
    # Status-related types
    "StatusValue",
    "ErrorMessage",
    "TracebackStr",
    # Configuration-related types
    "ServerURL",
    "ServerURLs",
    "ConfigPath",
    # NATS-related types
    "NATSSequence",
    "NATSSubject",
    # Serialization-related types
    "SerializedData",
    "SerializerType",
    # TypedDict structures
    "JobMetadata",
    "WorkerMetadata",
    "ConnectionMetrics",
    "EventMetadata",
    # Data classes
    "PointInTime",
    "ResourceUsage",
    "RetryConfig",
    "QueueStats",
    # Callback patterns
    "JobCallback",
    "WorkerCallback",
    "EventCallback",
    "ErrorCallback",
    # Data structures
    "JobDict",
    "WorkerDict",
    "EventDict",
    "ConfigDict",
    # Sequences
    "JobIDs",
    "WorkerIDs",
    "QueueNames",
    "Subjects",
    # Optional values
    "OptionalJobID",
    "OptionalWorkerID",
    "OptionalQueueName",
    "OptionalTimestamp",
    # Mappings
    "JobToResult",
    "JobToError",
    "WorkerToStatus",
    "QueueToStats",
    # Tuples
    "JobStatusTuple",
    "WorkerStatusTuple",
    "EventTuple",
    # Function parameters
    "JobFunction",
    "JobArgs",
    "JobKwargs",
    # Return types
    "JobResultType",
    "WorkerResultType",
    "EventResultType",
    # Iterators
    "JobIterator",
    "WorkerIterator",
    "EventIterator",
    "AsyncJobIterator",
    "AsyncWorkerIterator",
    "AsyncEventIterator",
    # Context managers
    "ConnectionContext",
    "JobContext",
    "WorkerContext",
    # Decorators
    "JobDecorator",
    "WorkerDecorator",
    "EventDecorator",
]
