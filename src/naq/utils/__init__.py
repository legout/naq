"""Utils package for NAQ.

This package contains various utility modules for common functionality
used throughout the NAQ codebase.
"""

# Make utils package importable

from .async_helpers import run_async_from_sync
from .context_managers import ResourceManagementError, managed_resource
from .error_handling import ErrorHandler
from .logging import setup_logging
from .nats_helpers import build_subject, parse_subject, stream_exists
from .types import (
    # Basic type aliases
    JSONValue,
    JSONDict,
    JSONList,
    
    # Function-related types
    SyncCallable,
    AsyncCallable,
    AnyCallable,
    
    # Time-related types
    Timestamp,
    DurationSeconds,
    DurationMilliseconds,
    
    # ID-related types
    JobID,
    WorkerID,
    QueueName,
    StreamName,
    SubjectName,
    
    # Status-related types
    StatusValue,
    ErrorMessage,
    TracebackStr,
    
    # Data classes
    PointInTime,
    ResourceUsage,
    RetryConfig,
    QueueStats,
    
    # TypedDict structures
    JobMetadata,
    WorkerMetadata,
    ConnectionMetrics,
    EventMetadata,
)

__all__ = [
    "ResourceManagementError",
    "managed_resource",
    "ErrorHandler",
    "run_async_from_sync",
    "setup_logging",
    "build_subject",
    "parse_subject",
    "stream_exists",
    
    # Basic type aliases
    "JSONValue",
    "JSONDict",
    "JSONList",
    
    # Function-related types
    "SyncCallable",
    "AsyncCallable",
    "AnyCallable",
    
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
    
    # Data classes
    "PointInTime",
    "ResourceUsage",
    "RetryConfig",
    "QueueStats",
    
    # TypedDict structures
    "JobMetadata",
    "WorkerMetadata",
    "ConnectionMetrics",
    "EventMetadata",
]