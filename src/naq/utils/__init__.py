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
    JobID,
    WorkerID,
    QueueName,
    StreamName,
    SubjectName,
    Timestamp,
    Duration,
    TTL,
    RetryDelayType,
    JobDependency,
    FunctionArgs,
    FunctionKwargs,
    ExceptionTypes,
    ConfigDict,
    NatsServers,
    NatsAuth,
    NatsTLS,
    QueueNames,
    SubjectNames,
    JobStatusData,
    EventData,
    WorkerStatusData,
    MetricsDict,
    StatsDict,
    SerializedData,
    DeserializedData,
    JobCallback,
    EventCallback,
    ErrorCallback,
    AsyncJobCallback,
    AsyncEventCallback,
    AsyncErrorCallback,
    
    # TypedDict classes
    JobInfo,
    WorkerInfo,
    QueueInfo,
    ConnectionInfo,
    JobFilter,
    WorkerFilter,
    QueueFilter,
    JobMetrics,
    WorkerMetrics,
    QueueMetrics,
    SystemMetrics,
    
    # msgspec.Struct classes
    ConnectionMetrics,
    JobTiming,
    WorkerTiming,
    QueueTiming,
    
    # Collection type aliases
    JobInfoList,
    WorkerInfoList,
    QueueInfoList,
    ConnectionInfoList,
    JobMetricsList,
    WorkerMetricsList,
    QueueMetricsList,
    SystemMetricsList,
    
    # Timing dict type aliases
    JobTimingDict,
    WorkerTimingDict,
    QueueTimingDict,
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
    "JobID",
    "WorkerID",
    "QueueName",
    "StreamName",
    "SubjectName",
    "Timestamp",
    "Duration",
    "TTL",
    "RetryDelayType",
    "JobDependency",
    "FunctionArgs",
    "FunctionKwargs",
    "ExceptionTypes",
    "ConfigDict",
    "NatsServers",
    "NatsAuth",
    "NatsTLS",
    "QueueNames",
    "SubjectNames",
    "JobStatusData",
    "EventData",
    "WorkerStatusData",
    "MetricsDict",
    "StatsDict",
    "SerializedData",
    "DeserializedData",
    "JobCallback",
    "EventCallback",
    "ErrorCallback",
    "AsyncJobCallback",
    "AsyncEventCallback",
    "AsyncErrorCallback",
    
    # TypedDict classes
    "JobInfo",
    "WorkerInfo",
    "QueueInfo",
    "ConnectionInfo",
    "JobFilter",
    "WorkerFilter",
    "QueueFilter",
    "JobMetrics",
    "WorkerMetrics",
    "QueueMetrics",
    "SystemMetrics",
    
    # msgspec.Struct classes
    "ConnectionMetrics",
    "JobTiming",
    "WorkerTiming",
    "QueueTiming",
    
    # Collection type aliases
    "JobInfoList",
    "WorkerInfoList",
    "QueueInfoList",
    "ConnectionInfoList",
    "JobMetricsList",
    "WorkerMetricsList",
    "QueueMetricsList",
    "SystemMetricsList",
    
    # Timing dict type aliases
    "JobTimingDict",
    "WorkerTimingDict",
    "QueueTimingDict",
]