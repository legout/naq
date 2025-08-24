"""Utils package for NAQ.

This package contains various utility modules for common functionality
used throughout the NAQ codebase.
"""

# Import from all sub-modules
from .async_helpers import run_async_from_sync
from .context_managers import ResourceManagementError, managed_resource
from .decorators import retry, log_errors, timing, RetryError
from .error_handling import ErrorHandler
from .logging import setup_logging
from .nats_helpers import build_subject, parse_subject, stream_exists
from .serialization import SerializationHelper, serialize_with_metadata, deserialize_with_metadata
from .timing import Stopwatch, measure_execution_time, measure_execution_time_cm
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
from .validation import validate_parameter, ensure_type, ValidationError, TypeConversionError

__all__ = [
    # From async_helpers
    "run_async_from_sync",
    
    # From context_managers
    "ResourceManagementError",
    "managed_resource",
    
    # From decorators
    "retry",
    "log_errors",
    "timing",
    "RetryError",
    
    # From error_handling
    "ErrorHandler",
    
    # From logging
    "setup_logging",
    
    # From nats_helpers
    "build_subject",
    "parse_subject",
    "stream_exists",
    
    # From serialization
    "SerializationHelper",
    "serialize_with_metadata",
    "deserialize_with_metadata",
    
    # From timing
    "Stopwatch",
    "measure_execution_time",
    "measure_execution_time_cm",
    
    # From validation
    "validate_parameter",
    "ensure_type",
    "ValidationError",
    "TypeConversionError",
    
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