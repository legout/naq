"""
Common type definitions for the NAQ job queue system.

This module contains type aliases, TypedDict definitions, and small data classes
that are frequently used across different modules and do not belong to a specific domain.
"""

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, TypedDict, TYPE_CHECKING
import time

if TYPE_CHECKING:
    from ..models.jobs import Job
import msgspec

# Type aliases for common data structures
JobID = str
WorkerID = str
QueueName = str
StreamName = str
SubjectName = str
Timestamp = float
Duration = float
TTL = int

# Type alias for retry delays (used in Job model)
RetryDelayType = Union[int, float, Sequence[Union[int, float]]]

# Type alias for job dependencies
JobDependency = Union[str, "Job", List[Union[str, "Job"]]]

# Type alias for function arguments
FunctionArgs = Tuple[Any, ...]
FunctionKwargs = Dict[str, Any]

# Type alias for exception types
ExceptionTypes = Optional[Tuple[type, ...]]

# Type alias for configuration dictionaries
ConfigDict = Dict[str, Any]

# Type alias for NATS connection parameters
NatsServers = List[str]
NatsAuth = Optional[Dict[str, Any]]
NatsTLS = Optional[Dict[str, Any]]

# Type alias for queue and stream names
QueueNames = List[str]
SubjectNames = List[str]

# Type alias for job status and event data
JobStatusData = Dict[str, Any]
EventData = Dict[str, Any]
WorkerStatusData = Dict[str, Any]

# Type alias for metrics and statistics
MetricsDict = Dict[str, Union[int, float, str]]
StatsDict = Dict[str, Union[int, float, str]]

# Type alias for serialized data
SerializedData = bytes
DeserializedData = Any

# Type alias for callback functions
JobCallback = Callable[[Any], Any]
EventCallback = Callable[[Any], None]
ErrorCallback = Callable[[Exception], Any]

# Type alias for async callback functions
AsyncJobCallback = Callable[[Any], Any]
AsyncEventCallback = Callable[[Any], None]
AsyncErrorCallback = Callable[[Exception], Any]


class JobInfo(TypedDict):
    """
    TypedDict for basic job information.
    
    This type is used for API responses and simplified job representations
    that don't require the full Job model.
    """
    job_id: JobID
    queue_name: QueueName
    status: str
    created_at: Timestamp
    started_at: Optional[Timestamp]
    finished_at: Optional[Timestamp]
    error: Optional[str]
    retry_count: int


class WorkerInfo(TypedDict):
    """
    TypedDict for basic worker information.
    
    This type is used for API responses and simplified worker representations
    that don't require the full Worker model.
    """
    worker_id: WorkerID
    status: str
    queue_names: QueueNames
    last_heartbeat: Optional[Timestamp]
    cpu_usage: Optional[float]
    memory_usage: Optional[float]
    jobs_processed: int
    jobs_failed: int


class QueueInfo(TypedDict):
    """
    TypedDict for basic queue information.
    
    This type is used for API responses and simplified queue representations
    that don't require the full Queue model.
    """
    name: QueueName
    stream_name: StreamName
    subject: SubjectName
    pending_jobs: int
    running_jobs: int
    completed_jobs: int
    failed_jobs: int
    total_jobs: int


class ConnectionInfo(TypedDict):
    """
    TypedDict for connection information.
    
    This type is used for API responses and connection status reporting.
    """
    connected: bool
    server_url: str
    client_id: str
    reconnects: int
    last_error: Optional[str]
    uptime_seconds: float


class JobFilter(TypedDict, total=False):
    """
    TypedDict for job filtering options.
    
    This type is used for filtering jobs in queries and API endpoints.
    All fields are optional.
    """
    status: Optional[str]
    queue_name: Optional[QueueName]
    worker_id: Optional[WorkerID]
    created_after: Optional[Timestamp]
    created_before: Optional[Timestamp]
    limit: Optional[int]
    offset: Optional[int]


class WorkerFilter(TypedDict, total=False):
    """
    TypedDict for worker filtering options.
    
    This type is used for filtering workers in queries and API endpoints.
    All fields are optional.
    """
    status: Optional[str]
    queue_name: Optional[QueueName]
    last_heartbeat_after: Optional[Timestamp]
    last_heartbeat_before: Optional[Timestamp]
    limit: Optional[int]
    offset: Optional[int]


class QueueFilter(TypedDict, total=False):
    """
    TypedDict for queue filtering options.
    
    This type is used for filtering queues in queries and API endpoints.
    All fields are optional.
    """
    name_pattern: Optional[str]
    has_jobs: Optional[bool]
    limit: Optional[int]
    offset: Optional[int]


class JobMetrics(TypedDict):
    """
    TypedDict for job metrics.
    
    This type is used for reporting job-related metrics and statistics.
    """
    total_jobs: int
    pending_jobs: int
    running_jobs: int
    completed_jobs: int
    failed_jobs: int
    retried_jobs: int
    cancelled_jobs: int
    avg_execution_time_ms: float
    max_execution_time_ms: float
    min_execution_time_ms: float


class WorkerMetrics(TypedDict):
    """
    TypedDict for worker metrics.
    
    This type is used for reporting worker-related metrics and statistics.
    """
    total_workers: int
    active_workers: int
    idle_workers: int
    busy_workers: int
    avg_cpu_usage: float
    max_cpu_usage: float
    avg_memory_usage: float
    max_memory_usage: float
    total_jobs_processed: int
    total_jobs_failed: int


class QueueMetrics(TypedDict):
    """
    TypedDict for queue metrics.
    
    This type is used for reporting queue-related metrics and statistics.
    """
    total_queues: int
    total_jobs: int
    avg_jobs_per_queue: float
    max_jobs_per_queue: int
    min_jobs_per_queue: int
    empty_queues: int
    non_empty_queues: int


class SystemMetrics(TypedDict):
    """
    TypedDict for system-wide metrics.
    
    This type is used for reporting system-wide metrics and statistics.
    """
    uptime_seconds: float
    total_jobs: int
    total_workers: int
    total_queues: int
    jobs_per_second: float
    avg_job_duration_ms: float
    system_load: float
    memory_usage_mb: float
    disk_usage_mb: float


class ConnectionMetrics(msgspec.Struct):
    """
    Struct for connection metrics.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    It provides metrics about NATS connections.
    """
    connected: bool
    server_url: str
    client_id: str
    reconnects: int
    last_error: Optional[str] = None
    uptime_seconds: float = 0.0
    bytes_sent: int = 0
    bytes_received: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    ping_rtt_ms: Optional[float] = None


class JobTiming(msgspec.Struct):
    """
    Struct for job timing information.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    It provides timing information for job execution.
    """
    job_id: JobID
    created_at: Timestamp
    started_at: Optional[Timestamp] = None
    finished_at: Optional[Timestamp] = None
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Calculate duration in milliseconds if start and finish times are available."""
        if self.started_at is not None and self.finished_at is not None:
            return (self.finished_at - self.started_at) * 1000
        return None
    
    @property
    def wait_time_ms(self) -> Optional[float]:
        """Calculate wait time in milliseconds if created and start times are available."""
        if self.started_at is not None:
            return (self.started_at - self.created_at) * 1000
        return None


class WorkerTiming(msgspec.Struct):
    """
    Struct for worker timing information.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    It provides timing information for worker operations.
    """
    worker_id: WorkerID
    started_at: Timestamp
    last_heartbeat: Optional[Timestamp] = None
    last_job_started: Optional[Timestamp] = None
    last_job_completed: Optional[Timestamp] = None
    
    @property
    def uptime_ms(self) -> float:
        """Calculate uptime in milliseconds."""
        return (time.time() - self.started_at) * 1000
    
    @property
    def time_since_last_heartbeat_ms(self) -> Optional[float]:
        """Calculate time since last heartbeat in milliseconds."""
        if self.last_heartbeat is not None:
            return (0.0 - self.last_heartbeat) * 1000
        return None


class QueueTiming(msgspec.Struct):
    """
    Struct for queue timing information.
    
    This class uses msgspec.Struct for efficient serialization and deserialization.
    It provides timing information for queue operations.
    """
    queue_name: QueueName
    created_at: Timestamp
    last_job_enqueued: Optional[Timestamp] = None
    last_job_started: Optional[Timestamp] = None
    last_job_completed: Optional[Timestamp] = None
    
    @property
    def uptime_ms(self) -> float:
        """Calculate uptime in milliseconds."""
        return (time.time() - self.created_at) * 1000
    
    @property
    def time_since_last_activity_ms(self) -> Optional[float]:
        """Calculate time since last activity in milliseconds."""
        last_activity = max(
            self.last_job_enqueued or 0.0,
            self.last_job_started or 0.0,
            self.last_job_completed or 0.0
        )
        if last_activity > 0.0:
            return (0.0 - last_activity) * 1000
        return None


# Type aliases for the timing structs
JobTimingDict = Dict[str, Union[JobID, Timestamp, Optional[float]]]
WorkerTimingDict = Dict[str, Union[WorkerID, Timestamp, Optional[float]]]
QueueTimingDict = Dict[str, Union[QueueName, Timestamp, Optional[float]]]

# Type aliases for collections of the above types
JobInfoList = List[JobInfo]
WorkerInfoList = List[WorkerInfo]
QueueInfoList = List[QueueInfo]
ConnectionInfoList = List[ConnectionInfo]
JobMetricsList = List[JobMetrics]
WorkerMetricsList = List[WorkerMetrics]
QueueMetricsList = List[QueueMetrics]
SystemMetricsList = List[SystemMetrics]