# src/naq/utils/types.py
"""
Common type definitions for NAQ.

This module provides type aliases, protocols, and common type definitions
used throughout the NAQ codebase.
"""

import sys
from typing import (
    Any, Dict, List, Optional, Union, Callable, Awaitable, TypeVar, Generic,
    Protocol, runtime_checkable
)
from abc import ABC, abstractmethod

# Python version compatibility
if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    try:
        from typing_extensions import ParamSpec
    except ImportError:
        # Fallback for older Python versions
        ParamSpec = TypeVar

# Basic type aliases
JobID = str
QueueName = str
WorkerID = str
Subject = str
StreamName = str
ConfigDict = Dict[str, Any]
Headers = Dict[str, str]
Metadata = Dict[str, Any]
JobData = Dict[str, Any]  # For backward compatibility

# Generic type variables
T = TypeVar('T')
P = ParamSpec('P') if 'ParamSpec' in locals() else TypeVar('P')
ReturnType = TypeVar('ReturnType')

# Function type aliases
SyncCallable = Callable[..., T]
AsyncCallable = Callable[..., Awaitable[T]]
CallbackFunction = Callable[[Any], None]
AsyncCallbackFunction = Callable[[Any], Awaitable[None]]

# Handler type aliases
JobHandler = Callable[[Any], Any]
AsyncJobHandler = Callable[[Any], Awaitable[Any]]
ErrorHandler = Callable[[Exception, str], Any]
AsyncErrorHandler = Callable[[Exception, str], Awaitable[Any]]

# Event type aliases
EventData = Dict[str, Any]
EventHandler = Callable[[EventData], None]
AsyncEventHandler = Callable[[EventData], Awaitable[None]]

# Configuration type aliases
NATSConfig = Dict[str, Any]
WorkerConfig = Dict[str, Any]
QueueConfig = Dict[str, Any]
SchedulerConfig = Dict[str, Any]

# Serialization type aliases
SerializedData = bytes
SerializerName = str
SerializationMetadata = Dict[str, Any]

# Time and duration type aliases
Timestamp = float
DurationSeconds = Union[int, float]
DurationMilliseconds = Union[int, float]

# Result type aliases
JobResult = Any
JobStatus = str
JobPriority = Union[int, str]

# Retry type aliases
RetryCount = int
RetryDelay = Union[int, float, List[Union[int, float]]]
RetryStrategy = str


# Protocols for structural typing
@runtime_checkable
class Serializable(Protocol):
    """Protocol for objects that can be serialized."""
    
    def serialize(self) -> SerializedData:
        """Serialize object to bytes."""
        ...
    
    @classmethod
    def deserialize(cls, data: SerializedData) -> 'Serializable':
        """Deserialize bytes to object."""
        ...


@runtime_checkable
class Configurable(Protocol):
    """Protocol for objects that can be configured."""
    
    def configure(self, config: ConfigDict) -> None:
        """Configure object with configuration dictionary."""
        ...
    
    def get_config(self) -> ConfigDict:
        """Get current configuration."""
        ...


@runtime_checkable
class Startable(Protocol):
    """Protocol for objects that can be started and stopped."""
    
    async def start(self) -> None:
        """Start the object."""
        ...
    
    async def stop(self) -> None:
        """Stop the object."""
        ...
    
    @property
    def is_running(self) -> bool:
        """Check if object is running."""
        ...


@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for objects that support health checks."""
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        ...


@runtime_checkable
class Monitorable(Protocol):
    """Protocol for objects that provide monitoring metrics."""
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        ...
    
    def reset_metrics(self) -> None:
        """Reset metrics counters."""
        ...


# Abstract base classes
class JobProcessor(ABC):
    """Abstract base class for job processors."""
    
    @abstractmethod
    async def process_job(self, job: Any) -> JobResult:
        """Process a job and return result."""
        pass
    
    @abstractmethod
    async def handle_job_error(self, job: Any, error: Exception) -> None:
        """Handle job processing error."""
        pass


class EventProcessor(ABC):
    """Abstract base class for event processors."""
    
    @abstractmethod
    async def process_event(self, event: EventData) -> None:
        """Process an event."""
        pass


class ConnectionManager(ABC):
    """Abstract base class for connection managers."""
    
    @abstractmethod
    async def get_connection(self) -> Any:
        """Get a connection."""
        pass
    
    @abstractmethod
    async def close_connection(self, connection: Any) -> None:
        """Close a connection."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check connection health."""
        pass


# Generic container types
class Result(Generic[T]):
    """Generic result container that can hold success or error."""
    
    def __init__(self, value: Optional[T] = None, error: Optional[Exception] = None):
        if value is not None and error is not None:
            raise ValueError("Result cannot have both value and error")
        if value is None and error is None:
            raise ValueError("Result must have either value or error")
        
        self._value = value
        self._error = error
    
    @property
    def is_success(self) -> bool:
        """Check if result is successful."""
        return self._error is None
    
    @property
    def is_error(self) -> bool:
        """Check if result is an error."""
        return self._error is not None
    
    @property
    def value(self) -> T:
        """Get the value (raises if error)."""
        if self._error is not None:
            raise self._error
        return self._value
    
    @property
    def error(self) -> Optional[Exception]:
        """Get the error (None if success)."""
        return self._error
    
    def unwrap_or(self, default: T) -> T:
        """Get value or default if error."""
        return self._value if self._error is None else default
    
    def map(self, func: Callable[[T], ReturnType]) -> 'Result[ReturnType]':
        """Apply function to value if success."""
        if self._error is not None:
            return Result(error=self._error)
        try:
            return Result(value=func(self._value))
        except Exception as e:
            return Result(error=e)
    
    @classmethod
    def success(cls, value: T) -> 'Result[T]':
        """Create successful result."""
        return cls(value=value)
    
    @classmethod
    def failure(cls, error: Exception) -> 'Result[T]':
        """Create error result."""
        return cls(error=error)


class Optional(Generic[T]):
    """Optional container that can hold a value or be empty."""
    
    def __init__(self, value: Optional[T] = None):
        self._value = value
    
    @property
    def has_value(self) -> bool:
        """Check if optional has a value."""
        return self._value is not None
    
    @property
    def is_empty(self) -> bool:
        """Check if optional is empty."""
        return self._value is None
    
    @property
    def value(self) -> T:
        """Get the value (raises if empty)."""
        if self._value is None:
            raise ValueError("Optional is empty")
        return self._value
    
    def unwrap_or(self, default: T) -> T:
        """Get value or default if empty."""
        return self._value if self._value is not None else default
    
    def map(self, func: Callable[[T], ReturnType]) -> 'Optional[ReturnType]':
        """Apply function to value if present."""
        if self._value is None:
            return Optional()
        return Optional(func(self._value))
    
    def filter(self, predicate: Callable[[T], bool]) -> 'Optional[T]':
        """Filter value with predicate."""
        if self._value is None or not predicate(self._value):
            return Optional()
        return self
    
    @classmethod
    def of(cls, value: T) -> 'Optional[T]':
        """Create optional with value."""
        return cls(value)
    
    @classmethod
    def empty(cls) -> 'Optional[T]':
        """Create empty optional."""
        return cls()


# Enum-like constants
class JobStatus:
    """Job status constants."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class WorkerStatus:
    """Worker status constants."""
    IDLE = "idle"
    BUSY = "busy"
    STARTING = "starting"
    STOPPING = "stopping"
    ERROR = "error"


class EventType:
    """Event type constants."""
    JOB_CREATED = "job.created"
    JOB_STARTED = "job.started"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_RETRIED = "job.retried"
    WORKER_STARTED = "worker.started"
    WORKER_STOPPED = "worker.stopped"
    SCHEDULER_STARTED = "scheduler.started"
    SCHEDULER_STOPPED = "scheduler.stopped"


class LogLevel:
    """Log level constants."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Type guards
def is_async_callable(obj: Any) -> bool:
    """Check if object is an async callable."""
    import asyncio
    return asyncio.iscoroutinefunction(obj)


def is_callable(obj: Any) -> bool:
    """Check if object is callable."""
    return callable(obj)


def is_serializable(obj: Any) -> bool:
    """Check if object implements Serializable protocol."""
    return isinstance(obj, Serializable)


def is_configurable(obj: Any) -> bool:
    """Check if object implements Configurable protocol."""
    return isinstance(obj, Configurable)


# Utility type functions
def ensure_list(value: Union[T, List[T]]) -> List[T]:
    """Ensure value is a list."""
    if isinstance(value, list):
        return value
    return [value]


def ensure_dict(value: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
    """Ensure value is a dictionary."""
    if isinstance(value, dict):
        return value
    return {}


def coerce_to_int(value: Any, default: int = 0) -> int:
    """Coerce value to integer."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def coerce_to_float(value: Any, default: float = 0.0) -> float:
    """Coerce value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def coerce_to_str(value: Any, default: str = "") -> str:
    """Coerce value to string."""
    if value is None:
        return default
    return str(value)