"""
NAQ Common Type Definitions

This module provides comprehensive type definitions used throughout the NAQ job queue system.
It consolidates type definitions from various modules, provides protocol definitions for common
patterns, and includes utility types and validation helpers.

The module is organized into logical sections:
- Basic Type Aliases: Common type aliases used throughout the codebase
- Protocol Definitions: Protocol interfaces for common patterns
- Generic Types: Generic types for common patterns
- Type Validation Helpers: Utilities for type validation and checking
- Model Types: Type definitions related to core models
- Configuration Types: Type definitions related to configuration
- Event Types: Type definitions related to events
- Job Types: Type definitions related to jobs
- Worker Types: Type definitions related to workers
- Utility Types: Additional utility types and helpers
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any, AsyncContextManager, AsyncGenerator, AsyncIterator, Awaitable, Callable, ContextManager,
    Dict, Generic, Iterable, Iterator, List, Literal, Optional, Protocol, Sequence, Set, Tuple,
    Type, TypeVar, Union, runtime_checkable
)

# Import existing types from models for consolidation
from ..models.enums import (
    JOB_STATUS,
    JobEventType,
    WorkerEventType,
    VALID_RETRY_STRATEGIES,
    RetryDelayType
)
from ..settings import RETRY_STRATEGY
from ..models.events import JobEvent, WorkerEvent
from ..models.jobs import Job, JobResult
from ..models.schedules import Schedule

# =============================================================================
# Basic Type Aliases
# =============================================================================

# Primitive type aliases
JSONValue = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
JSONObject = Dict[str, JSONValue]
JSONArray = List[JSONValue]
JSONType = Union[JSONObject, JSONArray]

# Numeric type aliases
Seconds = float
Milliseconds = float
Microseconds = float
Timestamp = float
Duration = float

# String type aliases
JobID = str
WorkerID = str
QueueName = str
Subject = str
StreamName = str
BucketName = str
TraceID = str
SpanID = str

# Collection type aliases
JobList = List[JobID]
WorkerList = List[WorkerID]
QueueList = List[QueueName]
SubjectList = List[Subject]
EventList = List[Union[JobEvent, WorkerEvent]]

# Dictionary type aliases
JobDict = Dict[JobID, Job]
WorkerDict = Dict[WorkerID, Any]
QueueDict = Dict[QueueName, Any]
ConfigDict = Dict[str, Any]
MetadataDict = Dict[str, Any]
ContextDict = Dict[str, Any]

# Function type aliases
SyncFunction = Callable[..., Any]
AsyncFunction = Callable[..., Awaitable[Any]]
JobFunction = Union[SyncFunction, AsyncFunction]
ErrorHandler = Callable[[Exception], Any]
AsyncErrorHandler = Callable[[Exception], Awaitable[Any]]
ValidatorFunction = Callable[[Any], bool]
AsyncValidatorFunction = Callable[[Any], Awaitable[bool]]

# =============================================================================
# Protocol Definitions
# =============================================================================

@runtime_checkable
class Serializable(Protocol):
    """
    Protocol for objects that can be serialized to bytes.
    
    This protocol defines the interface for objects that can be serialized
    to and deserialized from bytes, which is essential for job storage
    and transmission.
    
    Example:
        ```python
        class MyJob:
            def serialize(self) -> bytes:
                return json.dumps(self.data).encode()
            
            @classmethod
            def deserialize(cls, data: bytes) -> 'MyJob':
                return cls(json.loads(data.decode()))
        
        def process_job(job: Serializable) -> None:
            data = job.serialize()
            # ... process data
            restored_job = type(job).deserialize(data)
        ```
    """
    
    def serialize(self) -> bytes:
        """Serialize the object to bytes."""
        ...
    
    @classmethod
    def deserialize(cls, data: bytes) -> Serializable:
        """Deserialize bytes to an object instance."""
        ...


@runtime_checkable
class Configurable(Protocol):
    """
    Protocol for objects that can be configured.
    
    This protocol defines the interface for objects that can accept
    configuration updates and validate their configuration state.
    
    Example:
        ```python
        class ConfigurableWorker:
            def __init__(self):
                self.config = {}
            
            def update_config(self, config: Dict[str, Any]) -> None:
                self.config.update(config)
            
            def validate_config(self) -> List[str]:
                errors = []
                if 'max_concurrent' not in self.config:
                    errors.append("max_concurrent is required")
                return errors
        ```
    """
    
    def update_config(self, config: Dict[str, Any]) -> None:
        """Update the object's configuration."""
        ...
    
    def validate_config(self) -> List[str]:
        """Validate the current configuration and return any errors."""
        ...


@runtime_checkable
class Executable(Protocol):
    """
    Protocol for executable objects.
    
    This protocol defines the interface for objects that can be executed,
    either synchronously or asynchronously.
    
    Example:
        ```python
        class MyTask:
            def execute(self, *args, **kwargs) -> Any:
                return f"Task executed with {args}, {kwargs}"
            
            async def execute_async(self, *args, **kwargs) -> Any:
                await asyncio.sleep(0.1)
                return f"Async task executed with {args}, {kwargs}"
        ```
    """
    
    def execute(self, *args, **kwargs) -> Any:
        """Execute the task synchronously."""
        ...
    
    async def execute_async(self, *args, **kwargs) -> Any:
        """Execute the task asynchronously."""
        ...


@runtime_checkable
class Retryable(Protocol):
    """
    Protocol for objects that support retry logic.
    
    This protocol defines the interface for objects that can determine
    if they should be retried after a failure and calculate retry delays.
    
    Example:
        ```python
        class RetryableOperation:
            def __init__(self, max_retries: int = 3):
                self.max_retries = max_retries
                self.retry_count = 0
            
            def should_retry(self, exception: Exception) -> bool:
                return self.retry_count < self.max_retries
            
            def get_retry_delay(self) -> float:
                return 2.0 ** self.retry_count
            
            def increment_retry_count(self) -> None:
                self.retry_count += 1
        ```
    """
    
    def should_retry(self, exception: Exception) -> bool:
        """Determine if the operation should be retried."""
        ...
    
    def get_retry_delay(self) -> float:
        """Get the delay before the next retry attempt."""
        ...
    
    def increment_retry_count(self) -> None:
        """Increment the retry count."""
        ...


@runtime_checkable
class Monitorable(Protocol):
    """
    Protocol for objects that can be monitored.
    
    This protocol defines the interface for objects that can provide
    metrics and status information for monitoring purposes.
    
    Example:
        ```python
        class MonitorableService:
            def get_metrics(self) -> Dict[str, Any]:
                return {
                    'uptime': time.time() - self.start_time,
                    'requests_processed': self.request_count,
                    'error_rate': self.error_count / self.request_count
                }
            
            def get_status(self) -> str:
                return 'healthy' if self.error_count < 10 else 'degraded'
        ```
    """
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for the object."""
        ...
    
    def get_status(self) -> str:
        """Get the current status of the object."""
        ...


@runtime_checkable
class EventProducer(Protocol):
    """
    Protocol for objects that produce events.
    
    This protocol defines the interface for objects that can emit events
    and register event handlers.
    
    Example:
        ```python
        class EventSource:
            def __init__(self):
                self.handlers = []
            
            def emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
                event = {'type': event_type, 'data': data, 'timestamp': time.time()}
                for handler in self.handlers:
                    handler(event)
            
            def register_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
                self.handlers.append(handler)
        ```
    """
    
    def emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Emit an event of the specified type with the given data."""
        ...
    
    def register_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register an event handler."""
        ...


# =============================================================================
# Generic Types
# =============================================================================

# Type variables for generic types
T = TypeVar('T')
R = TypeVar('R')  # Return type
E = TypeVar('E', bound=Exception)  # Exception type
K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type

# Generic container types
class Result(Generic[T, E]):
    """
    Generic result type that can contain either a success value or an error.
    
    This type provides a structured way to handle operations that can either
    succeed with a value or fail with an error, avoiding the need for exceptions
    in many cases.
    
    Example:
        ```python
        def divide(a: float, b: float) -> Result[float, ValueError]:
            if b == 0:
                return Result.error(ValueError("Cannot divide by zero"))
            return Result.ok(a / b)
        
        result = divide(10, 2)
        if result.is_ok():
            print(f"Result: {result.value}")
        else:
            print(f"Error: {result.error}")
        ```
    """
    
    def __init__(self, value: Optional[T] = None, error: Optional[E] = None):
        self._value = value
        self._error = error
        self._is_ok = error is None
    
    @classmethod
    def ok(cls, value: T) -> Result[T, E]:
        """Create a successful result."""
        return cls(value=value)
    
    @classmethod
    def error(cls, error: E) -> Result[T, E]:
        """Create a failed result."""
        return cls(error=error)
    
    def is_ok(self) -> bool:
        """Check if the result is successful."""
        return self._is_ok
    
    def is_error(self) -> bool:
        """Check if the result is an error."""
        return not self._is_ok
    
    def value(self) -> T:
        """Get the success value."""
        if not self._is_ok:
            raise ValueError("Cannot get value from error result")
        return self._value
    
    def error(self) -> E:
        """Get the error value."""
        if self._is_ok:
            raise ValueError("Cannot get error from successful result")
        return self._error
    
    def map(self, func: Callable[[T], R]) -> Result[R, E]:
        """Apply a function to the success value if present."""
        if self._is_ok:
            try:
                return Result.ok(func(self._value))
            except Exception as e:
                return Result.error(e)
        return Result.error(self._error)
    
    def flat_map(self, func: Callable[[T], Result[R, E]]) -> Result[R, E]:
        """Apply a function that returns a Result to the success value if present."""
        if self._is_ok:
            return func(self._value)
        return Result.error(self._error)


class PaginatedResult(Generic[T]):
    """
    Generic paginated result type.
    
    This type represents a paginated collection of items with metadata
    about pagination state.
    
    Example:
        ```python
        def get_users(page: int, page_size: int) -> PaginatedResult[User]:
            users = db.query(User).offset(page * page_size).limit(page_size).all()
            total = db.query(User).count()
            return PaginatedResult(
                items=users,
                page=page,
                page_size=page_size,
                total=total
            )
        ```
    """
    
    def __init__(
        self,
        items: List[T],
        page: int,
        page_size: int,
        total: int,
        has_next: Optional[bool] = None,
        has_prev: Optional[bool] = None
    ):
        self.items = items
        self.page = page
        self.page_size = page_size
        self.total = total
        self.has_next = has_next if has_next is not None else (page + 1) * page_size < total
        self.has_prev = has_prev if has_prev is not None else page > 0
    
    @property
    def total_pages(self) -> int:
        """Calculate the total number of pages."""
        return (self.total + self.page_size - 1) // self.page_size
    
    @property
    def is_first_page(self) -> bool:
        """Check if this is the first page."""
        return self.page == 0
    
    @property
    def is_last_page(self) -> bool:
        """Check if this is the last page."""
        return not self.has_next


class BatchResult(Generic[T]):
    """
    Generic batch operation result type.
    
    This type represents the result of a batch operation, including
    successful items and failed items with their errors.
    
    Example:
        ```python
        def process_batch(items: List[Item]) -> BatchResult[ProcessedItem]:
            successful = []
            failed = []
            
            for item in items:
                try:
                    processed = process_item(item)
                    successful.append(processed)
                except Exception as e:
                    failed.append(BatchError(item=item, error=e))
            
            return BatchResult(successful=successful, failed=failed)
        ```
    """
    
    def __init__(self, successful: List[T], failed: List[BatchError]):
        self.successful = successful
        self.failed = failed
    
    @property
    def total_count(self) -> int:
        """Get the total number of items processed."""
        return len(self.successful) + len(self.failed)
    
    @property
    def success_rate(self) -> float:
        """Calculate the success rate."""
        if self.total_count == 0:
            return 0.0
        return len(self.successful) / self.total_count
    
    @property
    def is_complete_success(self) -> bool:
        """Check if all items were processed successfully."""
        return len(self.failed) == 0
    
    @property
    def is_complete_failure(self) -> bool:
        """Check if all items failed to process."""
        return len(self.successful) == 0


@dataclass
class BatchError(Generic[T]):
    """Represents an error in a batch operation."""
    item: T
    error: Exception
    error_message: str = field(init=False)
    
    def __post_init__(self):
        self.error_message = str(self.error)


# =============================================================================
# Type Validation Helpers
# =============================================================================

class TypeValidationError(TypeError):
    """Raised when type validation fails."""
    
    def __init__(self, message: str, expected_type: Type, actual_value: Any):
        super().__init__(message)
        self.expected_type = expected_type
        self.actual_value = actual_value
        self.actual_type = type(actual_value)


def validate_type(value: Any, expected_type: Type, field_name: str = "value") -> None:
    """
    Validate that a value matches the expected type.
    
    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: Name of the field being validated (for error messages)
        
    Raises:
        TypeValidationError: If the value doesn't match the expected type
        
    Example:
        ```python
        validate_type(42, int, "age")  # OK
        validate_type("42", int, "age")  # Raises TypeValidationError
        ```
    """
    if not isinstance(value, expected_type):
        raise TypeValidationError(
            f"Expected {field_name} to be of type {expected_type.__name__}, "
            f"got {type(value).__name__}",
            expected_type,
            value
        )


def validate_optional_type(value: Any, expected_type: Type, field_name: str = "value") -> None:
    """
    Validate that a value is either None or matches the expected type.
    
    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: Name of the field being validated (for error messages)
        
    Raises:
        TypeValidationError: If the value is not None and doesn't match the expected type
        
    Example:
        ```python
        validate_optional_type(None, int, "age")  # OK
        validate_optional_type(42, int, "age")  # OK
        validate_optional_type("42", int, "age")  # Raises TypeValidationError
        ```
    """
    if value is not None and not isinstance(value, expected_type):
        raise TypeValidationError(
            f"Expected {field_name} to be None or of type {expected_type.__name__}, "
            f"got {type(value).__name__}",
            expected_type,
            value
        )


def validate_collection_type(
    collection: Iterable[Any],
    item_type: Type,
    collection_name: str = "collection"
) -> None:
    """
    Validate that all items in a collection match the expected type.
    
    Args:
        collection: The collection to validate
        item_type: The expected type for all items
        collection_name: Name of the collection being validated (for error messages)
        
    Raises:
        TypeValidationError: If any item doesn't match the expected type
        
    Example:
        ```python
        validate_collection_type([1, 2, 3], int, "numbers")  # OK
        validate_collection_type([1, "2", 3], int, "numbers")  # Raises TypeValidationError
        ```
    """
    for i, item in enumerate(collection):
        if not isinstance(item, item_type):
            raise TypeValidationError(
                f"Expected all items in {collection_name} to be of type {item_type.__name__}, "
                f"got {type(item).__name__} at index {i}",
                item_type,
                item
            )


def validate_dict_types(
    dictionary: Dict[Any, Any],
    key_type: Type,
    value_type: Type,
    dict_name: str = "dictionary"
) -> None:
    """
    Validate that all keys and values in a dictionary match the expected types.
    
    Args:
        dictionary: The dictionary to validate
        key_type: The expected type for all keys
        value_type: The expected type for all values
        dict_name: Name of the dictionary being validated (for error messages)
        
    Raises:
        TypeValidationError: If any key or value doesn't match the expected type
        
    Example:
        ```python
        validate_dict_types({"a": 1, "b": 2}, str, int, "mapping")  # OK
        validate_dict_types({"a": 1, 2: "b"}, str, int, "mapping")  # Raises TypeValidationError
        ```
    """
    for key, value in dictionary.items():
        if not isinstance(key, key_type):
            raise TypeValidationError(
                f"Expected all keys in {dict_name} to be of type {key_type.__name__}, "
                f"got {type(key).__name__} for key {key}",
                key_type,
                key
            )
        
        if not isinstance(value, value_type):
            raise TypeValidationError(
                f"Expected all values in {dict_name} to be of type {value_type.__name__}, "
                f"got {type(value).__name__} for key {key}",
                value_type,
                value
            )


def is_type_compatible(value: Any, expected_type: Type) -> bool:
    """
    Check if a value is compatible with an expected type.
    
    This function is more flexible than isinstance() as it handles:
    - Union types
    - Optional types
    - Generic types
    
    Args:
        value: The value to check
        expected_type: The expected type
        
    Returns:
        True if the value is compatible with the expected type
        
    Example:
        ```python
        is_type_compatible(42, int)  # True
        is_type_compatible(None, Optional[int])  # True
        is_type_compatible("42", Union[int, str])  # True
        ```
    """
    # Handle None case for Optional types
    if value is None:
        return hasattr(expected_type, '__args__') and type(None) in expected_type.__args__
    
    # Handle Union types
    if hasattr(expected_type, '__origin__') and expected_type.__origin__ is Union:
        return any(is_type_compatible(value, t) for t in expected_type.__args__)
    
    # Handle generic types (List, Dict, etc.)
    if hasattr(expected_type, '__origin__'):
        origin_type = expected_type.__origin__
        if isinstance(value, origin_type):
            return True
    
    # Basic type check
    return isinstance(value, expected_type)


# =============================================================================
# Model Types
# =============================================================================

@dataclass
class JobDependency:
    """
    Represents a job dependency relationship.
    
    This class encapsulates information about dependencies between jobs,
    including the dependency type and any conditions that must be met.
    
    Example:
        ```python
        dependency = JobDependency(
            job_id="job-123",
            dependency_type="completion",
            condition={"success": True}
        )
        ```
    """
    job_id: JobID
    dependency_type: Literal["completion", "success", "failure"] = "completion"
    condition: Optional[Dict[str, Any]] = None
    
    def is_satisfied(self, job_result: JobResult) -> bool:
        """
        Check if the dependency is satisfied by the given job result.
        
        Args:
            job_result: The result of the dependent job
            
        Returns:
            True if the dependency is satisfied
        """
        if self.dependency_type == "completion":
            return True  # Any completion satisfies this dependency
        elif self.dependency_type == "success":
            return job_result.status == JOB_STATUS.COMPLETED
        elif self.dependency_type == "failure":
            return job_result.status == JOB_STATUS.FAILED
        
        return False


@dataclass
class JobMetrics:
    """
    Metrics for job execution and performance.
    
    This class collects various metrics about job execution,
    including timing information, resource usage, and success rates.
    
    Example:
        ```python
        metrics = JobMetrics(
            execution_time=1.5,
            wait_time=0.5,
            memory_usage_mb=10.2,
            cpu_usage_percent=5.0
        )
        ```
    """
    execution_time: Optional[float] = None
    wait_time: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    network_io_bytes: Optional[int] = None
    disk_io_bytes: Optional[int] = None
    
    @property
    def total_time(self) -> Optional[float]:
        """Get the total time from enqueue to completion."""
        if self.wait_time is not None and self.execution_time is not None:
            return self.wait_time + self.execution_time
        return None


@dataclass
class WorkerMetrics:
    """
    Metrics for worker performance and health.
    
    This class collects various metrics about worker performance,
    including job processing rates, resource usage, and health status.
    
    Example:
        ```python
        metrics = WorkerMetrics(
            jobs_processed=100,
            jobs_failed=2,
            avg_job_duration=1.2,
            memory_usage_mb=50.5
        )
        ```
    """
    jobs_processed: int = 0
    jobs_failed: int = 0
    jobs_retried: int = 0
    avg_job_duration: Optional[float] = None
    max_job_duration: Optional[float] = None
    min_job_duration: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    uptime_seconds: Optional[float] = None
    last_heartbeat: Optional[Timestamp] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate the success rate."""
        total = self.jobs_processed + self.jobs_failed
        if total == 0:
            return 0.0
        return self.jobs_processed / total
    
    @property
    def failure_rate(self) -> float:
        """Calculate the failure rate."""
        total = self.jobs_processed + self.jobs_failed
        if total == 0:
            return 0.0
        return self.jobs_failed / total


@dataclass
class QueueMetrics:
    """
    Metrics for queue performance and status.
    
    This class collects various metrics about queue performance,
    including job counts, processing rates, and queue size.
    
    Example:
        ```python
        metrics = QueueMetrics(
            pending_jobs=50,
            running_jobs=5,
            completed_jobs=1000,
            failed_jobs=10
        )
        ```
    """
    pending_jobs: int = 0
    running_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    retried_jobs: int = 0
    cancelled_jobs: int = 0
    avg_wait_time: Optional[float] = None
    avg_execution_time: Optional[float] = None
    throughput_per_second: Optional[float] = None
    last_updated: Optional[Timestamp] = field(default_factory=time.time)
    
    @property
    def total_jobs(self) -> int:
        """Get the total number of jobs."""
        return (
            self.pending_jobs + self.running_jobs + self.completed_jobs +
            self.failed_jobs + self.retried_jobs + self.cancelled_jobs
        )
    
    @property
    def completion_rate(self) -> float:
        """Calculate the completion rate."""
        total = self.completed_jobs + self.failed_jobs
        if total == 0:
            return 0.0
        return self.completed_jobs / total


# =============================================================================
# Configuration Types
# =============================================================================

@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.
    
    This class encapsulates retry configuration parameters,
    including the maximum number of retries, delay strategy,
    and which exceptions should be retried.
    
    Example:
        ```python
        retry_config = RetryConfig(
            max_retries=3,
            delay=1.0,
            strategy="exponential",
            retry_on=(ValueError, KeyError)
        )
        ```
    """
    max_retries: int = 0
    delay: RetryDelayType = 0
    strategy: str = "linear"
    retry_on: Optional[Tuple[Type[Exception], ...]] = None
    ignore_on: Optional[Tuple[Type[Exception], ...]] = None
    
    def __post_init__(self):
        """Validate the retry configuration."""
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        
        if self.strategy not in VALID_RETRY_STRATEGIES and not any(
            self.strategy == strategy.value for strategy in RETRY_STRATEGY
        ):
            raise ValueError(
                f"Invalid retry strategy '{self.strategy}'. "
                f"Must be one of: {', '.join(sorted(VALID_RETRY_STRATEGIES))}"
            )
        
        if isinstance(self.delay, (int, float)) and self.delay < 0:
            raise ValueError("delay must be non-negative")
        
        if isinstance(self.delay, Sequence) and any(d < 0 for d in self.delay):
            raise ValueError("All delay values must be non-negative")


@dataclass
class TimeoutConfig:
    """
    Configuration for timeout behavior.
    
    This class encapsulates timeout configuration parameters,
    including the timeout duration and timeout handling strategy.
    
    Example:
        ```python
        timeout_config = TimeoutConfig(
            duration=30.0,
            strategy="raise_exception"
        )
        ```
    """
    duration: float
    strategy: Literal["raise_exception", "return_default", "continue"] = "raise_exception"
    default_value: Any = None
    
    def __post_init__(self):
        """Validate the timeout configuration."""
        if self.duration < 0:
            raise ValueError("duration must be non-negative")


@dataclass
class ThrottleConfig:
    """
    Configuration for throttling behavior.
    
    This class encapsulates throttling configuration parameters,
    including rate limits and burst capacity.
    
    Example:
        ```python
        throttle_config = ThrottleConfig(
            max_requests=100,
            time_window=60.0,
            burst_capacity=10
        )
        ```
    """
    max_requests: int
    time_window: float  # in seconds
    burst_capacity: Optional[int] = None
    
    def __post_init__(self):
        """Validate the throttle configuration."""
        if self.max_requests <= 0:
            raise ValueError("max_requests must be positive")
        
        if self.time_window <= 0:
            raise ValueError("time_window must be positive")
        
        if self.burst_capacity is not None and self.burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")


# =============================================================================
# Event Types
# =============================================================================

@dataclass
class EventFilter:
    """
    Filter for events based on various criteria.
    
    This class provides a way to filter events based on event types,
    time ranges, and other criteria.
    
    Example:
        ```python
        filter = EventFilter(
            event_types=[JobEventType.COMPLETED, JobEventType.FAILED],
            start_time=time.time() - 3600,  # Last hour
            queue_names=["high_priority"]
        )
        ```
    """
    event_types: Optional[List[Union[JobEventType, WorkerEventType]]] = None
    start_time: Optional[Timestamp] = None
    end_time: Optional[Timestamp] = None
    queue_names: Optional[List[QueueName]] = None
    worker_ids: Optional[List[WorkerID]] = None
    job_ids: Optional[List[JobID]] = None
    min_duration: Optional[Milliseconds] = None
    max_duration: Optional[Milliseconds] = None
    
    def matches(self, event: Union[JobEvent, WorkerEvent]) -> bool:
        """
        Check if an event matches this filter.
        
        Args:
            event: The event to check
            
        Returns:
            True if the event matches the filter
        """
        # Check event type
        if self.event_types is not None and event.event_type not in self.event_types:
            return False
        
        # Check timestamp
        if self.start_time is not None and event.timestamp < self.start_time:
            return False
        
        if self.end_time is not None and event.timestamp > self.end_time:
            return False
        
        # Check queue name (for job events)
        if isinstance(event, JobEvent):
            if self.queue_names is not None and event.queue_name not in self.queue_names:
                return False
            
            if self.job_ids is not None and event.job_id not in self.job_ids:
                return False
            
            if self.min_duration is not None and (
                event.duration_ms is None or event.duration_ms < self.min_duration
            ):
                return False
            
            if self.max_duration is not None and (
                event.duration_ms is None or event.duration_ms > self.max_duration
            ):
                return False
        
        # Check worker ID (for worker events)
        if isinstance(event, WorkerEvent):
            if self.worker_ids is not None and event.worker_id not in self.worker_ids:
                return False
        
        return True


@dataclass
class EventSubscription:
    """
    Subscription for events with filtering and callback.
    
    This class represents a subscription to events with optional
    filtering and a callback function to handle matching events.
    
    Example:
        ```python
        def handle_event(event: JobEvent) -> None:
            print(f"Job {event.job_id} completed in {event.duration_ms}ms")
        
        subscription = EventSubscription(
            callback=handle_event,
            filter=EventFilter(event_types=[JobEventType.COMPLETED])
        )
        ```
    """
    callback: Callable[[Union[JobEvent, WorkerEvent]], None]
    filter: Optional[EventFilter] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    is_active: bool = True
    
    def handle_event(self, event: Union[JobEvent, WorkerEvent]) -> None:
        """
        Handle an event if it matches the filter and the subscription is active.
        
        Args:
            event: The event to handle
        """
        if not self.is_active:
            return
        
        if self.filter is None or self.filter.matches(event):
            self.callback(event)


# =============================================================================
# Job Types
# =============================================================================

@dataclass
class JobPriority:
    """
    Priority configuration for jobs.
    
    This class encapsulates job priority information,
    including the priority level and any priority-related metadata.
    
    Example:
        ```python
        priority = JobPriority(
            level=10,
            reason="urgent_customer_request"
        )
        ```
    """
    level: int
    reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate the priority configuration."""
        if self.level < 0:
            raise ValueError("priority level must be non-negative")


@dataclass
class JobProgress:
    """
    Progress tracking for jobs.
    
    This class provides a way to track and report progress
    for long-running jobs.
    
    Example:
        ```python
        progress = JobProgress(
            total=100,
            completed=25,
            message="Processing items"
        )
        ```
    """
    total: int
    completed: int = 0
    message: Optional[str] = None
    percentage: Optional[float] = field(init=False)
    
    def __post_init__(self):
        """Calculate percentage and validate values."""
        if self.total <= 0:
            raise ValueError("total must be positive")
        
        if self.completed < 0:
            raise ValueError("completed must be non-negative")
        
        if self.completed > self.total:
            raise ValueError("completed cannot exceed total")
        
        self.percentage = (self.completed / self.total) * 100
    
    def update(self, completed: int, message: Optional[str] = None) -> None:
        """
        Update the progress.
        
        Args:
            completed: The new completed count
            message: Optional progress message
        """
        if completed < 0:
            raise ValueError("completed must be non-negative")
        
        if completed > self.total:
            raise ValueError("completed cannot exceed total")
        
        self.completed = completed
        self.percentage = (self.completed / self.total) * 100
        
        if message is not None:
            self.message = message
    
    def is_complete(self) -> bool:
        """Check if the job is complete."""
        return self.completed >= self.total


# =============================================================================
# Worker Types
# =============================================================================

@dataclass
class WorkerCapabilities:
    """
    Capabilities and features supported by a worker.
    
    This class describes the capabilities of a worker,
    including supported job types, concurrency limits, and features.
    
    Example:
        ```python
        capabilities = WorkerCapabilities(
            max_concurrent_jobs=5,
            supported_job_types=["data_processing", "report_generation"],
            features=["progress_tracking", "cancellation"]
        )
        ```
    """
    max_concurrent_jobs: int = 1
    supported_job_types: Optional[List[str]] = None
    features: Optional[List[str]] = None
    resource_limits: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate the worker capabilities."""
        if self.max_concurrent_jobs <= 0:
            raise ValueError("max_concurrent_jobs must be positive")
        
        if self.supported_job_types is None:
            self.supported_job_types = []
        
        if self.features is None:
            self.features = []
        
        if self.resource_limits is None:
            self.resource_limits = {}
    
    def supports_job_type(self, job_type: str) -> bool:
        """
        Check if the worker supports a specific job type.
        
        Args:
            job_type: The job type to check
            
        Returns:
            True if the worker supports the job type
        """
        return not self.supported_job_types or job_type in self.supported_job_types
    
    def has_feature(self, feature: str) -> bool:
        """
        Check if the worker has a specific feature.
        
        Args:
            feature: The feature to check
            
        Returns:
            True if the worker has the feature
        """
        return feature in self.features


@dataclass
class WorkerHealth:
    """
    Health status information for a worker.
    
    This class provides information about the health status
    of a worker, including its current state and any issues.
    
    Example:
        ```python
        health = WorkerHealth(
            status="healthy",
            last_check=time.time(),
            issues=[]
        )
        ```
    """
    status: Literal["healthy", "degraded", "unhealthy", "unknown"]
    last_check: Timestamp
    issues: List[str] = field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Initialize default values."""
        if self.metadata is None:
            self.metadata = {}
    
    def is_healthy(self) -> bool:
        """Check if the worker is healthy."""
        return self.status == "healthy"
    
    def add_issue(self, issue: str) -> None:
        """
        Add an issue to the health status.
        
        Args:
            issue: The issue to add
        """
        self.issues.append(issue)
        if self.status == "healthy":
            self.status = "degraded"
    
    def clear_issues(self) -> None:
        """Clear all issues and reset status to healthy."""
        self.issues.clear()
        self.status = "healthy"


# =============================================================================
# Utility Types
# =============================================================================

@dataclass
class TimeWindow:
    """
    Represents a time window with start and end times.
    
    This class provides a convenient way to work with time ranges,
    including checking if a timestamp falls within the window.
    
    Example:
        ```python
        window = TimeWindow(
            start_time=time.time() - 3600,  # 1 hour ago
            end_time=time.time()  # now
        )
        
        if window.contains(timestamp):
            print("Timestamp is within the window")
        ```
    """
    start_time: Timestamp
    end_time: Timestamp
    
    def __post_init__(self):
        """Validate the time window."""
        if self.start_time > self.end_time:
            raise ValueError("start_time must be less than or equal to end_time")
    
    @property
    def duration(self) -> float:
        """Get the duration of the time window."""
        return self.end_time - self.start_time
    
    def contains(self, timestamp: Timestamp) -> bool:
        """
        Check if a timestamp falls within this time window.
        
        Args:
            timestamp: The timestamp to check
            
        Returns:
            True if the timestamp is within the window
        """
        return self.start_time <= timestamp <= self.end_time
    
    def overlaps(self, other: TimeWindow) -> bool:
        """
        Check if this time window overlaps with another.
        
        Args:
            other: The other time window
            
        Returns:
            True if the windows overlap
        """
        return self.start_time <= other.end_time and other.start_time <= self.end_time
    
    def intersection(self, other: TimeWindow) -> Optional[TimeWindow]:
        """
        Get the intersection of this time window with another.
        
        Args:
            other: The other time window
            
        Returns:
            The intersection time window, or None if they don't overlap
        """
        if not self.overlaps(other):
            return None
        
        return TimeWindow(
            start_time=max(self.start_time, other.start_time),
            end_time=min(self.end_time, other.end_time)
        )


@dataclass
class ResourceUsage:
    """
    Resource usage information.
    
    This class provides information about resource usage,
    including CPU, memory, disk, and network usage.
    
    Example:
        ```python
        usage = ResourceUsage(
            cpu_percent=25.5,
            memory_mb=1024.0,
            disk_mb=2048.0,
            network_bytes_in=1024,
            network_bytes_out=2048
        )
        ```
    """
    cpu_percent: Optional[float] = None
    memory_mb: Optional[float] = None
    disk_mb: Optional[float] = None
    network_bytes_in: Optional[int] = None
    network_bytes_out: Optional[int] = None
    timestamp: Timestamp = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "cpu_percent": self.cpu_percent,
            "memory_mb": self.memory_mb,
            "disk_mb": self.disk_mb,
            "network_bytes_in": self.network_bytes_in,
            "network_bytes_out": self.network_bytes_out,
            "timestamp": self.timestamp
        }


@dataclass
class SystemInfo:
    """
    System information and capabilities.
    
    This class provides information about the system,
    including OS, hardware, and software capabilities.
    
    Example:
        ```python
        info = SystemInfo(
            os_name="Linux",
            os_version="5.4.0",
            cpu_count=8,
            memory_gb=16.0,
            python_version="3.9.0"
        )
        ```
    """
    os_name: Optional[str] = None
    os_version: Optional[str] = None
    cpu_count: Optional[int] = None
    memory_gb: Optional[float] = None
    disk_gb: Optional[float] = None
    python_version: Optional[str] = None
    hostname: Optional[str] = None
    architecture: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "os_name": self.os_name,
            "os_version": self.os_version,
            "cpu_count": self.cpu_count,
            "memory_gb": self.memory_gb,
            "disk_gb": self.disk_gb,
            "python_version": self.python_version,
            "hostname": self.hostname,
            "architecture": self.architecture
        }


# Import uuid for ID generation
import uuid

# Export all public types and classes
__all__ = [
    # Basic Type Aliases
    "JSONValue",
    "JSONObject",
    "JSONArray",
    "JSONType",
    "Seconds",
    "Milliseconds",
    "Microseconds",
    "Timestamp",
    "Duration",
    "JobID",
    "WorkerID",
    "QueueName",
    "Subject",
    "StreamName",
    "BucketName",
    "TraceID",
    "SpanID",
    "JobList",
    "WorkerList",
    "QueueList",
    "SubjectList",
    "EventList",
    "JobDict",
    "WorkerDict",
    "QueueDict",
    "ConfigDict",
    "MetadataDict",
    "ContextDict",
    "SyncFunction",
    "AsyncFunction",
    "JobFunction",
    "ErrorHandler",
    "AsyncErrorHandler",
    "ValidatorFunction",
    "AsyncValidatorFunction",
    
    # Protocol Definitions
    "Serializable",
    "Configurable",
    "Executable",
    "Retryable",
    "Monitorable",
    "EventProducer",
    
    # Generic Types
    "Result",
    "PaginatedResult",
    "BatchResult",
    "BatchError",
    
    # Type Variables
    "T",
    "R",
    "E",
    "K",
    "V",
    
    # Type Validation Helpers
    "TypeValidationError",
    "validate_type",
    "validate_optional_type",
    "validate_collection_type",
    "validate_dict_types",
    "is_type_compatible",
    
    # Model Types
    "JobDependency",
    "JobMetrics",
    "WorkerMetrics",
    "QueueMetrics",
    
    # Configuration Types
    "RetryConfig",
    "TimeoutConfig",
    "ThrottleConfig",
    
    # Event Types
    "EventFilter",
    "EventSubscription",
    
    # Job Types
    "JobPriority",
    "JobProgress",
    
    # Worker Types
    "WorkerCapabilities",
    "WorkerHealth",
    
    # Utility Types
    "TimeWindow",
    "ResourceUsage",
    "SystemInfo",
    
    # Re-exported types from models
    "JOB_STATUS",
    "JobEventType",
    "WorkerEventType",
    "VALID_RETRY_STRATEGIES",
    "RetryDelayType",
    "JobEvent",
    "WorkerEvent",
    "Job",
    "JobResult",
    "Schedule",
]