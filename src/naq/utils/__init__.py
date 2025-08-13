"""
NAQ Utils Package

This package provides comprehensive utilities for the NAQ job queue system,
including retry mechanisms, validation, configuration management, timing utilities,
and other common patterns used throughout the codebase.

The package is organized into focused modules for better maintainability:

- async_helpers: Asynchronous execution utilities
- retry: Retry mechanisms and backoff strategies
- validation: Data validation and schema checking
- config: Configuration management utilities
- logging_utils: Logging configuration and utilities
- decorators: Comprehensive collection of decorators for common patterns
- error_handling: Centralized error handling patterns and utilities
- timing: Performance timing, timeout management, scheduling, and benchmarking utilities
- serialization: Job and result serialization utilities

For backward compatibility, the main utilities are also available through
the parent naq.utils module.
"""

# Core utilities - commonly used across the codebase
from .async_helpers import (
    run_async,
    run_async_in_thread,
    run_sync,
    run_sync_in_async,
    AsyncToSyncBridge,
    SyncToAsyncBridge,
)

# Backward compatibility aliases
run_async_from_sync = run_async

from .logging import (
    setup_logging,
    get_logger,
    log_function_call,
    log_performance,
    log_errors,
    LogContext,
    PerformanceTimer,
)

# Retry utilities
from .retry import (
    RetryConfig,
    RetryError,
    retry,
    retry_async,
    retry_context,
    retry_async_context,
    with_retry,
    with_retry_async,
    calculate_backoff,
    is_retryable_error,
)

# Decorators utilities
from .decorators import (
    with_nats_connection,
    with_jetstream_context,
    retry_with_backoff,
    timing,
    error_handler,
    log_function_call,
    deprecated,
    singleton,
    BackoffStrategy,
)

# Validation utilities
from .validation import (
    ValidationError,
    validate_type,
    validate_range,
    validate_choice,
    validate_required,
    validate_string,
    validate_url,
    validate_email,
    validate_dict,
    validate_job_config,
    validate_connection_config,
    validate_event_config,
    validate_dataclass,
    Validator,
)

# Configuration utilities
from .config import (
    ConfigError,
    ConfigSource,
    EnvironmentConfigSource,
    FileConfigSource,
    DictConfigSource,
    ConfigManager,
    create_default_config_manager,
    load_dataclass_from_config,
    JobConfig,
    ConnectionConfig,
    EventConfig,
    LoggingConfig,
    NaqConfig,
    load_naq_config,
    validate_config,
    DEFAULT_CONFIG_SCHEMA,
)

# Error handling utilities
from .error_handling import (
    ErrorSeverity,
    ErrorCategory,
    RecoveryStrategy,
    ErrorContext,
    RecoveryConfig,
    ErrorHandler,
    ErrorReporter,
    create_error_context,
    error_context,
    get_current_error_context,
    wrap_naq_exception,
    get_default_error_handler,
    get_default_error_reporter,
    handle_error_with_default_handler,
    handle_error_with_default_handler_async,
    report_error_with_default_reporter,
    with_error_handling,
    with_error_context,
)

# Timing utilities
from .timing import (
    TimeoutError,
    TimingError,
    SchedulingError,
    BenchmarkError,
    TimingResult,
    TimingStats,
    Timer,
    AsyncTimer,
    time_function,
    time_async_function,
    timeout_context,
    timeout_context_sync,
    wait_with_timeout,
    run_with_timeout,
    Scheduler,
    BenchmarkResult,
    BenchmarkComparison,
    Benchmark,
    compare_benchmarks,
    timing_context,
    timing_context_sync,
    NestedTimer,
    AsyncNestedTimer,
)
# Serialization utilities
from .serialization import (
    Serializer,
    PickleSerializer,
    JsonSerializer,
    get_serializer,
    _normalize_retry_strategy,
)

# Type definitions and protocols
from .types import (
    # Type aliases
    JobID,
    JobName,
    QueueName,
    WorkerID,
    EventID,
    ScheduleID,
    StreamName,
    Subject,
    Payload,
    Headers,
    Timestamp,
    Duration,
    RetryCount,
    Priority,
    Progress,
    Status,
    ResultType,
    ErrorType,
    MetricName,
    MetricValue,
    ResourceUsage,
    SystemInfo,
    
    # Protocol definitions
    Serializable,
    Configurable,
    Executable,
    Retryable,
    Monitorable,
    EventProducer,
    EventConsumer,
    JobProcessor,
    JobScheduler,
    JobQueue,
    JobStorage,
    EventStorage,
    ScheduleStorage,
    MetricsCollector,
    ResourceMonitor,
    SystemMonitor,
    
    # Generic types
    JobFunction,
    JobFunctionSync,
    JobFunctionAsync,
    JobFunctionGeneric,
    JobFunctionSyncGeneric,
    JobFunctionAsyncGeneric,
    JobResult,
    JobResultSync,
    JobResultAsync,
    JobResultGeneric,
    JobResultSyncGeneric,
    JobResultAsyncGeneric,
    JobConfig,
    JobConfigSync,
    JobConfigAsync,
    JobConfigGeneric,
    JobConfigSyncGeneric,
    JobConfigAsyncGeneric,
    EventData,
    EventDataGeneric,
    EventConfig,
    EventConfigGeneric,
    ScheduleConfig,
    ScheduleConfigGeneric,
    RetryConfig,
    RetryConfigGeneric,
    RetryStrategy,
    RetryStrategyGeneric,
    BackoffStrategy,
    BackoffStrategyGeneric,
    ValidationConfig,
    ValidationConfigGeneric,
    ValidationRule,
    ValidationRuleGeneric,
    ValidationError,
    ValidationErrorGeneric,
    ValidationResult,
    ValidationResultGeneric,
    ValidationContext,
    ValidationContextGeneric,
    MetricsConfig,
    MetricsConfigGeneric,
    MetricsData,
    MetricsDataGeneric,
    ResourceConfig,
    ResourceConfigGeneric,
    ResourceData,
    ResourceDataGeneric,
    SystemConfig,
    SystemConfigGeneric,
    SystemData,
    SystemDataGeneric,
    
    # Type validation helpers
    validate_job_id,
    validate_job_name,
    validate_queue_name,
    validate_worker_id,
    validate_event_id,
    validate_schedule_id,
    validate_stream_name,
    validate_subject,
    validate_payload,
    validate_headers,
    validate_timestamp,
    validate_duration,
    validate_retry_count,
    validate_priority,
    validate_progress,
    validate_status,
    validate_result_type,
    validate_error_type,
    validate_metric_name,
    validate_metric_value,
    validate_resource_usage,
    validate_system_info,
    is_valid_job_id,
    is_valid_job_name,
    is_valid_queue_name,
    is_valid_worker_id,
    is_valid_event_id,
    is_valid_schedule_id,
    is_valid_stream_name,
    is_valid_subject,
    is_valid_payload,
    is_valid_headers,
    is_valid_timestamp,
    is_valid_duration,
    is_valid_retry_count,
    is_valid_priority,
    is_valid_progress,
    is_valid_status,
    is_valid_result_type,
    is_valid_error_type,
    is_valid_metric_name,
    is_valid_metric_value,
    is_valid_resource_usage,
    is_valid_system_info,
    
    # Model-related types
    JobStatus,
    JobEventType,
    JobPriority,
    Job,
    JobEvent,
    JobResult,
    Schedule,
    ScheduleStatus,
    ScheduleType,
    ScheduleTrigger,
    WorkerStatus,
    Worker,
    QueueStatus,
    Queue,
    EventStatus,
    Event,
    MetricStatus,
    Metric,
    ResourceStatus,
    Resource,
    SystemStatus,
    System,
)
# NATS helpers utilities
from .nats_helpers import (
    # Connection utilities
    ConnectionMetrics,
    ConnectionMonitor,
    connection_monitor,
    test_nats_connection,
    test_nats_connection_sync,
    wait_for_nats_connection,
    wait_for_nats_connection_sync,
    get_connection_metrics,
    reset_connection_metrics,
    is_jetstream_enabled,
    is_jetstream_enabled_sync,
    
    # Stream utilities
    StreamConfigHelper,
    create_stream_with_retry,
    create_stream_with_retry_sync,
    ensure_stream_exists,
    ensure_stream_exists_sync,
    
    # KV store utilities
    KVOperationConfig,
    kv_get_with_retry,
    kv_get_with_retry_sync,
    kv_put_with_retry,
    kv_put_with_retry_sync,
    kv_delete_with_retry,
    kv_delete_with_retry_sync,
    create_kv_bucket_with_retry,
    create_kv_bucket_with_retry_sync,
    
    # Subject utilities
    SubjectParts,
    build_subject,
    parse_subject,
    create_subject_wildcard,
    matches_subject_pattern,
    
    # Message handling utilities
    MessageHandlerConfig,
    create_consumer_with_retry,
    create_consumer_with_retry_sync,
    publish_message_with_retry,
    publish_message_with_retry_sync,
    request_with_retry,
    request_with_retry_sync,
    batch_publish,
    batch_publish_sync,
    
    # Context managers
    nats_connection_context,
    jetstream_context_from_connection,
    nats_jetstream_context,
    nats_subscription,
    jetstream_subscription,
    nats_request_context,
    nats_connection_context_sync,
    jetstream_context_sync,
    nats_jetstream_context_sync,
    nats_request_context_sync,
)

__all__ = [
    # Async utilities
    "run_async",
    "run_async_in_thread",
    "run_sync",
    "run_sync_in_async",
    "AsyncToSyncBridge",
    "SyncToAsyncBridge",
    "run_async_from_sync",  # Backward compatibility alias
    
    # Logging utilities
    "setup_logging",
    "get_logger",
    "log_function_call",
    "log_performance",
    "log_errors",
    "LogContext",
    "PerformanceTimer",
    
    # Retry utilities
    "RetryConfig",
    "RetryError",
    "retry",
    "retry_async",
    "retry_context",
    "retry_async_context",
    "with_retry",
    "with_retry_async",
    "calculate_backoff",
    "is_retryable_error",
    
    # Decorators utilities
    "with_nats_connection",
    "with_jetstream_context",
    "retry_with_backoff",
    "timing",
    "error_handler",
    "log_function_call",
    "deprecated",
    "singleton",
    "BackoffStrategy",
    
    # Validation utilities
    "ValidationError",
    "validate_type",
    "validate_range",
    "validate_choice",
    "validate_required",
    "validate_string",
    "validate_url",
    "validate_email",
    "validate_dict",
    "validate_job_config",
    "validate_connection_config",
    "validate_event_config",
    "validate_dataclass",
    "Validator",
    
    # Configuration utilities
    "ConfigError",
    "ConfigSource",
    "EnvironmentConfigSource",
    "FileConfigSource",
    "DictConfigSource",
    "ConfigManager",
    "create_default_config_manager",
    "load_dataclass_from_config",
    "JobConfig",
    "ConnectionConfig",
    "EventConfig",
    "LoggingConfig",
    "NaqConfig",
    "load_naq_config",
    "validate_config",
    "DEFAULT_CONFIG_SCHEMA",
    
    # Error handling utilities
    "ErrorSeverity",
    "ErrorCategory",
    "RecoveryStrategy",
    "ErrorContext",
    "RecoveryConfig",
    "ErrorHandler",
    "ErrorReporter",
    "create_error_context",
    "error_context",
    "get_current_error_context",
    "wrap_naq_exception",
    "get_default_error_handler",
    "get_default_error_reporter",
    "handle_error_with_default_handler",
    "handle_error_with_default_handler_async",
    "report_error_with_default_reporter",
    "with_error_handling",
    "with_error_context",
    
    # Timing utilities
    "TimeoutError",
    "TimingError",
    "SchedulingError",
    "BenchmarkError",
    "TimingResult",
    "TimingStats",
    "Timer",
    "AsyncTimer",
    "time_function",
    "time_async_function",
    "timeout_context",
    "timeout_context_sync",
    "wait_with_timeout",
    "run_with_timeout",
    "Scheduler",
    "BenchmarkResult",
# Serialization utilities
    "Serializer",
    "PickleSerializer",
    "JsonSerializer",
    "get_serializer",
    "_normalize_retry_strategy",
    "BenchmarkComparison",
    "Benchmark",
    "compare_benchmarks",
    "timing_context",
    "timing_context_sync",
    "NestedTimer",
    "AsyncNestedTimer",
    
    # NATS helpers utilities
    # Connection utilities
    "ConnectionMetrics",
    "ConnectionMonitor",
    "connection_monitor",
    "test_nats_connection",
    "test_nats_connection_sync",
    "wait_for_nats_connection",
    "wait_for_nats_connection_sync",
    "get_connection_metrics",
    "reset_connection_metrics",
    "is_jetstream_enabled",
    "is_jetstream_enabled_sync",
    
    # Stream utilities
    "StreamConfigHelper",
    "create_stream_with_retry",
    "create_stream_with_retry_sync",
    "ensure_stream_exists",
    "ensure_stream_exists_sync",
    
    # KV store utilities
    "KVOperationConfig",
    "kv_get_with_retry",
    "kv_get_with_retry_sync",
    "kv_put_with_retry",
    "kv_put_with_retry_sync",
    "kv_delete_with_retry",
    "kv_delete_with_retry_sync",
    "create_kv_bucket_with_retry",
    "create_kv_bucket_with_retry_sync",
    
    # Subject utilities
    "SubjectParts",
    "build_subject",
    "parse_subject",
    "create_subject_wildcard",
    "matches_subject_pattern",
    
    # Message handling utilities
    "MessageHandlerConfig",
    "create_consumer_with_retry",
    "create_consumer_with_retry_sync",
    "publish_message_with_retry",
    "publish_message_with_retry_sync",
    "request_with_retry",
    "request_with_retry_sync",
    "batch_publish",
    "batch_publish_sync",
    
    # Context managers
    "nats_connection_context",
    "jetstream_context_from_connection",
    "nats_jetstream_context",
    "nats_subscription",
    "jetstream_subscription",
    "nats_request_context",
    "nats_connection_context_sync",
    "jetstream_context_sync",
    "nats_jetstream_context_sync",
    "nats_request_context_sync",
]

# Version info
__version__ = "1.0.0"