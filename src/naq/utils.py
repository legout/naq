# src/naq/utils.py
"""
NAQ Utilities - Backward Compatibility Layer

This module provides backward compatibility by re-exporting utilities from the new
structured utils package. For new code, consider importing directly from the
specific submodules (e.g., from naq.utils.retry import retry).
"""

# Import from the new utils package structure
from .utils.async_helpers import (
    run_async,
    run_async_in_thread,
    run_sync,
    run_sync_in_async,
    AsyncToSyncBridge,
    SyncToAsyncBridge,
)

from .utils.logging import (
    setup_logging,
    get_logger,
    log_function_call,
    log_performance,
    log_errors,
    LogContext,
    PerformanceTimer,
)
from .utils.context_managers import (
    # Connection context managers
    nats_connection,
    jetstream_context,
    nats_jetstream,
    nats_kv_store,
    
    # Retry context managers
    retry_context,
    retry_async_context,
    
    # Logging context managers
    LogContext,
    PerformanceTimer,
    
    # New context managers from Task 08
    managed_resource,
    managed_resource_sync,
    timeout_context,
    timeout_context_sync,
    error_context,
    error_context_sync,
    operation_context,
    operation_context_sync,
    combined_context,
    combined_context_sync,
)

from .utils.retry import (
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

from .utils.decorators import (
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

from .utils.validation import (
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

from .utils.config import (
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
from .utils.serialization import (
    Serializer,
    PickleSerializer,
    JsonSerializer,
    get_serializer,
    _normalize_retry_strategy,
)

# Backward compatibility aliases
# Keep the old function names for existing code
run_async_from_sync = run_async

# Re-export all utilities for backward compatibility
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
    
# Context managers
    "nats_connection",
    "jetstream_context",
    "nats_jetstream",
    "nats_kv_store",
    "managed_resource",
    "managed_resource_sync",
    "timeout_context",
    "timeout_context_sync",
    "error_context",
    "error_context_sync",
    "operation_context",
    "operation_context_sync",
    "combined_context",
    "combined_context_sync",
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
# Serialization utilities
    "Serializer",
    "PickleSerializer",
    "JsonSerializer",
    "get_serializer",
    "_normalize_retry_strategy",
    "EventConfig",
    "LoggingConfig",
    "NaqConfig",
    "load_naq_config",
    "validate_config",
    "DEFAULT_CONFIG_SCHEMA",
]