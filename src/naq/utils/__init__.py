# src/naq/utils/__init__.py
"""
NAQ utility functions and decorators for common patterns.

This package provides reusable utilities to reduce code duplication and improve 
maintainability across the NAQ codebase.
"""

# Import utilities for public API
from .decorators import (
    retry,
    timing,
    log_errors,
    rate_limit,
)

from .context_managers import (
    managed_resource,
    timeout_context,
    error_context,
    performance_context,
)

from .async_helpers import (
    run_in_thread,
    gather_with_concurrency,
    retry_async,
    sync_to_async,
    async_to_sync,
)

from .error_handling import (
    ErrorHandler,
    create_error_context,
    wrap_naq_exception,
    async_error_handler_context,
    get_global_error_handler,
)

from .logging import (
    StructuredLogger,
    JSONFormatter,
    setup_structured_logging,
)

from .serialization import (
    SerializationHelper,
    serialize_with_metadata,
    deserialize_with_metadata,
)

from .validation import (
    ValidationHelper,
    validate_config,
    validate_json_schema,
    VALID_QUEUE_NAME,
)

from .timing import (
    Timer,
    measure_performance,
    benchmark_function,
)

from .nats_helpers import (
    create_subject,
    parse_subject,
    ensure_stream_exists,
    safe_kv_operation,
)

from .types import (
    JobID,
    QueueName,
    Subject,
    ConfigDict,
)

__all__ = [
    # Decorators
    'retry',
    'timing', 
    'log_errors',
    'rate_limit',
    
    # Context managers
    'managed_resource',
    'timeout_context',
    'error_context',
    'performance_context',
    
    # Async helpers
    'run_in_thread',
    'gather_with_concurrency',
    'retry_async',
    'sync_to_async',
    'async_to_sync',
    
    # Error handling
    'ErrorHandler',
    'create_error_context',
    'wrap_naq_exception',
    'async_error_handler_context',
    'get_global_error_handler',
    
    # Logging
    'StructuredLogger',
    'JSONFormatter',
    'setup_structured_logging',
    
    # Serialization
    'SerializationHelper',
    'serialize_with_metadata',
    'deserialize_with_metadata',
    
    # Validation
    'ValidationHelper',
    'validate_config',
    'validate_json_schema',
    'VALID_QUEUE_NAME',
    
    # Timing
    'Timer',
    'measure_performance',
    'benchmark_function',
    
    # NATS helpers
    'create_subject',
    'parse_subject',
    'ensure_stream_exists',
    'safe_kv_operation',
    
    # Types
    'JobID',
    'QueueName',
    'Subject',
    'ConfigDict',
    
    # Legacy utilities for backward compatibility
    'run_async_from_sync',
    'setup_logging',
]

# Legacy utilities for backward compatibility
# These will be imported from the main naq module if needed
run_async_from_sync = None
setup_logging = None

def __getattr__(name):
    """Lazy import for legacy utilities to avoid circular imports."""
    if name == 'run_async_from_sync':
        global run_async_from_sync
        if run_async_from_sync is None:
            from ..utils import run_async_from_sync as _func
            run_async_from_sync = _func
        return run_async_from_sync
    elif name == 'setup_logging':
        global setup_logging
        if setup_logging is None:
            from ..utils import setup_logging as _func
            setup_logging = _func
        return setup_logging
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")