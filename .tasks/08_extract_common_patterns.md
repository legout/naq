# Task 08: Extract Common Code Patterns

## Overview
Extract repeated code patterns throughout the NAQ codebase into reusable utilities, decorators, and helper functions to reduce code duplication and improve maintainability.

## Current State Analysis
After splitting large files and analyzing the codebase, several patterns are repeated across multiple modules:

### Identified Patterns

1. **Error Handling Patterns** (~30+ locations)
   - Try/catch with logging and error conversion
   - Retry logic with exponential backoff
   - Connection error recovery

2. **Logging Patterns** (~50+ locations)
   - Structured logging with context
   - Performance timing logs
   - Error logging with traceback

3. **Async/Await Patterns** (~25+ locations)
   - Async function synchronous wrappers
   - Timeout handling
   - Context manager patterns

4. **Serialization Patterns** (~15+ locations)
   - Job serialization/deserialization
   - Result serialization/deserialization
   - Error handling for serialization

5. **NATS Operation Patterns** (~40+ locations)
   - Stream creation with error handling
   - KV store operations with retries
   - Subject building and parsing

6. **Validation Patterns** (~20+ locations)
   - Parameter validation
   - Type checking and conversion
   - Configuration validation

## Target Structure

### Utils Package Organization
```
src/naq/utils/
├── __init__.py              # Public API exports
├── decorators.py           # Common decorators (retry, timing, etc.)
├── context_managers.py     # Resource management contexts
├── async_helpers.py        # Async/await utilities
├── error_handling.py       # Standardized error handling  
├── logging.py              # Logging utilities and formatters
├── serialization.py        # Serialization helpers
├── validation.py           # Validation utilities
├── timing.py               # Performance timing utilities
├── nats_helpers.py         # NATS-specific utilities
└── types.py                # Common type definitions
```

## Detailed Implementation

### 1. decorators.py - Common Decorators

**Retry Decorator with Configurable Backoff**
```python
from functools import wraps
from typing import Callable, Optional, Union, Tuple, Type
import asyncio
import time
import random

def retry(
    max_attempts: int = 3,
    delay: Union[float, Tuple[float, float]] = 1.0,
    backoff: str = "exponential",  # "linear", "exponential", "jitter"
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with configurable backoff strategies.
    
    Args:
        max_attempts: Maximum retry attempts
        delay: Base delay (float) or range (tuple)
        backoff: Backoff strategy ("linear", "exponential", "jitter")
        exceptions: Exception types to retry on
        on_retry: Callback function called on retry
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay
                    if isinstance(delay, tuple):
                        base_delay = random.uniform(delay[0], delay[1])
                    else:
                        base_delay = delay
                    
                    if backoff == "exponential":
                        actual_delay = base_delay * (2 ** attempt)
                    elif backoff == "jitter":
                        actual_delay = base_delay + random.uniform(0, base_delay)
                    else:  # linear
                        actual_delay = base_delay
                    
                    if on_retry:
                        await on_retry(attempt + 1, e, actual_delay)
                    
                    await asyncio.sleep(actual_delay)
            
            raise last_exception
        
        @wraps(func)  
        def sync_wrapper(*args, **kwargs):
            # Similar logic for sync functions
            pass
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

def timing(
    logger: Optional[logging.Logger] = None,
    threshold_ms: Optional[float] = None,
    message: Optional[str] = None
):
    """Timing decorator with optional slow operation logging."""
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                if logger and (threshold_ms is None or duration_ms > threshold_ms):
                    msg = message or f"{func.__name__} execution time"
                    logger.info(f"{msg}: {duration_ms:.2f}ms")
                    
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Similar logic for sync functions
            pass
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

def log_errors(
    logger: Optional[logging.Logger] = None,
    level: int = logging.ERROR,
    reraise: bool = True,
    message: Optional[str] = None
):
    """Error logging decorator."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if logger:
                    error_msg = message or f"Error in {func.__name__}"
                    logger.log(level, f"{error_msg}: {e}", exc_info=True)
                
                if reraise:
                    raise
                return None
        return wrapper
    return decorator
```

### 2. context_managers.py - Resource Management

**Resource Context Managers**
```python
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Any

@asynccontextmanager
async def managed_resource(
    acquire_func: Callable[[], Awaitable[Any]],
    release_func: Callable[[Any], Awaitable[None]],
    on_error: Optional[Callable[[Exception], Awaitable[None]]] = None
) -> AsyncIterator[Any]:
    """Generic resource management context manager."""
    resource = None
    try:
        resource = await acquire_func()
        yield resource
    except Exception as e:
        if on_error:
            await on_error(e)
        raise
    finally:
        if resource is not None:
            try:
                await release_func(resource)
            except Exception as cleanup_error:
                logger.warning(f"Resource cleanup error: {cleanup_error}")

@asynccontextmanager
async def timeout_context(seconds: float) -> AsyncIterator[None]:
    """Context manager for timeout operations."""
    try:
        async with asyncio.timeout(seconds):
            yield
    except asyncio.TimeoutError:
        logger.warning(f"Operation timed out after {seconds} seconds")
        raise

@asynccontextmanager
async def error_context(
    operation_name: str,
    logger: Optional[logging.Logger] = None,
    suppress_exceptions: Tuple[Type[Exception], ...] = ()
) -> AsyncIterator[None]:
    """Context manager for operation error handling."""
    try:
        yield
    except suppress_exceptions:
        if logger:
            logger.debug(f"{operation_name} completed with suppressed exception")
    except Exception as e:
        if logger:
            logger.error(f"Error in {operation_name}: {e}", exc_info=True)
        raise
```

### 3. async_helpers.py - Async Utilities

**Async Function Helpers**
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Optional, TypeVar

T = TypeVar('T')

async def run_in_thread(
    func: Callable[..., T], 
    *args, 
    executor: Optional[ThreadPoolExecutor] = None,
    **kwargs
) -> T:
    """Run synchronous function in thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

async def gather_with_concurrency(
    tasks: List[Awaitable[T]], 
    concurrency: int = 10
) -> List[T]:
    """Execute tasks with limited concurrency."""
    semaphore = asyncio.Semaphore(concurrency)
    
    async def bounded_task(task):
        async with semaphore:
            return await task
    
    bounded_tasks = [bounded_task(task) for task in tasks]
    return await asyncio.gather(*bounded_tasks)

async def retry_async(
    func: Callable[..., Awaitable[T]],
    *args,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs
) -> T:
    """Retry async function with exponential backoff."""
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts - 1:
                break
            
            wait_time = delay * (backoff ** attempt)
            await asyncio.sleep(wait_time)
    
    raise last_exception

def sync_to_async(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """Convert synchronous function to async."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await run_in_thread(func, *args, **kwargs)
    return wrapper

def async_to_sync(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """Convert async function to synchronous (with new event loop)."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper
```

### 4. error_handling.py - Standardized Error Handling

**Error Handling Utilities**
```python
from typing import Type, Callable, Optional, Any, Dict
import traceback
import logging

class ErrorHandler:
    """Centralized error handling with configurable strategies."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.error_callbacks: Dict[Type[Exception], Callable] = {}
    
    def register_handler(
        self, 
        exception_type: Type[Exception], 
        handler: Callable[[Exception], Any]
    ):
        """Register error handler for specific exception type."""
        self.error_callbacks[exception_type] = handler
    
    async def handle_error(
        self, 
        error: Exception, 
        context: str = "",
        reraise: bool = True
    ) -> Optional[Any]:
        """Handle error with registered handlers."""
        error_type = type(error)
        
        # Log the error
        self.logger.error(
            f"Error in {context}: {error}", 
            exc_info=True,
            extra={'error_type': error_type.__name__, 'context': context}
        )
        
        # Try specific handler first
        if error_type in self.error_callbacks:
            try:
                return await self._call_handler(self.error_callbacks[error_type], error)
            except Exception as handler_error:
                self.logger.error(f"Error handler failed: {handler_error}")
        
        # Try parent class handlers
        for registered_type, handler in self.error_callbacks.items():
            if isinstance(error, registered_type):
                try:
                    return await self._call_handler(handler, error)
                except Exception as handler_error:
                    self.logger.error(f"Error handler failed: {handler_error}")
        
        if reraise:
            raise error
        return None
    
    async def _call_handler(self, handler: Callable, error: Exception) -> Any:
        """Call error handler (async or sync)."""
        if asyncio.iscoroutinefunction(handler):
            return await handler(error)
        else:
            return handler(error)

def create_error_context(operation_name: str) -> Dict[str, Any]:
    """Create error context with operation details."""
    return {
        'operation': operation_name,
        'timestamp': time.time(),
        'traceback': traceback.format_exc(),
        'thread_id': threading.get_ident(),
    }

def wrap_naq_exception(
    error: Exception, 
    context: str = "",
    original_traceback: bool = True
) -> NaqException:
    """Wrap exception in NAQ-specific exception with context."""
    message = f"{context}: {error}" if context else str(error)
    
    if isinstance(error, NaqException):
        return error
    
    # Map common exceptions to NAQ exceptions
    if isinstance(error, ConnectionError):
        return NaqConnectionError(message) from (error if original_traceback else None)
    elif isinstance(error, (ValueError, TypeError)):
        return ConfigurationError(message) from (error if original_traceback else None)
    elif isinstance(error, (pickle.PicklingError, json.JSONDecodeError)):
        return SerializationError(message) from (error if original_traceback else None)
    else:
        return NaqException(message) from (error if original_traceback else None)
```

### 5. logging.py - Logging Utilities

**Enhanced Logging Utilities**
```python
import logging
import json
import time
from typing import Dict, Any, Optional
from contextlib import contextmanager

class StructuredLogger:
    """Structured logging with consistent formatting."""
    
    def __init__(self, name: str, extra_fields: Optional[Dict[str, Any]] = None):
        self.logger = logging.getLogger(name)
        self.extra_fields = extra_fields or {}
    
    def _log_with_context(self, level: int, message: str, **kwargs):
        """Log with structured context."""
        extra = {**self.extra_fields, **kwargs}
        self.logger.log(level, message, extra=extra)
    
    def info(self, message: str, **kwargs):
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def error(self, message: str, **kwargs):  
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    @contextmanager
    def operation_context(self, operation_name: str, **context):
        """Context manager for operation logging."""
        start_time = time.perf_counter()
        operation_id = generate_id()
        
        context.update({
            'operation': operation_name,
            'operation_id': operation_id,
            'start_time': start_time
        })
        
        self.info(f"Starting {operation_name}", **context)
        
        try:
            yield operation_id
            duration_ms = (time.perf_counter() - start_time) * 1000
            self.info(
                f"Completed {operation_name}",
                duration_ms=duration_ms,
                **context
            )
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self.error(
                f"Failed {operation_name}: {e}",
                error_type=type(e).__name__,
                duration_ms=duration_ms,
                **context
            )
            raise

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': record.created,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                log_entry[key] = value
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, default=str)

def setup_structured_logging(
    level: str = "INFO",
    format_type: str = "json",
    extra_fields: Optional[Dict[str, Any]] = None
) -> StructuredLogger:
    """Setup structured logging configuration."""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create new handler
    handler = logging.StreamHandler()
    
    if format_type == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
    
    logger.addHandler(handler)
    
    return StructuredLogger("naq", extra_fields)
```

### 6. serialization.py - Serialization Helpers

**Serialization Utilities**
```python
from typing import Any, Dict, Type, Optional
import json
import pickle
from abc import ABC, abstractmethod

class SerializationHelper:
    """Centralized serialization utilities."""
    
    @staticmethod
    def safe_serialize(
        data: Any, 
        serializer: str = "pickle",
        fallback_serializer: Optional[str] = None
    ) -> bytes:
        """Serialize data with fallback option."""
        try:
            if serializer == "pickle":
                return pickle.dumps(data)
            elif serializer == "json":
                return json.dumps(data, default=str).encode('utf-8')
            else:
                raise ValueError(f"Unsupported serializer: {serializer}")
        except Exception as e:
            if fallback_serializer:
                logger.warning(f"Serialization with {serializer} failed, using {fallback_serializer}")
                return SerializationHelper.safe_serialize(data, fallback_serializer)
            raise SerializationError(f"Serialization failed: {e}") from e
    
    @staticmethod
    def safe_deserialize(
        data: bytes, 
        serializer: str = "pickle",
        expected_type: Optional[Type] = None
    ) -> Any:
        """Deserialize data with type checking."""
        try:
            if serializer == "pickle":
                result = pickle.loads(data)
            elif serializer == "json":
                result = json.loads(data.decode('utf-8'))
            else:
                raise ValueError(f"Unsupported serializer: {serializer}")
            
            if expected_type and not isinstance(result, expected_type):
                raise SerializationError(
                    f"Deserialized object is {type(result)}, expected {expected_type}"
                )
            
            return result
        except Exception as e:
            raise SerializationError(f"Deserialization failed: {e}") from e

def serialize_with_metadata(
    data: Any,
    serializer: str = "pickle",
    metadata: Optional[Dict[str, Any]] = None
) -> bytes:
    """Serialize data with metadata header."""
    metadata = metadata or {}
    metadata['serializer'] = serializer
    metadata['timestamp'] = time.time()
    
    payload = {
        'metadata': metadata,
        'data': data
    }
    
    return SerializationHelper.safe_serialize(payload, serializer)

def deserialize_with_metadata(data: bytes) -> Tuple[Any, Dict[str, Any]]:
    """Deserialize data and return data + metadata."""
    try:
        # Try pickle first (most common)
        payload = pickle.loads(data)
    except:
        try:
            # Try JSON fallback
            payload = json.loads(data.decode('utf-8'))
        except Exception as e:
            raise SerializationError(f"Could not deserialize data: {e}") from e
    
    if isinstance(payload, dict) and 'metadata' in payload:
        return payload['data'], payload['metadata']
    else:
        # Legacy data without metadata
        return payload, {}
```

## Common Pattern Extraction Strategy

### Phase 1: Pattern Analysis
1. **Identify Repeated Code**
   - Use AST analysis to find similar code patterns
   - Manual review of large functions
   - Metrics-driven approach (cyclomatic complexity, duplicate lines)

2. **Categorize Patterns**
   - Group by functionality (error handling, logging, etc.)
   - Identify dependencies between patterns
   - Determine extraction priority

### Phase 2: Utility Implementation  
1. **Create Utility Functions**
   - Extract most common patterns first
   - Ensure backward compatibility
   - Add comprehensive tests

2. **Create Decorators**
   - Convert function wrappers to decorators
   - Support both async and sync functions
   - Add configuration options

### Phase 3: Codebase Integration
1. **Systematic Replacement**
   - Replace patterns file by file
   - Test each replacement thoroughly
   - Maintain exact same functionality

2. **Performance Validation**
   - Benchmark before/after performance
   - Ensure no performance regressions
   - Optimize if necessary

## Integration Points

### Service Layer Integration
```python
# Services use common patterns
class BaseService:
    def __init__(self, config):
        self.config = config
        self.logger = StructuredLogger(self.__class__.__name__)
        self.error_handler = ErrorHandler(self.logger)
    
    @retry(max_attempts=3, backoff="exponential")
    @timing(threshold_ms=1000)
    async def _nats_operation(self, operation_name: str, func: Callable):
        """Common NATS operation pattern."""
        async with self.logger.operation_context(operation_name):
            try:
                return await func()
            except Exception as e:
                await self.error_handler.handle_error(e, operation_name)
```

### Worker Integration
```python
# Worker uses common patterns
class Worker:
    @timing(logger=worker_logger, threshold_ms=5000)
    @log_errors(logger=worker_logger)
    async def process_job(self, job: Job) -> JobResult:
        async with timeout_context(job.timeout or 3600):
            return await self._execute_job(job)
```

## Files to Create

### New Files
- `src/naq/utils/__init__.py`
- `src/naq/utils/decorators.py`
- `src/naq/utils/context_managers.py`
- `src/naq/utils/async_helpers.py`
- `src/naq/utils/error_handling.py`
- `src/naq/utils/logging.py`
- `src/naq/utils/serialization.py`
- `src/naq/utils/validation.py`
- `src/naq/utils/timing.py`
- `src/naq/utils/nats_helpers.py`
- `src/naq/utils/types.py`

### Files to Refactor (replace patterns)
- All modules in `src/naq/queue/`
- All modules in `src/naq/worker/`
- All modules in `src/naq/services/`
- `src/naq/scheduler.py`
- `src/naq/events/` (all files)
- `src/naq/cli/` (all files)

## Success Criteria

### Code Quality Metrics
- [ ] Reduce code duplication by >60%
- [ ] Reduce average function length by >30%
- [ ] Improve code complexity scores
- [ ] Consistent error handling across all modules
- [ ] Consistent logging patterns across all modules

### Functional Requirements
- [ ] All existing functionality preserved
- [ ] No performance regressions
- [ ] Improved error handling and logging
- [ ] Consistent patterns across codebase
- [ ] Easier maintenance and debugging

### Testing Requirements
- [ ] All utilities have comprehensive tests
- [ ] All pattern replacements tested
- [ ] Performance benchmarks show no regression
- [ ] Error handling improvements validated

## Dependencies
- **Depends on**: Tasks 01-04 (Split packages) - need focused modules to extract patterns
- **Blocks**: None - can be done in parallel with other tasks

## Estimated Time
- **Pattern Analysis**: 8-10 hours  
- **Utility Implementation**: 15-20 hours
- **Codebase Integration**: 20-25 hours
- **Testing**: 10-12 hours
- **Total**: 53-67 hours