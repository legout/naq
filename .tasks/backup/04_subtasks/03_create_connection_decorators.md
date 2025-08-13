# Subtask 03: Create Connection Decorators

## Overview
Create decorators to simplify connection management in functions and provide automatic connection injection.

## Objectives
- Implement decorators for automatic connection management
- Create decorators for JetStream context injection
- Develop utility decorators for common patterns
- Ensure decorators work with existing function signatures
- Provide clean, intuitive decorator interfaces

## Implementation Details

### Files to Create
- `src/naq/connection/decorators.py`

### Decorators to Implement

#### 1. `with_nats_connection` Decorator
```python
def with_nats_connection(config_key: Optional[str] = None):
    """Decorator to inject NATS connection into function.
    
    Args:
        config_key: Optional config key to use instead of global config
        
    Usage:
        @with_nats_connection()
        async def my_function(conn: nats.aio.client.Client, arg1: str):
            await conn.publish("subject", data)
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            config = get_config()
            if config_key:
                config = getattr(config, config_key)
            
            async with nats_connection(config) as conn:
                return await func(conn, *args, **kwargs)
        return wrapper
    return decorator
```

#### 2. `with_jetstream_context` Decorator
```python
def with_jetstream_context(config_key: Optional[str] = None):
    """Decorator to inject JetStream context into function.
    
    Args:
        config_key: Optional config key to use instead of global config
        
    Usage:
        @with_jetstream_context()
        async def my_function(js: JetStreamContext, stream_name: str):
            await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            config = get_config()
            if config_key:
                config = getattr(config, config_key)
            
            async with nats_jetstream(config) as (conn, js):
                return await func(js, *args, **kwargs)
        return wrapper
    return decorator
```

#### 3. `with_kv_store` Decorator
```python
def with_kv_store(bucket_name: str, config_key: Optional[str] = None):
    """Decorator to inject KeyValue store into function.
    
    Args:
        bucket_name: Name of the KV bucket to use
        config_key: Optional config key to use instead of global config
        
    Usage:
        @with_kv_store("my_bucket")
        async def my_function(kv: KeyValueStore, key: str, value: str):
            await kv.put(key, value)
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            config = get_config()
            if config_key:
                config = getattr(config, config_key)
            
            async with nats_kv_store(bucket_name, config) as kv:
                return await func(kv, *args, **kwargs)
        return wrapper
    return decorator
```

#### 4. `with_connection_monitoring` Decorator
```python
def with_connection_monitoring(func: Callable) -> Callable:
    """Decorator to add connection monitoring to function.
    
    Usage:
        @with_connection_monitoring
        @with_nats_connection()
        async def my_function(conn: nats.aio.client.Client):
            # Function will be automatically monitored
            pass
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = asyncio.get_event_loop().time()
        await connection_monitor.record_connection_start()
        
        try:
            result = await func(*args, **kwargs)
            duration = asyncio.get_event_loop().time() - start_time
            await connection_monitor.record_connection_end(duration)
            return result
        except Exception as e:
            await connection_monitor.record_connection_failure()
            raise
    return wrapper
```

#### 5. `retry_on_connection_failure` Decorator
```python
def retry_on_connection_failure(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0
):
    """Decorator to retry function on connection failures.
    
    Args:
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries
        backoff_factor: Multiplier for delay between retries
        
    Usage:
        @retry_on_connection_failure(max_retries=3)
        @with_nats_connection()
        async def my_function(conn: nats.aio.client.Client):
            # Function will retry on connection failures
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = retry_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (nats.errors.ConnectionClosedError, nats.errors.TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {current_delay}s: {e}")
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        logger.error(f"All {max_retries + 1} connection attempts failed")
            
            raise last_exception
        return wrapper
    return decorator
```

#### 6. Combined Decorator Examples
```python
# Example: Stream management with automatic connection and monitoring
@with_connection_monitoring
@with_jetstream_context()
async def create_stream(js: JetStreamContext, stream_name: str, subjects: List[str]):
    """Create stream with automatic connection management and monitoring."""
    await js.add_stream(name=stream_name, subjects=subjects)

# Example: Message publishing with retry logic
@retry_on_connection_failure(max_retries=3)
@with_nats_connection()
async def publish_message(conn: nats.aio.client.Client, subject: str, message: bytes):
    """Publish message with automatic connection management and retry logic."""
    await conn.publish(subject, message)

# Example: KV operations with specific bucket
@with_kv_store("worker_status")
async def update_worker_status(kv: KeyValueStore, worker_id: str, status: str):
    """Update worker status with automatic KV store connection."""
    await kv.put(worker_id, status)
```

### Requirements
- All decorators must preserve function metadata using `functools.wraps`
- Support both positional and keyword arguments
- Handle configuration properly with optional config keys
- Provide clear error messages and logging
- Support decorator composition (multiple decorators on same function)
- Type hints for all decorator parameters and return types
- Proper async/await handling

## Success Criteria
- [ ] All decorators implemented with proper error handling
- [ ] Decorator composition working correctly
- [ ] Function metadata preserved with `@wraps`
- [ ] Configuration integration working
- [ ] All decorators tested with unit tests
- [ ] Clear documentation and examples provided
- [ ] No breaking changes to existing function signatures

## Dependencies
- Task 01 (Connection Context Managers) for core connection functions
- Task 02 (Connection Utilities) for monitoring
- `functools` for decorator utilities
- `asyncio` for retry logic

## Estimated Time
- 3-4 hours