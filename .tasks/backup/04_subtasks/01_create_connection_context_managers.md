# Subtask 01: Create Connection Context Managers

## Overview
Create the core connection context managers that will replace the 44+ instances of repeated connection patterns throughout the codebase.

## Objectives
- Implement context managers for NATS connections
- Create JetStream context managers
- Develop combined connection managers
- Add KeyValue store context managers
- Ensure proper error handling and resource cleanup

## Implementation Details

### Files to Create
- `src/naq/connection/context_managers.py`

### Context Managers to Implement

#### 1. `nats_connection()` Context Manager
```python
@asynccontextmanager
async def nats_connection(config: Optional[Config] = None):
    """Context manager for NATS connections."""
    config = config or get_config()
    
    conn = await nats.connect(
        servers=config.nats.servers,
        name=config.nats.client_name,
        max_reconnect_attempts=config.nats.max_reconnect_attempts,
        reconnect_time_wait=config.nats.reconnect_time_wait,
        connection_timeout=config.nats.connection_timeout,
        drain_timeout=config.nats.drain_timeout,
        flush_timeout=config.nats.flush_timeout,
        ping_interval=config.nats.ping_interval,
        max_outstanding_pings=config.nats.max_outstanding_pings,
        # ... other config parameters
    )
    
    try:
        yield conn
    except Exception as e:
        logger.error(f"NATS connection error: {e}")
        raise
    finally:
        await conn.close()
```

#### 2. `jetstream_context()` Context Manager
```python
@asynccontextmanager  
async def jetstream_context(conn: nats.aio.client.Client):
    """Context manager for JetStream contexts."""
    try:
        js = conn.jetstream()
        yield js
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise
```

#### 3. `nats_jetstream()` Combined Context Manager
```python
@asynccontextmanager
async def nats_jetstream(config: Optional[Config] = None) -> Tuple[nats.aio.client.Client, JetStreamContext]:
    """Combined context manager for NATS connection and JetStream."""
    async with nats_connection(config) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js
```

#### 4. `nats_kv_store()` Context Manager
```python
@asynccontextmanager
async def nats_kv_store(bucket_name: str, config: Optional[Config] = None):
    """Context manager for NATS KeyValue operations.""" 
    async with nats_jetstream(config) as (conn, js):
        try:
            kv = await js.key_value(bucket_name)
            yield kv
        except Exception as e:
            logger.error(f"KV store error for bucket {bucket_name}: {e}")
            raise
```

### Requirements
- Use `@asynccontextmanager` decorator from `contextlib`
- Handle configuration properly with fallback to global config
- Implement comprehensive error handling with logging
- Ensure proper resource cleanup in all scenarios
- Support both individual and combined context managers
- Type hints for all parameters and return values

## Success Criteria
- [ ] All context managers implemented with proper error handling
- [ ] Resource cleanup guaranteed in all scenarios
- [ ] Configuration integration working correctly
- [ ] Type hints and documentation complete
- [ ] No memory leaks or resource issues
- [ ] All context managers tested with unit tests

## Dependencies
- Task 07 (YAML Configuration) for configuration schema
- Existing `src/naq/settings.py` for configuration management
- `nats` library and `nats.js` JetStream support

## Estimated Time
- 4-6 hours