# Task 06: Optimize NATS Connection Management

## Overview
Optimize and centralize NATS connection management throughout the codebase to eliminate the 44+ instances of repeated connection patterns, improve performance, and ensure consistent resource management.

## Current State Analysis
- 44+ instances of `get_nats_connection()` and `get_jetstream_context()` calls
- Inconsistent connection lifecycle management
- No connection pooling optimization
- Mixed connection configuration patterns
- Resource leaks potential from improper connection cleanup
- Performance overhead from repeated connection establishment

## Problems Identified

### 1. Code Duplication Pattern
```python
# Repeated pattern across 44+ locations:
nc = await get_nats_connection(nats_url)
try:
    js = await get_jetstream_context(nc)
    # ... operations
finally:
    await close_nats_connection(nc)
```

### 2. Inconsistent Error Handling
- Different error handling patterns in different modules
- Some places don't properly handle connection failures
- Inconsistent retry logic

### 3. Performance Issues
- New connections created for short-lived operations
- No connection reuse within same context
- Thread-local connections not optimally used

### 4. Resource Management Issues
- Potential connection leaks
- Inconsistent connection cleanup
- No centralized connection monitoring

## Target Solution

### 1. Connection Context Managers
Replace all connection patterns with context managers:

```python
# New pattern using context managers
async with nats_connection(config) as conn:
    async with jetstream_context(conn) as js:
        # Operations...
        
# Or combined:
async with nats_jetstream(config) as (conn, js):
    # Operations...
```

### 2. Connection Service Integration
Integrate with service layer for advanced patterns:

```python
async with ConnectionService(config) as conn_service:
    # Automatic connection pooling and management
    conn = await conn_service.get_connection()
    js = await conn_service.get_jetstream()
```

### 3. Configuration-Driven Connections
All connections use centralized configuration:

```python
# Configuration-driven connection parameters
@asynccontextmanager
async def nats_connection(config: Config = None):
    config = config or get_config()
    conn = await nats.connect(
        servers=config.nats.servers,
        max_reconnect_attempts=config.nats.max_reconnect_attempts,
        reconnect_time_wait=config.nats.reconnect_time_wait,
        # ... other config-driven parameters
    )
    try:
        yield conn
    finally:
        await conn.close()
```

## Implementation Strategy

### Phase 1: Create Connection Context Managers

**File: `src/naq/connection/context_managers.py`**

```python
from contextlib import asynccontextmanager
from typing import Tuple, Optional
import nats
from nats.js import JetStreamContext

@asynccontextmanager
async def nats_connection(config: Optional[Config] = None):
    """Context manager for NATS connections."""
    config = config or get_config()
    
    conn = await nats.connect(
        servers=config.nats.servers,
        name=config.nats.client_name,
        max_reconnect_attempts=config.nats.max_reconnect_attempts,
        reconnect_time_wait=config.nats.reconnect_time_wait,
        # ... other config parameters
    )
    
    try:
        yield conn
    except Exception as e:
        logger.error(f"NATS connection error: {e}")
        raise
    finally:
        await conn.close()

@asynccontextmanager  
async def jetstream_context(conn: nats.aio.client.Client):
    """Context manager for JetStream contexts."""
    try:
        js = conn.jetstream()
        yield js
    except Exception as e:
        logger.error(f"JetStream context error: {e}")
        raise

@asynccontextmanager
async def nats_jetstream(config: Optional[Config] = None) -> Tuple[nats.aio.client.Client, JetStreamContext]:
    """Combined context manager for NATS connection and JetStream."""
    async with nats_connection(config) as conn:
        async with jetstream_context(conn) as js:
            yield conn, js

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

### Phase 2: Create Connection Utilities

**File: `src/naq/connection/utils.py`**

```python
from typing import Dict, List
import asyncio
from dataclasses import dataclass

@dataclass
class ConnectionMetrics:
    """Connection usage metrics."""
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    average_connection_time: float = 0.0

class ConnectionMonitor:
    """Monitor connection usage and performance."""
    
    def __init__(self):
        self.metrics = ConnectionMetrics()
        self._connection_times: List[float] = []
    
    def record_connection_start(self):
        """Record connection start."""
        self.metrics.total_connections += 1
        self.metrics.active_connections += 1
    
    def record_connection_end(self, duration: float):
        """Record connection end."""
        self.metrics.active_connections -= 1
        self._connection_times.append(duration)
        self.metrics.average_connection_time = sum(self._connection_times) / len(self._connection_times)
    
    def record_connection_failure(self):
        """Record connection failure."""
        self.metrics.failed_connections += 1

# Global connection monitor
connection_monitor = ConnectionMonitor()

async def test_nats_connection(config: Optional[Config] = None) -> bool:
    """Test NATS connection health."""
    try:
        async with nats_connection(config) as conn:
            # Simple ping test
            await conn.flush(timeout=5.0)
            return True
    except Exception as e:
        logger.error(f"NATS connection test failed: {e}")
        return False

async def wait_for_nats_connection(config: Optional[Config] = None, timeout: int = 30) -> bool:
    """Wait for NATS connection to be available."""
    start_time = asyncio.get_event_loop().time()
    
    while (asyncio.get_event_loop().time() - start_time) < timeout:
        if await test_nats_connection(config):
            return True
        await asyncio.sleep(1.0)
    
    return False
```

### Phase 3: Connection Decorators

**File: `src/naq/connection/decorators.py`**

```python
from functools import wraps
from typing import Callable, Optional

def with_nats_connection(config_key: Optional[str] = None):
    """Decorator to inject NATS connection into function."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            config = get_config()
            async with nats_connection(config) as conn:
                return await func(conn, *args, **kwargs)
        return wrapper
    return decorator

def with_jetstream_context(config_key: Optional[str] = None):
    """Decorator to inject JetStream context into function."""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            config = get_config()
            async with nats_jetstream(config) as (conn, js):
                return await func(js, *args, **kwargs)
        return wrapper
    return decorator

# Usage examples:
@with_jetstream_context()
async def create_stream(js: JetStreamContext, stream_name: str):
    """Create stream with automatic connection management."""
    # Function automatically gets JetStream context
    await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])

@with_nats_connection()  
async def publish_message(conn: nats.aio.client.Client, subject: str, message: bytes):
    """Publish message with automatic connection management."""
    await conn.publish(subject, message)
```

## Migration Plan

### Step 1: Identify All Connection Usage
Create comprehensive list of all files and functions using NATS connections:

```bash
# Find all NATS connection usage
grep -r "get_nats_connection\|get_jetstream_context" src/naq --exclude-dir=__pycache__ > connection_usage.txt

# Analyze patterns
grep -r "await.*connect\|\.jetstream\(\)" src/naq --exclude-dir=__pycache__ >> connection_usage.txt
```

### Step 2: Categorize Connection Patterns

**Pattern A: Simple Connection + Operation**
```python
# Before
nc = await get_nats_connection(url)  
await nc.publish(subject, data)
await close_nats_connection(nc)

# After  
async with nats_connection() as conn:
    await conn.publish(subject, data)
```

**Pattern B: JetStream Operations**
```python
# Before
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
await js.add_stream(config)
await close_nats_connection(nc)

# After
async with nats_jetstream() as (conn, js):
    await js.add_stream(config)
```

**Pattern C: KV Store Operations**
```python
# Before
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
kv = await js.key_value(bucket)
await kv.put(key, value)
await close_nats_connection(nc)

# After
async with nats_kv_store(bucket) as kv:
    await kv.put(key, value)
```

### Step 3: Systematic Migration

**Migration Priority:**
1. **High-frequency patterns** (Queue operations, Worker job processing)
2. **Error-prone patterns** (Complex connection handling)
3. **Performance-critical patterns** (Event logging, bulk operations)
4. **Simple patterns** (CLI commands, utilities)

**Migration Process per File:**
1. Identify connection patterns in file
2. Replace with appropriate context manager
3. Test functionality unchanged
4. Update error handling if needed
5. Remove unused imports

### Step 4: Update Core Modules

**Queue Module Updates:**
```python
# In queue/core.py
async def enqueue(self, job: Job) -> None:
    async with nats_jetstream(self.config) as (conn, js):
        # Enqueue operations...

# In queue/scheduled.py  
async def store_scheduled_job(self, job_id: str, schedule: Schedule) -> None:
    async with nats_kv_store("scheduled_jobs", self.config) as kv:
        # Store operations...
```

**Worker Module Updates:**
```python
# In worker/core.py
async def process_message(self, msg) -> None:
    async with nats_jetstream(self.config) as (conn, js):
        # Message processing...

# In worker/status.py
async def update_status(self, status: WORKER_STATUS) -> None:
    async with nats_kv_store("worker_status", self.config) as kv:
        # Status update...
```

## Configuration Integration

### Connection Configuration Schema
```yaml
nats:
  servers: 
    - "nats://localhost:4222"
  client_name: "naq-client"
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
  connection_timeout: 10.0
  
  # Advanced connection settings
  drain_timeout: 30.0
  flush_timeout: 5.0
  ping_interval: 120.0
  max_outstanding_pings: 2
  
  # TLS settings (if needed)
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
```

### Environment Variable Support
```python
# Support existing environment variables
nats:
  servers: ["${NAQ_NATS_URL:nats://localhost:4222}"]
  client_name: "${NAQ_CLIENT_NAME:naq-client}"
  max_reconnect_attempts: ${NAQ_MAX_RECONNECT:5}
```

## Implementation Files

### New Files
- `src/naq/connection/context_managers.py`
- `src/naq/connection/utils.py` 
- `src/naq/connection/decorators.py`
- `src/naq/connection/__init__.py`

### Files to Refactor (replace connection patterns)
- `src/naq/queue/` (all files)
- `src/naq/worker/` (all files)
- `src/naq/scheduler.py`
- `src/naq/results.py`
- `src/naq/events/` (all files)
- `src/naq/cli/` (command files)

### Files to Update  
- `src/naq/connection.py` - Integrate with new patterns
- `src/naq/settings.py` - Add connection configuration

## Success Criteria

### Functional Requirements
- [ ] All 44+ connection patterns replaced with context managers
- [ ] No functional regressions in any NAQ operations
- [ ] Consistent error handling across all connection usage
- [ ] Proper resource cleanup guaranteed by context managers
- [ ] Configuration-driven connection parameters

### Performance Requirements
- [ ] Connection establishment time not increased
- [ ] Memory usage for connections not increased  
- [ ] Connection reuse optimizations where beneficial
- [ ] No connection leaks under any circumstances

### Code Quality Requirements
- [ ] No duplicate connection management code
- [ ] Consistent patterns across all modules
- [ ] Clear context manager interfaces
- [ ] Comprehensive error handling
- [ ] Connection monitoring and metrics

## Testing Strategy

### Unit Tests
- Context manager lifecycle (enter, exit, error conditions)
- Connection failure handling
- Configuration parameter usage
- Resource cleanup verification

### Integration Tests  
- End-to-end operations using new connection patterns
- Connection sharing between operations
- Error propagation through context managers
- Performance comparison before/after

### Load Tests
- High-frequency connection usage patterns
- Connection pool behavior under load
- Resource usage monitoring
- Connection failure recovery

## Migration Validation

### Pre-Migration Checklist
- [ ] Identify all 44+ connection usage locations
- [ ] Create test cases for each connection pattern
- [ ] Document current error handling behavior
- [ ] Baseline performance measurements

### Post-Migration Checklist  
- [ ] All connection patterns successfully migrated
- [ ] All tests pass without modification
- [ ] No performance regressions
- [ ] No memory leaks detected
- [ ] Error handling behavior unchanged
- [ ] Connection monitoring shows healthy patterns

## Dependencies
- **Depends on**: Task 07 (YAML Configuration) - for configuration-driven connections
- **Integrates with**: Task 05 (Service Layer) - connection service uses these patterns
- **Blocks**: Performance-critical tasks depend on efficient connections

## Estimated Time
- **Pattern Analysis**: 4-6 hours
- **Context Manager Implementation**: 8-10 hours
- **Migration Execution**: 12-15 hours
- **Testing and Validation**: 6-8 hours
- **Total**: 30-39 hours