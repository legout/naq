# Subtask 09: Update Connection Documentation

## Overview
Update and expand documentation for the new connection management system, including API documentation, migration guides, and best practices.

## Objectives
- Create comprehensive API documentation for new connection components
- Update existing documentation with new connection patterns
- Create migration guides for developers
- Document best practices for connection management
- Ensure documentation is complete and accurate

## Implementation Details

### Documentation Files to Create/Update

#### 1. API Documentation
**File: `docs/api/connection.md`**

```markdown
# Connection Management

The connection management system provides centralized, efficient handling of NATS connections throughout the NAQ library. It replaces the previous pattern of manual connection management with context managers, utilities, and decorators.

## Overview

The connection management system consists of:

- **Context Managers**: Automatic connection lifecycle management
- **Utilities**: Connection monitoring, testing, and performance tools
- **Decorators**: Automatic connection injection for functions
- **Configuration**: Centralized connection configuration

## Quick Start

### Basic Connection Usage

```python
import asyncio
from naq.connection import nats_connection, nats_jetstream, nats_kv_store

async def basic_example():
    # Simple NATS connection
    async with nats_connection() as conn:
        await conn.publish("subject", b"message")
    
    # Combined NATS and JetStream
    async with nats_jetstream() as (conn, js):
        await js.add_stream(name="mystream", subjects=["mystream.*"])
    
    # KeyValue store operations
    async with nats_kv_store("mybucket") as kv:
        await kv.put("key", "value")
        result = await kv.get("key")
```

### Using Decorators

```python
from naq.connection.decorators import with_nats_connection, with_jetstream_context

@with_nats_connection()
async def publish_message(conn, subject: str, message: bytes):
    await conn.publish(subject, message)

@with_jetstream_context()
async def create_stream(js: JetStreamContext, stream_name: str):
    await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])
```

## Context Managers

### `nats_connection(config=None)`

Context manager for NATS connections.

**Parameters:**
- `config` (Config, optional): Configuration object. If None, uses global config.

**Example:**
```python
async with nats_connection() as conn:
    await conn.publish("subject", b"message")
```

### `jetstream_context(conn)`

Context manager for JetStream contexts.

**Parameters:**
- `conn` (nats.aio.client.Client): NATS connection

**Example:**
```python
async with nats_connection() as conn:
    async with jetstream_context(conn) as js:
        await js.add_stream(name="mystream", subjects=["mystream.*"])
```

### `nats_jetstream(config=None)`

Combined context manager for NATS connection and JetStream.

**Parameters:**
- `config` (Config, optional): Configuration object. If None, uses global config.

**Returns:**
- Tuple of (connection, jetstream_context)

**Example:**
```python
async with nats_jetstream() as (conn, js):
    await js.add_stream(name="mystream", subjects=["mystream.*"])
```

### `nats_kv_store(bucket_name, config=None)`

Context manager for NATS KeyValue operations.

**Parameters:**
- `bucket_name` (str): Name of the KV bucket
- `config` (Config, optional): Configuration object. If None, uses global config.

**Example:**
```python
async with nats_kv_store("mybucket") as kv:
    await kv.put("key", "value")
    result = await kv.get("key")
```

## Utilities

### Connection Monitoring

```python
from naq.connection.utils import connection_monitor, get_connection_metrics

# Get current connection metrics
metrics = await get_connection_metrics()
print(f"Active connections: {metrics.active_connections}")
print(f"Total connections: {metrics.total_connections}")

# Reset metrics
await reset_connection_metrics()
```

### Connection Testing

```python
from naq.connection.utils import test_nats_connection, wait_for_nats_connection

# Test connection health
is_healthy = await test_nats_connection()

# Wait for connection to be available
await wait_for_nats_connection(timeout=30)
```

### Performance Measurement

```python
from naq.connection.utils import measure_connection_performance

# Measure connection performance
perf = await measure_connection_performance(iterations=10)
print(f"Average connection time: {perf['average_time']:.3f}s")
print(f"Success rate: {perf['success_rate']:.1%}")
```

## Decorators

### `@with_nats_connection(config_key=None)`

Decorator to inject NATS connection into function.

**Parameters:**
- `config_key` (str, optional): Config key to use instead of global config

**Example:**
```python
@with_nats_connection()
async def my_function(conn, arg1, arg2=None):
    await conn.publish("subject", f"{arg1}-{arg2}".encode())
```

### `@with_jetstream_context(config_key=None)`

Decorator to inject JetStream context into function.

**Parameters:**
- `config_key` (str, optional): Config key to use instead of global config

**Example:**
```python
@with_jetstream_context()
async def my_function(js, stream_name, subjects):
    await js.add_stream(name=stream_name, subjects=subjects)
```

### `@with_kv_store(bucket_name, config_key=None)`

Decorator to inject KeyValue store into function.

**Parameters:**
- `bucket_name` (str): Name of the KV bucket
- `config_key` (str, optional): Config key to use instead of global config

**Example:**
```python
@with_kv_store("status_bucket")
async def my_function(kv, key, value):
    await kv.put(key, value)
```

### `@with_connection_monitoring`

Decorator to add connection monitoring to function.

**Example:**
```python
@with_connection_monitoring
@with_nats_connection()
async def my_function(conn):
    # Function will be automatically monitored
    await conn.publish("subject", b"message")
```

### `@retry_on_connection_failure(max_retries=3, retry_delay=1.0, backoff_factor=2.0)`

Decorator to retry function on connection failures.

**Parameters:**
- `max_retries` (int): Maximum number of retry attempts
- `retry_delay` (float): Initial delay between retries
- `backoff_factor` (float): Multiplier for delay between retries

**Example:**
```python
@retry_on_connection_failure(max_retries=3)
@with_nats_connection()
async def my_function(conn):
    # Function will retry on connection failures
    await conn.publish("subject", b"message")
```

## Configuration

### Connection Configuration

Connection parameters are configured through the standard NAQ configuration system:

```yaml
nats:
  servers: 
    - "nats://localhost:4222"
  client_name: "naq-client"
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
  connection_timeout: 10.0
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

### Environment Variables

Connection parameters can also be configured via environment variables:

```bash
export NAQ_NATS_URL="nats://localhost:4222"
export NAQ_CLIENT_NAME="naq-client"
export NAQ_MAX_RECONNECT=5
export NAQ_RECONNECT_TIME_WAIT=2.0
```

## Best Practices

### 1. Use Context Managers

Always use context managers for connection management to ensure proper cleanup:

```python
# Good
async with nats_connection() as conn:
    await conn.publish("subject", b"message")

# Bad
conn = await get_nats_connection(url)
try:
    await conn.publish("subject", b"message")
finally:
    await close_nats_connection(conn)
```

### 2. Choose the Right Context Manager

Use the most specific context manager for your needs:

```python
# For simple NATS operations
async with nats_connection() as conn:
    await conn.publish("subject", b"message")

# For JetStream operations
async with nats_jetstream() as (conn, js):
    await js.add_stream(name="mystream", subjects=["mystream.*"])

# For KeyValue operations
async with nats_kv_store("mybucket") as kv:
    await kv.put("key", "value")
```

### 3. Use Decorators for Repeated Patterns

Use decorators for functions that frequently need connections:

```python
@with_jetstream_context()
async def create_stream(js: JetStreamContext, stream_name: str):
    await js.add_stream(name=stream_name, subjects=[f"{stream_name}.*"])

# Usage is simplified
await create_stream("mystream")
```

### 4. Monitor Connection Performance

Use connection monitoring to track performance:

```python
from naq.connection.utils import connection_monitor, get_connection_metrics

# Monitor your connections
metrics = await get_connection_metrics()
print(f"Connection performance: {metrics.average_connection_time:.3f}s")
```

### 5. Handle Connection Errors Properly

Always handle connection errors appropriately:

```python
try:
    async with nats_connection() as conn:
        await conn.publish("subject", b"message")
except nats.errors.ConnectionClosedError:
    # Handle connection closed
    logger.error("Connection to NATS server was closed")
except nats.errors.TimeoutError:
    # Handle timeout
    logger.error("Operation timed out")
except Exception as e:
    # Handle other errors
    logger.error(f"Unexpected error: {e}")
```

### 6. Use Connection Pooling Wisely

The connection system automatically manages connection pooling. Use context managers to ensure proper connection reuse:

```python
# Good - connection reuse within context
async with nats_jetstream() as (conn, js):
    await js.publish("stream1", b"message1")
    await js.publish("stream2", b"message2")

# Bad - multiple connections
async with nats_jetstream() as (conn1, js1):
    await js1.publish("stream1", b"message1")
async with nats_jetstream() as (conn2, js2):
    await js2.publish("stream2", b"message2")
```

## Migration Guide

### From Old Connection Patterns

#### Pattern 1: Simple Connection + Operation

**Before:**
```python
nc = await get_nats_connection(url)  
await nc.publish(subject, data)
await close_nats_connection(nc)
```

**After:**
```python
async with nats_connection() as conn:
    await conn.publish(subject, data)
```

#### Pattern 2: JetStream Operations

**Before:**
```python
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
await js.add_stream(config)
await close_nats_connection(nc)
```

**After:**
```python
async with nats_jetstream() as (conn, js):
    await js.add_stream(config)
```

#### Pattern 3: KV Store Operations

**Before:**
```python
nc = await get_nats_connection(url)
js = await get_jetstream_context(nc)
kv = await js.key_value(bucket)
await kv.put(key, value)
await close_nats_connection(nc)
```

**After:**
```python
async with nats_kv_store(bucket) as kv:
    await kv.put(key, value)
```

### Migration Steps

1. **Identify all connection usage** in your codebase
2. **Replace patterns** with appropriate context managers
3. **Update imports** to use new connection modules
4. **Test functionality** to ensure no regressions
5. **Update documentation** to reflect new patterns

## Troubleshooting

### Common Issues

#### Connection Timeouts

If you experience connection timeouts:

```python
# Check connection health
is_healthy = await test_nats_connection()
if not is_healthy:
    logger.error("NATS connection is not healthy")
    # Handle appropriately
```

#### Memory Issues

If you experience memory issues with many connections:

```python
# Monitor connection metrics
metrics = await get_connection_metrics()
if metrics.active_connections > 100:
    logger.warning("High number of active connections")
```

#### Performance Issues

If you experience performance issues:

```python
# Measure connection performance
perf = await measure_connection_performance(iterations=10)
if perf['average_time'] > 1.0:
    logger.warning("Slow connection performance detected")
```

## API Reference

### Context Managers

- `nats_connection(config=None)`: NATS connection context manager
- `jetstream_context(conn)`: JetStream context manager
- `nats_jetstream(config=None)`: Combined NATS and JetStream context manager
- `nats_kv_store(bucket_name, config=None)`: KeyValue store context manager

### Utilities

- `test_nats_connection(config=None)`: Test NATS connection health
- `wait_for_nats_connection(config=None, timeout=30)`: Wait for connection availability
- `measure_connection_performance(config=None, iterations=10)`: Measure connection performance
- `get_connection_metrics()`: Get connection metrics
- `reset_connection_metrics()`: Reset connection metrics

### Decorators

- `@with_nats_connection(config_key=None)`: Inject NATS connection
- `@with_jetstream_context(config_key=None)`: Inject JetStream context
- `@with_kv_store(bucket_name, config_key=None)`: Inject KeyValue store
- `@with_connection_monitoring`: Add connection monitoring
- `@retry_on_connection_failure(max_retries=3, retry_delay=1.0, backoff_factor=2.0)`: Retry on connection failures
```

#### 2. Migration Guide Documentation
**File: `docs/migration/connection_migration.md`**

```markdown
# Connection Management Migration Guide

This guide helps you migrate from the old connection management system to the new context manager-based approach.

## Overview

The new connection management system provides:

- **Automatic resource management** through context managers
- **Reduced code duplication** across the codebase
- **Improved error handling** and consistency
- **Better performance** through connection reuse
- **Enhanced monitoring** and metrics collection

## Before You Start

### Prerequisites

- Ensure you have the latest version of NAQ
- Backup your codebase before starting migration
- Run existing tests to establish a baseline

### Tools You'll Need

- Python 3.8+
- pytest for testing
- Your favorite IDE with search/replace capabilities

## Migration Process

### Step 1: Identify Connection Usage

First, identify all places in your codebase that use the old connection patterns:

```bash
# Find all old connection usage
grep -r "get_nats_connection\|get_jetstream_context" src/ --exclude-dir=__pycache__
grep -r "close_nats_connection" src/ --exclude-dir=__pycache__
```

### Step 2: Categorize Patterns

Categorize the identified patterns by type:

#### Pattern A: Simple Connection + Operation
```python
# Before
nc = await get_nats_connection(url)  
await nc.publish(subject, data)
await close_nats_connection(nc)

# After
async with nats_connection() as conn:
    await conn.publish(subject, data)
```

#### Pattern B: JetStream Operations
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

#### Pattern C: KV Store Operations
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

Migrate files in order of priority:

#### High Priority (Start Here)
1. **Queue operations** - Core functionality
2. **Worker job processing** - Critical path
3. **Event logging** - System-wide usage

#### Medium Priority
4. **Scheduler operations** - Important but less critical
5. **Status management** - Important for monitoring
6. **Result handling** - Important for job tracking

#### Low Priority
7. **CLI commands** - User-facing
8. **Utility functions** - Infrequent usage
9. **Dashboard operations** - Optional features

### Step 4: Update Configuration

Update your configuration to use the new connection parameters:

#### Before
```python
def __init__(self, nats_url: str):
    self.nats_url = nats_url
```

#### After
```python
def __init__(self, config: Optional[Config] = None):
    self.config = config or get_config()
```

### Step 5: Update Imports

Update your imports to use the new connection modules:

#### Before
```python
from naq.connection import get_nats_connection, get_jetstream_context, close_nats_connection
```

#### After
```python
from naq.connection.context_managers import nats_connection, nats_jetstream, nats_kv_store
from naq.connection.decorators import with_nats_connection, with_jetstream_context
```

## Migration Examples

### Example 1: Queue Operations

#### Before
```python
# src/naq/queue/core.py
class Queue:
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
    
    async def enqueue(self, job: Job) -> None:
        nc = await get_nats_connection(self.nats_url)
        try:
            js = await get_jetstream_context(nc)
            await js.publish(self.stream_name, job.serialize())
        finally:
            await close_nats_connection(nc)
    
    async def dequeue(self) -> Optional[Job]:
        nc = await get_nats_connection(self.nats_url)
        try:
            js = await get_jetstream_context(nc)
            subscriber = await js.pull_subscribe(self.subject, self.consumer_name)
            msg = await subscriber.next(timeout=1.0)
            if msg:
                return Job.deserialize(msg.data)
            return None
        finally:
            await close_nats_connection(nc)
```

#### After
```python
# src/naq/queue/core.py
class Queue:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or get_config()
    
    async def enqueue(self, job: Job) -> None:
        async with nats_jetstream(self.config) as (conn, js):
            await js.publish(self.stream_name, job.serialize())
    
    async def dequeue(self) -> Optional[Job]:
        async with nats_jetstream(self.config) as (conn, js):
            subscriber = await js.pull_subscribe(self.subject, self.consumer_name)
            msg = await subscriber.next(timeout=1.0)
            if msg:
                return Job.deserialize(msg.data)
            return None
```

### Example 2: Worker Operations

#### Before
```python
# src/naq/worker/core.py
class Worker:
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
    
    async def process_message(self, msg) -> None:
        nc = await get_nats_connection(self.nats_url)
        try:
            js = await get_jetstream_context(nc)
            # Process job
            await self._execute_job(msg.data)
            # Acknowledge message
            await msg.ack()
        except Exception as e:
            await msg.nak()
            raise
        finally:
            await close_nats_connection(nc)
```

#### After
```python
# src/naq/worker/core.py
class Worker:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or get_config()
    
    async def process_message(self, msg) -> None:
        async with nats_jetstream(self.config) as (conn, js):
            try:
                # Process job
                await self._execute_job(msg.data)
                # Acknowledge message
                await msg.ack()
            except Exception as e:
                await msg.nak()
                raise
```

### Example 3: Using Decorators

#### Before
```python
# src/naq/events/publisher.py
class EventPublisher:
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
    
    async def publish_event(self, event_type: str, data: dict) -> None:
        nc = await get_nats_connection(self.nats_url)
        try:
            await nc.publish(f"events.{event_type}", json.dumps(data).encode())
        finally:
            await close_nats_connection(nc)
```

#### After
```python
# src/naq/events/publisher.py
from naq.connection.decorators import with_nats_connection

class EventPublisher:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or get_config()
    
    @with_nats_connection()
    async def publish_event(self, conn, event_type: str, data: dict) -> None:
        await conn.publish(f"events.{event_type}", json.dumps(data).encode())
```

## Testing Your Migration

### Step 1: Run Existing Tests

After migrating each component, run the existing tests:

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run smoke tests
pytest tests/smoke/ -v
```

### Step 2: Create Migration Tests

Create specific tests to validate the migration:

```python
# tests/unit/test_migration.py
import pytest
from unittest.mock import AsyncMock, patch

class TestConnectionMigration:
    @pytest.mark.asyncio
    async def test_migration_preserves_functionality(self):
        """Test that migration preserves existing functionality."""
        # Test that old and new patterns produce the same results
        old_result = await self._old_pattern()
        new_result = await self._new_pattern()
        
        assert old_result == new_result
    
    async def _old_pattern(self):
        """Simulate old connection pattern."""
        # Implementation of old pattern
        pass
    
    async def _new_pattern(self):
        """Simulate new connection pattern."""
        # Implementation of new pattern
        pass
```

### Step 3: Performance Testing

Ensure that the migration doesn't degrade performance:

```python
# tests/performance/test_migration_performance.py
import pytest
import time

class TestMigrationPerformance:
    @pytest.mark.asyncio
    async def test_migration_performance(self):
        """Test that migration doesn't degrade performance."""
        # Measure old pattern performance
        old_time = await self._measure_old_pattern()
        
        # Measure new pattern performance
        new_time = await self._measure_new_pattern()
        
        # New pattern should not be significantly slower
        assert new_time <= old_time * 1.5  # Allow 50% overhead
```

## Common Issues and Solutions

### Issue 1: Import Errors

**Problem:**
```python
ImportError: No module named 'naq.connection.context_managers'
```

**Solution:**
Ensure you're importing from the correct modules:
```python
# Correct
from naq.connection.context_managers import nats_connection, nats_jetstream

# Incorrect
from naq.connection import nats_connection, nats_jetstream
```

### Issue 2: Configuration Errors

**Problem:**
```python
AttributeError: 'Config' object has no attribute 'nats'
```

**Solution:**
Ensure your configuration includes the NATS settings:
```yaml
nats:
  servers: ["nats://localhost:4222"]
  client_name: "naq-client"
  max_reconnect_attempts: 5
  reconnect_time_wait: 2.0
```

### Issue 3: Context Manager Errors

**Problem:**
```python
RuntimeError: cannot reuse already awaited coroutine
```

**Solution:**
Ensure you're using the context manager correctly:
```python
# Correct
async with nats_connection() as conn:
    await conn.publish("subject", b"message")

# Incorrect
conn = await nats_connection()
await conn.publish("subject", b"message")
```

### Issue 4: Performance Degradation

**Problem:**
The new connection patterns are slower than the old ones.

**Solution:**
1. Check for unnecessary connection creation
2. Use appropriate context managers for your use case
3. Monitor connection metrics for issues

## Validation Checklist

After completing your migration, check off these items:

### Functional Requirements
- [ ] All old connection patterns replaced
- [ ] No functional regressions in any operations
- [ ] Error handling preserved or improved
- [ ] Resource cleanup guaranteed
- [ ] Configuration integration working

### Performance Requirements
- [ ] Connection establishment time not increased
- [ ] Memory usage for connections not increased
- [ ] Connection reuse optimizations working
- [ ] No connection leaks under any circumstances

### Code Quality Requirements
- [ ] No duplicate connection management code
- [ ] Consistent patterns across all modules
- [ ] Clear context manager interfaces
- [ ] Comprehensive error handling
- [ ] Connection monitoring and metrics working

## Getting Help

If you encounter issues during migration:

1. **Check the documentation** at `docs/api/connection.md`
2. **Review the migration examples** in this guide
3. **Run the test suite** to identify specific failures
4. **Check the GitHub issues** for similar problems
5. **Create a new issue** with details about your problem

## Next Steps

After completing the migration:

1. **Update your CI/CD pipeline** to use the new connection patterns
2. **Update your documentation** to reflect the new approach
3. **Train your team** on the new connection management system
4. **Monitor connection metrics** in production
5. **Plan for future enhancements** to the connection system
```

#### 3. Best Practices Documentation
**File: `docs/best_practices/connection_management.md`**

```markdown
# Connection Management Best Practices

This document provides best practices for using the NAQ connection management system effectively.

## Overview

Proper connection management is crucial for the performance and reliability of your NAQ applications. This guide covers:

- **Connection lifecycle management**
- **Performance optimization**
- **Error handling patterns**
- **Resource management**
- **Monitoring and debugging**

## Core Principles

### 1. Always Use Context Managers

Context managers provide automatic resource management and should be your default choice:

```python
# Good - Automatic cleanup
async with nats_connection() as conn:
    await conn.publish("subject", b"message")

# Bad - Manual cleanup error-prone
conn = await get_nats_connection(url)
try:
    await conn.publish("subject", b"message")
finally:
    await close_nats_connection(conn)  # Easy to forget
```

### 2. Choose the Right Context Manager

Use the most specific context manager for your needs:

```python
# For simple NATS operations
async with nats_connection() as conn:
    await conn.publish("subject", b"message")

# For JetStream operations
async with nats_jetstream() as (conn, js):
    await js.add_stream(name="mystream", subjects=["mystream.*"])

# For KeyValue operations
async with nats_kv_store("mybucket") as kv:
    await kv.put("key", "value")
```

### 3. Leverage Connection Reuse

Context managers automatically handle connection reuse within their scope:

```python
# Good - Connection reuse within context
async with nats_jetstream() as (conn, js):
    await js.publish("stream1", b"message1")
    await js.publish("stream2", b"message2")
    await js.publish("stream3", b"message3")

# Bad - Multiple connections
async with nats_jetstream() as (conn1, js1):
    await js1.publish("stream1", b"message1")
async with nats_jetstream() as (conn2, js2):
    await js2.publish("stream2", b"message2")
```

## Performance Optimization

### 1. Minimize Connection Creation

Create connections only when needed and reuse them:

```python
# Good - Single connection for multiple operations
async with nats_connection() as conn:
    for i in range(100):
        await conn.publish(f"subject.{i}", f"message_{i}".encode())

# Bad - Multiple connections
for i in range(100):
    async with nats_connection() as conn:
        await conn.publish(f"subject.{i}", f"message_{i}".encode())
```

### 2. Use Appropriate Timeouts

Set appropriate timeouts for your operations:

```python
# Good - Reasonable timeouts
async with nats_connection() as conn:
    await conn.publish("subject", b"message", timeout=5.0)

# Bad - No timeout (can hang indefinitely)
await conn.publish("subject", b"message")
```

### 3. Batch Operations

When possible, batch operations to reduce connection overhead:

```python
# Good - Batch operations
async with nats_jetstream() as (conn, js):
    for i in range(10):
        await js.publish("batch_stream", f"message_{i}".encode())

# Bad - Individual operations
for i in range(10):
    async with nats_jetstream() as (conn, js):
        await js.publish("individual_stream", f"message_{i}".encode())
```

## Error Handling Patterns

### 1. Handle Connection Errors Gracefully

Always handle connection errors appropriately:

```python
# Good - Comprehensive error handling
try:
    async with nats_connection() as conn:
        await conn.publish("subject", b"message")
except nats.errors.ConnectionClosedError:
    logger.error("Connection to NATS server was closed")
    # Implement reconnection logic
except nats.errors.TimeoutError:
    logger.error("Operation timed out")
    # Implement retry logic
except nats.errors.APIError as e:
    logger.error(f"NATS API error: {e}")
    # Handle API-specific errors
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    # Handle other errors
```

### 2. Use Retry Logic for Transient Errors

Implement retry logic for transient connection errors:

```python
from naq.connection.decorators import retry_on_connection_failure

@retry_on_connection_failure(max_retries=3, retry_delay=1.0)
@with_nats_connection()
async def publish_with_retry(conn, subject: str, message: bytes):
    await conn.publish(subject, message)
```

### 3. Implement Circuit Breakers

For critical operations, implement circuit breakers:

```python
from naq.connection.utils import test_nats_connection

async def publish_with_circuit_breaker(subject: str, message: bytes):
    # Test connection health before publishing
    if not await test_nats_connection():
        logger.warning("NATS connection unhealthy, skipping publish")
        return
    
    try:
        async with nats_connection() as conn:
            await conn.publish(subject, message)
    except Exception as e:
        logger.error(f"Publish failed: {e}")
        raise
```

## Resource Management

### 1. Monitor Connection Metrics

Use connection monitoring to track resource usage:

```python
from naq.connection.utils import get_connection_metrics, connection_monitor

async def monitor_connections():
    metrics = await get_connection_metrics()
    logger.info(f"Connection metrics: {metrics}")
    
    # Alert on high connection counts
    if metrics.active_connections > 100:
        logger.warning("High number of active connections")
    
    # Alert on high failure rates
    if metrics.failed_connections > metrics.total_connections * 0.1:
        logger.warning("High connection failure rate")
```

### 2. Implement Connection Limits

Set limits on concurrent connections to prevent resource exhaustion:

```python
import asyncio
from naq.connection.utils import connection_monitor

class ConnectionLimiter:
    def __init__(self, max_connections: int = 50):
        self.max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
    
    async def __aenter__(self):
        await self._semaphore.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()

# Usage
async def limited_operation():
    async with ConnectionLimiter():
        async with nats_connection() as conn:
            await conn.publish("subject", b"message")
```

### 3. Clean Up Resources

Ensure proper cleanup of all resources:

```python
# Good - Proper cleanup
async def process_batch(items: List[Any]):
    async with nats_connection() as conn:
        for item in items:
            try:
                await process_item(conn, item)
            except Exception as e:
                logger.error(f"Failed to process item: {e}")
                continue

# Bad - Resource leaks
async def process_batch_bad(items: List[Any]):
    for item in items:
        conn = await get_nats_connection()  # Connection leak!
        try:
            await process_item(conn, item)
        except Exception as e:
            logger.error(f"Failed to process item: {e}")
            continue
        # Missing close_nats_connection(conn)!
```

## Configuration Management

### 1. Use Environment Variables for Configuration

Configure connections through environment variables:

```bash
export NAQ_NATS_URL="nats://localhost:4222"
export NAQ_CLIENT_NAME="my-service"
export NAQ_MAX_RECONNECT=5
export NAQ_RECONNECT_TIME_WAIT=2.0
```

### 2. Implement Configuration Validation

Validate configuration before using it:

```python
from naq.connection import get_config

def validate_config(config: Config) -> bool:
    """Validate NATS configuration."""
    if not config.nats.servers:
        logger.error("No NATS servers configured")
        return False
    
    if not config.nats.servers[0].startswith("nats://"):
        logger.error("Invalid NATS server URL format")
        return False
    
    return True

async def safe_operation():
    config = get_config()
    if not validate_config(config):
        raise ValueError("Invalid NATS configuration")
    
    async with nats_connection(config) as conn:
        await conn.publish("subject", b"message")
```

### 3. Use Configuration Profiles

Use different configuration profiles for different environments:

```python
# development.yaml
nats:
  servers: ["nats://localhost:4222"]
  client_name: "dev-client"

# production.yaml
nats:
  servers: ["nats://prod1:4222", "nats://prod2:4222"]
  client_name: "prod-client"
  max_reconnect_attempts: 10
  reconnect_time_wait: 5.0
```

## Monitoring and Debugging

### 1. Implement Connection Logging

Add comprehensive logging for connection operations:

```python
import logging
from naq.connection.context_managers import nats_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def logged_operation():
    logger.info("Starting connection operation")
    start_time = time.time()
    
    try:
        async with nats_connection() as conn:
            logger.info("Connection established successfully")
            await conn.publish("subject", b"message")
            logger.info("Message published successfully")
    except Exception as e:
        logger.error(f"Connection operation failed: {e}")
        raise
    finally:
        duration = time.time() - start_time
        logger.info(f"Connection operation completed in {duration:.3f}s")
```

### 2. Track Performance Metrics

Track performance metrics for connection operations:

```python
from naq.connection.utils import measure_connection_performance

async def performance_test():
    # Measure connection performance
    perf = await measure_connection_performance(iterations=10)
    
    logger.info(f"Connection performance metrics:")
    logger.info(f"  Average time: {perf['average_time']:.3f}s")
    logger.info(f"  Min time: {perf['min_time']:.3f}s")
    logger.info(f"  Max time: {perf['max_time']:.3f}s")
    logger.info(f"  Success rate: {perf['success_rate']:.1%}")
    
    # Alert on poor performance
    if perf['average_time'] > 1.0:
        logger.warning("Slow connection performance detected")
```

### 3. Debug Connection Issues

Use debugging techniques to troubleshoot connection issues:

```python
import traceback
from naq.connection.utils import test_nats_connection

async def debug_connection():
    # Test basic connectivity
    logger.info("Testing NATS connection...")
    is_healthy = await test_nats_connection()
    logger.info(f"Connection health: {is_healthy}")
    
    if not is_healthy:
        logger.error("Connection test failed")
        logger.error("Traceback:")
        logger.error(traceback.format_exc())
        return
    
    # Test specific operations
    try:
        async with nats_connection() as conn:
            logger.info("Testing publish operation...")
            await conn.publish("debug.subject", b"test message")
            logger.info("Publish operation successful")
    except Exception as e:
        logger.error(f"Publish operation failed: {e}")
        logger.error("Traceback:")
        logger.error(traceback.format_exc())
```

## Advanced Patterns

### 1. Connection Pooling

For high-throughput applications, implement connection pooling:

```python
import asyncio
from naq.connection.context_managers import nats_connection
from typing import List

class ConnectionPool:
    def __init__(self, size: int = 10):
        self.size = size
        self._pool: List[asyncio.Task] = []
        self._semaphore = asyncio.Semaphore(size)
    
    async def get_connection(self):
        await self._semaphore.acquire()
        if not self._pool:
            # Create new connection
            self._pool.append(asyncio.create_task(self._create_connection()))
        
        return await self._pool.pop()
    
    async def _create_connection(self):
        async with nats_connection() as conn:
            yield conn
    
    async def release_connection(self, conn):
        self._pool.append(conn)
        self._semaphore.release()

# Usage
pool = ConnectionPool(size=5)

async def pooled_operation():
    async with pool.get_connection() as conn:
        await conn.publish("subject", b"message")
```

### 2. Connection Health Checks

Implement regular health checks for connections:

```python
import asyncio
from naq.connection.utils import test_nats_connection

class ConnectionHealthChecker:
    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self._running = False
        self._task = None
    
    async def start(self):
        """Start the health checker."""
        self._running = True
        self._task = asyncio.create_task(self._health_check_loop())
    
    async def stop(self):
        """Stop the health checker."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _health_check_loop(self):
        """Health check loop."""
        while self._running:
            try:
                is_healthy = await test_nats_connection()
                logger.info(f"Connection health: {is_healthy}")
                
                if not is_healthy:
                    logger.warning("Connection health check failed")
                    # Implement recovery logic
                    
            except Exception as e:
                logger.error(f"Health check failed: {e}")
            
            await asyncio.sleep(self.check_interval)

# Usage
health_checker = ConnectionHealthChecker()
await health_checker.start()
# ... later
await health_checker.stop()
```

### 3. Circuit Breaker Pattern

Implement circuit breaker pattern for resilience:

```python
from naq.connection.utils import test_nats_connection
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = CircuitState.CLOSED
    
    async def call(self, func, *args, **kwargs):
        """Call the wrapped function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self):
        """Check if we should attempt to reset the circuit breaker."""
        return (time.time() - self.last_failure_time) > self.recovery_timeout
    
    def _on_success(self):
        """Handle successful call."""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

# Usage
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

async def protected_operation():
    return await circuit_breaker.call(
        risky_operation,
        "arg1", "arg2",
        keyword_arg="value"
    )
```

## Common Anti-Patterns

### 1. Avoid Manual Connection Management

```python
# Bad - Manual connection management
conn = await get_nats_connection(url)
try:
    await conn.publish("subject", b"message")
finally:
    await close_nats_connection(conn)  # Easy to forget

# Good - Context manager
async with nats_connection() as conn:
    await conn.publish("subject", b"message")
```

### 2. Avoid Creating Connections in Loops

```python
# Bad - Connection in loop
for i in range(100):
    async with nats_connection() as conn:
        await conn.publish(f"subject.{i}", f"message_{i}".encode())

# Good - Single connection
async with nats_connection() as conn:
    for i in range(100):
        await conn.publish(f"subject.{i}", f"message_{i}".encode())
```

### 3. Avoid Ignoring Connection Errors

```python
# Bad - Ignoring errors
async with nats_connection() as conn:
    await conn.publish("subject", b"message")  # What if this fails?

# Good - Handle errors
try:
    async with nats_connection() as conn:
        await conn.publish("subject", b"message")
except nats.errors.ConnectionClosedError:
    logger.error("Connection lost")
    # Implement recovery
```

## Summary

Key takeaways for effective connection management:

1. **Always use context managers** for automatic resource management
2. **Choose the right context manager** for your specific use case
3. **Leverage connection reuse** to improve performance
4. **Implement proper error handling** for all connection operations
5. **Monitor connection metrics** to identify issues early
6. **Follow configuration best practices** for different environments
7. **Use advanced patterns** like connection pooling and circuit breakers for high-throughput applications
8. **Avoid common anti-patterns** that lead to resource leaks and poor performance

By following these best practices, you'll ensure that your NAQ applications are reliable, performant, and easy to maintain.
```

### Requirements
- All new connection components documented with comprehensive API documentation
- Migration guide created with step-by-step instructions
- Best practices document covering all aspects of connection management
- Documentation updated to reflect new connection patterns
- Examples provided for all major use cases
- Troubleshooting section included
- Validation checklist for migration completion

## Success Criteria
- [ ] API documentation complete and accurate
- [ ] Migration guide comprehensive and easy to follow
- [ ] Best practices document covers all important topics
- [ ] All documentation accessible and well-organized
- [ ] Examples provided for all major use cases
- [ ] Troubleshooting information included
- [ ] Documentation meets team needs

## Dependencies
- Task 01-08 (All previous connection management tasks)
- Documentation tools (Sphinx, MkDocs, or similar)
- API documentation generation tools

## Estimated Time
- 6-8 hours