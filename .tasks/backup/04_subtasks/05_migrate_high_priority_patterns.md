# Subtask 05: Migrate High-Priority Connection Patterns

## Overview
Migrate the highest priority connection patterns to use the new context managers, focusing on queue operations, worker job processing, and event logging.

## Objectives
- Migrate queue module connection patterns
- Migrate worker core connection patterns  
- Migrate event system connection patterns
- Ensure no functional regressions
- Maintain performance characteristics

## Implementation Details

### Migration Priority Order

#### Phase 1: Queue Operations (Highest Priority)
**Files to migrate:**
- `src/naq/queue/core.py`
- `src/naq/queue/scheduled.py`
- `src/naq/queue/batch.py`

#### Phase 2: Worker Operations
**Files to migrate:**
- `src/naq/worker/core.py`
- `src/naq/worker/status.py`
- `src/naq/worker/failed.py`

#### Phase 3: Event System
**Files to migrate:**
- `src/naq/events/publisher.py`
- `src/naq/events/subscriber.py`
- `src/naq/events/manager.py`

### Migration Examples

#### Queue Core Migration
**Before (`src/naq/queue/core.py`):**
```python
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

**After:**
```python
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

#### Worker Core Migration
**Before (`src/naq/worker/core.py`):**
```python
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

async def update_job_status(self, job_id: str, status: str) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("job_status")
        await kv.put(job_id, status)
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
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

async def update_job_status(self, job_id: str, status: str) -> None:
    async with nats_kv_store("job_status", self.config) as kv:
        await kv.put(job_id, status)
```

#### Event System Migration
**Before (`src/naq/events/publisher.py`):**
```python
async def publish_event(self, event_type: str, data: dict) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        await nc.publish(f"events.{event_type}", json.dumps(data).encode())
    finally:
        await close_nats_connection(nc)

async def publish_system_event(self, event_type: str, data: dict) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        await js.publish("system_events", json.dumps(data).encode())
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
async def publish_event(self, event_type: str, data: dict) -> None:
    async with nats_connection(self.config) as conn:
        await conn.publish(f"events.{event_type}", json.dumps(data).encode())

async def publish_system_event(self, event_type: str, data: dict) -> None:
    async with nats_jetstream(self.config) as (conn, js):
        await js.publish("system_events", json.dumps(data).encode())
```

### Migration Process

#### Step 1: Backup and Prepare
```bash
# Create backup of files to be migrated
cp src/naq/queue/core.py src/naq/queue/core.py.backup
cp src/naq/worker/core.py src/naq/worker/core.py.backup
cp src/naq/events/publisher.py src/naq/events/publisher.py.backup

# Run tests to establish baseline
pytest tests/unit/test_queue_core.py -v
pytest tests/unit/test_worker_core.py -v
pytest tests/unit/test_events.py -v
```

#### Step 2: Systematic Migration
For each file:
1. Identify all connection patterns
2. Replace with appropriate context manager
3. Update imports if needed
4. Test functionality
5. Verify error handling

#### Step 3: Validation
```bash
# Run tests after migration
pytest tests/unit/test_queue_core.py -v
pytest tests/unit/test_worker_core.py -v
pytest tests/unit/test_events.py -v

# Run integration tests
pytest tests/integration/test_integration_worker.py -v
pytest tests/integration/test_integration_job.py -v
```

### Error Handling Considerations

#### Connection Error Handling
**Before:**
```python
try:
    nc = await get_nats_connection(url)
    # operations
except nats.errors.ConnectionClosedError:
    # handle connection error
finally:
    await close_nats_connection(nc)
```

**After:**
```python
try:
    async with nats_connection(config) as conn:
        # operations
except nats.errors.ConnectionClosedError:
    # handle connection error
# No need for finally block - context manager handles cleanup
```

#### JetStream Error Handling
**Before:**
```python
try:
    nc = await get_nats_connection(url)
    js = await get_jetstream_context(nc)
    await js.add_stream(config)
except nats.errors.StreamNotFound:
    # handle stream not found
except nats.errors.APIError:
    # handle API error
finally:
    await close_nats_connection(nc)
```

**After:**
```python
try:
    async with nats_jetstream(config) as (conn, js):
        await js.add_stream(config)
except nats.errors.StreamNotFound:
    # handle stream not found
except nats.errors.APIError:
    # handle API error
# Context manager handles cleanup automatically
```

### Configuration Integration

#### Update Configuration Usage
**Before:**
```python
def __init__(self, nats_url: str):
    self.nats_url = nats_url
```

**After:**
```python
def __init__(self, config: Optional[Config] = None):
    self.config = config or get_config()
```

#### Update Method Signatures
**Before:**
```python
async def process_job(self, job_data: bytes) -> None:
    nc = await get_nats_connection(self.nats_url)
    # ...
```

**After:**
```python
async def process_job(self, job_data: bytes) -> None:
    async with nats_jetstream(self.config) as (conn, js):
        # ...
```

### Requirements
- All high-priority files migrated to context managers
- No functional regressions in core functionality
- Proper error handling maintained
- Configuration integration working
- Performance characteristics preserved
- All existing tests passing
- New context manager tests added

## Success Criteria
- [ ] Queue operations fully migrated
- [ ] Worker operations fully migrated
- [ ] Event system fully migrated
- [ ] All existing tests pass
- [ ] No performance regressions
- [ ] Error handling preserved
- [ ] Configuration integration working

## Dependencies
- Task 01 (Connection Context Managers) - implemented context managers
- Task 04 (Identify Connection Patterns) - pattern analysis
- Existing test suite for validation

## Estimated Time
- 8-10 hours