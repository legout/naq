# Subtask 06: Migrate Medium-Priority Connection Patterns

## Overview
Migrate medium-priority connection patterns to use the new context managers, focusing on scheduler operations, status management, and result handling.

## Objectives
- Migrate scheduler connection patterns
- Migrate status management connection patterns
- Migrate result handling connection patterns
- Ensure compatibility with existing systems
- Maintain performance and reliability

## Implementation Details

### Migration Priority Order

#### Phase 1: Scheduler Operations
**Files to migrate:**
- `src/naq/scheduler.py`
- `src/naq/scheduler/` (if exists)

#### Phase 2: Status Management
**Files to migrate:**
- `src/naq/worker/status.py`
- `src/naq/queue/status.py` (if exists)

#### Phase 3: Result Handling
**Files to migrate:**
- `src/naq/results.py`
- `src/naq/results/` (if exists)

### Migration Examples

#### Scheduler Migration
**Before (`src/naq/scheduler.py`):**
```python
async def store_scheduled_job(self, job_id: str, schedule: Schedule) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("scheduled_jobs")
        await kv.put(job_id, schedule.serialize())
    finally:
        await close_nats_connection(nc)

async def get_scheduled_jobs(self) -> List[Schedule]:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("scheduled_jobs")
        entries = await kv.entries()
        return [Schedule.deserialize(entry.value) for entry in entries.values()]
    finally:
        await close_nats_connection(nc)

async def remove_scheduled_job(self, job_id: str) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("scheduled_jobs")
        await kv.delete(job_id)
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
async def store_scheduled_job(self, job_id: str, schedule: Schedule) -> None:
    async with nats_kv_store("scheduled_jobs", self.config) as kv:
        await kv.put(job_id, schedule.serialize())

async def get_scheduled_jobs(self) -> List[Schedule]:
    async with nats_kv_store("scheduled_jobs", self.config) as kv:
        entries = await kv.entries()
        return [Schedule.deserialize(entry.value) for entry in entries.values()]

async def remove_scheduled_job(self, job_id: str) -> None:
    async with nats_kv_store("scheduled_jobs", self.config) as kv:
        await kv.delete(job_id)
```

#### Status Management Migration
**Before (`src/naq/worker/status.py`):**
```python
async def update_worker_status(self, worker_id: str, status: WORKER_STATUS) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("worker_status")
        await kv.put(worker_id, status.value)
    finally:
        await close_nats_connection(nc)

async def get_worker_status(self, worker_id: str) -> Optional[WORKER_STATUS]:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("worker_status")
        entry = await kv.get(worker_id)
        if entry:
            return WORKER_STATUS(entry.value.decode())
        return None
    finally:
        await close_nats_connection(nc)

async def get_all_workers_status(self) -> Dict[str, WORKER_STATUS]:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("worker_status")
        entries = await kv.entries()
        return {
            key: WORKER_STATUS(entry.value.decode())
            for key, entry in entries.items()
        }
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
async def update_worker_status(self, worker_id: str, status: WORKER_STATUS) -> None:
    async with nats_kv_store("worker_status", self.config) as kv:
        await kv.put(worker_id, status.value)

async def get_worker_status(self, worker_id: str) -> Optional[WORKER_STATUS]:
    async with nats_kv_store("worker_status", self.config) as kv:
        entry = await kv.get(worker_id)
        if entry:
            return WORKER_STATUS(entry.value.decode())
        return None

async def get_all_workers_status(self) -> Dict[str, WORKER_STATUS]:
    async with nats_kv_store("worker_status", self.config) as kv:
        entries = await kv.entries()
        return {
            key: WORKER_STATUS(entry.value.decode())
            for key, entry in entries.items()
        }
```

#### Result Handling Migration
**Before (`src/naq/results.py`):**
```python
async def store_result(self, job_id: str, result: Any) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("job_results")
        await kv.put(job_id, cloudpickle.dumps(result))
    finally:
        await close_nats_connection(nc)

async def get_result(self, job_id: str) -> Optional[Any]:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("job_results")
        entry = await kv.get(job_id)
        if entry:
            return cloudpickle.loads(entry.value)
        return None
    finally:
        await close_nats_connection(nc)

async def cleanup_old_results(self, max_age_hours: int = 24) -> int:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("job_results")
        # Implementation for cleanup
        return cleaned_count
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
async def store_result(self, job_id: str, result: Any) -> None:
    async with nats_kv_store("job_results", self.config) as kv:
        await kv.put(job_id, cloudpickle.dumps(result))

async def get_result(self, job_id: str) -> Optional[Any]:
    async with nats_kv_store("job_results", self.config) as kv:
        entry = await kv.get(job_id)
        if entry:
            return cloudpickle.loads(entry.value)
        return None

async def cleanup_old_results(self, max_age_hours: int = 24) -> int:
    async with nats_kv_store("job_results", self.config) as kv:
        # Implementation for cleanup
        return cleaned_count
```

### Migration Process

#### Step 1: Backup and Prepare
```bash
# Create backup of files to be migrated
cp src/naq/scheduler.py src/naq/scheduler.py.backup
cp src/naq/worker/status.py src/naq/worker/status.py.backup
cp src/naq/results.py src/naq/results.py.backup

# Run tests to establish baseline
pytest tests/unit/test_scheduler.py -v
pytest tests/unit/test_worker_status.py -v
pytest tests/unit/test_results.py -v
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
pytest tests/unit/test_scheduler.py -v
pytest tests/unit/test_worker_status.py -v
pytest tests/unit/test_results.py -v

# Run integration tests
pytest tests/integration/test_integration_scheduler.py -v
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
async def get_scheduled_jobs(self) -> List[Schedule]:
    nc = await get_nats_connection(self.nats_url)
    # ...
```

**After:**
```python
async def get_scheduled_jobs(self) -> List[Schedule]:
    async with nats_kv_store("scheduled_jobs", self.config) as kv:
        # ...
```

### Error Handling Considerations

#### KV Store Error Handling
**Before:**
```python
try:
    nc = await get_nats_connection(url)
    js = await get_jetstream_context(nc)
    kv = await js.key_value(bucket)
    await kv.put(key, value)
except nats.errors.KeyValueNotFound:
    # handle bucket not found
except nats.errors.APIError:
    # handle API error
finally:
    await close_nats_connection(nc)
```

**After:**
```python
try:
    async with nats_kv_store(bucket, config) as kv:
        await kv.put(key, value)
except nats.errors.KeyValueNotFound:
    # handle bucket not found
except nats.errors.APIError:
    # handle API error
# Context manager handles cleanup automatically
```

#### Batch Operations Error Handling
**Before:**
```python
async def batch_update_status(self, updates: Dict[str, WORKER_STATUS]) -> None:
    nc = await get_nats_connection(self.nats_url)
    try:
        js = await get_jetstream_context(nc)
        kv = await js.key_value("worker_status")
        for worker_id, status in updates.items():
            await kv.put(worker_id, status.value)
    except Exception as e:
        logger.error(f"Batch status update failed: {e}")
        raise
    finally:
        await close_nats_connection(nc)
```

**After:**
```python
async def batch_update_status(self, updates: Dict[str, WORKER_STATUS]) -> None:
    async with nats_kv_store("worker_status", self.config) as kv:
        try:
            for worker_id, status in updates.items():
                await kv.put(worker_id, status.value)
        except Exception as e:
            logger.error(f"Batch status update failed: {e}")
            raise
```

### Performance Considerations

#### Connection Reuse
**Before:**
```python
# Multiple connections for related operations
nc1 = await get_nats_connection(url)
js1 = await get_jetstream_context(nc1)
kv1 = await js1.key_value("bucket1")

nc2 = await get_nats_connection(url)
js2 = await get_jetstream_context(nc2)
kv2 = await js2.key_value("bucket2")
```

**After:**
```python
# Single connection context for related operations
async with nats_jetstream(config) as (conn, js):
    kv1 = await js.key_value("bucket1")
    kv2 = await js.key_value("bucket2")
    # Operations using both buckets
```

#### Memory Efficiency
**Before:**
```python
# Multiple connections held in memory simultaneously
connections = []
for i in range(10):
    nc = await get_nats_connection(url)
    connections.append(nc)
```

**After:**
```python
# Context managers ensure proper cleanup
async def process_batch(self, items: List[Any]) -> None:
    async with nats_connection(config) as conn:
        for item in items:
            # Process item using single connection
            await self._process_item(conn, item)
```

### Requirements
- All medium-priority files migrated to context managers
- No functional regressions in medium-priority functionality
- Proper error handling maintained
- Configuration integration working
- Performance characteristics preserved
- All existing tests passing
- New context manager tests added

## Success Criteria
- [ ] Scheduler operations fully migrated
- [ ] Status management fully migrated
- [ ] Result handling fully migrated
- [ ] All existing tests pass
- [ ] No performance regressions
- [ ] Error handling preserved
- [ ] Configuration integration working

## Dependencies
- Task 01 (Connection Context Managers) - implemented context managers
- Task 04 (Identify Connection Patterns) - pattern analysis
- Task 05 (Migrate High-Priority Patterns) - high priority migration
- Existing test suite for validation

## Estimated Time
- 6-8 hours