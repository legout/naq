# SyncClient - Efficient Batch Operations

This example demonstrates using NAQ's `SyncClient` for efficient batch job operations. The SyncClient reuses NATS connections, making it much more efficient than individual sync calls.

## What You'll Learn

- How to use `SyncClient` for batch operations
- Connection reuse for better performance  
- Efficient job enqueueing patterns
- Proper resource management with context managers

## Why Use SyncClient?

When you need to enqueue many jobs or perform multiple NAQ operations, `SyncClient` offers significant advantages:

- **Connection Reuse**: Single NATS connection for all operations
- **Better Performance**: No connection overhead per operation
- **Resource Efficient**: Automatic connection cleanup
- **Simpler Code**: Context manager handles connection lifecycle

## Performance Comparison

### Without SyncClient (inefficient):
```python
# Each call creates and destroys a NATS connection
job1 = enqueue_sync(task1)  # Connect -> Enqueue -> Disconnect
job2 = enqueue_sync(task2)  # Connect -> Enqueue -> Disconnect  
job3 = enqueue_sync(task3)  # Connect -> Enqueue -> Disconnect
```

### With SyncClient (efficient):
```python
# Single connection for all operations
with SyncClient() as client:
    job1 = client.enqueue(task1)  # Reuse connection
    job2 = client.enqueue(task2)  # Reuse connection
    job3 = client.enqueue(task3)  # Reuse connection
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Worker** running: `naq worker default batch_jobs`
3. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Example

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Start worker for multiple queues
naq worker default batch_jobs

# Set secure serialization
export NAQ_JOB_SERIALIZER=json
```

### 2. Run the Example

```bash
cd examples/01-basics/02-sync-client
python sync_client_demo.py
```

## Expected Output

You'll see performance comparisons and batch operations:

```
🚀 SyncClient Performance Demo
=============================

📊 Performance Test: 10 jobs
Without SyncClient: 2.34 seconds
With SyncClient: 0.89 seconds
Improvement: 62% faster!

📦 Batch Operations Demo
✅ Enqueued 5 data processing jobs
✅ Enqueued 3 notification jobs  
✅ Scheduled 2 future jobs
✅ Purged test queue

🎯 All operations completed efficiently!
```

## Use Cases

**SyncClient is perfect for:**
- Batch job enqueueing
- Data import/export scripts
- Administrative operations
- Testing and development
- CLI tools and utilities

**Use regular async methods for:**
- Web application endpoints
- Long-running services
- Single job operations
- When you need async/await patterns

## Next Steps

- Learn about [worker management](../03-running-workers/)
- Explore [job retries](../../02-features/01-job-retries/) for robust processing
- Check out [production patterns](../../03-production/) for real applications