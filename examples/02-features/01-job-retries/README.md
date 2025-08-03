# Job Retries - Robust Error Handling

This example demonstrates NAQ's comprehensive retry system for handling transient failures and building robust applications.

## What You'll Learn

- Different retry strategies (linear, exponential)
- Custom retry delays and maximum attempts
- Selective retries based on exception types
- Retry monitoring and debugging
- Best practices for retry configuration

## Retry Strategies

NAQ supports multiple retry strategies:

### Linear Retry
Fixed delay between attempts:
```python
# Retry 3 times with 5-second delays
job = enqueue_sync(flaky_task, max_retries=3, retry_delay=5)

# Custom delays for each attempt
job = enqueue_sync(flaky_task, max_retries=3, retry_delay=[2, 5, 10])
```

### Exponential Backoff
Increasing delays to reduce system load:
```python
job = enqueue_sync(
    flaky_task,
    max_retries=4,
    retry_delay=2,
    retry_strategy="exponential"  # 2s, 4s, 8s, 16s
)
```

### Selective Retries
Only retry specific exception types:
```python
job = enqueue_sync(
    api_call,
    max_retries=3,
    retry_delay=5,
    retry_on=(ConnectionError, TimeoutError),  # Only retry these
    ignore_on=(ValueError, AuthenticationError)  # Never retry these
)
```

## When to Use Retries

**Good candidates for retries:**
- Network requests (API calls, database connections)
- File I/O operations
- External service interactions
- Temporary resource unavailability

**Poor candidates for retries:**
- Logic errors in code
- Invalid input data
- Authentication failures
- Permanent configuration issues

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Worker** running: `naq worker default retry_queue`
3. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start worker with debug logging to see retries
naq worker default retry_queue --log-level DEBUG
```

### 2. Run Examples

```bash
cd examples/02-features/01-job-retries

# Basic retry strategies
python basic_retries.py

# Advanced retry patterns
python advanced_retries.py

# Real-world retry scenarios
python realistic_scenarios.py
```

## Expected Output

You'll see retry behavior in action:

```
🔄 Attempt 1/3: API call failed (ConnectionError)
⏱️  Retrying in 5 seconds...

🔄 Attempt 2/3: API call failed (TimeoutError)  
⏱️  Retrying in 10 seconds...

✅ Attempt 3/3: API call succeeded!
```

## Retry Configuration Guidelines

### Conservative (Default)
For most applications:
```python
max_retries=3
retry_delay=5
retry_strategy="linear"
```

### Aggressive
For critical operations:
```python
max_retries=5
retry_delay=[1, 2, 4, 8, 16]
retry_strategy="exponential"
```

### Selective
For API integrations:
```python
max_retries=3
retry_delay=10
retry_on=(ConnectionError, TimeoutError, HTTPError)
ignore_on=(ValueError, AuthenticationError, PermissionError)
```

## Monitoring Retries

### Worker Logs
Enable DEBUG logging to see retry details:
```bash
naq worker default --log-level DEBUG
```

### Dashboard
Use the web dashboard to monitor failed jobs:
```bash
naq dashboard
# Visit http://localhost:8000
```

### Programmatic Monitoring
```python
from naq import list_workers_sync

workers = list_workers_sync()
for worker in workers:
    print(f"Worker {worker['worker_id']}: {worker['status']}")
```

## Best Practices

### 1. Start Conservative
Begin with simple retry settings and adjust based on real-world behavior.

### 2. Use Exponential Backoff
For external services to avoid overwhelming them during outages.

### 3. Set Maximum Limits
Always set reasonable maximum retry counts to prevent infinite loops.

### 4. Monitor Retry Patterns
Track which jobs retry frequently to identify systemic issues.

### 5. Consider Circuit Breakers
For external services, implement circuit breaker patterns.

### 6. Log Retry Attempts
Include retry information in your application logs.

## Production Considerations

### Resource Management
- High retry counts can consume worker slots
- Long retry delays can delay other jobs
- Consider separate queues for retry-heavy jobs

### Error Analysis
- Monitor retry success rates
- Identify patterns in failed jobs
- Alert on excessive retry rates

### Graceful Degradation
- Implement fallback mechanisms
- Consider partial success scenarios
- Plan for total failure modes

## Next Steps

- Explore [job dependencies](../02-job-dependencies/) for workflow coordination
- Learn about [scheduled jobs](../03-scheduled-jobs/) for time-based processing
- Check out [error handling](../../03-production/03-error-handling/) for production patterns