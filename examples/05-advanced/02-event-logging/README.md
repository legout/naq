# Event Logging System - Comprehensive Job and Worker Monitoring

This example demonstrates NAQ's comprehensive event logging system for monitoring job and worker lifecycle events, performance metrics, and system observability.

## What You'll Learn

- Job lifecycle event tracking (enqueue, start, complete, fail)
- Worker event monitoring (start, stop, heartbeat, status changes)
- Event filtering and real-time streaming
- Performance metrics collection through events
- Error pattern analysis and monitoring
- Scheduled job event handling
- High-volume event processing

## Event System Architecture

### Event Types

#### Job Events
- **ENQUEUED**: Job added to queue
- **STARTED**: Job processing began  
- **COMPLETED**: Job finished successfully
- **FAILED**: Job failed with error details
- **RETRY_SCHEDULED**: Job retry scheduled after failure
- **SCHEDULED**: Job scheduled for future execution
- **SCHEDULE_TRIGGERED**: Scheduled job activated and enqueued

#### Worker Events
- **WORKER_STARTED**: Worker process started
- **WORKER_STOPPED**: Worker process stopped
- **WORKER_IDLE**: Worker became idle (no active jobs)
- **WORKER_BUSY**: Worker started processing a job
- **WORKER_HEARTBEAT**: Regular worker status update
- **WORKER_ERROR**: Worker encountered an error

### Event Data Structure

Events capture comprehensive information:

```python
# Job Event Example
{
    "job_id": "job_12345",
    "event_type": "COMPLETED", 
    "timestamp": 1640995200.0,
    "worker_id": "worker_001",
    "queue_name": "default",
    "duration_ms": 2500,
    "message": "Job completed successfully",
    "details": {
        "performance_metrics": {"cpu_usage": 45.2}
    }
}

# Worker Event Example
{
    "worker_id": "worker_001",
    "event_type": "WORKER_HEARTBEAT",
    "timestamp": 1640995200.0, 
    "hostname": "worker-host-1",
    "pid": 1234,
    "active_jobs": 2,
    "concurrency_limit": 4,
    "current_job_id": "job_12345"
}
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Workers** for processing jobs
3. **Event Monitoring** terminal for real-time viewing
4. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start workers for event generation
naq worker default event_queue --log-level INFO &

# Start maintenance workers
naq worker maintenance --log-level INFO &
```

### 2. Start Event Monitoring

In a separate terminal, start real-time event monitoring:

```bash
# Stream all events in real-time
naq events list --follow

# Or filter specific event types
naq events list --follow --type=failed
naq events list --follow --type=completed
```

### 3. Run the Demo

```bash
cd examples/05-advanced/02-event-logging
python event_logging_demo.py
```

## Event Monitoring Commands

### Basic Event Listing

```bash
# List recent events
naq events list

# List with specific count
naq events list --limit=50

# List events from specific time
naq events list --since="2024-01-01T10:00:00"
```

### Real-time Streaming

```bash
# Stream all events
naq events list --follow

# Stream with color coding
naq events list --follow --format=json

# Stream specific worker events
naq events list --follow --worker-id=worker_001
```

### Event Filtering

```bash
# Filter by event type
naq events list --type=failed
naq events list --type=completed
naq events list --type=worker_started

# Filter by job ID
naq events list --job-id=job_12345

# Filter by queue
naq events list --queue-name=high_priority

# Filter by time range
naq events list --since="1 hour ago" --until="now"
```

### Advanced Queries

```bash
# Events with error details
naq events list --type=failed --format=json

# Performance events
naq events list --type=completed --format=json | jq '.duration_ms'

# Worker status events
naq events list --type=worker_heartbeat --worker-id=worker_001
```

## Event Analysis Patterns

### Performance Monitoring

Track job performance metrics:

```python
def analyze_job_performance(events):
    completion_events = [e for e in events if e.event_type == "COMPLETED"]
    durations = [e.duration_ms for e in completion_events]
    
    avg_duration = sum(durations) / len(durations)
    success_rate = len(completion_events) / total_jobs
    
    return {
        "avg_duration_ms": avg_duration,
        "success_rate": success_rate,
        "throughput": len(completion_events) / time_window
    }
```

### Error Pattern Detection

Identify recurring issues:

```python
def detect_error_patterns(events):
    error_events = [e for e in events if e.event_type == "FAILED"]
    
    error_counts = {}
    for event in error_events:
        error_type = event.error_type
        error_counts[error_type] = error_counts.get(error_type, 0) + 1
    
    return sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
```

### Worker Health Monitoring

Track worker status and utilization:

```python
def monitor_worker_health(events):
    heartbeat_events = [e for e in events if e.event_type == "WORKER_HEARTBEAT"]
    
    worker_stats = {}
    for event in heartbeat_events:
        worker_id = event.worker_id
        utilization = event.active_jobs / event.concurrency_limit
        
        if worker_id not in worker_stats:
            worker_stats[worker_id] = []
        worker_stats[worker_id].append(utilization)
    
    return {
        worker_id: {
            "avg_utilization": sum(utils) / len(utils),
            "max_utilization": max(utils)
        }
        for worker_id, utils in worker_stats.items()
    }
```

## Demonstration Scenarios

### 1. Job Lifecycle Events

The demo creates various job types to demonstrate:

- **Quick Processing Jobs**: Fast completion events
- **Network Jobs**: Higher failure rates, retry events
- **Batch Processing**: Progress tracking events
- **Long-running Jobs**: Extended duration metrics

### 2. Scheduled Job Events

Demonstrates scheduled job lifecycle:

- **SCHEDULED**: Job scheduled for future execution
- **SCHEDULE_TRIGGERED**: Scheduled time reached, job enqueued
- **Maintenance Tasks**: Regular system maintenance events

### 3. High-Volume Event Handling

Tests system with many concurrent jobs:

- Multiple job types running simultaneously
- Event stream performance under load
- Worker utilization monitoring
- Queue backlog tracking

### 4. Event Analysis

Real-time analysis of event patterns:

- Performance metrics calculation
- Error rate trending
- Worker efficiency analysis
- System health scoring

## Production Monitoring

### Event Storage

For production use, consider:

```python
# Store events in time-series database
def store_event(event):
    # InfluxDB, Prometheus, or similar
    metrics_db.write({
        'measurement': 'naq_events',
        'tags': {
            'event_type': event.event_type,
            'queue_name': event.queue_name,
            'worker_id': event.worker_id
        },
        'fields': {
            'duration_ms': event.duration_ms,
            'timestamp': event.timestamp
        }
    })
```

### Alerting Rules

Set up alerts for critical events:

```yaml
# Alert on high error rate
- alert: HighJobFailureRate
  expr: rate(naq_job_events{event_type="FAILED"}[5m]) > 0.1
  labels:
    severity: warning
    
# Alert on worker unavailability  
- alert: NoWorkersAvailable
  expr: naq_active_workers == 0
  labels:
    severity: critical
```

### Dashboard Metrics

Key metrics to monitor:

- **Job Metrics**: Success rate, duration, throughput
- **Worker Metrics**: Utilization, health, availability  
- **Queue Metrics**: Backlog size, processing rate
- **Error Metrics**: Failure patterns, retry rates
- **System Metrics**: Resource usage, performance trends

## Integration Examples

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "NAQ Event Monitoring",
    "panels": [
      {
        "title": "Job Success Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(naq_job_events{event_type=\"COMPLETED\"}[5m]) / rate(naq_job_events{event_type=~\"COMPLETED|FAILED\"}[5m])"
          }
        ]
      }
    ]
  }
}
```

### Elastic Stack Integration

```python
# Send events to Elasticsearch
def send_to_elastic(event):
    doc = {
        'timestamp': datetime.fromtimestamp(event.timestamp),
        'job_id': event.job_id,
        'event_type': event.event_type,
        'duration_ms': event.duration_ms,
        'worker_id': event.worker_id,
        'queue_name': event.queue_name
    }
    es.index(index='naq-events', doc_type='event', body=doc)
```

## Troubleshooting

### Event System Issues

```bash
# Check event system status
naq events list --limit=1

# Test event generation
naq events test

# Check NATS connectivity
nats pub naq.events.test "test message"
```

### Common Problems

1. **No Events Visible**
   - Verify NATS is running and accessible
   - Check NAQ configuration
   - Ensure workers are connected

2. **Missing Events**
   - Check event retention settings
   - Verify NATS JetStream configuration
   - Look for network connectivity issues

3. **Performance Issues**
   - Monitor event stream throughput
   - Check NATS resource usage
   - Consider event sampling for high volume

## Next Steps

- Explore [performance optimization](../03-performance-optimization/) for handling high event volumes
- Check out [monitoring dashboard](../../03-production/02-monitoring-dashboard/) for event visualization
- Implement [custom event handlers](../04-custom-event-handlers/) for specialized monitoring needs
- Set up [distributed tracing](../05-distributed-tracing/) for end-to-end job tracking