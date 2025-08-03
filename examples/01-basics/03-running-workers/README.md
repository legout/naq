# Running Workers - Production Worker Management

This example demonstrates how to properly start, configure, and manage NAQ workers in different scenarios.

## What You'll Learn

- How to start workers for different queues
- Worker configuration options
- Running multiple workers for scaling
- Proper worker shutdown and signal handling
- Monitoring worker health and status

## Worker Basics

Workers are the processes that fetch and execute jobs from NAQ queues. Key concepts:

- **Queue Specialization**: Workers can listen to specific queues
- **Concurrency**: Control how many jobs run simultaneously per worker
- **Heartbeats**: Workers report their status for monitoring
- **Graceful Shutdown**: Workers finish current jobs before stopping

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Library** installed with `pip install naq`
3. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Worker Command Examples

### Basic Worker Commands

```bash
# Start worker for default queue
naq worker default

# Start worker for specific queues
naq worker emails notifications

# Start worker with custom concurrency
naq worker data_processing --concurrency 5

# Start worker with custom name
naq worker default --worker-name "web-server-1"

# Start worker with custom NATS URL
naq worker default --nats-url "nats://prod-nats:4222"
```

### Worker Configuration Options

```bash
# Set heartbeat interval (default: 30 seconds)
naq worker default --heartbeat-interval 60

# Set worker TTL (default: 300 seconds)
naq worker default --worker-ttl 600

# Set shutdown timeout (default: 30 seconds)
naq worker default --shutdown-timeout 60

# Set log level
naq worker default --log-level DEBUG
```

## Running the Examples

### 1. Start NATS Server

```bash
cd docker && docker-compose up -d
```

### 2. Set Secure Configuration

```bash
export NAQ_JOB_SERIALIZER=json
```

### 3. Try Different Worker Scenarios

Each Python script demonstrates a different worker pattern:

```bash
# Basic single worker
python basic_worker.py

# Multiple specialized workers  
python specialized_workers.py

# High-performance worker setup
python high_performance_workers.py

# Worker monitoring
python worker_monitoring.py
```

## Production Patterns

### Single Queue Worker
Perfect for simple applications:
```bash
naq worker default --concurrency 10
```

### Specialized Workers
Different workers for different job types:
```bash
# Terminal 1: Email processing
naq worker emails --concurrency 5

# Terminal 2: Image processing  
naq worker images --concurrency 2

# Terminal 3: General tasks
naq worker default --concurrency 10
```

### High Availability Setup
Multiple workers for redundancy:
```bash
# Multiple workers for the same queues
naq worker default emails --worker-name "worker-1" &
naq worker default emails --worker-name "worker-2" &
naq worker default emails --worker-name "worker-3" &
```

## Worker Management

### Listing Active Workers
```bash
naq list-workers
```

### Starting the Scheduler
For scheduled and recurring jobs:
```bash
naq scheduler --enable-ha
```

### Starting the Dashboard
For web-based monitoring:
```bash
naq dashboard --host 0.0.0.0 --port 8080
```

## Monitoring and Health Checks

Workers automatically:
- Send heartbeats every 30 seconds (configurable)
- Report their status (idle, busy, stopping)
- Handle graceful shutdown on SIGTERM
- Update their TTL in the monitoring system

## Scaling Guidelines

### CPU-Intensive Jobs
- Lower concurrency (2-4 per CPU core)
- Dedicated workers for heavy processing

### I/O-Intensive Jobs  
- Higher concurrency (10-20 per worker)
- Multiple workers for redundancy

### Mixed Workloads
- Separate workers by job type
- Different concurrency settings per queue

## Production Deployment

### Systemd Service Example
```ini
[Unit]
Description=NAQ Worker
After=network.target

[Service]
Type=simple
User=naq
Environment=NAQ_JOB_SERIALIZER=json
ExecStart=/usr/local/bin/naq worker default --concurrency 10
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Docker Example
```dockerfile
FROM python:3.12-slim
RUN pip install naq
ENV NAQ_JOB_SERIALIZER=json
CMD ["naq", "worker", "default", "--concurrency", "10"]
```

## Troubleshooting

**Worker not starting?**
- Check NATS connection: `nats-server --version`
- Verify queue names match your job enqueueing
- Check environment variables

**Jobs not being processed?**
- Ensure worker is listening to correct queues
- Check for serialization errors in logs
- Verify job functions are importable

**Performance issues?**
- Adjust concurrency based on job type
- Monitor CPU and memory usage
- Consider queue specialization

## Next Steps

- Explore [job retries](../../02-features/01-job-retries/) for error handling
- Learn about [production monitoring](../../03-production/02-monitoring-dashboard/)
- Check out [high availability](../../05-advanced/02-high-availability/) patterns