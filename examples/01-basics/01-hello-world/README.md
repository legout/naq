# Hello World - Your First NAQ Job

This example demonstrates the absolute basics of NAQ: creating a simple job, running a worker, and processing the job.

## What You'll Learn

- How to create and enqueue your first job
- How to start a worker to process jobs
- Best practices for secure job processing

## Prerequisites

1. **Python 3.12+** - NAQ requires Python 3.12 or later
2. **NATS Server** - Running with JetStream enabled
3. **NAQ Library** - Install with `pip install naq`

## Quick Setup

### 1. Start NATS Server

Using Docker (recommended):
```bash
# From the project root directory
cd docker
docker-compose up -d
```

Or install NATS manually and start with JetStream:
```bash
nats-server -js
```

### 2. Configure Environment

Set the secure JSON serializer (recommended for production):
```bash
export NAQ_JOB_SERIALIZER=json
```

## Running the Example

### 1. Start the Worker

In one terminal, start a worker to process jobs from the 'default' queue:
```bash
naq worker default
```

You should see output like:
```
Worker naq-worker-hostname-12345-abc123 starting...
Listening to queues: ['default']
Worker is ready and waiting for jobs...
```

### 2. Run the Job Producer

In another terminal, run the example to enqueue jobs:
```bash
cd examples/01-basics/01-hello-world
python hello_world.py
```

### 3. Watch the Output

You'll see the worker process the jobs in real-time:
```
Processing job: say_hello
Hello, World!
Task completed in 2.1 seconds
Job completed successfully
```

## Understanding the Code

The example demonstrates:

1. **Secure Configuration**: Uses JSON serializer for safety
2. **Simple Job Function**: A basic function that can be executed by workers
3. **Job Enqueueing**: How to submit jobs to NAQ
4. **Error Handling**: Proper exception handling and logging

## Next Steps

- Try the [SyncClient example](../02-sync-client/) for efficient batch operations
- Learn about [running workers](../03-running-workers/) in production
- Explore [job retries](../../02-features/01-job-retries/) for robust error handling

## Troubleshooting

**Worker not picking up jobs?**
- Check that NATS is running: `nats-server --version`
- Verify the worker is listening to the correct queue
- Check logs for connection errors

**Jobs failing with serialization errors?**
- Ensure you're using `NAQ_JOB_SERIALIZER=json`
- Check that your job functions are importable (not lambdas or nested functions)

**Need help?**
- Check the [error handling example](../../03-production/03-error-handling/)
- Review the [troubleshooting guide](../../03-production/04-configuration/)