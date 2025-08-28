# NAQ Examples

This directory contains comprehensive examples demonstrating various features and use cases of the NAQ library. The examples are organized by complexity and feature set to help you learn NAQ progressively.

## Organization

The examples are structured in the following categories:

1. **Basics** - Introduction to core NAQ concepts
2. **Features** - Advanced features and capabilities
3. **Production** - Best practices for production environments
4. **Applications** - Real-world application integrations
5. **Advanced** - Complex scenarios and customizations

## Prerequisites

Before running any examples, ensure you have:

1. A running NATS server with JetStream enabled:
   ```bash
   cd docker
   docker-compose up -d
   ```

2. The required Python dependencies installed:
   ```bash
   pip install naq
   # For web integration examples:
   # pip install naq[dashboard]
   ```

3. For examples using JSON serialization (recommended for security):
   ```bash
   export NAQ_JOB_SERIALIZER=json
   ```

## Example Categories

### 1. Basics

Start here to learn the fundamental concepts of NAQ:

- **Hello World** (`01-basics/01-hello-world/`) - Basic job enqueueing and processing
- **Sync Client** (`01-basics/02-sync-client/`) - Using the SyncClient for batch operations
- **Running Workers** (`01-basics/03-running-workers/`) - Worker setup and monitoring
- **Multiple Queues** (`01-basics/04-multiple-queues/`) - Working with multiple queues

### 2. Features

Explore advanced NAQ features:

- **Job Retries** (`02-features/01-job-retries/`) - Configuring retry policies and handling failures
- **Job Dependencies** (`02-features/02-job-dependencies/`) - Creating job workflows with dependencies
- **Scheduled Jobs** (`02-features/03-scheduled-jobs/`) - Scheduling jobs for future execution
- **KV Store** (`02-features/04-kv-store/`) - Using the key-value store for job metadata

### 3. Production

Best practices for production environments:

- **Security Best Practices** (`03-production/01-security-best-practices/`) - Secure configuration and deployment
- **Monitoring Dashboard** (`03-production/02-monitoring-dashboard/`) - Setting up monitoring and observability
- **Error Handling** (`03-production/03-error-handling/`) - Robust error handling patterns

### 4. Applications

Real-world integrations:

- **Web Integration** (`04-applications/01-web-integration/`) - Integrating NAQ with web frameworks (Flask example)
- **Email System** (`04-applications/02-email-system/`) - Building an email processing system

### 5. Advanced

Complex scenarios and customizations:

- **Worker Scaling** (`05-advanced/01-worker-scaling/`) - Scaling workers for high throughput

## Running Examples

Each example directory contains:

1. A Python script demonstrating the feature
2. A README.md with detailed explanations
3. Any necessary configuration files

To run an example:

1. Navigate to the example directory:
   ```bash
   cd examples/01-basics/01-hello-world/
   ```

2. Follow the instructions in the example's README.md

3. Start the required services (NATS, workers, scheduler as needed)

## Testing Examples

We provide a verification script to check that all examples are syntactically correct:

```bash
uv run examples/verify_examples.py
```

Note: Some examples (like the Flask web integration) require additional dependencies that are not installed by default.

## Configuration

Examples use configuration files located in the `configs/` directory:

- `development.yaml` - Development environment settings
- `testing.yaml` - Testing environment settings
- `production.yaml` - Production environment settings

## Contributing

If you create new examples or improve existing ones, please:

1. Follow the existing directory structure
2. Include a comprehensive README.md
3. Ensure the example works with the latest NAQ version
4. Add any necessary configuration files
5. Update this index when adding new categories

## Troubleshooting

Common issues and solutions:

1. **NATS Connection Errors** - Ensure NATS is running with JetStream enabled
2. **Import Errors** - Verify NAQ is installed correctly
3. **Serialization Errors** - For untrusted environments, use `NAQ_JOB_SERIALIZER=json`
4. **Worker Not Processing Jobs** - Ensure the worker is running and listening to the correct queue

For detailed troubleshooting, check the main README.md and documentation.