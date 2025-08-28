# Multiple Queues Example

This example demonstrates how to work with multiple queues and retrieve results from jobs.

## Features Covered

1. Enqueuing jobs to different queues
2. Retrieving job results
3. Handling different job outcomes
4. Error handling for job execution

## Running the Example

1. Start NATS server:
   ```bash
   cd docker && docker-compose up -d
   ```

2. Set secure serializer:
   ```bash
   export NAQ_JOB_SERIALIZER=json
   ```

3. Start workers for both queues:
   ```bash
   naq worker data_processing notifications
   ```

4. Run the example:
   ```bash
   python multiple_queues_results.py
   ```

## What You'll Learn

- How to route jobs to specific queues
- How to fetch results from completed jobs
- How to handle different types of job outcomes
- Best practices for working with multiple queues

## Key Concepts

- **Queue Routing**: Directing jobs to specific queues based on their type
- **Result Retrieval**: Getting results from completed jobs
- **Error Handling**: Properly handling job failures and exceptions
- **Queue Isolation**: Keeping different types of work separate for better resource management