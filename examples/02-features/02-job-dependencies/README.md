# Job Dependencies - Workflow Coordination

This example demonstrates how to coordinate complex workflows using job dependencies, ensuring jobs execute in the correct order and handle failures gracefully.

## What You'll Learn

- Creating job dependencies and workflows
- Sequential and parallel job execution
- Dependency failure handling
- Workflow monitoring and debugging
- Advanced dependency patterns

## Dependency Patterns

### Sequential Dependencies
Jobs that must run in order:
```python
# Job B depends on Job A completing successfully
job_a = enqueue_sync(process_data, data="input.csv")
job_b = enqueue_sync(analyze_results, depends_on=[job_a])
```

### Parallel with Convergence
Multiple jobs feeding into one:
```python
# Jobs A, B, C run in parallel, Job D waits for all
job_a = enqueue_sync(fetch_user_data, user_id=1)
job_b = enqueue_sync(fetch_order_data, user_id=1) 
job_c = enqueue_sync(fetch_preference_data, user_id=1)
job_d = enqueue_sync(generate_report, depends_on=[job_a, job_b, job_c])
```

### Fan-out Pattern
One job creating multiple dependent jobs:
```python
# Job A creates data for jobs B, C, D to process
job_a = enqueue_sync(prepare_dataset, source="database")
job_b = enqueue_sync(train_model_a, depends_on=[job_a])
job_c = enqueue_sync(train_model_b, depends_on=[job_a])
job_d = enqueue_sync(validate_models, depends_on=[job_b, job_c])
```

## Dependency Behavior

### Success Dependencies
By default, jobs wait for their dependencies to complete successfully:
```python
job_a = enqueue_sync(risky_operation)
job_b = enqueue_sync(cleanup_after_success, depends_on=[job_a])
# job_b only runs if job_a succeeds
```

### Always-Run Dependencies
Jobs that run regardless of dependency outcome:
```python
job_a = enqueue_sync(risky_operation)
job_b = enqueue_sync(cleanup_always, depends_on=[job_a], run_after_failure=True)
# job_b runs whether job_a succeeds or fails
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Worker** running: `naq worker default workflow_queue`
3. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start worker for workflow processing
naq worker default workflow_queue --log-level INFO
```

### 2. Run Examples

```bash
cd examples/02-features/02-job-dependencies

# Basic dependency patterns
python basic_dependencies.py

# Complex workflow examples
python complex_workflows.py

# Failure handling scenarios
python failure_handling.py
```

## Expected Output

You'll see jobs executing in dependency order:

```
📤 Enqueued Job A: data-preparation
📤 Enqueued Job B: model-training (depends on A)
📤 Enqueued Job C: model-validation (depends on B)

✅ Job A completed: data ready
🔄 Job B starting: training model...
✅ Job B completed: model trained
🔄 Job C starting: validating model...
✅ Job C completed: validation passed
```

## Common Workflow Patterns

### Data Pipeline
```python
# ETL Pipeline Example
extract_job = enqueue_sync(extract_data, source="api")
transform_job = enqueue_sync(transform_data, depends_on=[extract_job])
load_job = enqueue_sync(load_data, depends_on=[transform_job])
validate_job = enqueue_sync(validate_data, depends_on=[load_job])
```

### Machine Learning Pipeline
```python
# ML Training Pipeline
data_prep = enqueue_sync(prepare_training_data)
feature_eng = enqueue_sync(engineer_features, depends_on=[data_prep])
train_model = enqueue_sync(train_model, depends_on=[feature_eng])
evaluate = enqueue_sync(evaluate_model, depends_on=[train_model])
deploy = enqueue_sync(deploy_model, depends_on=[evaluate])
```

### Report Generation
```python
# Multi-source Report
user_data = enqueue_sync(fetch_user_analytics)
sales_data = enqueue_sync(fetch_sales_data)
finance_data = enqueue_sync(fetch_finance_data)
generate_report = enqueue_sync(
    create_monthly_report,
    depends_on=[user_data, sales_data, finance_data]
)
```

## Monitoring Dependencies

### Dependency Status
Check job status including dependencies:
```python
from naq import get_job_status_sync

status = get_job_status_sync(job_id)
print(f"Job Status: {status['status']}")
print(f"Dependencies: {status['dependencies']}")
```

### Workflow Visualization
Use the dashboard to visualize workflows:
```bash
naq dashboard
# Visit http://localhost:8000 to see dependency graphs
```

## Best Practices

### 1. Keep Dependencies Simple
- Minimize the number of dependencies per job
- Avoid circular dependencies
- Use clear, descriptive job names

### 2. Handle Failures Gracefully
- Plan for dependency failures
- Use cleanup jobs with `run_after_failure=True`
- Implement retry strategies for transient failures

### 3. Design for Observability
- Add logging to track workflow progress
- Use descriptive job IDs and metadata
- Monitor dependency execution times

### 4. Optimize Performance
- Run independent jobs in parallel
- Minimize data passing between jobs
- Consider queue separation for different workflow types

## Error Handling

### Dependency Failures
When a dependency fails:
```python
# This job will not run if prepare_data fails
process_job = enqueue_sync(process_results, depends_on=[prepare_data])

# This cleanup job will always run
cleanup_job = enqueue_sync(
    cleanup_temp_files,
    depends_on=[prepare_data],
    run_after_failure=True
)
```

### Partial Failures
Handle workflows where some dependencies may fail:
```python
# Multiple data sources, some may fail
source_a = enqueue_sync(fetch_source_a)
source_b = enqueue_sync(fetch_source_b)
source_c = enqueue_sync(fetch_source_c)

# Process available data even if some sources fail
process_job = enqueue_sync(
    process_available_data,
    depends_on=[source_a, source_b, source_c],
    allow_partial_dependencies=True
)
```

## Advanced Patterns

### Conditional Dependencies
```python
# Job runs based on previous job results
validation_job = enqueue_sync(validate_input_data)
success_job = enqueue_sync(process_valid_data, depends_on=[validation_job])
failure_job = enqueue_sync(
    handle_invalid_data,
    depends_on=[validation_job],
    run_after_failure=True
)
```

### Dynamic Workflows
```python
# Create dependencies based on runtime conditions
def create_dynamic_workflow(item_count):
    prepare_job = enqueue_sync(prepare_batch_data, count=item_count)
    
    # Create processing jobs based on data size
    process_jobs = []
    for i in range(item_count // 100):  # One job per 100 items
        job = enqueue_sync(
            process_batch_chunk,
            chunk_id=i,
            depends_on=[prepare_job]
        )
        process_jobs.append(job)
    
    # Final aggregation
    final_job = enqueue_sync(
        aggregate_results,
        depends_on=process_jobs
    )
    
    return final_job
```

## Troubleshooting

### Stuck Workflows
Common issues and solutions:

1. **Circular Dependencies**
   - Check for jobs that depend on each other
   - Use dependency visualization tools
   
2. **Failed Dependencies**
   - Check logs for dependency failure reasons
   - Consider if jobs should run after failure
   
3. **Performance Issues**
   - Review dependency chains for bottlenecks
   - Consider parallel execution opportunities

### Debugging Commands
```bash
# List all jobs in a workflow
naq list-jobs --queue workflow_queue

# Check specific job status
naq job-status <job_id>

# Monitor worker activity
naq list-workers
```

## Next Steps

- Explore [scheduled jobs](../03-scheduled-jobs/) for time-based workflows
- Learn about [multiple queues](../04-multiple-queues/) for workflow separation
- Check out [high availability](../../05-advanced/02-high-availability/) for robust workflows