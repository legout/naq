# Error Handling - Robust Production Operations

This example demonstrates comprehensive error handling strategies for NAQ applications in production, including exception management, logging, monitoring, and recovery patterns.

## What You'll Learn

- Comprehensive exception handling patterns
- Error logging and monitoring
- Graceful degradation strategies
- Dead letter queue management
- Error recovery and remediation
- Production error handling best practices

## Error Handling Strategies

### Exception Classification
Categorize errors for appropriate handling:

```python
# Retryable errors (transient failures)
RETRYABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    TemporaryResourceError,
    RateLimitError
)

# Non-retryable errors (permanent failures)
NON_RETRYABLE_ERRORS = (
    ValueError,
    AuthenticationError,
    PermissionError,
    DataValidationError
)

# Critical errors (require immediate attention)
CRITICAL_ERRORS = (
    SecurityError,
    CorruptionError,
    SystemFailureError
)
```

### Defensive Programming
```python
def robust_job_function(data):
    """Example of defensive job implementation."""
    
    # Input validation
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")
    
    required_fields = ['id', 'type', 'payload']
    missing_fields = [f for f in required_fields if f not in data]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    try:
        # Main processing logic
        result = process_data(data)
        
        # Result validation
        if not validate_result(result):
            raise ProcessingError("Result validation failed")
        
        return result
        
    except ExternalServiceError as e:
        # Log and re-raise for retry
        logger.warning(f"External service error: {e}")
        raise
        
    except DataCorruptionError as e:
        # Critical error - alert and fail
        logger.critical(f"Data corruption detected: {e}")
        send_critical_alert(f"Data corruption in job {data['id']}")
        raise
        
    except Exception as e:
        # Unexpected error - log and re-raise
        logger.error(f"Unexpected error processing {data['id']}: {e}")
        raise ProcessingError(f"Unexpected error: {e}") from e
```

## Error Recovery Patterns

### Retry with Backoff
```python
from naq import enqueue_sync

# Automatic retries with exponential backoff
job = enqueue_sync(
    flaky_api_call,
    data={"endpoint": "/api/users"},
    max_retries=5,
    retry_delay=2,  # Start with 2 seconds
    retry_strategy="exponential",  # 2s, 4s, 8s, 16s, 32s
    retry_on=(ConnectionError, TimeoutError),
    ignore_on=(AuthenticationError, ValueError)
)
```

### Circuit Breaker Pattern
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit breaker is open")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise
    
    def on_success(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
```

### Fallback Mechanisms
```python
def process_with_fallback(data):
    """Process data with fallback options."""
    
    try:
        # Primary processing
        return primary_processor(data)
        
    except PrimaryServiceError:
        logger.warning("Primary service failed, trying secondary")
        try:
            return secondary_processor(data)
        except SecondaryServiceError:
            logger.warning("Secondary service failed, using local fallback")
            return local_fallback_processor(data)
    
    except Exception as e:
        logger.error(f"All processing methods failed: {e}")
        # Store for manual processing
        store_for_manual_review(data)
        raise ProcessingError("All fallback methods exhausted")
```

## Dead Letter Queue Management

### Dead Letter Configuration
```python
# Configure dead letter queue for failed jobs
job = enqueue_sync(
    unreliable_function,
    data={"id": 123},
    max_retries=3,
    dead_letter_queue="failed_jobs",  # Send to DLQ after max retries
    dead_letter_ttl=timedelta(days=7)  # Keep in DLQ for 7 days
)
```

### Dead Letter Processing
```python
def process_dead_letter_queue():
    """Process jobs from dead letter queue."""
    
    failed_jobs = get_dead_letter_jobs("failed_jobs")
    
    for job in failed_jobs:
        try:
            # Analyze failure reason
            failure_reason = analyze_failure(job)
            
            if failure_reason == "transient_error":
                # Retry with modified parameters
                retry_job_with_modifications(job)
                
            elif failure_reason == "data_issue":
                # Send for manual review
                send_for_manual_review(job)
                
            elif failure_reason == "system_issue":
                # Wait for system recovery
                reschedule_after_delay(job, delay=timedelta(hours=1))
                
            else:
                # Permanent failure - archive
                archive_failed_job(job)
                
        except Exception as e:
            logger.error(f"Error processing dead letter job {job.id}: {e}")
```

## Error Monitoring and Alerting

### Error Rate Monitoring
```python
def monitor_error_rates():
    """Monitor job error rates and send alerts."""
    
    # Get recent job statistics
    recent_jobs = get_jobs_in_timeframe(hours=1)
    
    total_jobs = len(recent_jobs)
    failed_jobs = len([j for j in recent_jobs if j.status == 'failed'])
    
    if total_jobs > 0:
        error_rate = failed_jobs / total_jobs
        
        if error_rate > ERROR_RATE_THRESHOLD:
            send_alert(
                level="WARNING",
                message=f"High error rate: {error_rate:.2%} ({failed_jobs}/{total_jobs})",
                details={
                    "error_rate": error_rate,
                    "failed_jobs": failed_jobs,
                    "total_jobs": total_jobs,
                    "timeframe": "last_hour"
                }
            )
```

### Error Pattern Analysis
```python
def analyze_error_patterns():
    """Analyze error patterns for insights."""
    
    failed_jobs = get_failed_jobs(hours=24)
    
    # Group by error type
    error_types = {}
    for job in failed_jobs:
        error_type = job.error_type
        if error_type not in error_types:
            error_types[error_type] = []
        error_types[error_type].append(job)
    
    # Report on most common errors
    for error_type, jobs in sorted(error_types.items(), key=lambda x: len(x[1]), reverse=True):
        count = len(jobs)
        percentage = count / len(failed_jobs) * 100
        
        logger.info(f"Error type '{error_type}': {count} occurrences ({percentage:.1f}%)")
        
        # Alert on significant error patterns
        if count > SIGNIFICANT_ERROR_COUNT:
            send_alert(
                level="WARNING",
                message=f"Significant error pattern detected: {error_type}",
                details={
                    "error_type": error_type,
                    "count": count,
                    "percentage": percentage
                }
            )
```

## Prerequisites

1. **NATS Server** running with JetStream
2. **NAQ Workers** running for error handling demonstration
3. **Dead Letter Queue** configuration
4. **Secure Configuration**: `export NAQ_JOB_SERIALIZER=json`

## Running the Examples

### 1. Start Services

```bash
# Start NATS
cd docker && docker-compose up -d

# Set secure serialization
export NAQ_JOB_SERIALIZER=json

# Start workers for error handling
naq worker default error_queue dlq_queue --log-level DEBUG &

# Start dashboard for monitoring
naq dashboard --port 8000 &
```

### 2. Run Examples

```bash
cd examples/03-production/03-error-handling

# Comprehensive error handling patterns
python error_patterns.py

# Dead letter queue management
python dead_letter_handling.py

# Error monitoring and alerting
python error_monitoring.py
```

### 3. Monitor Errors

```bash
# View dashboard for error monitoring
open http://localhost:8000

# Check worker logs for error details
naq worker-logs
```

## Error Logging Best Practices

### Structured Logging
```python
import logging
import json
from datetime import datetime

class StructuredLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        
    def log_error(self, error, context=None):
        """Log error with structured data."""
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {}
        }
        
        # Add traceback for unexpected errors
        if not isinstance(error, (ValueError, KeyError)):
            import traceback
            log_entry["traceback"] = traceback.format_exc()
        
        self.logger.error(json.dumps(log_entry))
```

### Error Context Preservation
```python
def job_with_error_context(job_id, data):
    """Job function that preserves error context."""
    
    error_context = {
        "job_id": job_id,
        "function": "job_with_error_context",
        "input_data_keys": list(data.keys()) if isinstance(data, dict) else None,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        return process_job_data(data)
        
    except Exception as e:
        # Enrich error with context
        enriched_error = EnrichedError(str(e), context=error_context)
        logger.error(f"Job failed: {enriched_error}")
        raise enriched_error from e
```

## Error Recovery Strategies

### Graceful Degradation
```python
def process_with_degradation(data):
    """Process data with graceful degradation."""
    
    try:
        # Full processing
        return full_processing(data)
        
    except NonCriticalError:
        # Partial processing - some features disabled
        logger.warning("Falling back to partial processing")
        return partial_processing(data)
        
    except CriticalError:
        # Minimal processing - core functionality only
        logger.error("Falling back to minimal processing")
        return minimal_processing(data)
```

### Compensating Actions
```python
def transactional_job(data):
    """Job with compensating actions for rollback."""
    
    actions_taken = []
    
    try:
        # Step 1: Update database
        db_result = update_database(data)
        actions_taken.append(("database", db_result["id"]))
        
        # Step 2: Call external API
        api_result = call_external_api(data)
        actions_taken.append(("api", api_result["transaction_id"]))
        
        # Step 3: Send notification
        notify_result = send_notification(data)
        actions_taken.append(("notification", notify_result["message_id"]))
        
        return {"status": "success", "actions": actions_taken}
        
    except Exception as e:
        # Rollback all actions
        logger.error(f"Job failed, rolling back {len(actions_taken)} actions")
        
        for action_type, action_id in reversed(actions_taken):
            try:
                rollback_action(action_type, action_id)
                logger.info(f"Rolled back {action_type} action {action_id}")
            except Exception as rollback_error:
                logger.critical(f"Failed to rollback {action_type} action {action_id}: {rollback_error}")
        
        raise
```

## Error Testing

### Chaos Engineering
```python
import random

class ChaosMonkey:
    """Inject failures for testing error handling."""
    
    def __init__(self, failure_rate=0.1):
        self.failure_rate = failure_rate
    
    def maybe_fail(self, error_type=Exception, message="Chaos monkey strike!"):
        """Randomly inject failures."""
        if random.random() < self.failure_rate:
            raise error_type(message)
    
    def maybe_delay(self, max_delay=5):
        """Randomly inject delays."""
        if random.random() < self.failure_rate:
            delay = random.uniform(0, max_delay)
            time.sleep(delay)

# Use in job functions for testing
chaos = ChaosMonkey(failure_rate=0.1)

def chaotic_job(data):
    chaos.maybe_fail(ConnectionError, "Simulated network failure")
    chaos.maybe_delay(max_delay=3)
    
    return process_data(data)
```

### Error Simulation
```python
def simulate_error_scenarios():
    """Simulate various error scenarios for testing."""
    
    scenarios = [
        ("network_timeout", lambda: TimeoutError("Network timeout")),
        ("invalid_data", lambda: ValueError("Invalid input data")),
        ("auth_failure", lambda: AuthenticationError("Authentication failed")),
        ("rate_limit", lambda: RateLimitError("Rate limit exceeded")),
        ("system_overload", lambda: SystemError("System overloaded"))
    ]
    
    for scenario_name, error_factory in scenarios:
        try:
            enqueue_sync(
                error_prone_job,
                scenario=scenario_name,
                error=error_factory(),
                max_retries=3
            )
        except Exception as e:
            logger.info(f"Scenario '{scenario_name}' triggered expected error: {e}")
```

## Production Considerations

### Error Budget Management
```python
class ErrorBudget:
    """Manage error budgets for SLA compliance."""
    
    def __init__(self, target_success_rate=0.999):
        self.target_success_rate = target_success_rate
        self.error_budget = 1 - target_success_rate
    
    def check_budget(self, timeframe_hours=24):
        """Check if error budget is exceeded."""
        
        jobs = get_jobs_in_timeframe(hours=timeframe_hours)
        if not jobs:
            return True
        
        failed_jobs = [j for j in jobs if j.status == 'failed']
        current_error_rate = len(failed_jobs) / len(jobs)
        
        budget_remaining = self.error_budget - current_error_rate
        
        if budget_remaining < 0:
            logger.critical(f"Error budget exceeded! Current rate: {current_error_rate:.3%}, Budget: {self.error_budget:.3%}")
            return False
        
        if budget_remaining < self.error_budget * 0.1:  # 10% of budget remaining
            logger.warning(f"Error budget running low: {budget_remaining:.3%} remaining")
        
        return True
```

### Incident Response
```python
def handle_critical_error(error, context):
    """Handle critical errors with incident response."""
    
    incident_id = generate_incident_id()
    
    # Log incident
    logger.critical(f"INCIDENT {incident_id}: {error}")
    
    # Create incident record
    incident = {
        "id": incident_id,
        "timestamp": datetime.utcnow(),
        "error": str(error),
        "context": context,
        "status": "OPEN",
        "severity": "CRITICAL"
    }
    
    # Alert incident response team
    alert_incident_team(incident)
    
    # Start automated remediation
    start_automated_remediation(incident)
    
    return incident_id
```

## Next Steps

- Explore [configuration management](../04-configuration/) for environment-specific error handling
- Learn about [high availability](../../05-advanced/02-high-availability/) for system resilience
- Check out [performance optimization](../../05-advanced/03-performance-optimization/) for preventing resource-related errors
- Implement [monitoring dashboard](../02-monitoring-dashboard/) for error visualization