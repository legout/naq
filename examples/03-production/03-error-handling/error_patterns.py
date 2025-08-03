#!/usr/bin/env python3
"""
Comprehensive Error Handling Patterns

This example demonstrates production-ready error handling patterns for NAQ:
- Exception classification and handling
- Retry strategies with selective error handling
- Circuit breaker implementation
- Graceful degradation and fallback mechanisms
- Error context preservation and logging

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default error_queue --log-level DEBUG`
4. Run this script: `python error_patterns.py`
"""

import os
import time
import json
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from contextlib import contextmanager

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging with detailed formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Custom Exception Classes
class RetryableError(Exception):
    """Base class for retryable errors."""
    pass


class NonRetryableError(Exception):
    """Base class for non-retryable errors."""
    pass


class CriticalError(Exception):
    """Base class for critical errors requiring immediate attention."""
    pass


class NetworkError(RetryableError):
    """Network-related errors."""
    pass


class ServiceUnavailableError(RetryableError):
    """External service unavailable."""
    pass


class RateLimitError(RetryableError):
    """Rate limiting errors."""
    pass


class AuthenticationError(NonRetryableError):
    """Authentication failures."""
    pass


class ValidationError(NonRetryableError):
    """Data validation errors."""
    pass


class PermissionError(NonRetryableError):
    """Permission denied errors."""
    pass


class DataCorruptionError(CriticalError):
    """Data corruption detected."""
    pass


class SystemFailureError(CriticalError):
    """Critical system failures."""
    pass


@dataclass
class ErrorContext:
    """Structured error context information."""
    job_id: str
    function_name: str
    timestamp: str
    input_data: Dict[str, Any]
    attempt_number: int
    worker_id: Optional[str] = None
    queue_name: Optional[str] = None


class CircuitBreaker:
    """
    Circuit breaker implementation for external service calls.
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    @contextmanager
    def call(self):
        """Context manager for circuit breaker calls."""
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker moving to HALF_OPEN state")
            else:
                raise ServiceUnavailableError("Circuit breaker is OPEN")
        
        try:
            yield
            self._on_success()
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call."""
        self.failure_count = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker moved to CLOSED state")
    
    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker moved to OPEN state after {self.failure_count} failures")


class StructuredLogger:
    """
    Structured logging for error handling.
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
    
    def log_error(self, error: Exception, context: ErrorContext, severity: str = "ERROR"):
        """Log error with structured context."""
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "job_id": context.job_id,
            "function": context.function_name,
            "attempt": context.attempt_number,
            "worker_id": context.worker_id,
            "queue": context.queue_name,
            "input_data_keys": list(context.input_data.keys()) if context.input_data else []
        }
        
        # Add traceback for unexpected errors
        if not isinstance(error, (ValidationError, AuthenticationError, PermissionError)):
            import traceback
            log_entry["traceback"] = traceback.format_exc()
        
        self.logger.error(json.dumps(log_entry))
    
    def log_retry(self, error: Exception, context: ErrorContext, retry_count: int, next_retry: float):
        """Log retry attempt."""
        
        retry_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event": "retry_scheduled",
            "error_type": type(error).__name__,
            "job_id": context.job_id,
            "retry_count": retry_count,
            "next_retry_in_seconds": next_retry
        }
        
        self.logger.info(json.dumps(retry_entry))


# Global instances
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
error_logger = StructuredLogger("naq.error_handling")


def simulate_external_api_call(endpoint: str, timeout: float = 5.0) -> Dict[str, Any]:
    """
    Simulate external API call with various failure modes.
    
    Args:
        endpoint: API endpoint to call
        timeout: Request timeout
        
    Returns:
        API response data
        
    Raises:
        NetworkError: For network issues
        ServiceUnavailableError: For service unavailability
        RateLimitError: For rate limiting
        AuthenticationError: For auth issues
    """
    logger.info(f"📡 Calling external API: {endpoint}")
    
    # Simulate call delay
    time.sleep(random.uniform(0.5, 2.0))
    
    # Simulate various failure modes
    failure_type = random.random()
    
    if failure_type < 0.15:  # 15% network errors
        raise NetworkError(f"Network timeout connecting to {endpoint}")
    elif failure_type < 0.25:  # 10% service unavailable
        raise ServiceUnavailableError(f"Service at {endpoint} is temporarily unavailable")
    elif failure_type < 0.30:  # 5% rate limiting
        raise RateLimitError(f"Rate limit exceeded for {endpoint}")
    elif failure_type < 0.35:  # 5% auth errors
        raise AuthenticationError(f"Authentication failed for {endpoint}")
    else:  # 65% success
        return {
            "endpoint": endpoint,
            "status": "success",
            "data": f"Response from {endpoint}",
            "timestamp": datetime.now().isoformat()
        }


def robust_api_call_with_circuit_breaker(endpoint: str, job_id: str) -> Dict[str, Any]:
    """
    Make API call with circuit breaker protection.
    
    Args:
        endpoint: API endpoint
        job_id: Job identifier for context
        
    Returns:
        API response or fallback data
    """
    context = ErrorContext(
        job_id=job_id,
        function_name="robust_api_call_with_circuit_breaker",
        timestamp=datetime.now().isoformat(),
        input_data={"endpoint": endpoint},
        attempt_number=1
    )
    
    try:
        with circuit_breaker.call():
            result = simulate_external_api_call(endpoint)
            logger.info(f"✅ API call succeeded: {endpoint}")
            return result
            
    except ServiceUnavailableError as e:
        error_logger.log_error(e, context, "WARNING")
        
        # Fallback to cached data
        logger.warning(f"🔄 Circuit breaker triggered, using fallback for {endpoint}")
        return {
            "endpoint": endpoint,
            "status": "fallback",
            "data": f"Cached data for {endpoint}",
            "timestamp": datetime.now().isoformat(),
            "source": "cache"
        }
    
    except (NetworkError, RateLimitError) as e:
        error_logger.log_error(e, context, "WARNING")
        raise  # Let NAQ retry these
    
    except AuthenticationError as e:
        error_logger.log_error(e, context, "ERROR")
        raise  # Don't retry auth errors


def process_data_with_validation(data: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    """
    Process data with comprehensive validation and error handling.
    
    Args:
        data: Input data to process
        job_id: Job identifier
        
    Returns:
        Processing results
    """
    context = ErrorContext(
        job_id=job_id,
        function_name="process_data_with_validation",
        timestamp=datetime.now().isoformat(),
        input_data=data,
        attempt_number=1
    )
    
    logger.info(f"⚙️  Processing data for job {job_id}")
    
    try:
        # Input validation
        if not isinstance(data, dict):
            raise ValidationError("Input data must be a dictionary")
        
        required_fields = ['id', 'type', 'payload']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise ValidationError(f"Missing required fields: {missing_fields}")
        
        # Type validation
        if not isinstance(data['id'], (int, str)):
            raise ValidationError("Field 'id' must be int or string")
        
        if data['type'] not in ['process', 'analyze', 'export']:
            raise ValidationError(f"Invalid type '{data['type']}', must be one of: process, analyze, export")
        
        # Simulate processing
        processing_time = random.uniform(0.5, 3.0)
        time.sleep(processing_time)
        
        # Simulate occasional processing errors
        if random.random() < 0.10:  # 10% chance of processing error
            raise SystemFailureError("Processing system encountered an internal error")
        
        # Simulate data corruption detection
        if random.random() < 0.02:  # 2% chance of data corruption
            raise DataCorruptionError(f"Data corruption detected in record {data['id']}")
        
        result = {
            "job_id": job_id,
            "processed_id": data['id'],
            "type": data['type'],
            "result": f"Processed {data['type']} operation for {data['id']}",
            "processing_time": processing_time,
            "timestamp": datetime.now().isoformat(),
            "status": "completed"
        }
        
        logger.info(f"✅ Data processing completed for job {job_id}")
        return result
        
    except ValidationError as e:
        error_logger.log_error(e, context, "ERROR")
        raise  # Don't retry validation errors
    
    except DataCorruptionError as e:
        error_logger.log_error(e, context, "CRITICAL")
        # Send critical alert
        send_critical_alert(f"Data corruption in job {job_id}: {e}")
        raise  # Don't retry corruption errors
    
    except SystemFailureError as e:
        error_logger.log_error(e, context, "ERROR")
        raise  # Let NAQ retry system errors
    
    except Exception as e:
        error_logger.log_error(e, context, "ERROR")
        raise ProcessingError(f"Unexpected error in job {job_id}: {e}") from e


def multi_step_job_with_rollback(data: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    """
    Multi-step job with rollback capability.
    
    Args:
        data: Input data
        job_id: Job identifier
        
    Returns:
        Job results
    """
    context = ErrorContext(
        job_id=job_id,
        function_name="multi_step_job_with_rollback",
        timestamp=datetime.now().isoformat(),
        input_data=data,
        attempt_number=1
    )
    
    actions_taken = []
    
    try:
        logger.info(f"🔄 Starting multi-step job {job_id}")
        
        # Step 1: Database update
        logger.info(f"📝 Step 1: Updating database for {data['id']}")
        db_result = simulate_database_update(data)
        actions_taken.append(("database", db_result["transaction_id"]))
        time.sleep(0.5)
        
        # Step 2: External API call
        logger.info(f"📡 Step 2: External API call for {data['id']}")
        api_result = robust_api_call_with_circuit_breaker("/api/process", job_id)
        actions_taken.append(("api", api_result.get("transaction_id", "unknown")))
        time.sleep(0.5)
        
        # Step 3: File system operation
        logger.info(f"💾 Step 3: File system operation for {data['id']}")
        file_result = simulate_file_operation(data)
        actions_taken.append(("file", file_result["file_id"]))
        time.sleep(0.5)
        
        # Step 4: Notification
        logger.info(f"📧 Step 4: Sending notification for {data['id']}")
        notification_result = simulate_notification(data)
        actions_taken.append(("notification", notification_result["message_id"]))
        
        result = {
            "job_id": job_id,
            "status": "completed",
            "steps_completed": len(actions_taken),
            "actions": actions_taken,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"✅ Multi-step job {job_id} completed successfully")
        return result
        
    except Exception as e:
        error_logger.log_error(e, context, "ERROR")
        
        # Rollback all completed actions
        logger.warning(f"🔄 Rolling back {len(actions_taken)} actions for job {job_id}")
        
        rollback_errors = []
        for action_type, action_id in reversed(actions_taken):
            try:
                rollback_action(action_type, action_id)
                logger.info(f"✅ Rolled back {action_type} action {action_id}")
            except Exception as rollback_error:
                rollback_errors.append(f"{action_type}:{rollback_error}")
                error_logger.log_error(rollback_error, context, "CRITICAL")
        
        if rollback_errors:
            raise CriticalError(f"Rollback failures: {rollback_errors}") from e
        
        raise


def graceful_degradation_job(data: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    """
    Job with graceful degradation capabilities.
    
    Args:
        data: Input data
        job_id: Job identifier
        
    Returns:
        Processing results with degradation level
    """
    context = ErrorContext(
        job_id=job_id,
        function_name="graceful_degradation_job",
        timestamp=datetime.now().isoformat(),
        input_data=data,
        attempt_number=1
    )
    
    logger.info(f"🎯 Starting graceful degradation job {job_id}")
    
    try:
        # Attempt full processing
        logger.info("🚀 Attempting full processing...")
        result = full_processing(data)
        result["degradation_level"] = "none"
        result["features"] = ["full_analysis", "advanced_processing", "real_time_sync"]
        return result
        
    except CriticalError:
        error_logger.log_error(CriticalError("Full processing failed"), context, "ERROR")
        raise  # Don't degrade for critical errors
    
    except (NetworkError, ServiceUnavailableError):
        # Degrade to offline processing
        logger.warning("⚠️  Network issues, degrading to offline processing...")
        try:
            result = offline_processing(data)
            result["degradation_level"] = "offline"
            result["features"] = ["basic_analysis", "batch_processing"]
            return result
        except Exception as e:
            error_logger.log_error(e, context, "ERROR")
            
    except Exception:
        # Degrade to minimal processing
        logger.warning("⚠️  Multiple failures, degrading to minimal processing...")
        try:
            result = minimal_processing(data)
            result["degradation_level"] = "minimal"
            result["features"] = ["essential_only"]
            return result
        except Exception as e:
            error_logger.log_error(e, context, "CRITICAL")
            raise


# Simulation functions
def simulate_database_update(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate database update operation."""
    if random.random() < 0.05:  # 5% failure rate
        raise NetworkError("Database connection timeout")
    
    return {
        "transaction_id": f"tx_{random.randint(1000, 9999)}",
        "updated_records": 1,
        "timestamp": datetime.now().isoformat()
    }


def simulate_file_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate file system operation."""
    if random.random() < 0.08:  # 8% failure rate
        raise OSError("Disk space insufficient")
    
    return {
        "file_id": f"file_{random.randint(1000, 9999)}",
        "size_bytes": random.randint(1000, 50000),
        "path": f"/tmp/job_{data['id']}.dat"
    }


def simulate_notification(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate notification sending."""
    if random.random() < 0.03:  # 3% failure rate
        raise RateLimitError("Notification rate limit exceeded")
    
    return {
        "message_id": f"msg_{random.randint(1000, 9999)}",
        "recipient": "admin@example.com",
        "status": "sent"
    }


def rollback_action(action_type: str, action_id: str):
    """Simulate action rollback."""
    if random.random() < 0.05:  # 5% rollback failure rate
        raise Exception(f"Failed to rollback {action_type} action {action_id}")
    
    logger.info(f"🔄 Rolled back {action_type} action: {action_id}")


def full_processing(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate full processing."""
    if random.random() < 0.20:  # 20% failure rate
        raise ServiceUnavailableError("Full processing service unavailable")
    
    return {
        "result": "full_processing_complete",
        "quality": "high",
        "processing_time": 2.5
    }


def offline_processing(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate offline processing."""
    if random.random() < 0.10:  # 10% failure rate
        raise Exception("Offline processing failed")
    
    return {
        "result": "offline_processing_complete",
        "quality": "medium",
        "processing_time": 1.5
    }


def minimal_processing(data: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate minimal processing."""
    return {
        "result": "minimal_processing_complete",
        "quality": "basic",
        "processing_time": 0.5
    }


def send_critical_alert(message: str):
    """Simulate sending critical alert."""
    logger.critical(f"🚨 CRITICAL ALERT: {message}")
    # In production, this would send to PagerDuty, Slack, etc.


class ProcessingError(Exception):
    """Custom processing error."""
    pass


def demonstrate_retry_patterns():
    """Demonstrate various retry patterns."""
    print("📍 Retry Patterns Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Selective retry based on error type
        print("📤 Job 1: Selective retries (retries network errors, not auth errors)")
        job1 = client.enqueue(
            robust_api_call_with_circuit_breaker,
            endpoint="/api/users",
            job_id="retry_job_1",
            queue_name="error_queue",
            max_retries=4,
            retry_delay=2,
            retry_strategy="exponential",
            retry_on=(NetworkError, ServiceUnavailableError, RateLimitError),
            ignore_on=(AuthenticationError, ValidationError)
        )
        jobs.append(job1)
        print(f"  ✅ Enqueued: {job1.job_id}")
        
        # Data validation with no retries
        print("\n📤 Job 2: Data validation (no retries for validation errors)")
        job2 = client.enqueue(
            process_data_with_validation,
            data={"id": "test_123", "type": "process", "payload": {"key": "value"}},
            job_id="validation_job_2",
            queue_name="error_queue",
            max_retries=3,
            retry_delay=1,
            retry_on=(SystemFailureError,),
            ignore_on=(ValidationError, DataCorruptionError)
        )
        jobs.append(job2)
        print(f"  ✅ Enqueued: {job2.job_id}")
        
        return jobs


def demonstrate_circuit_breaker_pattern():
    """Demonstrate circuit breaker pattern."""
    print("\n📍 Circuit Breaker Pattern Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Multiple API calls that will likely trigger circuit breaker
        print("📤 Multiple API calls (circuit breaker will activate after failures)")
        
        for i in range(8):
            job = client.enqueue(
                robust_api_call_with_circuit_breaker,
                endpoint=f"/api/data/{i+1}",
                job_id=f"circuit_breaker_job_{i+1}",
                queue_name="error_queue",
                max_retries=2,
                retry_delay=1
            )
            jobs.append(job)
            print(f"  ✅ API call {i+1}: {job.job_id}")
        
        return jobs


def demonstrate_rollback_pattern():
    """Demonstrate rollback pattern."""
    print("\n📍 Multi-step Job with Rollback Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Multi-step jobs with rollback capability
        test_data = [
            {"id": "multi_001", "type": "process", "payload": {"action": "create"}},
            {"id": "multi_002", "type": "analyze", "payload": {"action": "update"}},
            {"id": "multi_003", "type": "export", "payload": {"action": "delete"}}
        ]
        
        print("📤 Multi-step jobs with rollback capability")
        for i, data in enumerate(test_data):
            job = client.enqueue(
                multi_step_job_with_rollback,
                data=data,
                job_id=f"rollback_job_{i+1}",
                queue_name="error_queue",
                max_retries=2,
                retry_delay=3
            )
            jobs.append(job)
            print(f"  ✅ Multi-step job {i+1}: {job.job_id}")
        
        return jobs


def demonstrate_graceful_degradation():
    """Demonstrate graceful degradation."""
    print("\n📍 Graceful Degradation Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Jobs that degrade gracefully when services are unavailable
        print("📤 Jobs with graceful degradation")
        
        degradation_data = [
            {"id": "degrade_001", "priority": "high"},
            {"id": "degrade_002", "priority": "medium"},
            {"id": "degrade_003", "priority": "low"}
        ]
        
        for i, data in enumerate(degradation_data):
            job = client.enqueue(
                graceful_degradation_job,
                data=data,
                job_id=f"degradation_job_{i+1}",
                queue_name="error_queue",
                max_retries=1,  # Limited retries for degradation
                retry_delay=2
            )
            jobs.append(job)
            print(f"  ✅ Degradation job {i+1}: {job.job_id}")
        
        return jobs


def main():
    """
    Main function demonstrating comprehensive error handling patterns.
    """
    print("🚀 NAQ Comprehensive Error Handling Demo")
    print("=" * 50)
    
    try:
        # Demonstrate different error handling patterns
        retry_jobs = demonstrate_retry_patterns()
        circuit_jobs = demonstrate_circuit_breaker_pattern()
        rollback_jobs = demonstrate_rollback_pattern()
        degradation_jobs = demonstrate_graceful_degradation()
        
        all_jobs = retry_jobs + circuit_jobs + rollback_jobs + degradation_jobs
        
        print(f"\n🎉 Enqueued {len(all_jobs)} jobs demonstrating error handling!")
        
        print("\n" + "=" * 50)
        print("📊 Error Handling Pattern Summary:")
        print("=" * 50)
        print(f"Retry patterns: {len(retry_jobs)} jobs")
        print(f"Circuit breaker: {len(circuit_jobs)} jobs")
        print(f"Rollback patterns: {len(rollback_jobs)} jobs")
        print(f"Graceful degradation: {len(degradation_jobs)} jobs")
        
        print("\n🎯 Error Handling Highlights:")
        print("   • Selective retries based on error type")
        print("   • Circuit breaker for external service protection")
        print("   • Multi-step jobs with rollback capability")
        print("   • Graceful degradation when services fail")
        print("   • Comprehensive error logging and monitoring")
        
        print("\n💡 Watch for these patterns in logs:")
        print("   • Structured error logging with context")
        print("   • Circuit breaker state changes")
        print("   • Rollback operations for failed multi-step jobs")
        print("   • Service degradation messages")
        print("   • Critical alerts for data corruption")
        
        print("\n📋 Production Error Handling Tips:")
        print("   • Classify errors by retry-ability")
        print("   • Implement circuit breakers for external services")
        print("   • Design rollback mechanisms for multi-step operations")
        print("   • Plan graceful degradation for service outages")
        print("   • Use structured logging for debugging")
        print("   • Monitor error rates and patterns")
        print("   • Set up appropriate alerting for critical errors")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default error_queue)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        print("   - Check worker logs for detailed error information")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())