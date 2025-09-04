#!/usr/bin/env python3
"""
Dependency Failure Handling

This example demonstrates how NAQ handles failures in job dependency chains:
- Dependency failure propagation
- Cleanup jobs that always run
- Partial failure recovery
- Circuit breaker patterns

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default workflow_queue --log-level INFO`
4. Run this script: `python failure_handling.py`
"""

import os
import time
import random
from typing import List, Dict, Any

import msgspec
from loguru import logger
from naq import NatsClient
from naq.config import NatsConfig, QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
logger.add(level="INFO")


class DataProcessingError(Exception):
    """Custom exception for data processing failures."""
    pass


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


class NetworkError(Exception):
    """Custom exception for network-related failures."""
    pass


class ExtractionResult(msgspec.Struct):
    """Result of data extraction operation."""
    source_name: str
    records_extracted: int
    extracted_at: float
    status: str


def flaky_data_extraction(source_name: str, failure_rate: float = 0.5) -> ExtractionResult:
    """
    Simulate data extraction with configurable failure rate.
    
    Args:
        source_name: Name of the data source
        failure_rate: Probability of failure (0.0 to 1.0)
        
    Returns:
        Extraction results
        
    Raises:
        NetworkError: When simulating network failures
    """
    logger.info(f"Extracting data from {source_name} (failure rate: {failure_rate:.0%})")
    
    # Simulate extraction time
    time.sleep(1)
    
    if random.random() < failure_rate:
        logger.error(f"Data extraction from {source_name} failed!")
        raise NetworkError(f"Network connection to {source_name} failed")
    
    result = ExtractionResult(
        source_name=source_name,
        records_extracted=random.randint(1000, 5000),
        extracted_at=time.time(),
        status="success"
    )
    
    logger.info(f"Successfully extracted {result.records_extracted} records from {source_name}")
    return result


class ProcessingResult(msgspec.Struct):
    """Result of data processing operation."""
    dataset_name: str
    operation: str
    processed_at: float
    records_processed: int
    status: str


def unreliable_processing(dataset_name: str, operation: str, failure_rate: float = 0.3) -> ProcessingResult:
    """
    Simulate data processing with potential failures.
    
    Args:
        dataset_name: Name of the dataset to process
        operation: Type of processing operation
        failure_rate: Probability of failure
        
    Returns:
        Processing results
        
    Raises:
        DataProcessingError: When processing fails
    """
    logger.info(f"Processing {dataset_name} with {operation} (failure rate: {failure_rate:.0%})")
    
    # Simulate processing time
    time.sleep(2)
    
    if random.random() < failure_rate:
        logger.error(f"Processing {dataset_name} failed!")
        raise DataProcessingError(f"Processing failed during {operation}")
    
    result = ProcessingResult(
        dataset_name=dataset_name,
        operation=operation,
        processed_at=time.time(),
        records_processed=random.randint(800, 1200),
        status="completed"
    )
    
    logger.info(f"Successfully processed {dataset_name} with {operation}")
    return result


class ValidationResult(msgspec.Struct):
    """Result of data validation operation."""
    dataset_name: str
    actual_records: int
    min_records: int
    validated_at: float
    status: str


def strict_validation(dataset_name: str, min_records: int) -> ValidationResult:
    """
    Simulate strict data validation that may fail.
    
    Args:
        dataset_name: Name of the dataset to validate
        min_records: Minimum required number of records
        
    Returns:
        Validation results
        
    Raises:
        ValidationError: When validation fails
    """
    logger.info(f"Validating {dataset_name} (minimum {min_records} records required)")
    
    # Simulate validation time
    time.sleep(1)
    
    actual_records = random.randint(min_records - 200, min_records + 200)
    
    if actual_records < min_records:
        logger.error(f"Validation failed: {actual_records} < {min_records} records")
        raise ValidationError(f"Insufficient records: {actual_records} < {min_records}")
    
    result = ValidationResult(
        dataset_name=dataset_name,
        actual_records=actual_records,
        min_records=min_records,
        validated_at=time.time(),
        status="passed"
    )
    
    logger.info(f"Validation passed: {actual_records} records found")
    return result


def cleanup_temporary_data(dataset_names: List[str]) -> str:
    """
    Cleanup temporary files and data (always runs).
    
    Args:
        dataset_names: List of datasets to clean up
        
    Returns:
        Cleanup status
    """
    logger.info(f"Cleaning up temporary data for {len(dataset_names)} datasets")
    
    # Simulate cleanup time
    time.sleep(1)
    
    logger.info("Temporary data cleaned up successfully")
    return f"Cleaned up {len(dataset_names)} datasets"


def send_failure_alert(workflow_name: str, failed_job_ids: List[str]) -> str:
    """
    Send alert notification for workflow failures.
    
    Args:
        workflow_name: Name of the failed workflow
        failed_job_ids: List of failed job IDs
        
    Returns:
        Alert status
    """
    logger.warning(f"Sending failure alert for workflow: {workflow_name}")
    
    # Simulate alert sending
    time.sleep(0.5)
    
    logger.info(f"Failure alert sent for {len(failed_job_ids)} failed jobs")
    return f"Alert sent for {workflow_name}"


class FallbackResult(msgspec.Struct):
    """Result of fallback processing operation."""
    dataset_name: str
    fallback_method: str
    processed_at: float
    records_processed: int
    status: str


def fallback_processing(dataset_name: str, fallback_method: str) -> FallbackResult:
    """
    Perform fallback processing when primary processing fails.
    
    Args:
        dataset_name: Name of the dataset
        fallback_method: Type of fallback processing
        
    Returns:
        Fallback processing results
    """
    logger.info(f"Performing fallback processing for {dataset_name} using {fallback_method}")
    
    # Simulate fallback processing
    time.sleep(1.5)
    
    result = FallbackResult(
        dataset_name=dataset_name,
        fallback_method=fallback_method,
        processed_at=time.time(),
        records_processed=random.randint(500, 800),  # Lower yield than primary
        status="fallback_completed"
    )
    
    logger.info(f"Fallback processing completed for {dataset_name}")
    return result


def demonstrate_failure_propagation() -> List[Any]:
    """
    Demonstrate how failures propagate through dependency chains.
    
    Returns:
        List of created jobs
    """
    logger.info("Failure Propagation Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure workflow queue
    queue_config = QueueConfig(
        name="workflow_queue",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Job 1: High failure rate extraction
        logger.info("Job 1: Flaky data extraction (70% failure rate)")
        extraction_job = client.enqueue(
            flaky_data_extraction,
            source_name="unreliable_source",
            failure_rate=0.7,  # High failure rate
            max_retries=2,
            retry_delay=3
        )
        jobs.append(extraction_job)
        logger.info(f"Enqueued: {extraction_job.job_id}")
        
        # Job 2: Processing (depends on extraction)
        logger.info("Job 2: Processing (depends on Job 1 - will NOT run if Job 1 fails)")
        processing_job = client.enqueue(
            unreliable_processing,
            dataset_name="unreliable_data",
            operation="transformation",
            failure_rate=0.2,
            depends_on=[extraction_job]
        )
        jobs.append(processing_job)
        logger.info(f"Enqueued: {processing_job.job_id}")
        logger.info(f"Depends on: {extraction_job.job_id}")
        
        # Job 3: Validation (depends on processing)
        logger.info("Job 3: Validation (depends on Job 2 - will NOT run if Job 1 or 2 fails)")
        validation_job = client.enqueue(
            strict_validation,
            dataset_name="unreliable_data",
            min_records=1000,
            depends_on=[processing_job]
        )
        jobs.append(validation_job)
        logger.info(f"Enqueued: {validation_job.job_id}")
        logger.info(f"Depends on: {processing_job.job_id}")
        
        # Job 4: Cleanup (always runs regardless of failures)
        logger.info("Job 4: Cleanup (ALWAYS runs, even if dependencies fail)")
        cleanup_job = client.enqueue(
            cleanup_temporary_data,
            dataset_names=["unreliable_data"],
            depends_on=[extraction_job],
            run_after_failure=True  # This makes it run even if dependencies fail
        )
        jobs.append(cleanup_job)
        logger.info(f"Enqueued: {cleanup_job.job_id}")
        logger.info(f"Depends on: {extraction_job.job_id} (runs after success OR failure)")
        
        return jobs


def demonstrate_failure_recovery() -> List[Any]:
    """
    Demonstrate recovery patterns when jobs fail.
    
    Returns:
        List of created jobs
    """
    logger.info("Failure Recovery Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure workflow queue
    queue_config = QueueConfig(
        name="workflow_queue",
        job_timeout=30.0,
        max_retries=1,
        retry_delay=3.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Primary processing path
        logger.info("Primary Processing Path:")
        
        # Job 1: Reliable extraction
        extraction_job = client.enqueue(
            flaky_data_extraction,
            source_name="primary_source",
            failure_rate=0.1,  # Low failure rate
        )
        jobs.append(extraction_job)
        logger.info(f"Primary extraction: {extraction_job.job_id}")
        
        # Job 2: Primary processing (medium failure rate)
        primary_processing = client.enqueue(
            unreliable_processing,
            dataset_name="primary_data",
            operation="advanced_processing",
            failure_rate=0.6,  # High failure rate
            depends_on=[extraction_job],
            max_retries=1  # Limited retries
        )
        jobs.append(primary_processing)
        logger.info(f"Primary processing: {primary_processing.job_id}")
        
        # Fallback processing path
        logger.info("Fallback Processing Path:")
        
        # Job 3: Fallback processing (runs if primary fails)
        fallback_processing_job = client.enqueue(
            fallback_processing,
            dataset_name="primary_data",
            fallback_method="simple_processing",
            depends_on=[primary_processing],
            run_after_failure=True  # Run when primary processing fails
        )
        jobs.append(fallback_processing_job)
        logger.info(f"Fallback processing: {fallback_processing_job.job_id}")
        logger.info(f"Runs when primary processing fails")
        
        # Final validation (runs after either primary or fallback)
        logger.info("Final Validation (runs after either path succeeds):")
        final_validation = client.enqueue(
            strict_validation,
            dataset_name="final_data",
            min_records=500,  # Lower threshold for fallback
            depends_on=[primary_processing, fallback_processing_job],
            run_after_failure=True  # Run after either succeeds
        )
        jobs.append(final_validation)
        logger.info(f"Final validation: {final_validation.job_id}")
        logger.info(f"Runs after either primary or fallback completes")
        
        return jobs


def demonstrate_partial_failure_handling() -> List[Any]:
    """
    Demonstrate handling of partial failures in parallel workflows.
    
    Returns:
        List of created jobs
    """
    logger.info("Partial Failure Handling Demo")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure workflow queue
    queue_config = QueueConfig(
        name="workflow_queue",
        job_timeout=30.0,
        max_retries=3,
        retry_delay=2.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Multiple parallel extractions with different failure rates
        logger.info("Parallel Extractions (different failure rates):")
        sources = [
            ("reliable_source", 0.1),    # 10% failure rate
            ("medium_source", 0.4),      # 40% failure rate
            ("unreliable_source", 0.8),  # 80% failure rate
            ("backup_source", 0.2)       # 20% failure rate
        ]
        
        extraction_jobs = []
        for source_name, failure_rate in sources:
            job = client.enqueue(
                flaky_data_extraction,
                source_name=source_name,
                failure_rate=failure_rate,
                max_retries=2,
                retry_delay=2
            )
            extraction_jobs.append(job)
            jobs.append(job)
            logger.info(f"{source_name}: {job.job_id} ({failure_rate:.0%} failure rate)")
        
        # Processing that can work with partial data
        logger.info("Partial Data Processing (works with available data):")
        partial_processing = client.enqueue(
            unreliable_processing,
            dataset_name="combined_data",
            operation="partial_aggregation",
            failure_rate=0.1,  # Low failure since it's more tolerant
            depends_on=extraction_jobs,
            run_after_failure=True,  # Process whatever data is available
            max_retries=3
        )
        jobs.append(partial_processing)
        logger.info(f"Partial processing: {partial_processing.job_id}")
        logger.info(f"Processes data from successful extractions only")
        
        # Alert for failed extractions
        logger.info("Failure Alert (notifies about any extraction failures):")
        alert_job = client.enqueue(
            send_failure_alert,
            workflow_name="Partial Failure Demo",
            failed_job_ids=[job.job_id for job in extraction_jobs],
            depends_on=extraction_jobs,
            run_after_failure=True  # Always send alert
        )
        jobs.append(alert_job)
        logger.info(f"Failure alert: {alert_job.job_id}")
        logger.info(f"Sends alert if any extractions fail")
        
        return jobs


def demonstrate_circuit_breaker_pattern():
    """
    Demonstrate circuit breaker pattern for external service failures.
    """
    print("\n📍 Circuit Breaker Pattern Demo")
    print("-" * 40)
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure workflow queue
    queue_config = QueueConfig(
        name="workflow_queue",
        job_timeout=30.0,
        max_retries=1,
        retry_delay=1.0
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        jobs = []
        
        # Multiple attempts to call failing service
        print("📤 External Service Calls (circuit breaker simulation):")
        
        # First few jobs will likely fail
        service_jobs = []
        for i in range(5):
            job = client.enqueue(
                flaky_data_extraction,
                source_name=f"external_service_call_{i+1}",
                failure_rate=0.9,  # Very high failure rate
                max_retries=1,  # Limited retries for circuit breaker
                retry_delay=1
            )
            service_jobs.append(job)
            jobs.append(job)
            print(f"  ✅ Service call {i+1}: {job.job_id}")
        
        # Fallback to local processing
        print("\n📤 Fallback to Local Processing:")
        local_fallback_jobs = []
        for i, service_job in enumerate(service_jobs):
            fallback_job = client.enqueue(
                fallback_processing,
                dataset_name=f"local_data_{i+1}",
                fallback_method="local_cache",
                depends_on=[service_job],
                run_after_failure=True  # Use local data when service fails
            )
            local_fallback_jobs.append(fallback_job)
            jobs.append(fallback_job)
            print(f"  ✅ Local fallback {i+1}: {fallback_job.job_id}")
        
        # Aggregate results from available sources
        print("\n📤 Result Aggregation (from available sources):")
        aggregation_job = client.enqueue(
            unreliable_processing,
            dataset_name="aggregated_results",
            operation="final_aggregation",
            failure_rate=0.1,
            depends_on=service_jobs + local_fallback_jobs,
            run_after_failure=True  # Aggregate whatever is available
        )
        jobs.append(aggregation_job)
        print(f"  ✅ Aggregation: {aggregation_job.job_id}")
        print(f"  📊 Combines results from successful calls and fallbacks")
        
        return jobs


def main():
    """
    Main function demonstrating failure handling patterns.
    """
    print("🚀 NAQ Dependency Failure Handling Demo")
    print("=" * 50)
    
    try:
        # Demonstrate different failure handling patterns
        propagation_jobs = demonstrate_failure_propagation()
        recovery_jobs = demonstrate_failure_recovery()
        partial_jobs = demonstrate_partial_failure_handling()
        circuit_jobs = demonstrate_circuit_breaker_pattern()
        
        all_jobs = propagation_jobs + recovery_jobs + partial_jobs + circuit_jobs
        
        print(f"\n🎉 Created {len(all_jobs)} jobs demonstrating failure handling!")
        
        print("\n" + "=" * 50)
        print("📊 Failure Handling Pattern Summary:")
        print("=" * 50)
        print(f"Failure propagation: {len(propagation_jobs)} jobs")
        print(f"Recovery patterns: {len(recovery_jobs)} jobs")
        print(f"Partial failures: {len(partial_jobs)} jobs")
        print(f"Circuit breaker: {len(circuit_jobs)} jobs")
        
        print("\n🎯 Failure Handling Highlights:")
        print("   • Propagation: Failed dependencies prevent downstream jobs")
        print("   • Recovery: Fallback jobs run when primary jobs fail")
        print("   • Partial: Process available data from successful jobs")
        print("   • Circuit breaker: Fallback to local processing")
        print("   • Always-run: Cleanup and alerts run regardless of failures")
        
        print("\n💡 Watch for these patterns:")
        print("   • Jobs blocked by failed dependencies")
        print("   • Cleanup jobs running after failures")
        print("   • Fallback jobs activating when primary jobs fail")
        print("   • Partial processing with available data")
        print("   • Alert notifications for failed workflows")
        
        print("\n📋 Production Failure Handling Tips:")
        print("   • Use run_after_failure=True for cleanup and alerts")
        print("   • Implement fallback processing for critical workflows")
        print("   • Design for partial success scenarios")
        print("   • Monitor failure rates and adjust retry strategies")
        print("   • Set up proper alerting for workflow failures")
        
        print("\n🔧 Debugging Failed Workflows:")
        print("   • Check worker logs for failure details")
        print("   • Use 'naq dashboard' to visualize failed dependencies")
        print("   • Monitor retry attempts and success rates")
        print("   • Review dependency chains for bottlenecks")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default workflow_queue)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())