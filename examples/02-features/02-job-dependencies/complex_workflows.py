#!/usr/bin/env python3
"""
Complex Workflow Examples

This example demonstrates advanced dependency patterns and real-world workflows:
- Multi-stage data pipelines
- Conditional job execution
- Dynamic workflow creation
- Parallel processing with synchronization points

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default workflow_queue pipeline_queue --log-level INFO`
4. Run this script: `python complex_workflows.py`
"""

import os
import time
import random
from typing import List, Dict, Any, Optional

import msgspec
from loguru import logger
from naq import NatsClient
from naq.config import NatsConfig, QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
logger.add(level="INFO")


class ExtractResult(msgspec.Struct):
    """Result of data extraction operation."""
    source_name: str
    source_type: str
    records_extracted: int
    extracted_at: float
    status: str


def extract_data_source(source_name: str, source_type: str) -> ExtractResult:
    """
    Extract data from various sources.
    
    Args:
        source_name: Name of the data source
        source_type: Type of source (database, api, file)
        
    Returns:
        Extracted data metadata
        
    Raises:
        ConnectionError: If connection to data source fails
    """
    logger.info(f"Extracting data from {source_type}: {source_name}")
    
    # Simulate extraction time based on source type
    extraction_times = {"database": 3, "api": 2, "file": 1}
    time.sleep(extraction_times.get(source_type, 2))
    
    # Simulate occasional failures for realistic testing
    if random.random() < 0.1:  # 10% failure rate
        raise ConnectionError(f"Failed to connect to {source_name}")
    
    result = ExtractResult(
        source_name=source_name,
        source_type=source_type,
        records_extracted=random.randint(1000, 10000),
        extracted_at=time.time(),
        status="success"
    )
    
    logger.info(f"Extracted {result.records_extracted} records from {source_name}")
    return result


class TransformResult(msgspec.Struct):
    """Result of data transformation operation."""
    dataset_name: str
    transformation_type: str
    input_records: int
    output_records: int
    transformed_at: float
    status: str


def transform_data(dataset_name: str, transformation_type: str, input_records: int) -> TransformResult:
    """
    Transform extracted data.
    
    Args:
        dataset_name: Name of the dataset
        transformation_type: Type of transformation
        input_records: Number of input records
        
    Returns:
        Transformation results
    """
    logger.info(f"Transforming {dataset_name} with {transformation_type}")
    
    # Simulate transformation time based on data size
    processing_time = input_records / 2000  # 2000 records per second
    time.sleep(min(processing_time, 5))  # Cap at 5 seconds for demo
    
    result = TransformResult(
        dataset_name=dataset_name,
        transformation_type=transformation_type,
        input_records=input_records,
        output_records=int(input_records * 0.95),  # Some data is filtered
        transformed_at=time.time(),
        status="success"
    )
    
    logger.info(f"Transformed {dataset_name}: {result.input_records} → {result.output_records} records")
    return result


class ValidationResult(msgspec.Struct):
    """Result of data validation operation."""
    dataset_name: str
    expected_records: int
    actual_records: int
    quality_score: float
    validated_at: float
    status: str


def validate_data_quality(dataset_name: str, expected_records: int) -> ValidationResult:
    """
    Validate data quality and completeness.
    
    Args:
        dataset_name: Name of the dataset to validate
        expected_records: Expected number of records
        
    Returns:
        Validation results
    """
    logger.info(f"Validating data quality for {dataset_name}")
    
    # Simulate validation time
    time.sleep(1)
    
    # Simulate validation results
    actual_records = expected_records + random.randint(-100, 100)
    quality_score = random.uniform(0.85, 1.0)
    
    result = ValidationResult(
        dataset_name=dataset_name,
        expected_records=expected_records,
        actual_records=actual_records,
        quality_score=quality_score,
        validated_at=time.time(),
        status="passed" if quality_score > 0.9 else "warning"
    )
    
    logger.info(f"Validation complete: {dataset_name} (quality: {quality_score:.2f})")
    return result


class LoadResult(msgspec.Struct):
    """Result of data loading operation."""
    target_table: str
    datasets_loaded: List[str]
    total_records: int
    loaded_at: float
    status: str


def load_to_warehouse(datasets: List[str], target_table: str) -> LoadResult:
    """
    Load transformed data to data warehouse.
    
    Args:
        datasets: List of datasets to load
        target_table: Target table in warehouse
        
    Returns:
        Load results
    """
    logger.info(f"Loading {len(datasets)} datasets to warehouse table: {target_table}")
    
    # Simulate loading time
    time.sleep(3)
    
    total_records = sum(random.randint(1000, 5000) for _ in datasets)
    
    result = LoadResult(
        target_table=target_table,
        datasets_loaded=datasets,
        total_records=total_records,
        loaded_at=time.time(),
        status="success"
    )
    
    logger.info(f"Loaded {total_records} records to {target_table}")
    return result


def train_ml_model(model_name: str, dataset_size: int, algorithm: str) -> Dict[str, Any]:
    """
    Train a machine learning model.
    
    Args:
        model_name: Name of the model
        dataset_size: Size of training dataset
        algorithm: ML algorithm to use
        
    Returns:
        Training results
    """
    print(f"🤖 Training ML model: {model_name} using {algorithm}")
    
    # Simulate training time based on dataset size
    training_time = dataset_size / 10000  # Scale with data size
    time.sleep(min(training_time, 8))  # Cap at 8 seconds for demo
    
    accuracy = random.uniform(0.75, 0.95)
    
    result = {
        "model_name": model_name,
        "algorithm": algorithm,
        "dataset_size": dataset_size,
        "accuracy": accuracy,
        "trained_at": time.time(),
        "status": "completed"
    }
    
    print(f"✅ Model {model_name} trained (accuracy: {accuracy:.2f})")
    return result


def evaluate_model(model_name: str, test_dataset_size: int) -> Dict[str, Any]:
    """
    Evaluate a trained model.
    
    Args:
        model_name: Name of the model to evaluate
        test_dataset_size: Size of test dataset
        
    Returns:
        Evaluation results
    """
    print(f"📊 Evaluating model: {model_name}")
    
    # Simulate evaluation time
    time.sleep(2)
    
    metrics = {
        "precision": random.uniform(0.8, 0.95),
        "recall": random.uniform(0.8, 0.95),
        "f1_score": random.uniform(0.8, 0.95)
    }
    
    result = {
        "model_name": model_name,
        "test_dataset_size": test_dataset_size,
        "metrics": metrics,
        "evaluated_at": time.time(),
        "status": "completed"
    }
    
    print(f"✅ Model {model_name} evaluated (F1: {metrics['f1_score']:.2f})")
    return result


def deploy_model(model_name: str, environment: str) -> Dict[str, Any]:
    """
    Deploy a model to production.
    
    Args:
        model_name: Name of the model to deploy
        environment: Target environment (staging, production)
        
    Returns:
        Deployment results
    """
    print(f"🚀 Deploying model {model_name} to {environment}")
    
    # Simulate deployment time
    time.sleep(3)
    
    result = {
        "model_name": model_name,
        "environment": environment,
        "endpoint_url": f"https://{environment}.ml-api.com/models/{model_name}",
        "deployed_at": time.time(),
        "status": "deployed"
    }
    
    print(f"✅ Model {model_name} deployed to {environment}")
    return result


def send_notification(recipients: List[str], subject: str, workflow_name: str) -> str:
    """
    Send workflow completion notification.
    
    Args:
        recipients: List of email recipients
        subject: Email subject
        workflow_name: Name of completed workflow
        
    Returns:
        Notification status
    """
    print(f"📧 Sending notification: {subject}")
    
    # Simulate email sending
    time.sleep(1)
    
    print(f"✅ Notification sent to {len(recipients)} recipients")
    return f"Notification sent for {workflow_name}"


def create_etl_pipeline() -> List[Any]:
    """
    Create a complex ETL (Extract, Transform, Load) pipeline.
    
    Returns:
        List of created jobs
    """
    logger.info("ETL Pipeline Workflow")
    
    # Create configuration for pipeline queue
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5,
        max_reconnect_attempts=3
    )
    
    pipeline_queue_config = QueueConfig(
        name="pipeline_queue",
        stream_name="NAQ_JOBS",
        consumer_name="complex_workflows_consumer"
    )
    
    workflow_queue_config = QueueConfig(
        name="workflow_queue",
        stream_name="NAQ_JOBS",
        consumer_name="complex_workflows_consumer"
    )
    
    with NatsClient(nats_config=nats_config, queue_config=pipeline_queue_config) as client:
        jobs = []
        
        # Stage 1: Extract data from multiple sources
        logger.info("Stage 1: Data Extraction (Parallel)")
        sources = [
            ("user_database", "database"),
            ("sales_api", "api"),
            ("product_files", "file"),
            ("analytics_database", "database")
        ]
        
        extract_jobs = []
        for source_name, source_type in sources:
            job = client.enqueue(
                extract_data_source,
                source_name=source_name,
                source_type=source_type,
                max_retries=3,
                retry_delay=5,
                retry_on=(ConnectionError,)
            )
            extract_jobs.append(job)
            jobs.append(job)
            logger.info(f"Extract {source_name}: {job.job_id}")
        
        # Stage 2: Transform data (depends on extraction)
        logger.info("Stage 2: Data Transformation (Parallel after extraction)")
        transform_jobs = []
        transformations = [
            ("user_data", "normalization"),
            ("sales_data", "aggregation"),
            ("product_data", "enrichment"),
            ("analytics_data", "filtering")
        ]
        
        for i, (dataset_name, transform_type) in enumerate(transformations):
            job = client.enqueue(
                transform_data,
                dataset_name=dataset_name,
                transformation_type=transform_type,
                input_records=5000,  # Default for demo
                depends_on=[extract_jobs[i]]  # Each transform depends on its extract job
            )
            transform_jobs.append(job)
            jobs.append(job)
            logger.info(f"Transform {dataset_name}: {job.job_id}")
        
        # Stage 3: Validate data quality (depends on transformation)
        logger.info("Stage 3: Data Quality Validation")
        validate_jobs = []
        for i, (dataset_name, _) in enumerate(transformations):
            job = client.enqueue(
                validate_data_quality,
                dataset_name=dataset_name,
                expected_records=4750,  # After 5% filtering
                depends_on=[transform_jobs[i]]
            )
            validate_jobs.append(job)
            jobs.append(job)
            logger.info(f"Validate {dataset_name}: {job.job_id}")
        
        # Stage 4: Load to warehouse (depends on all validations)
        logger.info("Stage 4: Load to Data Warehouse")
        load_job = client.enqueue(
            load_to_warehouse,
            datasets=[name for name, _ in transformations],
            target_table="main_warehouse",
            depends_on=validate_jobs
        )
        jobs.append(load_job)
        logger.info(f"Load to warehouse: {load_job.job_id}")
        
        # Stage 5: Send completion notification
        logger.info("Stage 5: Pipeline Completion Notification")
        # Switch to workflow queue for notification
        client.queue_config = workflow_queue_config
        notification_job = client.enqueue(
            send_notification,
            recipients=["admin@company.com", "data-team@company.com"],
            subject="ETL Pipeline Completed",
            workflow_name="Daily ETL Pipeline",
            depends_on=[load_job],
            run_after_failure=True  # Send notification even if load fails
        )
        # Switch back to pipeline queue
        client.queue_config = pipeline_queue_config
        jobs.append(notification_job)
        logger.info(f"Notification job: {notification_job.job_id}")
        
        return jobs


def create_ml_training_pipeline() -> List[Any]:
    """
    Create a machine learning training and deployment pipeline.
    
    Returns:
        List of created jobs
    """
    logger.info("ML Training Pipeline Workflow")
    
    # Create configuration for pipeline queue
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5,
        max_reconnect_attempts=3
    )
    
    pipeline_queue_config = QueueConfig(
        name="pipeline_queue",
        stream_name="NAQ_JOBS",
        consumer_name="complex_workflows_consumer"
    )
    
    workflow_queue_config = QueueConfig(
        name="workflow_queue",
        stream_name="NAQ_JOBS",
        consumer_name="complex_workflows_consumer"
    )
    
    with NatsClient(nats_config=nats_config, queue_config=pipeline_queue_config) as client:
        jobs = []
        
        # Stage 1: Prepare training data
        logger.info("Stage 1: Data Preparation")
        data_prep_job = client.enqueue(
            extract_data_source,
            source_name="ml_training_data",
            source_type="database"
        )
        jobs.append(data_prep_job)
        logger.info(f"Data preparation: {data_prep_job.job_id}")
        
        # Stage 2: Feature engineering
        logger.info("Stage 2: Feature Engineering")
        feature_job = client.enqueue(
            transform_data,
            dataset_name="ml_features",
            transformation_type="feature_engineering",
            input_records=10000,
            depends_on=[data_prep_job]
        )
        jobs.append(feature_job)
        logger.info(f"Feature engineering: {feature_job.job_id}")
        
        # Stage 3: Train multiple models in parallel
        logger.info("Stage 3: Model Training (Parallel)")
        algorithms = ["random_forest", "gradient_boosting", "neural_network"]
        training_jobs = []
        
        for algorithm in algorithms:
            job = client.enqueue(
                train_ml_model,
                model_name=f"model_{algorithm}",
                dataset_size=10000,
                algorithm=algorithm,
                depends_on=[feature_job]
            )
            training_jobs.append(job)
            jobs.append(job)
            logger.info(f"Train {algorithm}: {job.job_id}")
        
        # Stage 4: Evaluate all models
        logger.info("Stage 4: Model Evaluation")
        evaluation_jobs = []
        
        for i, algorithm in enumerate(algorithms):
            job = client.enqueue(
                evaluate_model,
                model_name=f"model_{algorithm}",
                test_dataset_size=2000,
                depends_on=[training_jobs[i]]
            )
            evaluation_jobs.append(job)
            jobs.append(job)
            logger.info(f"Evaluate {algorithm}: {job.job_id}")
        
        # Stage 5: Deploy best model (depends on all evaluations)
        logger.info("Stage 5: Model Deployment")
        deployment_job = client.enqueue(
            deploy_model,
            model_name="best_model",  # Would be selected based on evaluation
            environment="production",
            depends_on=evaluation_jobs
        )
        jobs.append(deployment_job)
        logger.info(f"Deploy model: {deployment_job.job_id}")
        
        # Stage 6: Send deployment notification
        logger.info("Stage 6: Deployment Notification")
        # Switch to workflow queue for notification
        client.queue_config = workflow_queue_config
        notification_job = client.enqueue(
            send_notification,
            recipients=["ml-team@company.com", "devops@company.com"],
            subject="ML Model Deployed",
            workflow_name="ML Training Pipeline",
            depends_on=[deployment_job]
        )
        # Switch back to pipeline queue
        client.queue_config = pipeline_queue_config
        jobs.append(notification_job)
        logger.info(f"Notification job: {notification_job.job_id}")
        
        return jobs


def create_conditional_workflow():
    """
    Create a workflow with conditional execution paths.
    """
    print("\n📍 Conditional Workflow")
    print("-" * 40)
    
    with NatsClient(nats_config=nats_config, queue_config=workflow_queue_config) as client:
        jobs = []
        
        # Stage 1: Initial data validation
        print("📤 Stage 1: Initial Data Validation")
        validation_job = client.enqueue(
            validate_data_quality,
            dataset_name="input_data",
            expected_records=10000
        )
        jobs.append(validation_job)
        print(f"  ✅ Data validation: {validation_job.job_id}")
        
        # Stage 2a: Success path - process valid data
        print("\n📤 Stage 2a: Success Path - Process Valid Data")
        success_processing = client.enqueue(
            transform_data,
            dataset_name="input_data",
            transformation_type="full_processing",
            input_records=10000,
            depends_on=[validation_job]
        )
        jobs.append(success_processing)
        print(f"  ✅ Success processing: {success_processing.job_id}")
        
        # Stage 2b: Failure path - handle invalid data
        print("\n📤 Stage 2b: Failure Path - Handle Invalid Data")
        failure_handling = client.enqueue(
            transform_data,
            dataset_name="input_data",
            transformation_type="error_recovery",
            input_records=10000,
            depends_on=[validation_job],
            run_after_failure=True  # Run if validation fails
        )
        jobs.append(failure_handling)
        print(f"  ✅ Failure handling: {failure_handling.job_id}")
        
        # Stage 3: Final notification (runs regardless of path taken)
        print("\n📤 Stage 3: Final Notification")
        final_notification = client.enqueue(
            send_notification,
            recipients=["data-team@company.com"],
            subject="Conditional Workflow Completed",
            workflow_name="Conditional Processing",
            depends_on=[success_processing, failure_handling],
            run_after_failure=True
        )
        jobs.append(final_notification)
        print(f"  ✅ Final notification: {final_notification.job_id}")
        
        return jobs


def main():
    """
    Main function demonstrating complex workflow patterns.
    """
    print("🚀 NAQ Complex Workflows Demo")
    print("=" * 50)
    
    try:
        # Create different types of complex workflows
        etl_jobs = create_etl_pipeline()
        ml_jobs = create_ml_training_pipeline()
        conditional_jobs = create_conditional_workflow()
        
        all_jobs = etl_jobs + ml_jobs + conditional_jobs
        
        print(f"\n🎉 Created {len(all_jobs)} jobs across 3 complex workflows!")
        
        print("\n" + "=" * 50)
        print("📊 Complex Workflow Summary:")
        print("=" * 50)
        print(f"ETL Pipeline: {len(etl_jobs)} jobs")
        print(f"ML Training Pipeline: {len(ml_jobs)} jobs")
        print(f"Conditional Workflow: {len(conditional_jobs)} jobs")
        
        print("\n🎯 Workflow Highlights:")
        print("   • ETL: Multi-stage data processing with validation")
        print("   • ML: Parallel model training with evaluation")
        print("   • Conditional: Success/failure path handling")
        print("   • Notifications: Always-run completion alerts")
        
        print("\n💡 Advanced Patterns Demonstrated:")
        print("   • Multi-stage pipelines with dependencies")
        print("   • Parallel processing with synchronization")
        print("   • Conditional execution paths")
        print("   • Retry strategies for transient failures")
        print("   • Always-run cleanup and notification jobs")
        
        print("\n📊 Monitoring Recommendations:")
        print("   • Use 'naq dashboard' for visual workflow tracking")
        print("   • Monitor job completion times by stage")
        print("   • Set up alerts for workflow failures")
        print("   • Track resource usage during parallel stages")
        
        print("\n📋 Production Considerations:")
        print("   • Implement circuit breakers for external services")
        print("   • Use separate queues for different workflow types")
        print("   • Monitor dependency chain lengths")
        print("   • Plan for partial workflow recovery")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running for both queues?")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())