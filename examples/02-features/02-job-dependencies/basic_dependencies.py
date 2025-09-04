#!/usr/bin/env python3
"""
Basic Job Dependencies

This example demonstrates fundamental dependency patterns in NAQ:
- Sequential job execution
- Parallel jobs with convergence
- Simple dependency chains
- Basic error handling

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start worker: `naq worker default workflow_queue --log-level INFO`
4. Run this script: `python basic_dependencies.py`
"""

import os
import time
from typing import List, Dict, Any

from loguru import logger

from naq import NatsClient, setup_logging
from naq.config import NatsConfig, QueueConfig

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


def prepare_data(dataset_name: str, size: int) -> Dict[str, Any]:
    """
    Simulate data preparation step.
    
    Args:
        dataset_name: Name of the dataset to prepare
        size: Size of the dataset in records
        
    Returns:
        Dataset information
    """
    logger.info(f"📊 Preparing dataset: {dataset_name} ({size} records)")
    
    # Simulate data preparation time
    time.sleep(2)
    
    result = {
        "dataset_name": dataset_name,
        "size": size,
        "prepared_at": time.time(),
        "status": "ready"
    }
    
    logger.success(f"✅ Dataset {dataset_name} prepared successfully")
    return result


def process_data(dataset_name: str, operation: str) -> Dict[str, Any]:
    """
    Simulate data processing step that depends on data preparation.
    
    Args:
        dataset_name: Name of the dataset to process
        operation: Type of processing operation
        
    Returns:
        Processing results
    """
    logger.info(f"⚙️  Processing dataset: {dataset_name} with operation: {operation}")
    
    # Simulate processing time
    time.sleep(3)
    
    result = {
        "dataset_name": dataset_name,
        "operation": operation,
        "processed_at": time.time(),
        "records_processed": 1000,
        "status": "completed"
    }
    
    logger.success(f"✅ Dataset {dataset_name} processed with {operation}")
    return result


def validate_results(dataset_name: str, expected_records: int) -> Dict[str, Any]:
    """
    Simulate result validation step.
    
    Args:
        dataset_name: Name of the dataset to validate
        expected_records: Expected number of records
        
    Returns:
        Validation results
    """
    logger.info(f"🔍 Validating results for dataset: {dataset_name}")
    
    # Simulate validation time
    time.sleep(1)
    
    result = {
        "dataset_name": dataset_name,
        "expected_records": expected_records,
        "actual_records": expected_records,  # Assume validation passes
        "validated_at": time.time(),
        "status": "valid"
    }
    
    logger.success(f"✅ Validation passed for {dataset_name}")
    return result


def generate_report(datasets: List[str], report_type: str) -> Dict[str, Any]:
    """
    Simulate report generation that depends on multiple datasets.
    
    Args:
        datasets: List of dataset names to include
        report_type: Type of report to generate
        
    Returns:
        Report information
    """
    logger.info(f"📋 Generating {report_type} report from {len(datasets)} datasets")
    
    # Simulate report generation time
    time.sleep(2)
    
    result = {
        "report_type": report_type,
        "datasets": datasets,
        "generated_at": time.time(),
        "pages": len(datasets) * 10,
        "status": "completed"
    }
    
    logger.success(f"✅ Report generated: {report_type} ({result['pages']} pages)")
    return result


def cleanup_temp_files(job_ids: List[str]) -> str:
    """
    Simulate cleanup operation that always runs.
    
    Args:
        job_ids: List of job IDs to clean up after
        
    Returns:
        Cleanup status
    """
    logger.info(f"🧹 Cleaning up temporary files for {len(job_ids)} jobs")
    
    # Simulate cleanup time
    time.sleep(1)
    
    logger.success("✅ Temporary files cleaned up successfully")
    return f"Cleaned up files for {len(job_ids)} jobs"


def demonstrate_sequential_dependencies() -> list:
    """
    Demonstrate simple sequential job dependencies.
    
    Returns:
        List of enqueued jobs
    """
    logger.info("📍 Sequential Dependencies Demo")
    logger.info("-" * 40)
    
    # Create configuration
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5,
        max_reconnect_attempts=3
    )
    
    queue_config = QueueConfig(
        name="workflow_queue",
        stream_name="NAQ_JOBS",
        consumer_name="basic_dependencies_consumer"
    )
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Step 1: Prepare data
        logger.info("📤 Step 1: Data preparation")
        prepare_job = client.enqueue(
            prepare_data,
            dataset_name="user_activity",
            size=10000
        )
        logger.info(f"  ✅ Enqueued preparation job: {prepare_job.job_id}")
        
        # Step 2: Process data (depends on preparation)
        logger.info("\n📤 Step 2: Data processing (depends on Step 1)")
        process_job = client.enqueue(
            process_data,
            dataset_name="user_activity",
            operation="aggregation",
            depends_on=[prepare_job]
        )
        logger.info(f"  ✅ Enqueued processing job: {process_job.job_id}")
        logger.info(f"  🔗 Depends on: {prepare_job.job_id}")
        
        # Step 3: Validate results (depends on processing)
        logger.info("\n📤 Step 3: Result validation (depends on Step 2)")
        validate_job = client.enqueue(
            validate_results,
            dataset_name="user_activity",
            expected_records=1000,
            depends_on=[process_job]
        )
        logger.info(f"  ✅ Enqueued validation job: {validate_job.job_id}")
        logger.info(f"  🔗 Depends on: {process_job.job_id}")
        
        return [prepare_job, process_job, validate_job]


def demonstrate_parallel_convergence() -> list:
    """
    Demonstrate parallel jobs converging to a single job.
    
    Returns:
        List of enqueued jobs
    """
    logger.info("\n📍 Parallel Convergence Demo")
    logger.info("-" * 40)
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        parallel_jobs = []
        
        # Create multiple parallel data preparation jobs
        datasets = [
            ("sales_data", 5000),
            ("user_data", 8000),
            ("product_data", 3000)
        ]
        
        logger.info("📤 Creating parallel preparation jobs:")
        for dataset_name, size in datasets:
            job = client.enqueue(
                prepare_data,
                dataset_name=dataset_name,
                size=size
            )
            parallel_jobs.append(job)
            logger.info(f"  ✅ {dataset_name}: {job.job_id}")
        
        # Create convergence job that depends on all parallel jobs
        logger.info("\n📤 Creating convergence job (depends on all parallel jobs):")
        report_job = client.enqueue(
            generate_report,
            datasets=[name for name, _ in datasets],
            report_type="monthly_summary",
            depends_on=parallel_jobs
        )
        logger.info(f"  ✅ Report job: {report_job.job_id}")
        logger.info(f"  🔗 Depends on: {[job.job_id for job in parallel_jobs]}")
        
        return parallel_jobs + [report_job]


def demonstrate_cleanup_dependencies() -> list:
    """
    Demonstrate cleanup jobs that run regardless of success/failure.
    
    Returns:
        List of enqueued jobs
    """
    logger.info("\n📍 Cleanup Dependencies Demo")
    logger.info("-" * 40)
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Main processing job
        logger.info("📤 Creating main processing job:")
        main_job = client.enqueue(
            process_data,
            dataset_name="temp_analysis",
            operation="machine_learning"
        )
        logger.info(f"  ✅ Main job: {main_job.job_id}")
        
        # Cleanup job that runs whether main job succeeds or fails
        logger.info("\n📤 Creating cleanup job (runs after success or failure):")
        cleanup_job = client.enqueue(
            cleanup_temp_files,
            job_ids=[main_job.job_id],
            depends_on=[main_job],
            run_after_failure=True  # This makes it run even if main_job fails
        )
        logger.info(f"  ✅ Cleanup job: {cleanup_job.job_id}")
        logger.info(f"  🔗 Depends on: {main_job.job_id} (runs after success OR failure)")
        
        return [main_job, cleanup_job]


def demonstrate_fan_out_pattern() -> list:
    """
    Demonstrate fan-out pattern: one job creating work for multiple dependent jobs.
    
    Returns:
        List of enqueued jobs
    """
    logger.info("\n📍 Fan-out Pattern Demo")
    logger.info("-" * 40)
    
    with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Central data preparation
        logger.info("📤 Creating central preparation job:")
        central_job = client.enqueue(
            prepare_data,
            dataset_name="master_dataset",
            size=50000
        )
        logger.info(f"  ✅ Central job: {central_job.job_id}")
        
        # Multiple processing jobs that depend on the central job
        processing_operations = ["analysis", "transformation", "validation", "export"]
        processing_jobs = []
        
        logger.info("\n📤 Creating dependent processing jobs:")
        for operation in processing_operations:
            job = client.enqueue(
                process_data,
                dataset_name="master_dataset",
                operation=operation,
                depends_on=[central_job]
            )
            processing_jobs.append(job)
            logger.info(f"  ✅ {operation} job: {job.job_id}")
        
        logger.info(f"  🔗 All jobs depend on: {central_job.job_id}")
        
        # Final convergence job
        logger.info("\n📤 Creating final convergence job:")
        final_job = client.enqueue(
            generate_report,
            datasets=["master_dataset"],
            report_type="comprehensive_analysis",
            depends_on=processing_jobs
        )
        logger.info(f"  ✅ Final job: {final_job.job_id}")
        logger.info(f"  🔗 Depends on all processing jobs")
        
        return [central_job] + processing_jobs + [final_job]


def main() -> int:
    """
    Main function demonstrating basic dependency patterns.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    logger.info("🚀 NAQ Basic Job Dependencies Demo")
    logger.info("=" * 50)
    
    try:
        # Demonstrate different dependency patterns
        sequential_jobs = demonstrate_sequential_dependencies()
        parallel_jobs = demonstrate_parallel_convergence()
        cleanup_jobs = demonstrate_cleanup_dependencies()
        fanout_jobs = demonstrate_fan_out_pattern()
        
        all_jobs = sequential_jobs + parallel_jobs + cleanup_jobs + fanout_jobs
        
        logger.info(f"\n🎉 Enqueued {len(all_jobs)} jobs with dependencies!")
        
        logger.info("\n" + "=" * 50)
        logger.info("📊 Dependency Pattern Summary:")
        logger.info("=" * 50)
        logger.info(f"Sequential chain: {len(sequential_jobs)} jobs")
        logger.info(f"Parallel convergence: {len(parallel_jobs)} jobs")
        logger.info(f"Cleanup pattern: {len(cleanup_jobs)} jobs")
        logger.info(f"Fan-out pattern: {len(fanout_jobs)} jobs")
        
        logger.info("\n🎯 Dependency Highlights:")
        logger.info("   • Sequential: Jobs run one after another")
        logger.info("   • Parallel: Multiple jobs run simultaneously")
        logger.info("   • Convergence: Multiple jobs feed into one")
        logger.info("   • Cleanup: Jobs that always run (success or failure)")
        logger.info("   • Fan-out: One job enables multiple dependent jobs")
        
        logger.info("\n💡 Watch for these patterns in worker logs:")
        logger.info("   • Jobs waiting for dependencies to complete")
        logger.info("   • Parallel execution of independent jobs")
        logger.info("   • Sequential execution of dependent jobs")
        logger.info("   • Cleanup jobs running after failures")
        
        logger.info("\n📋 Next Steps:")
        logger.info("   • Try complex_workflows.py for advanced patterns")
        logger.info("   • Check failure_handling.py for error scenarios")
        logger.info("   • Monitor jobs with 'naq list-workers'")
        logger.info("   • Use 'naq dashboard' for visual workflow tracking")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.error("\n🔧 Troubleshooting:")
        logger.error("   - Is NATS running? (cd docker && docker-compose up -d)")
        logger.error("   - Are workers running? (naq worker default workflow_queue)")
        logger.error("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())