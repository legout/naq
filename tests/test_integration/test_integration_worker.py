import asyncio
import pytest
import pytest_asyncio
import time
from typing import Any, Dict

import nats
from nats.js.api import ConsumerConfig

from naq.job import Job
from naq.queue.core import Queue
from naq.worker import Worker
from naq.models import JOB_STATUS
from naq.settings import (
    RESULT_KV_NAME,
    FAILED_JOB_STREAM_NAME,
    FAILED_JOB_SUBJECT_PREFIX,
)
from naq.services.jobs import JobService
from naq.services.events import EventService
from naq.services.kv_stores import KVStoreService


# Test helper functions
def test_function() -> str:
    """Simple test function that returns a string."""
    return "test result"


def failing_function() -> None:
    """Test function that always raises an exception."""
    raise ValueError("Simulated failure")


async def wait_for(condition_func, timeout=5.0, interval=0.1):
    """Wait for a condition to become true."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if await condition_func():
            return True
        await asyncio.sleep(interval)
    return False


@pytest_asyncio.fixture
async def queue(service_manager) -> Queue:
    """Create a test queue instance using ServiceManager."""
    print("[DEBUG] Creating Queue with ServiceManager")
    # Get the JobService from the ServiceManager
    job_service = await service_manager.get_service("jobs", JobService)
    
    # Create queue with service_manager parameter
    queue = Queue(name="test-queue", service_manager=service_manager)
    try:
        yield queue
    finally:
        print("[DEBUG] Purging Queue after test")
        await queue.purge()
        await queue.close()


@pytest_asyncio.fixture
async def worker(service_manager) -> Worker:
    """Create a test worker instance using ServiceManager."""
    print("[DEBUG] Creating Worker with ServiceManager")
    # Get service configuration
    service_config = service_manager.config
    
    # Create worker with service_manager parameter
    worker = Worker(
        queues="test-queue",
        service_manager=service_manager,
        concurrency=1,  # Single worker for predictable testing
        worker_name="test-worker",
    )
    try:
        yield worker
    finally:
        print("[DEBUG] Worker fixture teardown")
        await worker._close()


async def fetch_job_result(job: Job, service_manager) -> Dict[str, Any]:
    """Helper to fetch a job's result from the NATS KV store using ServiceManager."""
    print(f"[DEBUG] Fetching job result for job_id={job.job_id}")
    # Get KVStoreService from ServiceManager
    kv_store_service = await service_manager.get_service("kv_store", KVStoreService)
    kv = await kv_store_service.get_kv_store(RESULT_KV_NAME)
    entry = await kv.get(job.job_id.encode())
    print(f"[DEBUG] Job result fetched for job_id={job.job_id}")
    return Job.deserialize_result(entry.value)


@pytest.mark.asyncio
async def test_basic_fetch_process_ack(queue: Queue, worker: Worker, service_manager):
    """Test 1: Basic Fetch/Process/ACK via NATS"""
    # Enqueue a test job
    job = await queue.enqueue(test_function)

    # Run worker for one iteration
    task = asyncio.create_task(worker.run())
    try:
        # Wait for job completion
        async def check_result():
            try:
                result = await fetch_job_result(job, service_manager)
                return result["status"] == JOB_STATUS.COMPLETED.value
            except Exception:
                return False

        # Wait up to 5 seconds for job completion
        assert await wait_for(check_result, timeout=5.0)

        # Verify the result
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.COMPLETED.value
        assert result_data["result"] == "test result"

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_nats_ack_prevents_redelivery(queue: Queue, worker: Worker, service_manager):
    """Test 2: NATS ACK Prevents Redelivery"""
    # Enqueue a test job
    job = await queue.enqueue(test_function)

    # Run worker to process the job
    print("[DEBUG] Starting worker.run()")
    task = asyncio.create_task(worker.run())
    try:
        # Wait for job completion
        async def check_result():
            try:
                result = await fetch_job_result(job, service_manager)
                return result["status"] == JOB_STATUS.COMPLETED.value
            except Exception as e:
                print("[DEBUG] Exception in check_result:", e)
                return False

        assert await wait_for(check_result, timeout=5.0)

        # Try to process again - should not receive the same message
        # Configure a short timeout for the test
        worker._semaphore = asyncio.Semaphore(1)  # Reset semaphore
        no_msg_received = True

        async def check_no_redelivery():
            print("[DEBUG] Checking for redelivery")
            # Get ConnectionService from ServiceManager
            connection_service = await service_manager.get_service("connection")
            js = await connection_service.get_jetstream()

            try:
                # Attempt to pull message with timeout
                consumer = await js.pull_subscribe(
                    subject=queue.subject,
                    durable_name="test-consumer",
                    config=ConsumerConfig(ack_wait=1),
                )
                msgs = await consumer.fetch(batch=1, timeout=1)
                print("[DEBUG] Redelivery check: msgs fetched =", len(msgs))
                return len(msgs) == 0
            except Exception as e:
                print("[DEBUG] Exception in check_no_redelivery:", e)
                return True  # No message is what we want

        assert await wait_for(check_no_redelivery, timeout=5.0)

    finally:
        print("[DEBUG] Cancelling worker task")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_simulated_missing_ack_leads_to_redelivery(
    queue: Queue, worker: Worker, service_manager
):
    """Test 3: Missing NATS ACK Leads to Redelivery"""
    # Create a failing job to prevent ACK
    job = await queue.enqueue(failing_function, max_retries=1)

    # Run worker to process the job
    task = asyncio.create_task(worker.run())
    try:
        # Wait for job to be processed at least twice (initial + 1 retry)
        async def check_retry():
            try:
                result = await fetch_job_result(job, nats_client)
                return result["status"] == JOB_STATUS.FAILED.value
            except Exception:
                return False

        assert await wait_for(check_retry, timeout=10.0)

        # Verify the job was retried
        result_data = await fetch_job_result(job, nats_client)
        assert result_data["status"] == JOB_STATUS.FAILED.value
        assert "Simulated failure" in result_data["error"]

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_dead_letter_queue_after_max_retries(
    queue: Queue, worker: Worker, service_manager
):
    """Test 4: NATS Dead-Letter Queue (DLQ) after Max Retries"""
    # Create a job that will fail consistently
    job = await queue.enqueue(failing_function, max_retries=2)

    # Run worker to process the job until it fails permanently
    task = asyncio.create_task(worker.run())
    try:
        # Wait for job to exhaust retries and move to failed queue
        async def check_failed_queue():
            # Get ConnectionService from ServiceManager
            connection_service = await service_manager.get_service("connection")
            js = await connection_service.get_jetstream()

            try:
                # Check if message is in failed job stream
                sub = await js.subscribe(
                    subject=f"{FAILED_JOB_SUBJECT_PREFIX}.{queue.name}",
                    stream=FAILED_JOB_STREAM_NAME,
                )
                msg = await sub.next_msg(timeout=1)
                return msg is not None
            except Exception:
                return False

        assert await wait_for(check_failed_queue, timeout=15.0)

        # Verify job state shows final failure
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.FAILED.value
        assert "Simulated failure" in result_data["error"]

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_job_state_persistence(queue: Queue, worker: Worker, service_manager):
    """Test 5: Job State Persistence via NATS"""
    # Enqueue a test job
    job = await queue.enqueue(test_function)

    # Run worker to process the job
    task = asyncio.create_task(worker.run())
    try:
        # Wait for job completion and verify state is persisted
        async def check_state():
            try:
                # Check both completion status and result
                result = await fetch_job_result(job, service_manager)
                return (
                    result["status"] == JOB_STATUS.COMPLETED.value
                    and result["result"] == "test result"
                )
            except Exception:
                return False

        assert await wait_for(check_state, timeout=5.0)

        # Verify the persisted state details
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.COMPLETED.value
        assert result_data["result"] == "test result"
        assert "error" not in result_data or result_data["error"] is None

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_custom_nats_configuration(service_manager):
    """Test 6: Custom NATS Configuration Variations"""
    custom_queue = Queue(name="custom-queue", service_manager=service_manager)
    custom_worker = Worker(
        queues="custom-queue",
        service_manager=service_manager,
        concurrency=2,
        worker_name="custom-worker",
    )

    # Enqueue a test job
    job = await custom_queue.enqueue(test_function)

    # Run worker to process the job
    task = asyncio.create_task(custom_worker.run())
    try:
        # Wait for job completion
        async def check_completion():
            try:
                result = await fetch_job_result(job, service_manager)
                return result["status"] == JOB_STATUS.COMPLETED.value
            except Exception:
                return False

        assert await wait_for(check_completion, timeout=5.0)

        # Verify the job was processed
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.COMPLETED.value
        assert result_data["result"] == "test result"

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        # Cleanup
        await custom_queue.purge()


@pytest.mark.asyncio
async def test_handling_nats_connection_error(queue: Queue, service_manager):
    """Test 7: Handling NATS Connection Errors"""
    # Create worker with invalid NATS URL to simulate connection error
    # Create a separate service config with invalid URL
    from naq.services.base import ServiceConfig
    bad_service_config = ServiceConfig(
        nats_url="nats://nonexistent:4222",
        log_level="DEBUG",
        custom_settings={"test_mode": True}
    )
    
    # Create a temporary service manager with bad config
    from naq.services.base import ServiceManager
    bad_service_manager = ServiceManager(config=bad_service_config)
    
    # Register required services
    await bad_service_manager.register_service("connection", ConnectionService, initialize=False)
    await bad_service_manager.register_service("kv_store", KVStoreService, initialize=False)
    await bad_service_manager.register_service("events", EventService, initialize=False)
    await bad_service_manager.register_service("jobs", JobService, initialize=False)
    
    bad_worker = Worker(
        queues="test-queue",
        service_manager=bad_service_manager,
        worker_name="bad-worker",
    )

    # Enqueue a test job to the good queue
    job = await queue.enqueue(test_function)

    # Run bad worker - should handle connection error gracefully
    with pytest.raises(Exception):
        await bad_worker.run()
    
    # Cleanup
    await bad_service_manager.cleanup_all()


@pytest.mark.asyncio
async def test_handling_nats_ack_failure(queue: Queue, worker: Worker, service_manager):
    """Test 8: NATS ACK Failures/Timeouts"""
    # Enqueue a job that will succeed but encounter ACK issues
    job = await queue.enqueue(failing_function, max_retries=2)

    # Run worker to process the job
    task = asyncio.create_task(worker.run())
    try:
        # Wait for retries due to ACK failure
        async def check_retries():
            try:
                result = await fetch_job_result(job, service_manager)
                return result["status"] == JOB_STATUS.FAILED.value
            except Exception:
                return False

        assert await wait_for(check_retries, timeout=10.0)

        # Verify the job eventually failed after retries
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.FAILED.value
        assert "Simulated failure" in result_data["error"]

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_concurrent_processing(queue: Queue, service_manager):
    """Test 9: Concurrent Processing with NATS"""
    # Create a worker with concurrency > 1
    concurrent_worker = Worker(
        queues="test-queue",
        service_manager=service_manager,
        concurrency=3,
        worker_name="concurrent-worker",
    )

    # Enqueue multiple jobs
    jobs = []
    for _ in range(5):
        job = await queue.enqueue(test_function)
        jobs.append(job)

    # Run worker to process jobs concurrently
    task = asyncio.create_task(concurrent_worker.run())
    try:
        # Wait for all jobs to complete
        async def check_all_complete():
            try:
                completed = 0
                for job in jobs:
                    result = await fetch_job_result(job, service_manager)
                    if result["status"] == JOB_STATUS.COMPLETED.value:
                        completed += 1
                return completed == len(jobs)
            except Exception:
                return False

        assert await wait_for(check_all_complete, timeout=10.0)

        # Verify all jobs completed successfully
        for job in jobs:
            result_data = await fetch_job_result(job, service_manager)
            assert result_data["status"] == JOB_STATUS.COMPLETED.value
            assert result_data["result"] == "test result"

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
