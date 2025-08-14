import asyncio
import time
import pytest
import pytest_asyncio
from typing import Any, Dict

from naq.models import Job
from naq.models.enums import JOB_STATUS
from naq.queue import Queue
from naq.worker import Worker
from naq.scheduler import Scheduler
from naq.services.base import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService
from naq.services.jobs import JobService
from naq.services.scheduler import SchedulerService
from naq.settings import RESULT_KV_NAME


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
        print(f"[DEBUG] Checking condition at {time.time() - start_time:.2f}s")
        result = await condition_func()
        print(f"[DEBUG] Condition result: {result}")
        if result:
            return True
        await asyncio.sleep(interval)
    print("[DEBUG] Timeout reached")
    return False


@pytest_asyncio.fixture
async def service_manager(nats_server) -> ServiceManager:
    """Create a test service manager instance."""
    print("[DEBUG] Creating ServiceManager with nats_url:", nats_server)
    service_manager = ServiceManager()
    
    # Register service types
    service_manager.register_service_type("connection", ConnectionService)
    service_manager.register_service_type("kv_store", KVStoreService)
    service_manager.register_service_type("job", JobService)
    service_manager.register_service_type("scheduler", SchedulerService)
    
    try:
        yield service_manager
    finally:
        print("[DEBUG] Cleaning up ServiceManager")
        await service_manager.cleanup_all()


@pytest_asyncio.fixture
async def queue_with_services(service_manager) -> Queue:
    """Create a test queue instance using services."""
    print("[DEBUG] Creating Queue with service manager")
    queue = Queue(name="test-service-queue", service_manager=service_manager)
    yield queue
    # Note: We don't purge or close the queue here because the service_manager
    # cleanup will happen after this fixture teardown, and purging needs the connection
    # which will already be closed by then.


@pytest_asyncio.fixture
async def worker_with_services(service_manager, nats_server) -> Worker:
    """Create a test worker instance using services."""
    print("[DEBUG] Creating Worker with service manager")
    worker = Worker(
        queues="test-service-queue",
        service_manager=service_manager,
        nats_url=nats_server,
        concurrency=1,  # Single worker for predictable testing
        worker_name="test-service-worker",
    )
    yield worker
    # Note: We don't close the worker here because the service_manager
    # cleanup will happen after this fixture teardown, and closing needs the connection
    # which will already be closed by then.


@pytest_asyncio.fixture
async def scheduler_with_services(service_manager, nats_server) -> Scheduler:
    """Create a test scheduler instance using services."""
    print("[DEBUG] Creating Scheduler with service manager")
    scheduler = Scheduler(
        service_manager=service_manager,
        nats_url=nats_server,
        poll_interval=0.1,  # Fast polling for tests
        instance_id="test-service-scheduler",
        enable_ha=False,  # Disable HA for simpler testing
    )
    yield scheduler
    # Note: We don't close the scheduler here because the service_manager
    # cleanup will happen after this fixture teardown, and closing needs the connection
    # which will already be closed by then.


async def fetch_job_result(job: Job, service_manager) -> Dict[str, Any]:
    """Helper to fetch a job's result using service layer."""
    print(f"[DEBUG] Fetching job result for job_id={job.job_id}")
    print(f"[DEBUG] Service manager type: {type(service_manager)}")
    print(f"[DEBUG] Service manager: {service_manager}")
    kv_store_service = await service_manager.get_service("kv_store")
    result_data = await kv_store_service.get(RESULT_KV_NAME, job.job_id)
    print(f"[DEBUG] Job result fetched for job_id={job.job_id}")
    return Job.deserialize_result(result_data)


@pytest.mark.asyncio
async def test_basic_fetch_process_ack_with_services(
    queue_with_services: Queue, 
    worker_with_services: Worker, 
    service_manager: ServiceManager
):
    """Test basic job processing using service layer."""
    # Enqueue a test job
    job = await queue_with_services.enqueue(test_function)

    # Run worker for one iteration
    print("[DEBUG] Starting worker task")
    task = asyncio.create_task(worker_with_services.run())
    # Give the worker more time to initialize
    await asyncio.sleep(0.5)
    
    # Wait for job completion
    async def check_result():
        try:
            print(f"[DEBUG] Checking result for job {job.job_id}")
            result = await fetch_job_result(job, service_manager)
            print(f"[DEBUG] Result: {result}")
            return result["status"] == JOB_STATUS.COMPLETED.value
        except Exception as e:
            print(f"[DEBUG] Error checking result: {e}")
            return False

    try:
        # Wait up to 10 seconds for job completion
        print("[DEBUG] Waiting for job completion")
        assert await wait_for(check_result, timeout=10.0)

        # Verify the result
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.COMPLETED.value
        assert result_data["result"] == "test result"

    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_scheduler_integration_with_services(
    queue_with_services: Queue,
    scheduler_with_services: Scheduler,
    worker_with_services: Worker,
    service_manager: ServiceManager
):
    """Test scheduler integration using service layer."""
    # Schedule a job to run immediately
    from datetime import datetime, timedelta
    job = await queue_with_services.enqueue_at(
        datetime.now() + timedelta(seconds=0.1),  # Run almost immediately
        test_function
    )

    # Run scheduler and worker
    scheduler_task = asyncio.create_task(scheduler_with_services.run())
    worker_task = asyncio.create_task(worker_with_services.run())
    
    try:
        # Wait for job completion
        async def check_result():
            try:
                result = await fetch_job_result(job, service_manager)
                return result["status"] == JOB_STATUS.COMPLETED.value
            except Exception:
                return False

        # Wait up to 5 seconds for job completion
        assert await wait_for(check_result, timeout=10.0)

        # Verify the result
        result_data = await fetch_job_result(job, service_manager)
        assert result_data["status"] == JOB_STATUS.COMPLETED.value
        assert result_data["result"] == "test result"

    finally:
        scheduler_task.cancel()
        worker_task.cancel()
        await asyncio.gather(scheduler_task, worker_task, return_exceptions=True)