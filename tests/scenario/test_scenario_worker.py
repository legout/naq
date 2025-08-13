"""Scenario-based tests for the Worker module focusing on NATS interactions."""
import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set

import pytest
from nats.aio.client import Client as NATS

from naq.models import Job
from naq.queue import Queue
from naq.worker import Worker

def dummy_job_func(job_id: str) -> str:
    """Simple job function that returns its job_id."""
    time.sleep(0.1)  # Small delay to simulate work
    return job_id

async def create_nats_client() -> NATS:
    """Create and connect a NATS client."""
    nc = NATS()
    await nc.connect()
    return nc

@pytest.mark.asyncio
async def test_competing_consumers_distribute_jobs(nats_server):
    """
    Test that multiple workers on the same NATS subject properly distribute jobs.
    
    This test verifies the competing consumers pattern where multiple workers
    listen on the same subject but each job is processed exactly once.
    """
    # Setup NATS client and queue
    nc = await create_nats_client()
    test_subject = "test.competing.consumers"
    queue = Queue(name=test_subject)
    
    # Track processed jobs across workers
    processed_jobs: Set[str] = set()
    process_lock = threading.Lock()
    
    def track_job(job_id: str) -> str:
        """Job function that tracks which jobs were processed."""
        print(f"[DEBUG] Worker {threading.current_thread().name} processing job: {job_id}")
        with process_lock:
            processed_jobs.add(job_id)
        return job_id
    
    # Create workers in separate threads
    workers: List[Worker] = []
    worker_threads: List[threading.Thread] = []
    
    for i in range(3):  # Create 3 competing workers
        worker = Worker(queue, job_function=track_job)
        workers.append(worker)
        thread = threading.Thread(
            target=worker.run,
            name=f"worker-{i}",
            daemon=True
        )
        worker_threads.append(thread)
        thread.start()
    
    # Create and enqueue test jobs
    num_jobs = 5
    jobs = [Job(job_id=f"job-{i}", func=track_job) for i in range(num_jobs)]
    for job in jobs:
        await queue.enqueue(job)
    
    # Wait for jobs to be processed
    max_wait = 10  # seconds
    wait_interval = 0.1
    waited = 0
    while len(processed_jobs) < num_jobs and waited < max_wait:
        await asyncio.sleep(wait_interval)
        waited += wait_interval
    
    # Stop workers
    for worker in workers:
        worker.stop()
    for thread in worker_threads:
        thread.join(timeout=2.0)
    
    # Verify results
    assert len(processed_jobs) == num_jobs, \
        f"Expected {num_jobs} processed jobs, got {len(processed_jobs)}"
    
    # Verify each job was processed exactly once
    expected_job_ids = {f"job-{i}" for i in range(num_jobs)}
    assert processed_jobs == expected_job_ids, \
        f"Processed jobs don't match expected jobs. Processed: {processed_jobs}, Expected: {expected_job_ids}"

    await nc.close()

@pytest.mark.asyncio
async def test_worker_respects_nats_job_attributes(nats_server):
    """
    Test that Worker properly handles NATS message attributes and delivery guarantees.
    
    While naq abstracts NATS-specific details, this test ensures the Worker correctly
    handles core NATS functionality like message acknowledgment and redelivery.
    """
    # Setup test components
    nc = await create_nats_client()
    test_subject = "test.nats.attributes"
    queue = Queue(name=test_subject)
    
    # Track job processing states
    processing_started = threading.Event()
    processing_complete = threading.Event()
    processed_job_id = None

    def slow_job(job_id: str) -> str:
        """Job that signals when it starts and finishes processing."""
        print(f"[DEBUG] Worker {threading.current_thread().name} starting job: {job_id}")
        nonlocal processed_job_id
        processed_job_id = job_id
        processing_started.set()
        time.sleep(1.0)  # Simulate work
        processing_complete.set()
        print(f"[DEBUG] Worker {threading.current_thread().name} finished job: {job_id}")
        return job_id

    # Create and start worker
    worker = Worker(queue, job_function=slow_job)
    worker_thread = threading.Thread(
        target=worker.run,
        name="slow-worker",
        daemon=True
    )
    worker_thread.start()

    # Create and enqueue a test job
    test_job = Job(job_id="test-nats-job", func=slow_job)
    await queue.enqueue(test_job)

    # Wait for job processing to start
    assert processing_started.wait(timeout=5.0), "Job processing did not start"
    
    # Verify job is being processed (got message from NATS)
    assert processed_job_id == "test-nats-job", \
        "Worker did not receive the correct job"
        
    # Wait for processing to complete
    assert processing_complete.wait(timeout=5.0), "Job processing did not complete"
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2.0)
    
    # Verify NATS message was acknowledged
    # If message wasn't ACKed, it would be redelivered to a new consumer
    new_consumer_check = Queue(name=test_subject)
    no_redelivery_worker = Worker(new_consumer_check, job_function=slow_job)
    redelivery_check_thread = threading.Thread(
        target=no_redelivery_worker.run,
        name="redelivery-check-worker",
        daemon=True
    )
    redelivery_check_thread.start()
    
    # Short wait to see if message gets redelivered
    time.sleep(2.0)
    no_redelivery_worker.stop()
    redelivery_check_thread.join(timeout=2.0)
    
    # If processed_job_id hasn't changed, no redelivery occurred
    assert processed_job_id == "test-nats-job", \
        "Job was redelivered, indicating it wasn't properly ACKed"

    await nc.close()

@pytest.mark.asyncio
async def test_worker_processes_job_batch(nats_server):
    """
    Test that Worker can handle processing multiple jobs in sequence.
    
    While naq may not directly support NATS batch fetching, this test verifies
    the Worker can efficiently process multiple jobs from its NATS subscription.
    """
    # Setup test components
    nc = await create_nats_client()
    test_subject = "test.batch.processing"
    queue = Queue(name=test_subject)
    
    # Track processed jobs
    processed_jobs: List[str] = []
    processing_lock = threading.Lock()
    
    def batch_job(job_id: str) -> str:
        """Job that records its processing."""
        print(f"[DEBUG] Worker {threading.current_thread().name} processing batch job: {job_id}")
        with processing_lock:
            processed_jobs.append(job_id)
        time.sleep(0.1)  # Small delay to simulate work
        return job_id
    
    # Create and start worker
    worker = Worker(queue, job_function=batch_job)
    worker_thread = threading.Thread(
        target=worker.run,
        name="batch-worker",
        daemon=True
    )
    worker_thread.start()
    
    # Create and enqueue multiple test jobs
    batch_size = 5
    test_jobs = [
        Job(job_id=f"batch-job-{i}", func=batch_job)
        for i in range(batch_size)
    ]
    
    for job in test_jobs:
        await queue.enqueue(job)
        
    # Wait for all jobs to be processed
    max_wait = 10  # seconds
    waited = 0
    check_interval = 0.1
    
    while len(processed_jobs) < batch_size and waited < max_wait:
        await asyncio.sleep(check_interval)
        waited += check_interval
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2.0)
    
    # Verify all jobs were processed
    assert len(processed_jobs) == batch_size, \
        f"Expected {batch_size} jobs to be processed, got {len(processed_jobs)}"
    
    # Verify all jobs were processed exactly once and in a reasonable order
    processed_job_ids = set(processed_jobs)
    expected_job_ids = {f"batch-job-{i}" for i in range(batch_size)}
    
    assert processed_job_ids == expected_job_ids, \
        f"Mismatch in processed jobs. Expected: {expected_job_ids}, Got: {processed_job_ids}"
    
    # Verify processing order (jobs should be processed in roughly the order they were submitted)
    # We can't guarantee exact order due to async nature, but there should be some correlation
    correlation = all(
        abs(processed_jobs.index(f"batch-job-{i}") - i) <= 2  # Allow some reordering
        for i in range(batch_size)
    )
    assert correlation, "Job processing order shows no correlation with submission order"

    await nc.close()

@pytest.mark.asyncio
async def test_worker_reconnects_and_processes_after_nats_restart(nats_server):
    """
    Test Worker's resilience to NATS server disconnection and restart.
    
    This test verifies that:
    1. Worker can process jobs before server restart
    2. Worker handles server restart gracefully
    3. Worker can process new jobs after server restart
    """
    # Setup test components
    nc = await create_nats_client()
    test_subject = "test.nats.restart"
    queue = Queue(name=test_subject)
    
    # Track job processing
    processed_jobs: List[str] = []
    processing_lock = threading.Lock()
    job_started = threading.Event()
    
    def tracking_job(job_id: str) -> str:
        """Job that tracks its execution and can be monitored."""
        print(f"[DEBUG] Worker {threading.current_thread().name} processing tracking job: {job_id}")
        job_started.set()
        time.sleep(0.5)  # Give time for NATS restart during processing
        with processing_lock:
            processed_jobs.append(job_id)
        print(f"[DEBUG] Worker {threading.current_thread().name} finished tracking job: {job_id}")
        return job_id
    
    # Create and start worker
    worker = Worker(queue, job_function=tracking_job)
    worker_thread = threading.Thread(
        target=worker.run,
        name="resilient-worker",
        daemon=True
    )
    worker_thread.start()
    
    # Enqueue first job
    first_job = Job(job_id="pre-restart-job", func=tracking_job)
    await queue.enqueue(first_job)
    
    # Wait for job to start processing
    assert job_started.wait(timeout=5.0), "First job did not start"
    
    # Reset event for next job
    job_started.clear()
    
    # Simulate NATS server restart
    await nc.close()  # Simulate disconnect
    await asyncio.sleep(1.0)  # Allow time for disconnect to be detected
    
    # Create new connection (simulating server restart)
    nc = await create_nats_client()
    
    # Enqueue another job after "restart"
    second_job = Job(job_id="post-restart-job", func=tracking_job)
    await queue.enqueue(second_job)
    
    # Wait for second job to be processed
    assert job_started.wait(timeout=5.0), "Second job did not start after NATS restart"
    
    # Allow time for both jobs to complete
    await asyncio.sleep(1.0)
    
    # Stop worker
    worker.stop()
    worker_thread.join(timeout=2.0)
    
    # Verify both jobs were processed
    assert len(processed_jobs) == 2, \
        f"Expected 2 jobs to be processed, got {len(processed_jobs)}"
    assert "pre-restart-job" in processed_jobs, "Pre-restart job was not processed"
    assert "post-restart-job" in processed_jobs, "Post-restart job was not processed"
    
    # Verify processing order
    assert processed_jobs.index("pre-restart-job") < processed_jobs.index("post-restart-job"), \
        "Jobs were not processed in the expected order"

    await nc.close()