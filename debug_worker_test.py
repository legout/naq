#!/usr/bin/env python3
"""Debug script to test worker functionality."""

import asyncio
import logging
import sys
import time
from naq.queue import Queue
from naq.worker import Worker
from naq.services.base import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.kv_stores import KVStoreService

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_function():
    """Simple test function."""
    logger.info("Executing test function")
    return "test result"

async def main():
    """Main test function."""
    logger.info("Starting debug worker test")
    
    # Create service manager
    service_manager = ServiceManager()
    service_manager.register_service_type("connection", ConnectionService)
    service_manager.register_service_type("kv_store", KVStoreService)
    
    try:
        # Create queue and worker
        queue = Queue(name="debug-test-queue", service_manager=service_manager)
        worker = Worker(
            queues="debug-test-queue",
            service_manager=service_manager,
            concurrency=1,
            worker_name="debug-test-worker"
        )
        
        # Enqueue a job
        logger.info("Enqueuing test job")
        job = await queue.enqueue(test_function)
        logger.info(f"Enqueued job {job.job_id}")
        
        # Start worker
        logger.info("Starting worker")
        worker_task = asyncio.create_task(worker.run())
        
        # Give worker time to process
        await asyncio.sleep(2)
        
        # Check if job was processed
        logger.info("Checking job result")
        try:
            from naq.settings import RESULT_KV_NAME
            kv_store_service = await service_manager.get_service("kv_store")
            result_data = await kv_store_service.get(RESULT_KV_NAME, job.job_id)
            logger.info(f"Job result: {result_data}")
        except Exception as e:
            logger.error(f"Error getting job result: {e}")
        
        # Stop worker
        logger.info("Stopping worker")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
            
    finally:
        await service_manager.cleanup_all()

if __name__ == "__main__":
    asyncio.run(main())