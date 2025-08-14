#!/usr/bin/env python3

import asyncio
import logging
from naq.worker import Worker
from naq.services.base import ServiceManager
from naq.services.connection import ConnectionService
from naq.services.streams import StreamService

# Set up logging to see what's happening
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_worker():
    print("Creating service manager")
    service_manager = ServiceManager()
    
    # Register service types
    service_manager.register_service_type("connection", ConnectionService)
    service_manager.register_service_type("stream", StreamService)
    
    print("Creating worker")
    worker = Worker(
        queues="test-debug-queue",
        service_manager=service_manager,
        nats_url="nats://localhost:4222",
        concurrency=1,
        worker_name="debug-worker",
    )
    
    print("Starting worker")
    try:
        await worker.run()
    except Exception as e:
        print(f"Worker error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Closing worker")
        await worker._close()

if __name__ == "__main__":
    asyncio.run(test_worker())