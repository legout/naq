#!/usr/bin/env python3
"""
Test script to reproduce and diagnose the NotFoundError when subscribing to queue.
"""
import asyncio
import sys
import os

# Add src to path so we can import naq
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from naq.worker import Worker


async def test_worker_run():
    """Test Worker run to reproduce the NotFoundError."""
    print("Testing Worker run to diagnose NotFoundError...")
    
    try:
        # Create worker using the new async factory method
        worker = await Worker.create(queues=["test_queue"])
        print("✅ Worker created successfully!")
        
        # Try to run the worker for a short time to see the error
        print("Starting worker run (will stop after 5 seconds)...")
        
        # Create a task to run the worker
        worker_task = asyncio.create_task(worker.run())
        
        # Wait for 5 seconds then stop the worker
        await asyncio.sleep(5)
        
        # Stop the worker
        worker._running = False
        worker._shutdown_event.set()
        
        # Wait for the worker to finish
        try:
            await asyncio.wait_for(worker_task, timeout=10)
            print("✅ Worker stopped successfully!")
        except asyncio.TimeoutError:
            print("⚠️  Worker did not stop gracefully, cancelling task...")
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                print("✅ Worker task cancelled successfully!")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


async def main():
    """Main test function."""
    print("Testing NAQ Worker NotFoundError issue\n")
    
    success = await test_worker_run()
    
    if success:
        print("\n✅ Test completed successfully!")
    else:
        print("\n❌ Test failed with errors.")


if __name__ == "__main__":
    asyncio.run(main())