#!/usr/bin/env python3
"""
Test script to verify the Worker service registration fix.
"""
import asyncio
import sys
import os

# Add src to path so we can import naq
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from naq.worker import Worker


async def test_worker_creation():
    """Test that Worker can be created and services are properly registered."""
    print("Testing Worker creation with service registration...")
    
    try:
        # Create worker using the new async factory method
        worker = await Worker.create(queues=["test_queue"])
        print("✅ Worker created successfully!")
        
        # Test that services are accessible
        print("Testing service access...")
        
        # Test connection service
        connection = await worker._service_manager.get_service("connection")
        print(f"✅ Connection service: {type(connection)}")
        
        # Test stream service
        stream = await worker._service_manager.get_service("stream")
        print(f"✅ Stream service: {type(stream)}")
        
        # Test jobs service
        jobs = await worker._service_manager.get_service("jobs")
        print(f"✅ Jobs service: {type(jobs)}")
        
        # Test kv service
        kv = await worker._service_manager.get_service("kv")
        print(f"✅ KV service: {type(kv)}")
        
        # Test events service
        events = await worker._service_manager.get_service("events")
        print(f"✅ Events service: {type(events)}")
        
        print("🎉 All services are accessible! Fix appears to be working.")
        return True
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_old_worker_creation():
    """Test the old Worker creation method to see if it still fails."""
    print("\nTesting old Worker creation (should still fail)...")
    
    try:
        # Create worker using the old method
        worker = Worker(queues=["test_queue"])
        print("⚠️  Worker created with old method (unexpected)")
        
        # Try to access a service
        try:
            connection = await worker._service_manager.get_service("connection")
            print(f"⚠️  Connection service accessible with old method: {type(connection)}")
        except Exception as e:
            print(f"✅ Expected error with old method: {e}")
        return True
        
    except Exception as e:
        print(f"✅ Expected error with old method: {e}")
        return False


async def main():
    """Main test function."""
    print("Testing NAQ Worker service registration fix\n")
    
    # Test the new async factory method
    new_method_success = await test_worker_creation()
    
    # Test the old method
    old_method_success = await test_old_worker_creation()
    
    print(f"\nSummary:")
    print(f"New async factory method: {'✅ PASS' if new_method_success else '❌ FAIL'}")
    print(f"Old direct instantiation: {'❌ FAIL (expected)' if not old_method_success else '⚠️  UNEXPECTED PASS'}")
    
    if new_method_success:
        print("\n🎉 The fix appears to be working!")
        print("Use `await Worker.create()` instead of `Worker()` for proper service initialization.")
    else:
        print("\n❌ The fix is not working. Further investigation needed.")


if __name__ == "__main__":
    asyncio.run(main())