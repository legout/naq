"""
Verification script for the ServiceManager alias mechanism.

This script demonstrates that the new alias mechanism works correctly.
"""

import asyncio
from src.naq.services.base import ServiceManager, ServiceConfig
from src.naq.services.kv_stores import KVStoreService


async def main():
    """Main function to verify the alias mechanism."""
    print("Verifying ServiceManager alias mechanism...")
    
    # Create a service manager
    service_manager = ServiceManager()
    
    # Register a KVStoreService
    print("Registering 'kv' service...")
    await service_manager.register_service("kv", KVStoreService)
    
    # Add an alias
    print("Adding 'kv_store' alias for 'kv' service...")
    service_manager.add_alias("kv_store", "kv")
    
    # Verify the alias works
    print("Getting service via original name 'kv'...")
    kv_service_direct = await service_manager.get_service("kv", KVStoreService)
    print(f"Got service: {kv_service_direct}")
    
    print("Getting service via alias 'kv_store'...")
    kv_service_alias = await service_manager.get_service("kv_store", KVStoreService)
    print(f"Got service: {kv_service_alias}")
    
    # They should be the same instance
    if kv_service_direct is kv_service_alias:
        print("SUCCESS: Alias mechanism works correctly. Both references point to the same instance.")
    else:
        print("FAILURE: Alias mechanism failed. References are not the same instance.")
        
    # Verify has_service works with aliases
    if service_manager.has_service("kv_store"):
        print("SUCCESS: has_service correctly identifies aliases.")
    else:
        print("FAILURE: has_service does not correctly identify aliases.")
        

if __name__ == "__main__":
    asyncio.run(main())