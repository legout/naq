#!/usr/bin/env python3
"""
Example demonstrating the usage of KVStoreService for NATS KeyValue operations.

This example shows:
1. Basic put/get/delete operations
2. Transaction support
3. Working with multiple buckets
4. Error handling
"""

import asyncio
import sys
from typing import Dict, Any

# Add the src directory to the path so we can import naq
sys.path.insert(0, "src")

from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.exceptions import NaqException


async def basic_operations_example():
    """Demonstrate basic KV store operations."""
    print("=== Basic Operations Example ===")
    
    # Create a KV store service with default configuration
    config = KVStoreServiceConfig(
        nats_url="nats://localhost:4222",
        pool_size=5
    )
    kv_service = KVStoreService(config)
    
    # Store a simple value
    await kv_service.put("example", "message:1", "Hello, World!")
    print("Stored 'Hello, World!' with key 'message:1'")
    
    # Retrieve the value
    value = await kv_service.get("example", "message:1")
    print(f"Retrieved value: {value}")
    
    # Store a complex object (will be serialized with cloudpickle)
    user_data = {
        "id": 123,
        "name": "Alice",
        "preferences": {
            "theme": "dark",
            "language": "en"
        },
        "tags": ["admin", "active"]
    }
    await kv_service.put("example", "user:123", user_data)
    print("Stored complex user data")
    
    # Retrieve the complex object
    retrieved_user = await kv_service.get("example", "user:123")
    print(f"Retrieved user: {retrieved_user}")
    
    # Delete a key
    deleted = await kv_service.delete("example", "message:1")
    print(f"Deleted 'message:1': {deleted}")
    
    # Try to get the deleted key
    missing_value = await kv_service.get("example", "message:1", "default")
    print(f"Value for deleted key (with default): {missing_value}")
    
    print()


async def transaction_example():
    """Demonstrate transaction support."""
    print("=== Transaction Example ===")
    
    config = KVStoreServiceConfig()
    kv_service = KVStoreService(config)
    
    # Initialize inventory
    await kv_service.put("inventory", "item:widget", 100)
    await kv_service.put("inventory", "item:gadget", 50)
    
    print("Initial inventory:")
    widgets = await kv_service.get("inventory", "item:widget")
    gadgets = await kv_service.get("inventory", "item:gadget")
    print(f"  Widgets: {widgets}")
    print(f"  Gadgets: {gadgets}")
    
    try:
        # Perform a transaction to transfer inventory
        async with kv_service.kv_transaction("inventory") as tx:
            # Check current stock
            current_widgets = await tx.get("item:widget")
            current_gadgets = await tx.get("item:gadget")
            
            # Transfer 10 widgets to gadgets
            if current_widgets >= 10:
                await tx.put("item:widget", current_widgets - 10)
                await tx.put("item:gadget", current_gadgets + 10)
                print("Transaction completed: Transferred 10 widgets to gadgets")
            else:
                raise ValueError("Not enough widgets to transfer")
        
        # Verify the transaction
        final_widgets = await kv_service.get("inventory", "item:widget")
        final_gadgets = await kv_service.get("inventory", "item:gadget")
        print(f"Final inventory:")
        print(f"  Widgets: {final_widgets}")
        print(f"  Gadgets: {final_gadgets}")
        
    except Exception as e:
        print(f"Transaction failed: {e}")
    
    print()


async def multiple_buckets_example():
    """Demonstrate working with multiple buckets."""
    print("=== Multiple Buckets Example ===")
    
    config = KVStoreServiceConfig()
    kv_service = KVStoreService(config)
    
    # Store data in different buckets
    await kv_service.put("users", "user:123", {"name": "Alice", "role": "admin"})
    await kv_service.put("sessions", "session:abc", {"user_id": "123", "expires": "2023-12-31"})
    await kv_service.put("cache", "page:home", "<html>Home page content</html>")
    
    # List keys in each bucket
    print("Keys in 'users' bucket:")
    user_keys = await kv_service.keys("users")
    for key in user_keys:
        print(f"  {key}")
    
    print("Keys in 'sessions' bucket:")
    session_keys = await kv_service.keys("sessions")
    for key in session_keys:
        print(f"  {key}")
    
    print("Keys in 'cache' bucket:")
    cache_keys = await kv_service.keys("cache")
    for key in cache_keys:
        print(f"  {key}")
    
    print()


async def error_handling_example():
    """Demonstrate error handling."""
    print("=== Error Handling Example ===")
    
    config = KVStoreServiceConfig()
    kv_service = KVStoreService(config)
    
    # Try to use an invalid bucket name
    try:
        await kv_service.put("", "key", "value")
    except NaqException as e:
        print(f"Caught expected error for empty bucket name: {e}")
    
    # Try to use an invalid key
    try:
        await kv_service.put("bucket", "key with spaces", "value")
    except NaqException as e:
        print(f"Caught expected error for key with spaces: {e}")
    
    # Try to use a very long key
    try:
        long_key = "x" * 1000
        await kv_service.put("bucket", long_key, "value")
    except NaqException as e:
        print(f"Caught expected error for very long key: {e}")
    
    print()


async def main():
    """Run all examples."""
    print("KVStoreService Examples")
    print("======================")
    print()
    
    try:
        await basic_operations_example()
        await transaction_example()
        await multiple_buckets_example()
        await error_handling_example()
        
        print("All examples completed successfully!")
    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure NATS server is running on nats://localhost:4222")


if __name__ == "__main__":
    asyncio.run(main())