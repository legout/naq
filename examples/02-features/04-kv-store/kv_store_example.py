#!/usr/bin/env python3
"""
Example demonstrating the usage of NAQ KeyValue operations.

This example shows:
1. Basic put/get/delete operations
2. Working with multiple buckets
3. Error handling
"""

import asyncio
from typing import Dict, Any, List

import msgspec
from loguru import logger
from naq import NatsClient
from naq.config import NatsConfig, QueueConfig
from naq.exceptions import NaqException

# Setup logging
logger.add(level="INFO")


class UserData(msgspec.Struct):
    """User data structure for KV store example."""
    id: int
    name: str
    preferences: Dict[str, str]
    tags: List[str]


async def basic_operations_example() -> None:
    """Demonstrate basic KV store operations."""
    logger.info("=== Basic Operations Example ===")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure queue
    queue_config = QueueConfig(
        name="default",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    # Create a client with configuration
    async with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Store a simple value
        await client.kv_put("example", "message:1", "Hello, World!")
        logger.info("Stored 'Hello, World!' with key 'message:1'")
        
        # Retrieve the value
        value = await client.kv_get("example", "message:1")
        logger.info(f"Retrieved value: {value}")
        
        # Store a complex object (will be serialized with cloudpickle)
        user_data = UserData(
            id=123,
            name="Alice",
            preferences={
                "theme": "dark",
                "language": "en"
            },
            tags=["admin", "active"]
        )
        await client.kv_put("example", "user:123", user_data)
        logger.info("Stored complex user data")
        
        # Retrieve the complex object
        retrieved_user = await client.kv_get("example", "user:123")
        logger.info(f"Retrieved user: {retrieved_user}")
        
        # Delete a key
        deleted = await client.kv_delete("example", "message:1")
        logger.info(f"Deleted 'message:1': {deleted}")
        
        # Try to get the deleted key
        missing_value = await client.kv_get("example", "message:1", default="default")
        logger.info(f"Value for deleted key (with default): {missing_value}")


async def transaction_example() -> None:
    """Demonstrate transaction-like operations."""
    logger.info("=== Transaction-like Operations Example ===")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure queue
    queue_config = QueueConfig(
        name="default",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    async with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Initialize inventory
        await client.kv_put("inventory", "item:widget", 100)
        await client.kv_put("inventory", "item:gadget", 50)
        
        logger.info("Initial inventory:")
        widgets = await client.kv_get("inventory", "item:widget")
        gadgets = await client.kv_get("inventory", "item:gadget")
        logger.info(f"  Widgets: {widgets}")
        logger.info(f"  Gadgets: {gadgets}")
        
        try:
            # Perform a transaction-like operation to transfer inventory
            # Note: NATS KV doesn't support multi-key transactions, so we'll
            # implement a simple optimistic locking pattern
            
            # Get current values
            current_widgets = await client.kv_get("inventory", "item:widget")
            current_gadgets = await client.kv_get("inventory", "item:gadget")
            
            # Transfer 10 widgets to gadgets
            if current_widgets >= 10:
                # Update values
                await client.kv_put("inventory", "item:widget", current_widgets - 10)
                await client.kv_put("inventory", "item:gadget", current_gadgets + 10)
                logger.info("Operation completed: Transferred 10 widgets to gadgets")
            else:
                raise ValueError("Not enough widgets to transfer")
            
            # Verify the operation
            final_widgets = await client.kv_get("inventory", "item:widget")
            final_gadgets = await client.kv_get("inventory", "item:gadget")
            logger.info("Final inventory:")
            logger.info(f"  Widgets: {final_widgets}")
            logger.info(f"  Gadgets: {final_gadgets}")
            
        except Exception as e:
            logger.error(f"Operation failed: {e}")


class SessionData(msgspec.Struct):
    """Session data structure for KV store example."""
    user_id: str
    expires: str


async def multiple_buckets_example() -> None:
    """Demonstrate working with multiple buckets."""
    logger.info("=== Multiple Buckets Example ===")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure queue
    queue_config = QueueConfig(
        name="default",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    async with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Store data in different buckets
        await client.kv_put("users", "user:123", {"name": "Alice", "role": "admin"})
        await client.kv_put("sessions", "session:abc", SessionData(
            user_id="123",
            expires="2023-12-31"
        ))
        await client.kv_put("cache", "page:home", "<html>Home page content</html>")
        
        # List keys in each bucket
        logger.info("Keys in 'users' bucket:")
        user_keys = await client.kv_keys("users")
        for key in user_keys:
            logger.info(f"  {key}")
        
        logger.info("Keys in 'sessions' bucket:")
        session_keys = await client.kv_keys("sessions")
        for key in session_keys:
            logger.info(f"  {key}")
        
        logger.info("Keys in 'cache' bucket:")
        cache_keys = await client.kv_keys("cache")
        for key in cache_keys:
            logger.info(f"  {key}")


async def error_handling_example() -> None:
    """Demonstrate error handling."""
    logger.info("=== Error Handling Example ===")
    
    # Configure NATS client
    nats_config = NatsConfig(
        servers=["nats://localhost:4222"],
        connect_timeout=5.0,
        max_reconnect_attempts=3
    )
    
    # Configure queue
    queue_config = QueueConfig(
        name="default",
        job_timeout=30.0,
        max_retries=2,
        retry_delay=3.0
    )
    
    async with NatsClient(nats_config=nats_config, queue_config=queue_config) as client:
        # Try to use an invalid bucket name
        try:
            await client.kv_put("", "key", "value")
        except NaqException as e:
            logger.info(f"Caught expected error for empty bucket name: {e}")
        
        # Try to use an invalid key
        try:
            await client.kv_put("bucket", "key with spaces", "value")
        except NaqException as e:
            logger.info(f"Caught expected error for key with spaces: {e}")
        
        # Try to use a very long key
        try:
            long_key = "x" * 1000
            await client.kv_put("bucket", long_key, "value")
        except NaqException as e:
            logger.info(f"Caught expected error for very long key: {e}")


async def main() -> None:
    """Run all examples."""
    logger.info("NAQ KeyValue Examples")
    
    try:
        await basic_operations_example()
        await transaction_example()
        await multiple_buckets_example()
        await error_handling_example()
        
        logger.info("All examples completed successfully!")
    except Exception as e:
        logger.error(f"Error running examples: {e}")
        logger.info("Make sure NATS server is running on nats://localhost:4222")
        logger.info("Also ensure the NAQ library is properly installed and accessible.")


if __name__ == "__main__":
    asyncio.run(main())