# KV Store Example

This example demonstrates how to use the NAQ KVStoreService for NATS KeyValue operations.

## Features Covered

1. Basic put/get/delete operations
2. Transaction support
3. Working with multiple buckets
4. Error handling

## Running the Example

1. Start NATS server:
   ```bash
   cd docker && docker-compose up -d
   ```

2. Set secure serializer:
   ```bash
   export NAQ_JOB_SERIALIZER=json
   ```

3. Run the example:
   ```bash
   python kv_store_example.py
   ```

## What You'll Learn

- How to store and retrieve simple values
- How to work with complex objects (automatically serialized with cloudpickle)
- How to use transactions for atomic operations
- How to work with multiple buckets for different data types
- Best practices for error handling with KV operations

## Key Concepts

- **Buckets**: Logical grouping of keys (like namespaces)
- **Transactions**: Atomic operations across multiple keys
- **Serialization**: Automatic handling of complex Python objects
- **Error Handling**: Proper exception handling for KV operations