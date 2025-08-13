### Sub-task: Implement `nats_kv_store` Context Manager
**Description:** Create an asynchronous context manager for NATS Key-Value store operations, building upon the `nats_jetstream` context manager.
**Implementation Steps:**
- Implement the `nats_kv_store` async context manager in `src/naq/connection/context_managers.py`.
- This context manager should accept a `bucket_name` and an optional `Config` object.
- It should internally use `nats_jetstream` to obtain the JetStream context.
- It should then retrieve the Key-Value store for the specified `bucket_name`.
- Add error logging for KV store creation failures.
**Success Criteria:**
- `nats_kv_store` context manager is correctly implemented in `src/naq/connection/context_managers.py`.
- The context manager successfully provides a Key-Value store instance for the given bucket.
- Error handling for KV store access is in place and logs errors.
**Testing:**
- Unit tests for `nats_kv_store` to verify:
    - Successful retrieval of a Key-Value store.
    - Error handling for invalid bucket names or JetStream issues.
    - Proper integration with `nats_jetstream`.
**Documentation:**
- Add a docstring to the `nats_kv_store` context manager explaining its purpose, parameters, and usage.
- Update the API documentation for `src/naq/connection/context_managers.py` to include `nats_kv_store`.