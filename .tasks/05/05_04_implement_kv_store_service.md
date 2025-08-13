### Sub-task: Implement `kv_stores.py` - KeyValue Store Service
**Description:** Centralize KeyValue store operations and management, including pooling, transaction support, and TTL management for atomic operations.
**Implementation Steps:**
- Create `src/naq/services/kv_stores.py`.
- Implement `KVStoreService` inheriting from `BaseService` and accepting `ConnectionService` as a dependency.
- Include methods like `get_kv_store`, `put`, `get`, `delete`, and `kv_transaction` (async context manager).
- Implement KV store pooling and management.
**Success Criteria:**
- `src/naq/services/kv_stores.py` is created.
- `KVStoreService` provides methods for interacting with KeyValue stores.
- The `kv_transaction` context manager supports atomic KV operations.
**Testing:**
- Unit tests for `KVStoreService` methods, including `put`, `get`, `delete`, and `kv_transaction`.
- Integration tests to verify correct interaction with NATS KeyValue stores.
- Test transactional behavior and error handling.
**Documentation:**
- Document `src/naq/services/kv_stores.py`, explaining KV store operations and transaction support.
- Add usage examples for `KVStoreService` in `docs/examples.qmd`.