### Sub-task: Implement `with_nats_connection` Decorator
**Description:** Create an asynchronous decorator that injects a NATS connection into a decorated function, automatically managing its lifecycle.
**Implementation Steps:**
- Implement the `with_nats_connection` async decorator in `src/naq/connection/decorators.py`.
- The decorator should use `nats_connection` internally to provide the connection.
- It should ensure the decorated function receives the NATS connection as its first argument.
**Success Criteria:**
- `with_nats_connection` decorator is correctly implemented in `src/naq/connection/decorators.py`.
- Decorated functions receive a valid NATS connection.
- Connection lifecycle is managed automatically by the decorator.
**Testing:**
- Unit tests for `with_nats_connection` to verify:
    - A decorated function can successfully publish a message using the injected connection.
    - The connection is properly closed after the decorated function completes.
    - Error handling in the decorated function does not prevent connection closure.
**Documentation:**
- Add a docstring to the `with_nats_connection` decorator explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/connection/decorators.py` to include `with_nats_connection`.