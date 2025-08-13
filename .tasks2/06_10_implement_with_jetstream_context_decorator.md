### Sub-task: Implement `with_jetstream_context` Decorator
**Description:** Create an asynchronous decorator that injects a JetStream context into a decorated function, automatically managing the underlying NATS connection and JetStream context lifecycle.
**Implementation Steps:**
- Implement the `with_jetstream_context` async decorator in `src/naq/connection/decorators.py`.
- The decorator should use `nats_jetstream` internally to provide the JetStream context.
- It should ensure the decorated function receives the JetStream context as its first argument.
**Success Criteria:**
- `with_jetstream_context` decorator is correctly implemented in `src/naq/connection/decorators.py`.
- Decorated functions receive a valid JetStream context.
- Connection and JetStream context lifecycle are managed automatically by the decorator.
**Testing:**
- Unit tests for `with_jetstream_context` to verify:
    - A decorated function can successfully interact with JetStream (e.g., create a stream) using the injected context.
    - All resources are properly closed after the decorated function completes.
**Documentation:**
- Add a docstring to the `with_jetstream_context` decorator explaining its purpose, parameters, and usage examples.
- Update the API documentation for `src/naq/connection/decorators.py` to include `with_jetstream_context`.