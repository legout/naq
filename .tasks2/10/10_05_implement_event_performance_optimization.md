### Sub-task 5: Event Performance Optimization Implementation
**Description:** Implement performance optimizations for the event logging system within `EventService` and `AsyncJobEventLogger`, focusing on non-blocking asynchronous operations, event batching, and reliable background flushing to minimize impact on core job processing.
**Implementation Steps:**
- Modify `src/naq/events.py` (assuming `EventService` is defined here) to manage an `enabled` flag from configuration.
- In `EventService.log_event`, add a quick return if event logging is disabled (`self.enabled` is False) or if the internal logger (`_logger`) is not yet initialized.
- Implement `EventService._initialize_logger` to properly instantiate and start `AsyncJobEventLogger`, passing configurable `batch_size`, `flush_interval`, and `max_buffer_size` parameters.
- Implement `EventService._background_flush` as a persistent `asyncio.Task` that periodically calls `_logger.flush()` to ensure buffered events are written to storage, even under low event volume.
- Ensure `AsyncJobEventLogger` (likely in `src/naq/events/logger.py` or `src/naq/events.py`) is designed for non-blocking, batched writes to the underlying event storage.
**Success Criteria:**
- Event logging introduces minimal overhead (e.g., <5%) to the overall job processing time.
- The event system can efficiently handle and process a high volume of events (e.g., >1000 events/second) without performance degradation.
- Memory consumption for event buffering is bounded and configurable, preventing memory leaks or excessive resource usage.
- Failures in the event logging or storage layer do not cause critical job processing operations to fail or block.
**Testing:**
- Develop dedicated performance tests in a new file, e.g., `tests/performance/test_event_performance.py`.
- Conduct benchmarks comparing job execution times with event logging enabled versus disabled.
- Simulate high event throughput scenarios and monitor CPU, memory, and network usage.
- Introduce artificial delays or failures in the event storage to verify the non-blocking and fault-tolerant nature of the logging.
**Documentation:**
- Update documentation, possibly in `docs/architecture.qmd` or `docs/advanced.qmd`, to detail the event system's design for performance and scalability.
- Document any new configuration parameters related to event performance optimization, explaining their impact and recommended values.