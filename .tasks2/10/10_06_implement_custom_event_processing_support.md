### Sub-task 6: Custom Event Processing Support Implementation
**Description:** Implement a flexible mechanism within the `EventService` to allow external modules or users to register custom event processors, enabling extensible real-time event consumption and reaction without modifying core logic.
**Implementation Steps:**
- Modify `src/naq/events.py` (assuming `EventService` is defined here).
- In the `EventService.__init__` method, initialize a list or similar data structure to store registered custom processors (e.g., `_custom_processors: List[Callable[[JobEvent], Awaitable[None]]]`).
- Implement the `EventService.register_event_processor` method, which allows external functions (e.g., `Callable[[JobEvent], Awaitable[None]]`) to be added to the `_custom_processors` list.
- Modify the `EventService.log_event` method: after performing standard event logging, iterate through the `_custom_processors` list and asynchronously invoke each registered processor with the current event.
- Implement a helper method, `EventService._safe_process_event`, to wrap the execution of custom processors. This method should catch and log any exceptions raised by a custom processor, preventing it from disrupting other processors or the main event flow.
**Success Criteria:**
- Custom event processors can be successfully registered with the `EventService`.
- Registered processors receive events and execute their logic asynchronously, ensuring they do not block the primary event logging or job processing.
- Errors or exceptions within a custom processor are gracefully handled, logged, and do not cause the entire event system or application to crash.
**Testing:**
- Create or update unit tests in `tests/unit/test_unit_events.py` or `tests/integration/test_integration_events.py`.
- Test the `register_event_processor` method to ensure processors are correctly added.
- Write tests that simulate event logging and verify that registered custom processors are invoked with the correct event data.
- Include tests where custom processors intentionally raise exceptions to confirm the robust error handling of `_safe_process_event`.
**Documentation:**
- Update documentation, likely in `docs/advanced.qmd` or a new section within `docs/api/events.qmd`, to explain how to create, register, and use custom event processors.
- Provide clear code examples for implementing a custom processor (e.g., for sending alerts on specific event types).