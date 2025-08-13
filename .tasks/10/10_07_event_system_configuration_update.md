### Sub-task 7: Event System Configuration Update
**Description:** Define and integrate new configuration options within `NAQConfig` to control the behavior and performance of the event system, providing granular control to users.
**Implementation Steps:**
- Modify `src/naq/settings.py` to extend the `NAQConfig` structure.
- Add a new nested configuration section (e.g., `events`) or direct fields within `NAQConfig` to hold event-specific parameters.
- Include parameters such as:
    - `events.enabled`: A boolean flag to globally enable/disable event logging.
    - `events.batch_size`: An integer defining the number of events to batch before flushing.
    - `events.flush_interval`: A float representing the maximum time (in seconds) to wait before flushing batched events.
    - `events.max_buffer_size`: An integer specifying the maximum number of events to hold in the in-memory buffer.
- Ensure these new configuration parameters are correctly parsed from various sources (e.g., environment variables, config files) and are accessible throughout the application, particularly by the `EventService` and `AsyncJobEventLogger`.
**Success Criteria:**
- All defined event system parameters are available and configurable via the `NAQConfig` object.
- Changes to these configuration values correctly influence the runtime behavior of the `EventService` and `AsyncJobEventLogger` (e.g., enabling/disabling logging, adjusting batching behavior).
**Testing:**
- Write unit tests in `tests/unit/test_unit_settings.py` to verify the correct loading, parsing, and validation of the new event configuration options.
- Create integration tests that demonstrate the impact of different configuration values on actual event system behavior (e.g., verify no events are logged when `events.enabled` is false; observe batching behavior with different `batch_size` settings).
**Documentation:**
- Update the project's configuration documentation, possibly in `docs/installation.qmd` or a new `docs/configuration.qmd` file, to detail all new event-related configuration options.
- Provide clear explanations for each parameter, its default value, and its impact on the event system's functionality and performance.