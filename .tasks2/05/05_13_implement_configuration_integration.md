### Sub-task: Implement Configuration Integration for Services
**Description:** Ensure all services correctly integrate with the NAQ configuration system, allowing service parameters to be set via configuration or environment variables.
**Implementation Steps:**
- Design a clear mapping between service configuration parameters and the overall NAQ configuration system.
- Implement logic within each service's `__init__` method to retrieve configuration from the `config` dictionary passed by `ServiceManager`.
- Define default service configurations where applicable.
**Success Criteria:**
- Services correctly retrieve and apply configuration settings.
- Service behavior can be controlled via the NAQ configuration system.
**Testing:**
- Unit tests for each service to verify correct configuration parsing and application.
- Integration tests to ensure services behave as expected with different configuration inputs.
**Documentation:**
- Add a section in `docs/architecture.qmd` or a new configuration-specific document detailing how services are configured.
- Provide examples of service configuration in `docs/examples.qmd`.