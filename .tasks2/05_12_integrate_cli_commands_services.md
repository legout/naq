### Sub-task: Integrate Services into `src/naq/cli/` Commands
**Description:** Refactor CLI commands to use the new centralized service layer for all interactions with NAQ components, ensuring consistency and leveraging service benefits.
**Implementation Steps:**
- Identify all command files in `src/naq/cli/` that interact with NATS, queues, workers, or schedulers.
- Update these commands to use `ServiceManager` and inject relevant services (e.g., `JobService`, `SchedulerService`, `ConnectionService`).
- Replace direct component interactions with service calls.
**Success Criteria:**
- All CLI commands interact with NAQ components via the service layer.
- CLI commands function correctly with the new service integration.
**Testing:**
- Manually test each affected CLI command to ensure proper functionality.
- Implement automated tests for CLI commands that verify service layer integration.
**Documentation:**
- Update CLI usage examples in `docs/quickstart.qmd` or `docs/examples.qmd` if the user-facing CLI commands change due to service integration.