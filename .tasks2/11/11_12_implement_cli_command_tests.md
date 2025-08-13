### Sub-task 12: Implement CLI Command Tests
**Description:** Implement tests for the command-line interface (CLI) commands to ensure they function correctly and interact properly with the underlying service layer.
**Implementation Steps:**
- Create `tests/test_cli/test_commands.py`.
- Use `typer.testing.CliRunner` to invoke CLI commands.
- Test basic commands like `worker --help`, `events --help`, and `system config --validate`.
- Extend to include tests for starting workers, viewing events, and other CLI functionalities as they become relevant to the service layer.
**Success Criteria:**
- CLI commands execute successfully and display correct output.
- CLI commands correctly interact with the `ServiceManager` and its services (e.g., worker command starts a worker that uses the service layer).
- All CLI tests pass consistently.
**Testing:**
- Run `pytest tests/test_cli/test_commands.py`.
- Verify exit codes and command output.
**Documentation:**
- Update `docs/cli.qmd` (if exists, or create one) to describe the available CLI commands and their usage.