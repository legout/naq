### Sub-task 6: Move Event Monitoring Commands to `event_commands.py`
**Description:** This sub-task is to extract all event monitoring commands from `src/naq/cli.py` and move them into `src/naq/cli/event_commands.py`.
**Implementation Steps:**
- Create a new `typer.Typer` instance named `event_app` in `src/naq/cli/event_commands.py`.
- Move the logic from the `events()`, `event_history()`, `event_stats()`, and `worker_events()` functions into new commands under `event_app`.
- Update `main.py` to import and register `event_app`.
**Success Criteria:**
- `src/naq/cli/event_commands.py` contains all event-related CLI commands.
- The `event_app` is correctly registered in `src/naq/cli/main.py`.
- All `naq events` subcommands are functional.
**Testing:** Write unit tests for the commands in `src/naq/cli/event_commands.py`.
**Documentation:** Update the CLI documentation to reflect the new `naq events` command structure.