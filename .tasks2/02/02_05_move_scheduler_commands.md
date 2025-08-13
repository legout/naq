### Sub-task 5: Move Scheduler Commands to `scheduler_commands.py`
**Description:** This sub-task is to extract all scheduler-related commands from `src/naq/cli.py` and move them into `src/naq/cli/scheduler_commands.py`.
**Implementation Steps:**
- Create a new `typer.Typer` instance named `scheduler_app` in `src/naq/cli/scheduler_commands.py`.
- Move the logic from the `scheduler()` and `list_scheduled_jobs()` functions into new commands under `scheduler_app`.
- Update `main.py` to import and register `scheduler_app`.
**Success Criteria:**
- `src/naq/cli/scheduler_commands.py` contains all scheduler-related CLI commands.
- The `scheduler_app` is correctly registered in `src/naq/cli/main.py`.
- The `naq scheduler start` and `naq scheduler jobs` commands are functional.
**Testing:** Write unit tests for the commands in `src/naq/cli/scheduler_commands.py`.
**Documentation:** Update the CLI documentation to reflect the new `naq scheduler` command structure.