### Sub-task 4: Move Job and Queue Commands to `job_commands.py`
**Description:** This sub-task is to extract all job and queue management commands from `src/naq/cli.py` and move them into `src/naq/cli/job_commands.py`.
**Implementation Steps:**
- Create a new `typer.Typer` instance named `job_app` in `src/naq/cli/job_commands.py`.
- Move the logic from the `purge()` and any `job_control()` functions into new commands under `job_app`.
- Update `main.py` to import and register `job_app`.
**Success Criteria:**
- `src/naq/cli/job_commands.py` contains all job and queue-related CLI commands.
- The `job_app` is correctly registered in `src/naq/cli/main.py`.
- The `naq job purge` command is functional.
**Testing:** Write unit tests for the commands in `src/naq/cli/job_commands.py`.
**Documentation:** Update the CLI documentation to reflect the new `naq job` command structure.