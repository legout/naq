### Sub-task 3: Move Worker Commands to `worker_commands.py`
**Description:** This sub-task is to extract all worker-related commands from `src/naq/cli.py` and move them into `src/naq/cli/worker_commands.py`.
**Implementation Steps:**
- Create a new `typer.Typer` instance named `worker_app` in `src/naq/cli/worker_commands.py`.
- Move the logic from the `worker()` and `list_workers_command()` functions into new commands under `worker_app`.
- Update `main.py` to import and register `worker_app`.
**Success Criteria:**
- `src/naq/cli/worker_commands.py` contains all worker-related CLI commands.
- The `worker_app` is correctly registered in `src/naq/cli/main.py`.
- The `naq worker start` and `naq worker list` commands are functional.
**Testing:** Write unit tests for the `start` and `list` commands in `src/naq/cli/worker_commands.py`.
**Documentation:** Update the CLI documentation to reflect the new `naq worker` command structure.