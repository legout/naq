### Sub-task 7: Move System and Utility Commands to `system_commands.py`
**Description:** This sub-task is to extract all system and utility commands from `src/naq/cli.py` and move them into `src/naq/cli/system_commands.py`.
**Implementation Steps:**
- Create a new `typer.Typer` instance named `system_app` in `src/naq/cli/system_commands.py`.
- Move the logic from the `dashboard()` function and any other utility commands into new commands under `system_app`.
- Update `main.py` to import and register `system_app`.
**Success Criteria:**
- `src/naq/cli/system_commands.py` contains all system and utility CLI commands.
- The `system_app` is correctly registered in `src/naq/cli/main.py`.
- The `naq system dashboard` command is functional.
**Testing:** Write unit tests for the commands in `src/naq/cli/system_commands.py`.
**Documentation:** Update the CLI documentation to reflect the new `naq system` command structure.