### Sub-task 4: Implement CLI Event Monitoring Commands
**Description:** Develop and integrate a suite of CLI commands for real-time streaming, historical lookup, statistics, and worker monitoring of events, enhancing user visibility into the system's operations.
**Implementation Steps:**
- Create or modify the file `src/naq/cli/event_commands.py`.
- Implement the `stream` command, allowing real-time event streaming with filtering by `job_id`, `event_type`, `queue`, `worker`, and options for output `format` (`table`, `json`, `raw`), `follow` (live updates), and `tail` (historical events).
- Implement the `history` command, enabling users to retrieve the complete event history for a specific `job_id` with output `format` options.
- Implement the `stats` command, providing event statistics and analytics grouped by `hours`, `by_queue`, or `by_worker`.
- Implement the `workers` command, allowing monitoring of worker events and status, including an option to `show_inactive` workers.
- Develop helper functions (`display_event`, `display_event_table`, `display_stats_table`, `display_worker_table`) to render rich, user-friendly output in the CLI.
**Success Criteria:**
- All new CLI commands (`naq events stream`, `naq events history`, `naq events stats`, `naq events workers`) are fully functional and accessible from the command line.
- Filtering, grouping, and output formatting options work as specified for each command.
- Real-time streaming is efficient and displays events without significant delay.
**Testing:**
- Create new CLI integration tests in a suitable location, e.g., `tests/cli/test_cli_events.py`.
- Test each command with various combinations of arguments and options to ensure correct functionality and output.
- Verify that filtering and grouping logic works as expected.
- Test the `stream` command for both historical (`--tail`) and live (`--follow`) event display.
**Documentation:**
- Add a new section or update an existing one in the project's documentation, such as `docs/quickstart.qmd` or `docs/advanced.qmd`, detailing the new event monitoring CLI commands.
- Provide clear usage examples for each command, explaining all available options and their effects.