"""Main CLI application for naq.

This module serves as the main entry point for the naq CLI application.
It initializes the Typer app and registers all sub-commands.
"""

from typing import Optional

import typer
from rich.console import Console

from naq import __version__

# Create the main Typer application
app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Create a shared console instance for Rich output
console = Console()


def version_callback(value: bool) -> None:
    """Callback function to display version information."""
    if value:
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application's version and exit.",
    ),
) -> None:
    """
    naq CLI entry point.
    """
    pass


# Import and register subcommands
# These will be implemented in subsequent tasks
try:
    from . import worker_commands

    app.add_typer(
        worker_commands.worker_app, name="worker", help="Worker-related commands"
    )
except ImportError:
    pass  # Worker commands not yet implemented

try:
    from . import job_commands

    app.add_typer(
        job_commands.job_app, name="job", help="Job and queue management commands"
    )
except ImportError:
    pass  # Job commands not yet implemented

try:
    from . import scheduler_commands

    app.add_typer(
        scheduler_commands.scheduler_app,
        name="scheduler",
        help="Scheduler-related commands",
    )
except ImportError:
    pass  # Scheduler commands not yet implemented

try:
    from . import event_commands

    app.add_typer(
        event_commands.event_app, name="events", help="Event monitoring commands"
    )
except ImportError:
    pass  # Event commands not yet implemented

try:
    from . import system_commands

    app.add_typer(
        system_commands.system_app, name="system", help="System and utility commands"
    )
except ImportError:
    pass  # System commands not yet implemented
