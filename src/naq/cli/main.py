"""
Main CLI application entry point.

This module contains the main Typer application and serves as the central
entry point for the naq CLI. It registers subcommands from other modules
and handles global options like version display.
"""

import typer
from typing import Optional
from rich.console import Console

from .. import __version__

app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Create a shared console instance for Rich output
console = Console()


def version_callback(value: bool):
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
):
    """
    naq CLI entry point.
    """
    pass


# Placeholders for registering subcommand Typer instances
# These will be populated during the refactoring process
# when we move commands from cli.py to their respective modules
worker_app = typer.Typer()
job_app = typer.Typer()
scheduler_app = typer.Typer()
event_app = typer.Typer()
system_app = typer.Typer()

# Register subcommands (these will be implemented in their respective modules)
app.add_typer(worker_app, name="worker", help="Worker management commands")
app.add_typer(job_app, name="job", help="Job management commands")
app.add_typer(scheduler_app, name="scheduler", help="Scheduler management commands")
app.add_typer(event_app, name="event", help="Event monitoring commands")
app.add_typer(system_app, name="system", help="System utility commands")
