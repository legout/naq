# src/naq/cli/main.py
"""
Main CLI application and shared utilities for NAQ.

This module contains the main typer app, shared console instance,
version callback, and entry point for the NAQ CLI.
"""

import typer
from rich.console import Console

from .. import __version__

# Create main CLI app
app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Create a shared console instance for Rich output
console = Console()


def version_callback(value: bool):
    """Version callback for CLI --version option."""
    if value:
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


# Register all command modules
def register_commands():
    """Register all command modules with the main CLI app."""
    from .worker_commands import worker_app
    from .job_commands import job_app
    from .scheduler_commands import scheduler_app
    from .event_commands import event_app
    from .system_commands import system_app

    # Add subcommand groups
    app.add_typer(worker_app, name="worker")
    app.add_typer(job_app, name="job") 
    app.add_typer(scheduler_app, name="scheduler")
    app.add_typer(event_app, name="events")
    app.add_typer(system_app, name="system")


# Register all commands at module import time for entry point compatibility
register_commands()

# Add global version option
@app.callback()
def global_options(
    version: bool = typer.Option(
        False, "--version", "-v", callback=version_callback, 
        help="Show the version and exit."
    )
):
    """NAQ - A simple NATS-based queueing system, similar to RQ."""
    pass


# Alias for backward compatibility with tests
cli = app


def main():
    """Main entry point for the NAQ CLI."""
    # Commands are already registered at module import
    app()


if __name__ == "__main__":
    main()