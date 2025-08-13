import typer
from rich.console import Console
from typing import Optional

# Hardcoded version to avoid circular import
__version__ = "0.1.3"

# Create main CLI app
app = typer.Typer(
    name="naq",
    help="A simple NATS-based queueing system, similar to RQ.",
    add_completion=False,
)

# Shared console instance
console = Console()


def version_callback(value: bool):
    """Callback for version option."""
    if value:
        console.print(f"[cyan]naq[/cyan] version: [bold]{__version__}[/bold]")
        raise typer.Exit()


# Register subcommand groups
from .worker_commands import worker_app
from .job_commands import job_app
from .scheduler_commands import scheduler_app
from .event_commands import event_app
from .system_commands import system_app

app.add_typer(worker_app, name="worker")
app.add_typer(job_app, name="job")
app.add_typer(scheduler_app, name="scheduler")
app.add_typer(event_app, name="events")
app.add_typer(system_app, name="system")


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