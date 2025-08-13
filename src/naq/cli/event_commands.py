"""Event monitoring CLI commands for naq."""

from typing import Optional

import typer
from loguru import logger
from rich.console import Console

from ..settings import DEFAULT_NATS_URL
from ..utils import setup_logging

# Create a Typer instance for event commands
event_app = typer.Typer(
    name="events",
    help="Event monitoring commands",
    add_completion=False,
)


@event_app.command("monitor")
def events(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Monitor real-time events from the naq system.
    """
    setup_logging(log_level if log_level else None)
    console = Console()

    logger.info(f"Monitoring events from NATS at {nats_url}")
    console.print("[yellow]Event monitoring not yet implemented.[/yellow]")
    console.print("This command will display real-time events from the naq system.")


@event_app.command("history")
def event_history(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        "-n",
        help="Maximum number of events to display.",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Display historical events from the naq system.
    """
    setup_logging(log_level if log_level else None)
    console = Console()

    logger.info(f"Fetching event history from NATS at {nats_url}")
    console.print("[yellow]Event history not yet implemented.[/yellow]")
    console.print(
        f"This command will display the last {limit} events from the naq system."
    )


@event_app.command("stats")
def event_stats(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Display statistics about events in the naq system.
    """
    setup_logging(log_level if log_level else None)
    console = Console()

    logger.info(f"Fetching event statistics from NATS at {nats_url}")
    console.print("[yellow]Event statistics not yet implemented.[/yellow]")
    console.print(
        "This command will display statistics about events in the naq system."
    )


@event_app.command("worker")
def worker_events(
    worker_id: str = typer.Argument(
        ..., help="The ID of the worker to monitor events for"
    ),
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). "
            "Defaults to NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
) -> None:
    """
    Monitor events for a specific worker.
    """
    setup_logging(log_level if log_level else None)
    console = Console()

    logger.info(f"Monitoring events for worker {worker_id} from NATS at {nats_url}")
    console.print("[yellow]Worker event monitoring not yet implemented.[/yellow]")
    console.print(f"This command will display events for worker {worker_id}.")
