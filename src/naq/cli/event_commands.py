import typer
import asyncio
import json
from datetime import datetime
from loguru import logger
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.json import JSON

from rich.console import Console

# Create a shared console instance
console = Console()
from ..settings import DEFAULT_NATS_URL
from ..utils import setup_logging
from ..events.processor import AsyncJobEventProcessor
from ..events.storage import NATSJobEventStorage
from ..models import JobEvent

# Create event app
event_app = typer.Typer(help="Event monitoring commands")


@event_app.command("stream")
def stream_events(
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
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Monitor job events in real-time using the event processor.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Starting event processor for NATS at {nats_url}")
    
    # Create storage and processor
    storage = NATSJobEventStorage(nats_url=nats_url)
    processor = AsyncJobEventProcessor(storage)
    
    # Add a global handler that prints formatted events
    def print_event(event: "JobEvent") -> None:
        """Print a formatted representation of the event using rich."""
        console = Console()
        
        # Create a table for event details
        table = Table(title=f"Job Event: {event.event_type.value}", show_header=False)
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="white")
        
        # Add event details
        table.add_row("Job ID", event.job_id)
        table.add_row("Event Type", event.event_type.value)
        table.add_row("Timestamp", datetime.fromtimestamp(event.timestamp).strftime('%Y-%m-%d %H:%M:%S UTC'))
        
        if event.worker_id:
            table.add_row("Worker ID", event.worker_id)
        if event.queue_name:
            table.add_row("Queue", event.queue_name)
        if event.message:
            table.add_row("Message", event.message)
        if event.duration_ms:
            table.add_row("Duration", f"{event.duration_ms:.2f} ms")
        if event.error_type:
            table.add_row("Error Type", event.error_type)
        if event.error_message:
            table.add_row("Error Message", event.error_message)
        if event.nats_subject:
            table.add_row("NATS Subject", event.nats_subject)
        if event.nats_sequence:
            table.add_row("NATS Sequence", str(event.nats_sequence))
        
        # Add details if present
        if event.details:
            # Create a nicely formatted JSON representation of details
            details_json = json.dumps(event.details, indent=2, default=str)
            table.add_row("Details", JSON(details_json))
        
        # Print the event
        console.print(table)
        console.print()
    
    processor.add_global_handler(print_event)
    
    # Run the processor
    try:
        # Use asyncio.run to start the processor
        asyncio.run(_run_event_processor(processor))
    except KeyboardInterrupt:
        logger.info("Event processor interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.exception(f"Event processor failed unexpectedly: {e}")
        raise typer.Exit(code=1)
    finally:
        logger.info("Event processor finished.")


async def _run_event_processor(processor: AsyncJobEventProcessor) -> None:
    """Run the event processor with proper async context management."""
    async with processor:
        console = Console()
        console.print("[green]Event processor started. Listening for events...[/green]")
        console.print("[yellow]Press Ctrl+C to stop[/yellow]")
        
        # Keep running until cancelled
        while True:
            await asyncio.sleep(1)


@event_app.command("history")
def event_history(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter by job ID",
    ),
    event_type: Optional[str] = typer.Option(
        None,
        "--event-type",
        "-e",
        help="Filter by event type",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        "-l",
        help="Maximum number of events to display",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Display historical job events.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Retrieving event history from NATS at {nats_url}")
    
    # This would be implemented to query the event storage for historical events
    # For now, we'll show a placeholder message
    console.print("[yellow]Event history functionality will be implemented in a future version.[/yellow]")


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
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Display event statistics.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Retrieving event statistics from NATS at {nats_url}")
    
    # This would be implemented to calculate and display event statistics
    # For now, we'll show a placeholder message
    console.print("[yellow]Event statistics functionality will be implemented in a future version.[/yellow]")


@event_app.command("workers")
def worker_events(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    worker_id: Optional[str] = typer.Option(
        None,
        "--worker-id",
        "-w",
        help="Filter by worker ID",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to NAQ_LOG_LEVEL env var or CRITICAL.",
    ),
):
    """
    Display worker-specific events.
    """
    setup_logging(log_level if log_level else None)
    logger.info(f"Retrieving worker events from NATS at {nats_url}")
    
    # This would be implemented to query and display worker-specific events
    # For now, we'll show a placeholder message
    console.print("[yellow]Worker events functionality will be implemented in a future version.[/yellow]")