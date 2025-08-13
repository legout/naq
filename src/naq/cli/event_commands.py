"""
Event CLI commands.

This module contains all CLI commands related to event monitoring.
"""

import asyncio
import json
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ..connection import get_nats_connection, get_jetstream_context
from ..exceptions import NaqConnectionError
from ..settings import DEFAULT_NATS_URL, NAQ_PREFIX
from ..utils import setup_logging

event_app = typer.Typer(help="Event monitoring commands")
console = Console()

# Event stream configuration
EVENT_STREAM_NAME = f"{NAQ_PREFIX}_events"
EVENT_SUBJECT_PREFIX = f"{NAQ_PREFIX}.event"


@event_app.command()
def monitor(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
        show_default=True,
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help=(
            "Set logging level (e.g., DEBUG, INFO, WARNING, ERROR). Defaults to "
            "NAQ_LOG_LEVEL env var or CRITICAL."
        ),
    ),
    queue_name: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter events by queue name.",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter events by job ID.",
    ),
    event_type: Optional[str] = typer.Option(
        None,
        "--event-type",
        "-t",
        help="Filter events by event type (e.g., STARTED, COMPLETED, FAILED).",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output events in JSON format.",
    ),
):
    """
    Monitor job events in real-time.
    
    Listens to the event stream and displays job events as they occur.
    Events include job lifecycle events like enqueued, started, completed, failed, etc.
    """
    setup_logging(log_level if log_level else None)
    
    try:
        asyncio.run(_monitor_events(
            nats_url=nats_url,
            queue_name=queue_name,
            job_id=job_id,
            event_type=event_type,
            json_output=json_output,
        ))
    except KeyboardInterrupt:
        console.print("[yellow]Event monitoring stopped.[/yellow]")
    except Exception as e:
        console.print(f"[red]Error monitoring events: {e}[/red]")
        raise typer.Exit(1)


async def _monitor_events(
    nats_url: str,
    queue_name: Optional[str],
    job_id: Optional[str],
    event_type: Optional[str],
    json_output: bool,
):
    """Internal async function to monitor events."""
    # Connect to NATS
    try:
        nc = await get_nats_connection(nats_url)
        js = await get_jetstream_context(nc=nc)
    except Exception as e:
        raise NaqConnectionError(f"Failed to connect to NATS at {nats_url}: {e}")
    
    # Ensure event stream exists
    await _ensure_event_stream(js)
    
    # Set up subscription subject
    if queue_name:
        subject = f"{EVENT_SUBJECT_PREFIX}.{queue_name}"
    else:
        subject = f"{EVENT_SUBJECT_PREFIX}.*"
    
    console.print(f"[green]Monitoring events from NATS at {nats_url}[/green]")
    console.print(f"[blue]Listening to subject: {subject}[/blue]")
    
    # Create consumer
    durable_name = f"event-monitor-{nc.client_id}"
    
    async def message_handler(msg):
        """Handle incoming messages."""
        await _handle_event_message(msg, job_id, event_type, json_output)
    
    try:
        await js.subscribe(
            subject=subject,
            durable=durable_name,
            cb=message_handler,
        )
        
        # Keep the connection alive
        await asyncio.Future()  # Run forever
    except Exception as e:
        await nc.close()
        raise e


async def _handle_event_message(msg, job_id_filter, event_type_filter, json_output):
    """Handle incoming event messages."""
    try:
        # Decode and parse the event
        event_data = msg.data.decode()
        event_dict = json.loads(event_data)
        
        # Apply filters
        if job_id_filter and event_dict.get('job_id') != job_id_filter:
            await msg.ack()
            return
            
        if event_type_filter and event_dict.get('event_type') != event_type_filter.upper():
            await msg.ack()
            return
        
        # Output the event
        if json_output:
            console.print(json.dumps(event_dict, indent=2))
        else:
            _display_event_table(event_dict)
        
        await msg.ack()
    except Exception as e:
        console.print(f"[red]Error processing event: {e}[/red]")


def _display_event_table(event_dict: dict):
    """Display event in a formatted table."""
    event_type = event_dict.get('event_type', 'UNKNOWN')
    table = Table(title=f"Job Event: {event_type}", show_header=True)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="magenta")
    
    # Display key event fields
    table.add_row("Job ID", event_dict.get('job_id', 'N/A'))
    table.add_row("Event Type", event_type)
    table.add_row("Timestamp", str(event_dict.get('timestamp', 'N/A')))
    
    if event_dict.get('worker_id'):
        table.add_row("Worker ID", event_dict['worker_id'])
    
    if event_dict.get('queue_name'):
        table.add_row("Queue Name", event_dict['queue_name'])
    
    if event_dict.get('message'):
        table.add_row("Message", event_dict['message'])
    
    if event_dict.get('duration_ms'):
        table.add_row("Duration (ms)", str(event_dict['duration_ms']))
    
    if event_dict.get('error_type'):
        table.add_row("Error Type", event_dict['error_type'])
    
    if event_dict.get('error_message'):
        table.add_row("Error Message", event_dict['error_message'])
    
    console.print(table)
    console.print("")  # Empty line for spacing


async def _ensure_event_stream(js):
    """Ensure the event stream exists."""
    # Import errors inside function to avoid import issues
    try:
        from nats.js.errors import NotFoundError
    except ImportError:
        # Fallback if import fails
        class NotFoundError(Exception):
            pass
    
    try:
        await js.stream_info(EVENT_STREAM_NAME)
    except NotFoundError:
        # Stream doesn't exist, create it
        await js.add_stream(
            name=EVENT_STREAM_NAME,
            subjects=[f"{EVENT_SUBJECT_PREFIX}.*"],
        )
    except Exception:
        # If we can't check for the stream, try to create it
        try:
            await js.add_stream(
                name=EVENT_STREAM_NAME,
                subjects=[f"{EVENT_SUBJECT_PREFIX}.*"],
            )
        except Exception:
            # If we can't create it, continue anyway
            pass


@event_app.command()
def list_streams(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url",
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
        show_default=True,
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Set logging level.",
    ),
):
    """
    List all event streams in the NATS JetStream server.
    """
    setup_logging(log_level if log_level else None)
    
    try:
        streams = asyncio.run(_list_event_streams(nats_url))
        
        if not streams:
            console.print("[yellow]No event streams found.[/yellow]")
            return
            
        table = Table(title="Event Streams")
        table.add_column("Stream Name", style="cyan")
        table.add_column("Subjects", style="magenta")
        table.add_column("Messages", style="green")
        
        for stream in streams:
            table.add_row(
                stream["name"],
                ", ".join(stream["subjects"]),
                str(stream["messages"]),
            )
        
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing streams: {e}[/red]")
        raise typer.Exit(1)


async def _list_event_streams(nats_url: str):
    """List all event streams."""
    try:
        nc = await get_nats_connection(nats_url)
        js = await get_jetstream_context(nc=nc)
        
        # Get list of streams
        event_streams = []
        
        # Get stream list - simplified approach
        try:
            # Try to get info for our event stream specifically
            stream_info = await js.stream_info(EVENT_STREAM_NAME)
            event_streams.append({
                "name": stream_info.config.name,
                "subjects": stream_info.config.subjects or [],
                "messages": getattr(stream_info.state, 'messages', 0),
            })
        except Exception:
            # Event stream doesn't exist yet
            pass
        
        await nc.close()
        return event_streams
    except Exception as e:
        raise NaqConnectionError(f"Failed to list streams: {e}")
