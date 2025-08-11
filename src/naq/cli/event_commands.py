# src/naq/cli/event_commands.py
"""
Event monitoring commands for the NAQ CLI.

This module contains commands for monitoring job events, viewing event history,
and generating event statistics.
"""

import asyncio
import datetime
import json
from collections import Counter, defaultdict
from datetime import timezone
from typing import Optional

import typer
from rich.panel import Panel
from rich.table import Table

from .main import console
from ..settings import DEFAULT_NATS_URL


# Create event command group
event_app = typer.Typer(help="Event monitoring and analytics commands")


@event_app.command()
def stream(
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
        help="Filter events for a specific job ID.",
    ),
    event_type: Optional[str] = typer.Option(
        None,
        "--event-type",
        "-e", 
        help="Filter events by type (enqueued, started, completed, failed, retry_scheduled, scheduled, schedule_triggered).",
    ),
    queue_name: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter events for a specific queue.",
    ),
    worker_id: Optional[str] = typer.Option(
        None,
        "--worker",
        "-w",
        help="Filter events for a specific worker.",
    ),
    format_output: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
    ),
):
    """
    Monitor job events in real-time.
    
    This command connects to the NATS event stream and displays job lifecycle
    events as they occur. Supports filtering by job ID, event type, queue, and worker.
    """
    from ..events.processor import AsyncJobEventProcessor
    from ..models import JobEventType
    
    # Validate event type
    event_type_enum = None
    if event_type:
        try:
            event_type_enum = JobEventType(event_type.lower())
        except ValueError:
            console.print(f"[red]Error:[/red] Invalid event type '{event_type}'")
            console.print(f"Valid types: {', '.join([e.value for e in JobEventType])}")
            raise typer.Exit(code=1)
    
    async def run_events_monitor():
        """Run the events monitor."""
        processor = AsyncJobEventProcessor(nats_url=nats_url)
        
        # Create a simple handler to display events
        def display_event(event):
            if format_output == "json":
                console.print(json.dumps(event.to_dict(), indent=2))
            elif format_output == "raw":
                console.print(f"{event.timestamp:.2f} {event.job_id} {event.event_type.value} {event.message or ''}")
            else:  # table format
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Time", style="dim")
                table.add_column("Job ID", style="cyan")
                table.add_column("Event", style="green")
                table.add_column("Queue", style="blue")
                table.add_column("Worker", style="yellow")
                table.add_column("Message")
                
                # Format timestamp
                dt = datetime.datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]  # Include milliseconds
                
                table.add_row(
                    time_str,
                    event.job_id[:12] + "..." if len(event.job_id) > 15 else event.job_id,
                    event.event_type.value,
                    event.queue_name or "-",
                    event.worker_id[:8] + "..." if event.worker_id and len(event.worker_id) > 11 else (event.worker_id or "-"),
                    event.message or "-"
                )
                console.print(table)
        
        # Register the display handler
        processor.add_global_handler(display_event)
        
        try:
            # Start the processor
            await processor.start()
            
            console.print(Panel(
                "[bold green]NAQ Event Monitor Started[/bold green]\n"
                f"NATS URL: {nats_url}\n"
                f"Filters: job_id={job_id or 'all'}, event_type={event_type or 'all'}, "
                f"queue={queue_name or 'all'}, worker={worker_id or 'all'}\n"
                f"Format: {format_output}\n\n"
                "[dim]Press Ctrl+C to stop[/dim]",
                title="Event Monitor",
                border_style="green"
            ))
            
            # Stream events with filters
            async for event in processor.stream_job_events(
                job_id=job_id,
                event_type=event_type_enum,
                queue_name=queue_name,
                worker_id=worker_id
            ):
                # The display_event handler will be called automatically
                pass
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Event monitoring stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await processor.stop()
    
    # Run the async function
    asyncio.run(run_events_monitor())


@event_app.command("history")
def event_history(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    job_id: str = typer.Argument(
        help="Job ID to get event history for.",
    ),
    limit: int = typer.Option(
        50,
        "--limit",
        "-l",
        help="Maximum number of events to retrieve.",
    ),
):
    """
    Get event history for a specific job.
    
    This command retrieves the complete event history for a job from the event stream,
    showing all lifecycle events in chronological order.
    """
    from ..events.storage import NATSJobEventStorage
    
    async def get_job_events():
        """Retrieve events for a specific job."""
        storage = NATSJobEventStorage(nats_url=nats_url)
        
        try:
            events = await storage.get_events(job_id)
            
            if not events:
                console.print(f"[yellow]No events found for job '{job_id}'[/yellow]")
                return
            
            # Limit results
            if len(events) > limit:
                events = events[-limit:]  # Get the most recent events
                console.print(f"[dim]Showing last {limit} events (total: {len(events)})[/dim]\n")
            
            # Create a table for events
            table = Table(show_header=True, header_style="bold magenta", title=f"Event History for Job: {job_id}")
            table.add_column("Timestamp", style="dim")
            table.add_column("Event Type", style="green")
            table.add_column("Queue", style="blue")
            table.add_column("Worker", style="yellow")
            table.add_column("Message")
            table.add_column("Details", style="dim")
            
            for event in events:
                # Format timestamp
                dt = datetime.datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                
                # Format details
                details_str = ""
                if event.details:
                    key_details = []
                    if event.duration_ms is not None:
                        key_details.append(f"duration: {event.duration_ms}ms")
                    if event.error_type:
                        key_details.append(f"error: {event.error_type}")
                    details_str = ", ".join(key_details)
                
                table.add_row(
                    time_str,
                    event.event_type.value,
                    event.queue_name or "-",
                    (event.worker_id[:8] + "..." if event.worker_id and len(event.worker_id) > 11 else (event.worker_id or "-")),
                    event.message or "-",
                    details_str
                )
            
            console.print(table)
            
        except Exception as e:
            console.print(f"[red]Error retrieving events:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await storage.close()
    
    # Run the async function
    asyncio.run(get_job_events())


@event_app.command("stats")
def event_stats(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    hours: int = typer.Option(
        24,
        "--hours",
        "-h",
        help="Number of hours to look back for statistics.",
    ),
):
    """
    Show event statistics and system health metrics.
    
    This command provides an overview of system activity by analyzing
    events from the specified time period.
    """
    from ..events.storage import NATSJobEventStorage
    
    async def get_event_statistics():
        """Generate event statistics."""
        storage = NATSJobEventStorage(nats_url=nats_url)
        
        try:
            console.print(f"[dim]Analyzing events from the last {hours} hours...[/dim]\n")
            
            # This would require implementing event querying by time range
            # For now, we'll show a placeholder structure
            
            console.print(Panel(
                "[bold green]Event Statistics[/bold green]\n\n"
                "[yellow]Note:[/yellow] Detailed statistics require time-based event querying,\n"
                "which would be implemented by extending the event storage interface.\n\n"
                "[dim]This would show:[/dim]\n"
                "• Job completion rates\n"
                "• Error rates by queue\n"
                "• Worker activity\n"
                "• Average job duration\n"
                "• Queue throughput",
                title="Event Analytics",
                border_style="blue"
            ))
            
        except Exception as e:
            console.print(f"[red]Error retrieving statistics:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await storage.close()
    
    # Run the async function
    asyncio.run(get_event_statistics())